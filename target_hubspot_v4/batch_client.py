"""HubSpot batch sink base class."""

from abc import abstractmethod
from typing import List, Optional

from hotglue_etl_exceptions import InvalidCredentialsError, InvalidPayloadError
from hotglue_singer_sdk.target_sdk.client import HotglueBatchSink

from target_hubspot_v4.batch import is_whole_batch_failure, staged_to_tap_record
from target_hubspot_v4.client import HubspotSink
from target_hubspot_v4.sinks import FallbackSink

HUBSPOT_BATCH_LIMIT = 100


class HubspotBatchSink(HubspotSink, HotglueBatchSink):
    """Base batch sink for fallback HubSpot object streams."""

    max_size = HUBSPOT_BATCH_LIMIT

    @property
    def current_size(self):
        """Return the number of records buffered for the current batch."""
        return self._batch_records_read

    @property
    def unified_schema(self):
        return None

    @property
    def name(self):
        return self.stream_name

    def preprocess_record(self, record: dict, context: dict):
        return record

    def parse_fallback_properties(self, record: dict):
        """Normalize a fallback tap record into flat properties and associations."""
        record = dict(record)
        associations = record.pop("associations", None)

        if record.get("properties"):
            record = record["properties"]
        for key, value in record.items():
            record[key] = self.parse_objs(value)

        return record, associations

    @abstractmethod
    def prepare_staged_record(self, record: dict, context: dict) -> Optional[dict]:
        raise NotImplementedError()

    @abstractmethod
    def make_batch_request(self, records: List[dict]):
        raise NotImplementedError()

    @abstractmethod
    def parse_batch_response(self, response, staged_records: List[dict]) -> dict:
        raise NotImplementedError()

    def get_tap_record_email(self, record: dict) -> Optional[str]:
        """Return the contact email from a tap-shaped record, if present."""
        properties = record.get("properties") or record
        email = properties.get("email")
        return email if email else None

    def _error_classification_metadata(self, exc: Exception) -> dict:
        if isinstance(exc, (InvalidCredentialsError, InvalidPayloadError)):
            return {"hg_error_class": exc.__class__.__name__}
        return {}

    def _single_record_sink(self) -> FallbackSink:
        """Return a FallbackSink sharing this sink's state for single-record writes."""
        sink = FallbackSink(self._target, self.stream_name, self.schema, self.key_properties)
        sink.latest_state = self.latest_state
        sink.processed_hashes = self.processed_hashes
        return sink

    def _sync_single_sink_state(self, sink: FallbackSink) -> None:
        """Copy state back from a FallbackSink used for single-record writes."""
        self.processed_hashes = sink.processed_hashes
        self.latest_state = sink.latest_state

    def write_single_tap_record(
        self,
        tap_record: dict,
        external_id: Optional[str],
        context: dict,
    ) -> None:
        """Write one record through FallbackSink lookup and upsert logic."""
        record = dict(tap_record)
        if external_id:
            record[self._target.EXTERNAL_ID_KEY] = external_id
        sink = self._single_record_sink()
        FallbackSink.process_record(sink, record, context)
        self._sync_single_sink_state(sink)

    def build_staged_record_hash(self, staged: dict) -> str:
        """Hash a staged record the same way FallbackSink hashes preprocessed payloads."""
        payload = {"properties": staged["properties"]}
        if staged.get("associations"):
            payload["associations"] = staged["associations"]
        return self.build_record_hash(payload)

    def _fallback_batch_records(self, staged_records: List[dict], context: dict) -> None:
        """Write each staged batch record through single-record FallbackSink."""
        self.logger.warning(
            "Batch request for %s failed entirely; falling back to single-record writes",
            self.name,
        )
        for staged in staged_records:
            self.write_single_staged_record(staged, context)

    def write_single_staged_record(self, staged: dict, context: dict) -> None:
        """Fallback one staged batch record to single-record FallbackSink writes."""
        meta = staged.get("state") or {}
        external_id = meta.get("externalId")
        tap_record = staged_to_tap_record(staged)
        self.write_single_tap_record(tap_record, external_id, context)

    def process_record(self, record: dict, context: dict) -> None:
        """Stage a record for batch write, or queue it for single-record FallbackSink."""
        if not self.latest_state:
            self.init_state()

        external_id_key = self._target.EXTERNAL_ID_KEY
        external_id = None

        if self.name not in self.allows_externalid and (
            record.get(external_id_key) or record.get(external_id_key.lower())
        ):
            external_id = record.pop(external_id_key, None) or record.pop(external_id_key.lower(), None)

        tap_record = dict(record)
        # Batch upsert needs an email idProperty; no-email records use FallbackSink instead.
        if not self.get_tap_record_email(tap_record):
            context.setdefault("single_records", []).append(
                {"record": tap_record, "external_id": external_id}
            )
            return

        try:
            staged = self.prepare_staged_record(tap_record, context)
            if not staged:
                return
        except Exception as exc:
            self.logger.exception("Preprocess record error %s", exc)
            state_updates = {"error": str(exc)}
            state_updates.update(self._error_classification_metadata(exc))
            success = None if isinstance(exc, InvalidPayloadError) else False
            error_state = dict(success=success, **state_updates)
            if external_id:
                error_state["externalId"] = external_id
            self.update_state(error_state, record=tap_record)
            return

        record_hash = self.build_staged_record_hash(staged)
        if record_hash in self.processed_hashes:
            self.logger.info("Record of type %s already exists with hash: %s", self.name, record_hash)
            return

        existing_state = self.get_existing_state(record_hash)
        if existing_state:
            self.update_state(existing_state, is_duplicate=True, record=staged["properties"])
            return

        # objectWriteTraceId maps HubSpot 207 results/errors back to this staged row.
        trace_id = external_id or record_hash
        staged["trace_id"] = trace_id
        staged["state"] = {"hash": record_hash}
        if external_id:
            staged["state"]["externalId"] = external_id

        context.setdefault("records", []).append(staged)

    def process_batch(self, context: dict) -> None:
        """Flush staged batch records, then process any single-record queue."""
        if not self.latest_state:
            self.init_state()

        raw_records = context.get("records") or []
        try:
            if raw_records:
                try:
                    response = self.make_batch_request(raw_records)
                except Exception:
                    self.logger.exception("Batch request failed for %s", self.name)
                    self._fallback_batch_records(raw_records, context)
                else:
                    if is_whole_batch_failure(response):
                        self._fallback_batch_records(raw_records, context)
                    else:
                        result = self.parse_batch_response(response, raw_records)
                        for followup in result.get("association_followups", []):
                            FallbackSink.put_associations(self, followup["id"], followup["associations"])
                        for state in result.get("state_updates", []):
                            is_duplicate = state.pop("_duplicate", False)
                            if state.get("success"):
                                self.logger.info("%s processed id: %s", self.name, state.get("id"))
                            self.update_state(state, is_duplicate=is_duplicate)
        finally:
            for item in context.get("single_records") or []:
                self.write_single_tap_record(item["record"], item.get("external_id"), context)
            context["records"] = []
            context["single_records"] = []
