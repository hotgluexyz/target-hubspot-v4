"""HubSpot batch sink base class."""

from abc import abstractmethod
from typing import Iterator, List, Optional

from hotglue_etl_exceptions import InvalidCredentialsError, InvalidPayloadError
from hotglue_singer_sdk.target_sdk.client import HotglueBatchSink

from target_hubspot_v4.batch import (
    BATCH_KIND_UPSERT,
    batch_upsert_objects,
    dedupe_staged_by_key,
    is_whole_batch_failure,
    parse_batch_upsert_response,
    staged_to_tap_record,
)
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

    @property
    @abstractmethod
    def batch_id_property(self) -> str:
        """HubSpot idProperty used for batch/upsert (first lookup field)."""
        raise NotImplementedError()

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

    def prepare_staged_record(self, record: dict, context: dict) -> Optional[dict]:
        """Stage a record for batch upsert when it has the configured upsert key."""
        properties, associations = self.parse_fallback_properties(record)
        properties.pop("id", None)
        id_property = self.batch_id_property
        upsert_key = properties.get(id_property)
        if not upsert_key:
            return None

        if self.lookup_fields:
            existing_objects = self.perform_object_lookup(properties, self.lookup_fields)
            if existing_objects and len(existing_objects) > 1:
                raise InvalidPayloadError(
                    f"Multiple objects found for lookup fields {self.lookup_fields} on record {properties}"
                )

        return {
            "properties": properties,
            "associations": associations,
            "batch_kind": BATCH_KIND_UPSERT,
            id_property: upsert_key,
        }

    def iter_batch_requests(self, staged_records: List[dict]) -> Iterator[dict]:
        """Yield batch request specs for a flush of staged records."""
        deduped = dedupe_staged_by_key(staged_records, self.batch_id_property)
        config = self._target._config
        object_type = self.name
        id_property = self.batch_id_property

        yield {
            "records": deduped,
            "request": lambda records: batch_upsert_objects(config, object_type, id_property, records),
            "parse": lambda response, records: parse_batch_upsert_response(response, records, id_property),
        }

    def make_batch_request(self, records: List[dict]):
        """Satisfy HotglueBatchSink ABC; batch flushing uses iter_batch_requests instead."""
        for batch_spec in self.iter_batch_requests(records):
            return batch_spec["request"](batch_spec["records"])
        return None

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

    def _apply_batch_result(self, result: dict) -> None:
        """Apply parsed batch state updates and association followups."""
        for state in result.get("state_updates", []):
            is_duplicate = state.pop("_duplicate", False)
            if state.get("success"):
                self.logger.info("%s processed id: %s", self.name, state.get("id"))
            self.update_state(state, is_duplicate=is_duplicate)
        for followup in result.get("association_followups", []):
            FallbackSink.put_associations(self, followup["id"], followup["associations"])

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

    def _normalize_batch_parse_result(self, parsed) -> dict:
        if isinstance(parsed, tuple):
            state_updates, association_followups = parsed
            return {
                "state_updates": state_updates,
                "association_followups": association_followups,
            }
        return parsed

    def _process_staged_batch(self, staged_records: List[dict], context: dict) -> None:
        """Run each batch request group and fall back to single-record writes on failure."""
        for batch_spec in self.iter_batch_requests(staged_records):
            records = batch_spec["records"]
            if not records:
                continue
            try:
                response = batch_spec["request"](records)
            except Exception:
                self.logger.exception("Batch request failed for %s", self.name)
                self._fallback_batch_records(records, context)
            else:
                if is_whole_batch_failure(response):
                    self._fallback_batch_records(records, context)
                else:
                    self._apply_batch_result(
                        self._normalize_batch_parse_result(batch_spec["parse"](response, records))
                    )

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
        try:
            staged = self.prepare_staged_record(tap_record, context)
            if not staged:
                context.setdefault("single_records", []).append(
                    {"record": tap_record, "external_id": external_id}
                )
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
                self._process_staged_batch(raw_records, context)
        finally:
            for item in context.get("single_records") or []:
                self.write_single_tap_record(item["record"], item.get("external_id"), context)
            context["records"] = []
            context["single_records"] = []
