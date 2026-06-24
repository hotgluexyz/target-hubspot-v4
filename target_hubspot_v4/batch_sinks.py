"""Stream-specific batch fallback sinks."""

from typing import Iterator, List, Optional

from hotglue_etl_exceptions import InvalidPayloadError

from target_hubspot_v4.batch import (
    BATCH_KIND_CREATE,
    BATCH_KIND_UPDATE,
    BATCH_KIND_UPSERT,
    batch_create_objects,
    batch_update_objects,
    batch_upsert_objects,
    dedupe_staged_by_hubspot_id,
    dedupe_staged_by_key,
    parse_batch_create_response,
    parse_batch_update_response,
    parse_batch_upsert_response,
)
from target_hubspot_v4.batch_client import HubspotBatchSink


class ContactsFallbackSink(HubspotBatchSink):
    """Batched fallback sink for HubSpot contacts."""

    @property
    def batch_id_property(self) -> str:
        return (self.lookup_fields or ["email"])[0]


class CompaniesFallbackSink(HubspotBatchSink):
    """Batched fallback sink for HubSpot companies."""

    @property
    def batch_id_property(self) -> str:
        if self.lookup_fields:
            return self.lookup_fields[0]
        return "name"

    def prepare_staged_record(self, record: dict, context: dict) -> Optional[dict]:
        """Stage companies for batch/update, batch/upsert, or batch/create."""
        properties, associations = self.parse_fallback_properties(record)
        hubspot_id = properties.get("id")

        if hubspot_id:
            return {
                "properties": properties,
                "associations": associations,
                "batch_kind": BATCH_KIND_UPDATE,
                "id": str(hubspot_id),
            }

        properties.pop("id", None)
        if self.lookup_fields:
            upsert_key = properties.get(self.batch_id_property)
            if upsert_key:
                existing_objects = self.perform_object_lookup(properties, self.lookup_fields)
                if existing_objects and len(existing_objects) > 1:
                    raise InvalidPayloadError(
                        f"Multiple objects found for lookup fields {self.lookup_fields} on record {properties}"
                    )
                id_property = self.batch_id_property
                return {
                    "properties": properties,
                    "associations": associations,
                    "batch_kind": BATCH_KIND_UPSERT,
                    id_property: upsert_key,
                }

        return {
            "properties": properties,
            "associations": associations,
            "batch_kind": BATCH_KIND_CREATE,
        }

    def iter_batch_requests(self, staged_records: List[dict]) -> Iterator[dict]:
        """Route staged companies to batch/update, batch/upsert, or batch/create."""
        config = self._target._config
        object_type = self.name
        id_property = self.batch_id_property

        update_staged = [r for r in staged_records if r.get("batch_kind") == BATCH_KIND_UPDATE]
        upsert_staged = [r for r in staged_records if r.get("batch_kind") == BATCH_KIND_UPSERT]
        create_staged = [r for r in staged_records if r.get("batch_kind") == BATCH_KIND_CREATE]

        by_kind = {
            BATCH_KIND_UPDATE: dedupe_staged_by_hubspot_id(update_staged),
            BATCH_KIND_UPSERT: dedupe_staged_by_key(upsert_staged, id_property),
            BATCH_KIND_CREATE: create_staged,
        }

        if by_kind[BATCH_KIND_UPDATE]:
            yield {
                "records": by_kind[BATCH_KIND_UPDATE],
                "staged_records": update_staged,
                "request": lambda records: batch_update_objects(config, object_type, records),
                "parse": parse_batch_update_response,
            }
        if by_kind[BATCH_KIND_UPSERT]:
            yield {
                "records": by_kind[BATCH_KIND_UPSERT],
                "staged_records": upsert_staged,
                "request": lambda records: batch_upsert_objects(
                    config, object_type, id_property, records
                ),
                "parse": lambda response, records: parse_batch_upsert_response(
                    response, records, id_property
                ),
            }
        if by_kind[BATCH_KIND_CREATE]:
            yield {
                "records": by_kind[BATCH_KIND_CREATE],
                "staged_records": create_staged,
                "request": lambda records: batch_create_objects(config, object_type, records),
                "parse": parse_batch_create_response,
            }
