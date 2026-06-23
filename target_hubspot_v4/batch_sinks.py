"""Stream-specific batch fallback sinks."""

from typing import List, Optional

from hotglue_etl_exceptions import InvalidPayloadError

from target_hubspot_v4.batch import (
    batch_upsert_contacts,
    dedupe_contacts_by_email,
    parse_contact_batch_response,
)
from target_hubspot_v4.batch_client import HubspotBatchSink


class ContactsFallbackSink(HubspotBatchSink):
    """Batched fallback sink for HubSpot contacts using batch/upsert by email."""

    @property
    def endpoint(self):
        return "/contacts"

    def prepare_staged_record(self, record: dict, context: dict) -> Optional[dict]:
        """Stage a contact for batch upsert when it has an email."""
        properties, associations = self.parse_fallback_properties(record)
        properties.pop("id", None)
        email = properties.get("email")
        if not email:
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
            "email": email,
        }

    def make_batch_request(self, records: List[dict]):
        """Send a deduped contacts batch/upsert request."""
        deduped = dedupe_contacts_by_email(records)
        return batch_upsert_contacts(self._target._config, deduped)

    def parse_batch_response(self, response, staged_records: List[dict]) -> dict:
        """Parse a contacts batch/upsert response into state updates."""
        state_updates, association_followups = parse_contact_batch_response(response, staged_records)
        return {
            "state_updates": state_updates,
            "association_followups": association_followups,
        }
