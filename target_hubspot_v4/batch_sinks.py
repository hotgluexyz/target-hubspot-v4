"""Stream-specific batch fallback sinks."""

from target_hubspot_v4.batch_client import HubspotBatchSink


class ContactsFallbackSink(HubspotBatchSink):
    """Batched fallback sink for HubSpot contacts."""

    @property
    def batch_id_property(self) -> str:
        return (self.lookup_fields or ["email"])[0]


class CompaniesFallbackSink(HubspotBatchSink):
    """Batched fallback sink for HubSpot companies."""

    @property
    def supports_batch_create(self) -> bool:
        return True

    @property
    def batch_id_property(self) -> str:
        if self.lookup_fields:
            return self.lookup_fields[0]
        return "name"
