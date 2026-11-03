"""Colocated smoke-test runner for target-hubspot-v4 (hotglue-smoke-test).

Record → generate → run. Only ``record`` hits the network.

Scrubbing (see hotglue_smoke_test.vcr.target_scrub): collect Singer input, responses,
requests, and URIs; map by *value* (not field name); replace consistently on every
surface including rewritten data.singer. Structural ids/gids/cursors stay real.
Do not list PII (emails, names, list titles, etc.) in PRESERVE_* — after record,
commit the scrubbed data.singer + vcr.yaml together; do not hand-restore live values.
"""

import vcr

from hotglue_smoke_test.vcr.target import VCRTargetTestRunner


class TargetHubspotv4TestRunner(VCRTargetTestRunner):
    # HubSpot JSON keys that must stay literal for routing / enums (not Singer field values).
    PRESERVE_KEYS = {
        "grant_type",
        "token_type",
        "id",
        "contact_id",
        "title",
        "idProperty",
        "propertyName",
        "objectType",
        "associationCategory",
        # OAuth token body (HubSpot /oauth/v1/token); scope strings must not rewrite API paths.
        "scopes",
        "token_use",
        # Batch API request/response correlation (record hash).
        "objectWriteTraceId",
        # HubSpot company/deal property option codes (batch/create body).
        "industry",
        "domain",
        "status",
        "priority",
        # Unified activities (calls/tasks) — routing + HubSpot association type labels
        "type",
        # process_call hardcodes COMPLETED; must match cassette on replay
        "hs_call_status",
        "associationTypeId",
        # CRM list create (POST /crm/v3/lists) — enum-like API fields, not list titles
        "objectTypeId",
        "processingType",
        "message",
        "call_direction",
        "hs_call_direction",
        "hs_call_title",
        "dealname",
        "deal_name",
        "deal_title",
        "dealstage",
        "category",
        "amount",
    }

    # Non-PII literals the target branches on (not emails, person/company/list names).
    PRESERVE_VALUES = {
        "invalid",
        "invalid@example.com",
        "Lookup Tech Solutions",
        "subscribed",
        "unsubscribed",
        "MANUAL",
        "not-an-email",
        "Test",
        "User",
        "Test-1",
        "User-1",
        "User-updated",
        "test@domain.com",
        "test-11@domain.com",
        "test@example.com",
        "test-1@example.com",
    }

    def module(self) -> str:
        return "target_hubspot_v4"

    def launch(self):
        from target_hubspot_v4.target import TargetHubspotv4

        TargetHubspotv4.cli()

    def vcr_use_cassette(self, filter_query_parameters):
        # HubSpot may refresh OAuth more than once per run; replay must reuse the token interaction.
        record_mode = "all" if self.mode == "record" else "none"
        return vcr.use_cassette(
            self.vcr_cassette_path,
            decode_compressed_response=True,
            filter_headers=list(self.FILTER_HEADERS),
            filter_post_data_parameters=list(self.TOKEN_KEYS),
            filter_query_parameters=filter_query_parameters,
            match_on=["method", "scheme", "host", "port", "path", "query", "body"],
            before_record_response=self.before_record_response,
            record_mode=record_mode,
            allow_playback_repeats=True,
        )


if __name__ == "__main__":
    TargetHubspotv4TestRunner.main()
