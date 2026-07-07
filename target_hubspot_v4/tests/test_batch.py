"""Unit tests for HubSpot batch helpers and sink routing."""

from unittest.mock import MagicMock, patch

from target_hubspot_v4.batch import (
    BATCH_KIND_CREATE,
    BATCH_KIND_UPDATE,
    BATCH_KIND_UPSERT,
    build_trace_id,
    is_missing_batch_result_error,
    is_whole_batch_failure,
    missing_batch_result_error,
    parse_batch_update_response,
    parse_batch_upsert_response,
    summarize_batch_response,
)
from target_hubspot_v4.batch_sinks import CompaniesFallbackSink, ContactsFallbackSink


def _make_sink(sink_cls, stream_name, lookup_fields=None):
    """Build a batch sink with a mocked target and optional lookup_fields config."""
    config = {"access_token": "test-token"}
    if lookup_fields is not None:
        config["lookup_fields"] = {stream_name: lookup_fields}
    target = MagicMock()
    target.config = config
    target._config = config
    target.EXTERNAL_ID_KEY = "externalId"
    schema = {"type": "object", "properties": {"email": {"type": "string"}}}
    return sink_cls(target=target, stream_name=stream_name, schema=schema, key_properties=[])


def _staged(state_hash, **extra):
    """Build a minimal staged batch record for parse/response tests."""
    staged = {
        "properties": extra.pop("properties", {}),
        "state": {"hash": state_hash},
        "trace_id": state_hash,
    }
    staged.update(extra)
    return staged


class TestBatchHelpers:
    """Low-level batch.py helpers: trace ids, failure detection, dedupe parsing."""

    def test_build_trace_id_uses_record_hash(self):
        staged = {"trace_id": "abc123", "state": {"hash": "abc123", "externalId": "ext-1"}}
        assert build_trace_id(staged) == "abc123"

    def test_is_whole_batch_failure_parses_409_with_results(self):
        response = MagicMock(status_code=409)
        response.json.return_value = {
            "results": [{"objectWriteTraceId": "hash-1", "id": "1"}],
            "errors": [],
        }
        assert is_whole_batch_failure(response) is False

    def test_missing_batch_result_error_uses_batch_kind(self):
        assert missing_batch_result_error(BATCH_KIND_UPDATE) == "Missing result from batch update"
        assert is_missing_batch_result_error(missing_batch_result_error(BATCH_KIND_UPSERT)) is True
        assert is_missing_batch_result_error("Some other error") is False

    def test_summarize_batch_response_counts_results_and_errors(self):
        response = MagicMock(status_code=207)
        response.json.return_value = {
            "status": "COMPLETE",
            "numErrors": 1,
            "results": [{"objectWriteTraceId": "hash-1", "id": "1"}],
            "errors": [{"message": "boom"}],
        }
        assert summarize_batch_response(response, 2) == {
            "status_code": 207,
            "inputs": 2,
            "results": 1,
            "errors": 1,
            "num_errors": 1,
            "completion_status": "COMPLETE",
        }

    def test_parse_superseded_row_gets_success(self):
        response = MagicMock(status_code=207)
        response.json.return_value = {
            "results": [{"objectWriteTraceId": "winner", "id": "999"}],
            "errors": [],
        }
        staged_records = [
            _staged("loser", properties={"email": "a@example.com"}, email="a@example.com"),
            _staged("winner", properties={"email": "a@example.com"}, email="a@example.com"),
        ]
        state_updates, _ = parse_batch_upsert_response(response, staged_records, "email")
        loser = next(s for s in state_updates if s["hash"] == "loser")
        assert loser["success"] is True
        assert loser["_duplicate"] is True
        assert loser["id"] == "999"


class TestContactsStaging:
    """prepare_staged_record routing for contacts (update / upsert / single fallback)."""

    def test_explicit_id_stages_update(self):
        sink = _make_sink(ContactsFallbackSink, "contacts")
        staged = sink.prepare_staged_record(
            {"email": "a@example.com", "id": "123", "firstname": "Ann"},
            {},
        )
        assert staged["batch_kind"] == BATCH_KIND_UPDATE
        assert staged["id"] == "123"

    @patch.object(ContactsFallbackSink, "perform_object_lookup", return_value=[])
    def test_email_without_id_stages_upsert(self, lookup):
        sink = _make_sink(ContactsFallbackSink, "contacts")
        staged = sink.prepare_staged_record({"email": "a@example.com", "firstname": "Ann"}, {})
        assert staged["batch_kind"] == BATCH_KIND_UPSERT
        assert staged["email"] == "a@example.com"
        lookup.assert_not_called()

    def test_missing_email_returns_none(self):
        sink = _make_sink(ContactsFallbackSink, "contacts")
        assert sink.prepare_staged_record({"firstname": "Ann"}, {}) is None

    @patch.object(ContactsFallbackSink, "perform_object_lookup", return_value=[{"id": "456"}])
    def test_email_stages_upsert_without_staging_lookup(self, lookup):
        sink = _make_sink(ContactsFallbackSink, "contacts", lookup_fields=["email"])
        staged = sink.prepare_staged_record({"email": "a@example.com", "firstname": "Ann"}, {})
        assert staged["batch_kind"] == BATCH_KIND_UPSERT
        assert staged["email"] == "a@example.com"
        lookup.assert_not_called()

    def test_upsert_hash_ignores_resolved_lookup_id(self):
        sink = _make_sink(ContactsFallbackSink, "contacts")
        staged = sink.prepare_staged_record({"email": "a@example.com", "firstname": "Ann"}, {})
        assert sink.build_staged_record_hash(staged) == sink.build_record_hash(
            {"properties": {"email": "a@example.com", "firstname": "Ann"}}
        )


class TestCompaniesStaging:
    """prepare_staged_record routing for companies (update / upsert / create)."""

    def test_explicit_id_stages_update(self):
        sink = _make_sink(CompaniesFallbackSink, "companies")
        staged = sink.prepare_staged_record({"id": "123", "name": "Acme"}, {})
        assert staged["batch_kind"] == BATCH_KIND_UPDATE
        assert staged["id"] == "123"

    def test_name_without_id_stages_upsert(self):
        sink = _make_sink(CompaniesFallbackSink, "companies")
        staged = sink.prepare_staged_record({"name": "Acme"}, {})
        assert staged["batch_kind"] == BATCH_KIND_UPSERT
        assert staged["name"] == "Acme"

    def test_no_key_stages_create(self):
        sink = _make_sink(CompaniesFallbackSink, "companies")
        staged = sink.prepare_staged_record({"domain": "acme.example"}, {})
        assert staged["batch_kind"] == BATCH_KIND_CREATE

    @patch.object(CompaniesFallbackSink, "perform_object_lookup", return_value=[{"id": "789"}])
    def test_lookup_without_name_stages_update(self, _lookup):
        sink = _make_sink(CompaniesFallbackSink, "companies", lookup_fields=["name"])
        staged = sink.prepare_staged_record({"domain": "acme.example"}, {})
        assert staged["batch_kind"] == BATCH_KIND_UPDATE
        assert staged["id"] == "789"


class TestBatchRouting:
    """iter_batch_requests splits and batch response parsing."""

    def test_contacts_split_update_and_upsert(self):
        sink = _make_sink(ContactsFallbackSink, "contacts")
        staged_records = [
            {"batch_kind": BATCH_KIND_UPDATE, "id": "1", "properties": {"email": "a@example.com"}},
            {"batch_kind": BATCH_KIND_UPSERT, "email": "b@example.com", "properties": {"email": "b@example.com"}},
        ]
        specs = list(sink.iter_batch_requests(staged_records))
        assert len(specs) == 2
        assert specs[0]["records"][0]["id"] == "1"
        assert specs[1]["records"][0]["email"] == "b@example.com"

    def test_companies_split_all_three_kinds(self):
        sink = _make_sink(CompaniesFallbackSink, "companies")
        staged_records = [
            {"batch_kind": BATCH_KIND_UPDATE, "id": "1", "properties": {"name": "A"}},
            {"batch_kind": BATCH_KIND_UPSERT, "name": "B", "properties": {"name": "B"}},
            {"batch_kind": BATCH_KIND_CREATE, "properties": {"domain": "c.example"}},
        ]
        specs = list(sink.iter_batch_requests(staged_records))
        assert len(specs) == 3

    def test_update_response_maps_by_hubspot_id(self):
        response = MagicMock(status_code=207)
        response.json.return_value = {
            "results": [{"objectWriteTraceId": "hash-1", "id": "123"}],
            "errors": [],
        }
        staged_records = [
            _staged("hash-1", properties={"name": "Acme"}, id="123", batch_kind=BATCH_KIND_UPDATE),
        ]
        state_updates, _ = parse_batch_update_response(response, staged_records)
        assert state_updates[0]["success"] is True
        assert state_updates[0]["id"] == "123"

    def test_update_response_marks_missing_result_for_fallback(self):
        response = MagicMock(status_code=207)
        response.json.return_value = {
            "results": [{"objectWriteTraceId": "hash-1", "id": "123"}],
            "errors": [],
        }
        staged_records = [
            _staged("hash-1", properties={"name": "Acme"}, id="123", batch_kind=BATCH_KIND_UPDATE),
            _staged("hash-2", properties={"name": "Beta"}, id="456", batch_kind=BATCH_KIND_UPDATE),
        ]
        state_updates, _ = parse_batch_update_response(response, staged_records)
        missing = next(s for s in state_updates if s["hash"] == "hash-2")
        assert missing["success"] is False
        assert missing["error"] == missing_batch_result_error(BATCH_KIND_UPDATE)


class TestMissingBatchResultFallback:
    """Records omitted from HubSpot batch responses fall back to single writes."""

    def test_missing_batch_result_state_is_not_applied(self):
        sink = _make_sink(CompaniesFallbackSink, "companies")
        sink.update_state = MagicMock()
        sink.write_single_staged_record = MagicMock()

        staged = _staged(
            "hash-2",
            properties={"name": "Beta", "id": "456"},
            id="456",
            batch_kind=BATCH_KIND_UPDATE,
        )
        parsed = {
            "state_updates": [
                {"hash": "hash-1", "success": True, "id": "123"},
                {"hash": "hash-2", "success": False, "error": missing_batch_result_error(BATCH_KIND_UPDATE)},
            ],
            "association_followups": [],
        }

        applied_hashes = sink._apply_batch_result(parsed)

        assert applied_hashes == {"hash-1"}
        sink.update_state.assert_called_once()
        unapplied = sink._unapplied_staged_records([staged], applied_hashes)
        assert unapplied == [staged]


class TestFlushDuplicateStates:
    """In-flush duplicate rows inherit the winner bookmark state."""

    def test_duplicate_gets_winner_error_state(self):
        sink = _make_sink(ContactsFallbackSink, "contacts")
        sink.latest_state = {
            "bookmarks": {
                "contacts": [{"hash": "dup-hash", "success": False, "error": "boom"}],
            },
            "summary": {
                "contacts": {"success": 0, "fail": 0, "existing": 0, "updated": 0},
            },
        }
        sink.summary_init = True
        context = {"flush_duplicate_states": [{"hash": "dup-hash", "externalId": "ext-1"}]}
        sink._apply_flush_duplicate_states(context)
        bookmarks = sink.latest_state["bookmarks"]["contacts"]
        assert len(bookmarks) == 2
        dup = bookmarks[-1]
        assert dup["success"] is False
        assert dup["error"] == "boom"
        assert dup["externalId"] == "ext-1"
