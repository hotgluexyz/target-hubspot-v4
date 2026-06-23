"""HubSpot CRM batch API helpers."""

from typing import List, Optional, Tuple

import backoff
import requests

from target_hubspot_v4.utils import (
    SESSION,
    get_params_and_headers,
    giveup,
    logger,
    raise_etl_exceptions,
    raise_for_status,
)

CONTACTS_BATCH_UPSERT_URL = "https://api.hubapi.com/crm/v3/objects/contacts/batch/upsert"


def build_trace_id(staged: dict) -> str:
    """Return a unique batch trace id from externalId or record hash."""
    state = staged.get("state") or {}
    return staged.get("trace_id") or state.get("externalId") or state.get("hash")


def _last_staged_by_email(staged_records: List[dict]) -> dict:
    """Map each lowercase email to the last staged row (last-wins dedupe)."""
    by_email = {}
    for staged in staged_records:
        email = _get_email(staged)
        if not email:
            continue
        by_email[email.lower()] = staged
    return by_email


def dedupe_contacts_by_email(staged_records: List[dict]) -> List[dict]:
    """Keep the last staged record per email for batch API requests."""
    return list(_last_staged_by_email(staged_records).values())


def build_contact_upsert_payload(staged_records: List[dict]) -> dict:
    """Build batch/upsert payload with objectWriteTraceId on each input."""
    inputs = []
    for staged in staged_records:
        properties = dict(staged["properties"])
        email = properties.get("email") or staged.get("email")
        if not email:
            continue
        properties["email"] = email
        inputs.append(
            {
                "objectWriteTraceId": build_trace_id(staged),
                "id": email,
                "idProperty": "email",
                "properties": properties,
            }
        )
    return {"inputs": inputs}


@backoff.on_exception(
    backoff.constant,
    (requests.exceptions.RequestException, requests.exceptions.HTTPError),
    max_tries=5,
    jitter=None,
    giveup=giveup,
    interval=10,
)
def _post_batch_upsert(config: dict, payload: dict):
    """POST batch/upsert with the same retry behavior as request_push."""
    params, headers = get_params_and_headers(config, None)
    req = requests.Request(
        "POST", CONTACTS_BATCH_UPSERT_URL, json=payload, headers=headers, params=params
    ).prepare()
    logger.info("POST %s", req.url)
    return SESSION.send(req)


def batch_upsert_contacts(config: dict, staged_records: List[dict]):
    """POST contacts batch/upsert and return the raw response."""
    payload = build_contact_upsert_payload(staged_records)
    if not payload["inputs"]:
        return None

    resp = _post_batch_upsert(config, payload)

    if resp.status_code in (200, 201, 207, 400, 409):
        return resp

    raise_etl_exceptions(resp)
    raise_for_status(resp)
    return resp


def is_whole_batch_failure(response) -> bool:
    """Return True when the batch response has no per-record results or trace errors."""
    if response is None:
        return True
    if response.status_code in (200, 201, 207):
        return False
    if response.status_code != 400:
        return True
    try:
        body = response.json()
    except (ValueError, TypeError):
        return True
    if body.get("results"):
        return False
    if _errors_have_trace_ids(body.get("errors") or []):
        return False
    return True


def parse_contact_batch_response(
    response,
    staged_records: List[dict],
) -> Tuple[List[dict], List[dict]]:
    """Map batch/upsert results and errors back to staged records for state updates."""
    state_updates = []
    association_followups = []
    results_by_trace = {}
    errors_by_trace = {}
    winning_trace_ids = {
        email: build_trace_id(staged)
        for email, staged in _last_staged_by_email(staged_records).items()
    }

    if response is not None and not is_whole_batch_failure(response):
        body = response.json()
        for result in body.get("results") or []:
            trace_id = result.get("objectWriteTraceId")
            if trace_id:
                results_by_trace[trace_id] = result
        for error in body.get("errors") or []:
            message = error.get("message", "Batch upsert failed")
            for trace_id in error.get("context", {}).get("objectWriteTraceId") or []:
                errors_by_trace[trace_id] = message
            for email in error.get("context", {}).get("ids") or []:
                errors_by_trace.setdefault(email.lower(), message)

    for staged in staged_records:
        meta = dict(staged.get("state") or {})
        trace_id = build_trace_id(staged)
        email = (_get_email(staged) or "").lower()
        winner_trace_id = winning_trace_ids.get(email)

        if trace_id in errors_by_trace:
            state_updates.append(
                _error_state(meta, errors_by_trace[trace_id])
            )
            continue

        if email and winner_trace_id and trace_id != winner_trace_id:
            winner_result = results_by_trace.get(winner_trace_id)
            if winner_result and winner_result.get("id"):
                state_updates.append(dict(meta, id=winner_result["id"], _duplicate=True))
                continue
            if winner_trace_id in errors_by_trace:
                state_updates.append(_error_state(meta, errors_by_trace[winner_trace_id]))
                continue
            state_updates.append(
                dict(meta, success=False, error="Duplicate email superseded in batch")
            )
            continue

        result = results_by_trace.get(trace_id)
        if result and result.get("id"):
            state_updates.append(dict(meta, success=True, id=result["id"]))
            if staged.get("associations"):
                association_followups.append(
                    {"id": result["id"], "associations": staged["associations"]}
                )
            continue

        if email and email in errors_by_trace:
            state_updates.append(_error_state(meta, errors_by_trace[email]))
            continue

        state_updates.append(
            dict(meta, success=False, error="Missing result from batch upsert")
        )

    return state_updates, association_followups


def staged_to_tap_record(staged: dict) -> dict:
    """Rebuild a tap-shaped contact record from a staged batch record."""
    record = dict(staged["properties"])
    if staged.get("associations"):
        record["associations"] = staged["associations"]
    return record


def _error_state(meta: dict, message: str) -> dict:
    return dict(
        meta,
        success=None,
        error=message,
        hg_error_class="InvalidPayloadError",
    )


def _errors_have_trace_ids(errors: List[dict]) -> bool:
    for error in errors:
        if error.get("context", {}).get("objectWriteTraceId"):
            return True
    return False


def _get_email(staged: dict) -> Optional[str]:
    if staged.get("email"):
        return staged["email"]
    properties = staged.get("properties") or {}
    return properties.get("email")
