"""HubSpot CRM batch API helpers."""

from typing import Callable, List, Optional, Tuple

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

BATCH_UPSERT_URL = "https://api.hubapi.com/crm/v3/objects/{object_type}/batch/upsert"
BATCH_UPDATE_URL = "https://api.hubapi.com/crm/v3/objects/{object_type}/batch/update"
BATCH_CREATE_URL = "https://api.hubapi.com/crm/v3/objects/{object_type}/batch/create"

BATCH_KIND_UPDATE = "update"
BATCH_KIND_UPSERT = "upsert"
BATCH_KIND_CREATE = "create"


def build_trace_id(staged: dict) -> str:
    """Return a unique batch trace id from the staged record hash."""
    state = staged.get("state") or {}
    return staged.get("trace_id") or state.get("hash")


def get_hubspot_id(staged: dict) -> Optional[str]:
    """Return the HubSpot object id from a staged record."""
    if staged.get("id"):
        return str(staged["id"])
    properties = staged.get("properties") or {}
    if properties.get("id"):
        return str(properties["id"])
    return None


def get_upsert_key(staged: dict, id_property: str) -> Optional[str]:
    """Return the batch upsert key value from a staged record."""
    if staged.get(id_property):
        return staged[id_property]
    properties = staged.get("properties") or {}
    return properties.get(id_property)


def _normalize_key(value: str) -> str:
    return value.lower()


def _last_staged_by_key(
    staged_records: List[dict],
    key_fn: Callable[[dict], Optional[str]],
) -> dict:
    """Map each normalized key to the last staged row (last-wins dedupe)."""
    by_key = {}
    for staged in staged_records:
        raw_key = key_fn(staged)
        if not raw_key:
            continue
        by_key[_normalize_key(str(raw_key))] = staged
    return by_key


def dedupe_staged_by_key(staged_records: List[dict], id_property: str) -> List[dict]:
    """Keep the last staged record per upsert key for batch API requests."""
    return list(_last_staged_by_key(staged_records, lambda s: get_upsert_key(s, id_property)).values())


def dedupe_staged_by_hubspot_id(staged_records: List[dict]) -> List[dict]:
    """Keep the last staged record per HubSpot object id."""
    return list(_last_staged_by_key(staged_records, get_hubspot_id).values())


def _batch_properties(staged: dict, drop_id: bool = False) -> dict:
    properties = dict(staged["properties"])
    if drop_id:
        properties.pop("id", None)
    return properties


def build_batch_upsert_payload(staged_records: List[dict], id_property: str) -> dict:
    """Build batch/upsert payload with objectWriteTraceId on each input."""
    inputs = []
    for staged in staged_records:
        properties = _batch_properties(staged)
        upsert_key = get_upsert_key(staged, id_property)
        if not upsert_key:
            continue
        properties[id_property] = upsert_key
        inputs.append(
            {
                "objectWriteTraceId": build_trace_id(staged),
                "id": upsert_key,
                "idProperty": id_property,
                "properties": properties,
            }
        )
    return {"inputs": inputs}


def build_batch_update_payload(staged_records: List[dict]) -> dict:
    """Build batch/update payload with objectWriteTraceId on each input."""
    inputs = []
    for staged in staged_records:
        hubspot_id = get_hubspot_id(staged)
        if not hubspot_id:
            continue
        inputs.append(
            {
                "objectWriteTraceId": build_trace_id(staged),
                "id": hubspot_id,
                "properties": _batch_properties(staged, drop_id=True),
            }
        )
    return {"inputs": inputs}


def build_batch_create_payload(staged_records: List[dict]) -> dict:
    """Build batch/create payload with objectWriteTraceId on each input."""
    inputs = []
    for staged in staged_records:
        inputs.append(
            {
                "objectWriteTraceId": build_trace_id(staged),
                "properties": _batch_properties(staged, drop_id=True),
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
def _post_batch(config: dict, url: str, payload: dict):
    """POST a HubSpot batch endpoint with the same retry behavior as request_push."""
    params, headers = get_params_and_headers(config, None)
    req = requests.Request("POST", url, json=payload, headers=headers, params=params).prepare()
    logger.info("POST %s", req.url)
    return SESSION.send(req)


def _send_batch(config: dict, url: str, payload: dict):
    """POST a batch request and return responses that may include partial success."""
    if not payload.get("inputs"):
        return None

    resp = _post_batch(config, url, payload)
    if resp.status_code in (200, 201, 207, 400, 409):
        return resp

    raise_etl_exceptions(resp)
    raise_for_status(resp)
    return resp


def batch_upsert_objects(
    config: dict,
    object_type: str,
    id_property: str,
    staged_records: List[dict],
):
    """POST batch/upsert for a CRM object type and return the raw response."""
    payload = build_batch_upsert_payload(staged_records, id_property)
    url = BATCH_UPSERT_URL.format(object_type=object_type)
    return _send_batch(config, url, payload)


def batch_update_objects(config: dict, object_type: str, staged_records: List[dict]):
    """POST batch/update for a CRM object type and return the raw response."""
    payload = build_batch_update_payload(staged_records)
    url = BATCH_UPDATE_URL.format(object_type=object_type)
    return _send_batch(config, url, payload)


def batch_create_objects(config: dict, object_type: str, staged_records: List[dict]):
    """POST batch/create for a CRM object type and return the raw response."""
    payload = build_batch_create_payload(staged_records)
    url = BATCH_CREATE_URL.format(object_type=object_type)
    return _send_batch(config, url, payload)


def is_whole_batch_failure(response) -> bool:
    """Return True when the batch response has no per-record results or trace errors."""
    if response is None:
        return True
    if response.status_code in (200, 201, 207):
        return False
    if response.status_code not in (400, 409):
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


def _collect_traced_response_maps(response):
    """Parse batch results and errors indexed by trace id and lookup key."""
    results_by_trace = {}
    errors_by_trace = {}
    errors_by_key = {}
    if response is None or is_whole_batch_failure(response):
        return results_by_trace, errors_by_trace, errors_by_key

    body = response.json()
    for result in body.get("results") or []:
        trace_id = result.get("objectWriteTraceId")
        if trace_id:
            results_by_trace[trace_id] = result

    for error in body.get("errors") or []:
        message = error.get("message", "Batch request failed")
        for trace_id in error.get("context", {}).get("objectWriteTraceId") or []:
            errors_by_trace[trace_id] = message
        for err_id in error.get("context", {}).get("ids") or []:
            errors_by_key.setdefault(_normalize_key(str(err_id)), message)

    return results_by_trace, errors_by_trace, errors_by_key


def parse_batch_traced_response(
    response,
    staged_records: List[dict],
    record_key_fn: Callable[[dict], Optional[str]],
    missing_result_error: str,
) -> Tuple[List[dict], List[dict]]:
    """Map batch results and errors back to staged records for state updates."""
    state_updates = []
    association_followups = []
    results_by_trace, errors_by_trace, errors_by_key = _collect_traced_response_maps(response)
    winning_trace_ids = {
        key: build_trace_id(staged)
        for key, staged in _last_staged_by_key(staged_records, record_key_fn).items()
    }

    for staged in staged_records:
        meta = dict(staged.get("state") or {})
        trace_id = build_trace_id(staged)
        raw_key = record_key_fn(staged)
        record_key = _normalize_key(str(raw_key)) if raw_key else ""
        winner_trace_id = winning_trace_ids.get(record_key)

        if trace_id in errors_by_trace:
            state_updates.append(_error_state(meta, errors_by_trace[trace_id]))
            continue

        if record_key and winner_trace_id and trace_id != winner_trace_id:
            winner_result = results_by_trace.get(winner_trace_id)
            if winner_result and winner_result.get("id"):
                state_updates.append(dict(meta, success=True, id=winner_result["id"], _duplicate=True))
                if staged.get("associations"):
                    association_followups.append(
                        {"id": winner_result["id"], "associations": staged["associations"]}
                    )
                continue
            if winner_trace_id in errors_by_trace:
                state_updates.append(_error_state(meta, errors_by_trace[winner_trace_id]))
                continue
            state_updates.append(
                dict(meta, success=False, error="Duplicate batch key superseded in batch")
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

        if record_key and record_key in errors_by_key:
            state_updates.append(_error_state(meta, errors_by_key[record_key]))
            continue

        state_updates.append(dict(meta, success=False, error=missing_result_error))

    return state_updates, association_followups


def parse_batch_upsert_response(
    response,
    staged_records: List[dict],
    id_property: str,
) -> Tuple[List[dict], List[dict]]:
    """Map batch/upsert results and errors back to staged records for state updates."""
    return parse_batch_traced_response(
        response,
        staged_records,
        lambda staged: get_upsert_key(staged, id_property),
        "Missing result from batch upsert",
    )


def parse_batch_update_response(
    response,
    staged_records: List[dict],
) -> Tuple[List[dict], List[dict]]:
    """Map batch/update results and errors back to staged records for state updates."""
    return parse_batch_traced_response(
        response,
        staged_records,
        get_hubspot_id,
        "Missing result from batch update",
    )


def parse_batch_create_response(
    response,
    staged_records: List[dict],
) -> Tuple[List[dict], List[dict]]:
    """Map batch/create results and errors back to staged records for state updates."""
    return parse_batch_traced_response(
        response,
        staged_records,
        lambda _staged: None,
        "Missing result from batch create",
    )


def staged_to_tap_record(staged: dict) -> dict:
    """Rebuild a tap-shaped record from a staged batch record."""
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
