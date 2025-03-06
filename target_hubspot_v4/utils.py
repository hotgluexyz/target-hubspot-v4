import logging
from datetime import datetime, timedelta

import backoff
import requests

logger = logging.getLogger("target-hubspot-v4")
logging.basicConfig(
    level=logging.DEBUG, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

SESSION = requests.Session()

BASE_URL = "https://api.hubapi.com"


class InvalidAuthException(Exception):
    pass


class SourceUnavailableException(Exception):
    pass


def giveup(exc):
    return (
        exc.response is not None
        and 400 <= exc.response.status_code < 500
        and exc.response.status_code != 429
    )


def on_giveup(details):
    if len(details["args"]) == 2:
        url, params = details["args"]
    else:
        url = details["args"]
        params = {}

    raise Exception(
        "Giving up on request after {} tries with url {} and params {}".format(
            details["tries"], url, params
        )
    )


def acquire_access_token_from_refresh_token(config):
    payload = {
        "grant_type": "refresh_token",
        "redirect_uri": config["redirect_uri"],
        "refresh_token": config["refresh_token"],
        "client_id": config["client_id"],
        "client_secret": config["client_secret"],
    }

    resp = requests.post(BASE_URL + "/oauth/v1/token", data=payload)
    if resp.status_code == 403:
        raise InvalidAuthException(resp.content)

    resp.raise_for_status()
    auth = resp.json()
    config["access_token"] = auth["access_token"]
    config["refresh_token"] = auth["refresh_token"]
    config["token_expires"] = datetime.utcnow() + timedelta(
        seconds=auth["expires_in"] - 600
    )
    logger.info("Token refreshed. Expires at %s", config["token_expires"])


def get_params_and_headers(config, params):
    """
    This function makes a params object and headers object based on the
    authentication values available. If there is an `hapikey` in the config, we
    need that in `params` and not in the `headers`. Otherwise, we need to get an
    `access_token` to put in the `headers` and not in the `params`
    """
    params = params or {}
    hapikey = config.get("hapikey")
    if hapikey is None:
        if (
            config.get("token_expires") is None
            or config.get("token_expires") < datetime.utcnow()
        ):
            acquire_access_token_from_refresh_token(config)
        headers = {"Authorization": "Bearer {}".format(config["access_token"])}
    else:
        params["hapikey"] = hapikey
        headers = {}

    if "user_agent" in config:
        headers["User-Agent"] = config["user_agent"]

    headers["Content-Type"] = "application/json"

    return params, headers

def raise_for_status(response):
    http_error_msg = ''
    if isinstance(response.reason, bytes):
        try:
            reason = response.reason.decode('utf-8')
        except UnicodeDecodeError:
            reason = response.reason.decode('iso-8859-1')
    else:
        reason = response.reason

    if 400 <= response.status_code < 500:
        http_error_msg = u'%s Client Error: %s for url: %s' % (response.status_code, reason, response.url)

    elif 500 <= response.status_code < 600:
        http_error_msg = u'%s Server Error: %s for url: %s' % (response.status_code, reason, response.url)

    if http_error_msg:
        resp_json = response.json() if response.text else ""
        http_error_msg = f"{http_error_msg}, Api response: {resp_json}, Payload: {response.request.body}, Url: {response.url}"
        raise requests.exceptions.HTTPError(http_error_msg, response=response)

@backoff.on_exception(
    backoff.constant,
    (requests.exceptions.RequestException, requests.exceptions.HTTPError),
    max_tries=5,
    jitter=None,
    giveup=giveup,
    interval=10,
)
def request_push(config, url, payload, params=None, method="POST"):

    params, headers = get_params_and_headers(config, params)

    req = requests.Request(
        method, url, json=payload, headers=headers, params=params
    ).prepare()
    logger.info(f"{method} %s", req.url)
    resp = SESSION.send(req)
    logger.debug(resp.text)

    if resp.status_code == 403:
        raise SourceUnavailableException(resp.content)
    elif resp.status_code == 409:
        # ignore duplicate error and proceed
        return resp
    elif resp.status_code == 404:
        logger.warning("url not found: %s", url)
    else:
        resp_json = resp.json()
        if resp_json.get("status") == "error":
            logger.warning(f"API response: {resp_json.get('message')}")
        raise_for_status(resp)
    return resp


@backoff.on_exception(
    backoff.constant,
    (requests.exceptions.RequestException, requests.exceptions.HTTPError),
    max_tries=5,
    jitter=None,
    giveup=giveup,
    on_giveup=on_giveup,
    interval=10,
)
def request(config, url, params=None):

    params, headers = get_params_and_headers(config, params)

    req = requests.Request("GET", url, params=params, headers=headers).prepare()
    logger.info("GET %s", req.url)
    resp = SESSION.send(req)
    if resp.status_code == 403:
        raise SourceUnavailableException(resp.content)
    else:
        resp.raise_for_status()

    return resp


def search_contact_by_email(config, email):
    params, headers = get_params_and_headers(config, None)
    url = f"https://api.hubapi.com/crm/v3/objects/contacts/{email}?idProperty=email"
    req = requests.Request("GET", url, params=params, headers=headers).prepare()
    response = SESSION.send(req)
    if response.status_code == 200:
        return response.json()
    return None
