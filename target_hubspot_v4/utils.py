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


def search_contact_by_email(config, email, properties=[]):
    params, headers = get_params_and_headers(config, None)
    url = f"https://api.hubapi.com/crm/v3/objects/contacts/{email}?idProperty=email"
    if properties:
        url += f"&properties={','.join(properties)}"
    req = requests.Request("GET", url, params=params, headers=headers).prepare()
    response = SESSION.send(req)
    if response.status_code == 200:
        return response.json()
    return None

def search_call_by_id(config, id, properties=[]):
    params, headers = get_params_and_headers(config, None)
    url = f"https://api.hubapi.com/crm/v3/objects/calls/{id}"
    if properties:
        url += f"?properties={','.join(properties)}"
    req = requests.Request("GET", url, params=params, headers=headers).prepare()
    response = SESSION.send(req)
    if response.status_code == 200:
        return response.json()
    return None

def search_task_by_id(config, id, properties=[]):
    params, headers = get_params_and_headers(config, None)
    url = f"https://api.hubapi.com/crm/v3/objects/tasks/{id}"
    if properties:
        url += f"?properties={','.join(properties)}"
    req = requests.Request("GET", url, params=params, headers=headers).prepare()
    response = SESSION.send(req)
    if response.status_code == 200:
        return response.json()
    return None

COUNTRY_MAPPING = {
    "AF": "Afghanistan",
    "AL": "Albania",
    "DZ": "Algeria",
    "AS": "American samoa",
    "AD": "Andorra",
    "AO": "Angola",
    "AI": "Anguilla",
    "AQ": "Antarctica",
    "AG": "Antigua and barbuda",
    "AR": "Argentina",
    "AM": "Armenia",
    "AW": "Aruba",
    "AU": "Australia",
    "AT": "Austria",
    "AZ": "Azerbaijan",
    "BS": "Bahamas",
    "BH": "Bahrain",
    "BD": "Bangladesh",
    "BB": "Barbados",
    "BY": "Belarus",
    "BE": "Belgium",
    "BZ": "Belize",
    "BJ": "Benin",
    "BM": "Bermuda",
    "BT": "Bhutan",
    "BO": "Bolivia",
    "BA": "Bosnia and herzegovina",
    "BW": "Botswana",
    "BV": "Bouvet island",
    "BR": "Brazil",
    "IO": "British indian ocean territory",
    "BN": "Brunei darussalam",
    "BG": "Bulgaria",
    "BF": "Burkina faso",
    "BI": "Burundi",
    "KH": "Cambodia",
    "CM": "Cameroon",
    "CA": "Canada",
    "CV": "Cape verde",
    "KY": "Cayman islands",
    "CF": "Central african republic",
    "TD": "Chad",
    "CL": "Chile",
    "CN": "China",
    "CX": "Christmas island",
    "CC": "Cocos (keeling) islands",
    "CO": "Colombia",
    "KM": "Comoros",
    "CG": "Congo",
    "CD": "Congo, the democratic republic of",
    "CK": "Cook islands",
    "CR": "Costa rica",
    "CI": "C\u00e3\u201dte d'ivoire",
    "HR": "Croatia",
    "CU": "Cuba",
    "CY": "Cyprus",
    "CZ": "Czech republic",
    "DK": "Denmark",
    "DJ": "Djibouti",
    "DM": "Dominica",
    "DO": "Dominican republic",
    "EC": "Ecuador",
    "EG": "Egypt",
    "SV": "El salvador",
    "GQ": "Equatorial guinea",
    "ER": "Eritrea",
    "EE": "Estonia",
    "ET": "Ethiopia",
    "FK": "Falkland islands (malvinas)",
    "FO": "Faroe islands",
    "FJ": "Fiji",
    "FI": "Finland",
    "FR": "France",
    "GF": "French guiana",
    "PF": "French polynesia",
    "TF": "French southern territories",
    "GA": "Gabon",
    "GM": "Gambia",
    "GE": "Georgia",
    "DE": "Germany",
    "GH": "Ghana",
    "GI": "Gibraltar",
    "GR": "Greece",
    "GL": "Greenland",
    "GD": "Grenada",
    "GP": "Guadeloupe",
    "GU": "Guam",
    "GT": "Guatemala",
    "GN": "Guinea",
    "GW": "Guinea",
    "GY": "Guyana",
    "HT": "Haiti",
    "HM": "Heard island and mcdonald islands",
    "HN": "Honduras",
    "HK": "Hong kong",
    "HU": "Hungary",
    "IS": "Iceland",
    "IN": "India",
    "ID": "Indonesia",
    "IR": "Iran, islamic republic of",
    "IQ": "Iraq",
    "IE": "Ireland",
    "IL": "Israel",
    "IT": "Italy",
    "JM": "Jamaica",
    "JP": "Japan",
    "JO": "Jordan",
    "KZ": "Kazakhstan",
    "KE": "Kenya",
    "KI": "Kiribati",
    "KP": "Korea, democratic people's republic of",
    "KR": "Korea, republic of",
    "KW": "Kuwait",
    "KG": "Kyrgyzstan",
    "LA": "Lao people's democratic republic",
    "LV": "Latvia",
    "LB": "Lebanon",
    "LS": "Lesotho",
    "LR": "Liberia",
    "LY": "Libyan arab jamahiriya",
    "LI": "Liechtenstein",
    "LT": "Lithuania",
    "LU": "Luxembourg",
    "MO": "Macao",
    "MK": "Macedonia, the former yugoslav republic of",
    "MG": "Madagascar",
    "MW": "Malawi",
    "MY": "Malaysia",
    "MV": "Maldives",
    "ML": "Mali",
    "MT": "Malta",
    "MH": "Marshall islands",
    "MQ": "Martinique",
    "MR": "Mauritania",
    "MU": "Mauritius",
    "YT": "Mayotte",
    "MX": "Mexico",
    "FM": "Micronesia, federated states of",
    "MD": "Moldova, republic of",
    "MC": "Monaco",
    "MN": "Mongolia",
    "MS": "Montserrat",
    "MA": "Morocco",
    "MZ": "Mozambique",
    "MM": "Myanmar",
    "NA": "Namibia",
    "NR": "Nauru",
    "NP": "Nepal",
    "NL": "Netherlands",
    "AN": "Netherlands antilles",
    "NC": "New caledonia",
    "NZ": "New zealand",
    "NI": "Nicaragua",
    "NE": "Niger",
    "NG": "Nigeria",
    "NU": "Niue",
    "NF": "Norfolk island",
    "MP": "Northern mariana islands",
    "NO": "Norway",
    "OM": "Oman",
    "PK": "Pakistan",
    "PW": "Palau",
    "PS": "Palestinian territory, occupied",
    "PA": "Panama",
    "PG": "Papua new guinea",
    "PY": "Paraguay",
    "PE": "Peru",
    "PH": "Philippines",
    "PN": "Pitcairn",
    "PL": "Poland",
    "PT": "Portugal",
    "PR": "Puerto rico",
    "QA": "Qatar",
    "RE": "R\u00e3\u2030union",
    "RO": "Romania",
    "RU": "Russian federation",
    "RW": "Rwanda",
    "SH": "Saint helena",
    "KN": "Saint kitts and nevis",
    "LC": "Saint lucia",
    "PM": "Saint pierre and miquelon",
    "VC": "Saint vincent and the grenadines",
    "WS": "Samoa",
    "SM": "San marino",
    "ST": "Sao tome and principe",
    "SA": "Saudi arabia",
    "SN": "Senegal",
    "CS": "Serbia and montenegro",
    "SC": "Seychelles",
    "SL": "Sierra leone",
    "SG": "Singapore",
    "SK": "Slovakia",
    "SI": "Slovenia",
    "SB": "Solomon islands",
    "SO": "Somalia",
    "ZA": "South africa",
    "GS": "South georgia and south sandwich islands",
    "ES": "Spain",
    "LK": "Sri lanka",
    "SD": "Sudan",
    "SR": "Suriname",
    "SJ": "Svalbard and jan mayen",
    "SZ": "Swaziland",
    "SE": "Sweden",
    "CH": "Switzerland",
    "SY": "Syrian arab republic",
    "TW": "Taiwan, province of china",
    "TJ": "Tajikistan",
    "TZ": "Tanzania, united republic of",
    "TH": "Thailand",
    "TL": "Timor",
    "TG": "Togo",
    "TK": "Tokelau",
    "TO": "Tonga",
    "TT": "Trinidad and tobago",
    "TN": "Tunisia",
    "TR": "Turkey",
    "TM": "Turkmenistan",
    "TC": "Turks and caicos islands",
    "TV": "Tuvalu",
    "UG": "Uganda",
    "UA": "Ukraine",
    "AE": "United arab emirates",
    "GB": "United kingdom",
    "US": "United states",
    "UM": "United states minor outlying islands",
    "UY": "Uruguay",
    "UZ": "Uzbekistan",
    "VU": "Vanuatu",
    "VN": "Viet nam",
    "VG": "Virgin islands, british",
    "VI": "Virgin islands, u.s.",
    "WF": "Wallis and futuna",
    "EH": "Western sahara",
    "YE": "Yemen",
    "ZW": "Zimbabwe"
}

def map_country(country_code: str) -> str:
    return COUNTRY_MAPPING.get(country_code, country_code)

def flatten_string_list(string_list) -> list[str]:
    """
    Takes list of strings and/or lists of strings and flattens them into a single list of strings.
    For example:
    ["a", ["b", "c"], "d"] -> ["a", "b", "c", "d"]
    """
    return [v for item in string_list for v in (item if isinstance(item, list) else [item])]
