from hotglue_singer_sdk.target_sdk.client import HotglueSink
from hotglue_singer_sdk.plugin_base import PluginBase
from typing import Dict, List, Optional, Any
from target_hubspot_v4.auth import HubspotAuthenticator, HubspotApiKeyAuthenticator
import ast
import json
import backoff
import requests
from target_hubspot_v4 import utils

class HubspotSink(HotglueSink):

    def __init__(
        self,
        target: PluginBase,
        stream_name: str,
        schema: Dict,
        key_properties: Optional[List[str]],
    ) -> None:
        """Initialize target sink."""
        self._target = target
        super().__init__(target, stream_name, schema, key_properties)

    auth_state = {}
    marketing_sinks = ["campaigns"]

    @property
    def current_division(self):
        return self.config.get("current_division")
    
    @property
    def base_url(self):
        if self.name in self.marketing_sinks:
            return "https://api.hubapi.com/marketing/v3"
        return "https://api.hubapi.com/crm/v3/objects"

    api_key = None
    
    @property
    def authenticator(self):
        # auth with hapikey
        if self.config.get("hapikey"):
            self.api_key = self.config.get("hapikey")
            return HubspotApiKeyAuthenticator()
        # auth with acces token
        url = "https://api.hubapi.com/oauth/v1/token"
        return HubspotAuthenticator(
            self._target, self.auth_state, url
        )

    @property
    def params(
        self
    ) -> Dict[str, Any]:
        if self.api_key:
            return {"hapikey": self.api_key}
        return {}

    @property
    def lookup_fields(self):
        lookup_field_configuration = self.config.get("lookup_fields", {})
        stream_lookup_field = lookup_field_configuration.get(self.name.lower())

        if not stream_lookup_field and self.name == 'contacts':
            return ['email']

        if isinstance(stream_lookup_field, str):
            return [stream_lookup_field]
        return stream_lookup_field

    @property
    def lookup_method(self):
        return self.config.get("lookup_method", "all")
    
    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        headers.update(self.authenticator.auth_headers or {})
        return headers
    
    def parse_objs(self, obj):
        try:
            try:
                obj = ast.literal_eval(obj)
            except:
                obj = json.loads(obj)
        except:
            pass
        # hubspot doesn't allow dicts or strings
        if isinstance(obj, dict) or isinstance(obj, list):
            obj = json.dumps(obj)
        return obj

    def validate_response(self, response: requests.Response) -> None:
        utils.raise_etl_exceptions(response)
        return super().validate_response(response)

    @backoff.on_exception(
        backoff.constant,
    (requests.exceptions.RequestException, requests.exceptions.HTTPError),
    max_tries=5,
    jitter=None,
    giveup=utils.giveup,
    on_giveup=utils.on_giveup,
    interval=10,
    )
    def request_api(self, method, endpoint, request_data=None):
        return super().request_api(method, endpoint=endpoint, request_data=request_data)
