from target_hotglue.client import HotglueSink
from singer_sdk.plugin_base import PluginBase
from typing import Dict, List, Optional, Any
from target_hubspot_v4.auth import HubspotAuthenticator, HubspotApiKeyAuthenticator
from target_hotglue.auth import Authenticator

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

    @property
    def current_division(self):
        return self.config.get("current_division")

    base_url = "https://api.hubapi.com/crm/v3/objects"
    api_key = None

    @property
    def default_headers(self):
        headers = self.http_headers
        if self.authenticator:
            headers.update(self.authenticator.auth_headers)
        return headers
    
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