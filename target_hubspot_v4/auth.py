import json
from datetime import datetime
from typing import Any, Dict, Optional

import logging
import requests
import backoff



class HubspotAuthenticator:
    """API Authenticator for OAuth 2.0 flows."""

    def __init__(
        self,
        target,
        state,
        auth_endpoint: Optional[str] = None,
    ) -> None:
        """Init authenticator.

        Args:
            stream: A stream for a RESTful endpoint.
        """
        self.target_name: str = target.name
        self._config: Dict[str, Any] = target._config
        self._auth_headers: Dict[str, Any] = {}
        self._auth_params: Dict[str, Any] = {}
        self.logger: logging.Logger = target.logger
        self._auth_endpoint = auth_endpoint
        self._config_file = target.config_file
        self._target = target
        self.state = state

    @property
    def auth_headers(self) -> dict:
        if not self.is_token_valid():
            self.update_access_token()
        result = {}
        result["Authorization"] = f"Bearer {self._config.get('access_token')}"
        return result

    @property
    def oauth_request_body(self) -> dict:
        """Define the OAuth request body for the hubspot API."""
        return {
        "grant_type": "refresh_token",
        "redirect_uri": self._config.get("redirect_uri"),
        "refresh_token": self._config.get("refresh_token"),
        "client_id": self._config.get("client_id"),
        "client_secret": self._config.get("client_secret"),
    }

    def is_token_valid(self) -> bool:
        access_token = self._config.get("access_token")
        now = round(datetime.utcnow().timestamp())
        expires_in = self._config.get("expires_in")
        if  expires_in is not None:
            expires_in = int(expires_in)
        if not access_token:
            return False
        if not expires_in:
            return False
        return not ((expires_in - now) < 120)

    @backoff.on_exception(backoff.expo, Exception, max_tries=3)
    def update_access_token(self) -> None:
        self.logger.info(f"Oauth request - endpoint: {self._auth_endpoint}, body: {self.oauth_request_body}")
        token_response = requests.post(
            self._auth_endpoint, data=self.oauth_request_body
        )
        try:
            token_response.raise_for_status()
            self.logger.info("OAuth authorization attempt was successful.")
        except Exception as ex:
            self.state.update({"auth_error_response": token_response.text})
            raise RuntimeError(
                f"Failed OAuth login, response was '{token_response.text()}'. {ex}"
            )
        token_json = token_response.json()
        #Log the refresh_token
        self.logger.info(f"Latest refresh token: {token_json['refresh_token']}")
        self.access_token = token_json["access_token"]
        self._config["access_token"] = token_json["access_token"]
        self._config["refresh_token"] = token_json["refresh_token"]
        now = round(datetime.utcnow().timestamp())
        self._config["expires_in"] = now + token_json["expires_in"]

        with open(self._target.config_file, "w") as outfile:
            json.dump(self._config, outfile, indent=4)

class HubspotApiKeyAuthenticator:
    auth_headers = {}