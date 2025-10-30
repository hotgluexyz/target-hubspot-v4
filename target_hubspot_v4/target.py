"""Hubspot-v4 target class."""

from target_hotglue.target import TargetHotglue
from typing import List, Optional, Union
from pathlib import PurePath
from singer_sdk import typing as th
from typing import Type
from singer_sdk.sinks import Sink
import re


from target_hubspot_v4.sinks import (
    FallbackSink,
    SubscriptionSink,
)
from target_hubspot_v4.unified import UnifiedSink


class TargetHubspotv4(TargetHotglue):
    """Sample target for Hubspot-v4."""

    def __init__(
        self,
        config: Optional[Union[dict, PurePath, str, List[Union[PurePath, str]]]] = None,
        parse_env_config: bool = False,
        validate_config: bool = True,
        state: str = None
    ) -> None:
        self.config_file = config[0]
        super().__init__(config, parse_env_config, validate_config)

    name = "target-hubspot-v4"
    SINK_TYPES = [FallbackSink]

    config_jsonschema = th.PropertiesList(
        th.Property(
            "hapikey",
            th.StringType,
        ),
        th.Property(
            "client_id",
            th.StringType,
        ),
        th.Property(
            "client_secret",
            th.StringType,
        ),
        th.Property(
            "refresh_token",
            th.StringType,
        ),
    ).to_dict()

    def get_sink_class(self, stream_name: str) -> Type[Sink]:
        # Check if unified sinks are enabled
        if self.config.get("unified_api_schema", False):
            return UnifiedSink

        # Use SubscriptionSink for subscription-related streams
        # Matches: subscribe, unsubscribe, subscriptions, subscription, subscription_preferences, etc.
        if re.search(r'subscri(be|ption)', stream_name.lower()):
            return SubscriptionSink

        for sink_class in self.SINK_TYPES:
            return FallbackSink

if __name__ == "__main__":
    TargetHubspotv4.cli()
