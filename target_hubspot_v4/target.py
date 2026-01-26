"""Hubspot-v4 target class."""

from hotglue_singer_sdk.target_sdk.target import TargetHotglue
from typing import List, Optional, Union, Type
from pathlib import PurePath
from hotglue_singer_sdk import typing as th
from hotglue_singer_sdk.sinks import Sink
from hotglue_singer_sdk.helpers.capabilities import AlertingLevel

from target_hubspot_v4.sinks import (
    FallbackSink,
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
    alerting_level = AlertingLevel.WARNING
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

        for sink_class in self.SINK_TYPES:
            return FallbackSink

if __name__ == "__main__":
    TargetHubspotv4.cli()
