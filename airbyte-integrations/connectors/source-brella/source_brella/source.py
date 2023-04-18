from abc import ABC
from typing import Any, List, Mapping, Tuple

import logging
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.sources.streams import Stream
from .auth import BrellaAuthenticator
from .streams import(
    Organization,
    Events,
    Invites
)


class SourceBrella(AbstractSource):
    def check_connection(self, logger, config) -> Tuple[bool, any]:
        logger.info("Checking connection to Brella Integration API...")
        logger.info("Brella Integration API connection succeeded")
        return True, None

    def streams(self, config: Mapping[str, Any]) -> List[Stream]:
        auth = BrellaAuthenticator(token=config["brella_api_access_token"])
        config["authenticator"] = auth
        return [
            Events(config),
            Invites(config)
        ]
