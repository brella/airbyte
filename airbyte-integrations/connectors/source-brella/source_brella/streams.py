from abc import ABC
from typing import Any, Iterable, List, Mapping, MutableMapping, Optional, Tuple, Dict

import requests
import logging
from airbyte_cdk.sources.streams.http import HttpStream
from airbyte_cdk.models import SyncMode


# Basic full refresh stream
class BrellaStream(HttpStream, ABC):
    url_base = "https://api.brella.io/api/integration/"
    primary_key = "id"

    def __init__(self, config: Dict):
        super().__init__(authenticator=config["authenticator"])
        self.config = config

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        """
        TODO: Override this method to define a pagination strategy. If you will not be using pagination, no action is required - just return None.

        This method should return a Mapping (e.g: dict) containing whatever information required to make paginated requests. This dict is passed
        to most other methods in this class to help you form headers, request bodies, query params, etc..

        For example, if the API accepts a 'page' parameter to determine which page of the result to return, and a response from the API contains a
        'page' number, then this method should probably return a dict {'page': response.json()['page'] + 1} to increment the page count by 1.
        The request_params method should then read the input next_page_token and set the 'page' param to next_page_token['page'].

        :param response: the most recent response from the API
        :return If there is another page in the result, a mapping (e.g: dict) containing information needed to query the next page in the response.
                If there are no more pages in the result, return None.
        """
        return None

    def request_params(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> MutableMapping[str, Any]:
        return {}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        self.logger.info(f"Parsing response")
        records = []
        if response.status_code is requests.codes.OK:
            json_response = response.json()

            if isinstance(records, dict):
                records = json_response.get("data", [])
                for record in records:
                    yield record
            else:
                yield json_response

# Basic incremental stream
class IncrementalBrellaStream(BrellaStream, ABC):
    # TODO: Fill in to checkpoint stream reads after N records. This prevents re-reading of data if the stream fails for any reason.
    state_checkpoint_interval = None
    cursor_field = "updated_at"

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        """
        Override to determine the latest state after reading the latest record. This typically compared the cursor_field from the latest record and
        the current state and picks the 'most' recent cursor. This is how a stream's state is determined. Required for incremental.
        """
        return {}

    #def read_records(
        #self,
        #sync_mode: SyncMode,
        #cursor_field: List[str] = None,
        #stream_state: Mapping[str, Any] = None,
        #stream_slice: Optional[Mapping[str, Any]] = None
    #) -> Iterable[Mapping[str, Any]]:
        #self.logger.info(f"Read Records from incremental stream")
        #return super().read_records(sync_mode, stream_slice=stream_slice, stream_state=stream_state)


class BrellaSubStream(IncrementalBrellaStream):
    parent_stream_class: object = None
    slice_key: str = None
    nested_record: str = "id"
    nested_record_field_name: str = None
    nested_substream = None
    nested_substream_list_field_id = None

    def get_updated_state(self, current_stream_state: MutableMapping[str, Any], latest_record: Mapping[str, Any]) -> Mapping[str, Any]:
        updated_state = super().get_updated_state(current_stream_state, latest_record)
        updated_state[self.parent_stream.name] = stream_state_cache.cached_state.get(self.parent_stream.name)
        return updated_state

    def request_params(self, next_page_token: Mapping[str, Any] = None, **kwargs) -> MutableMapping[str, Any]:
        params = {}
        if next_page_token:
            params.update(**next_page_token)
        return params

    def parent_stream(self) -> object:
        self.logger.info(f"Finding parent stream")
        if self.parent_stream_class:
            return self.parent_stream_class(self.config)
        else:
            return None

    def read_records(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Optional[Mapping[str, Any]] = None,
        **kwargs,
    ) -> Iterable[Mapping[str, Any]]:
        self.logger.info(f"Substream Read Records")
        slice_data = stream_slice.get(self.slice_key)

        if isinstance(slice_data, list) and self.nested_record_field_name is not None and len(slice_data) > 0:
            slice_data = slice_data[0].get(self.nested_record_field_name)

        self.logger.info(f"Reading {self.name} for {self.slice_key}: {slice_data}")
        records = super().read_records(stream_slice=stream_slice, **kwargs)

        for record in records:
            yield record


class Organization(IncrementalBrellaStream):
    def __init__(self, organization_id: int, **kwargs):
        super().__init__(**kwargs)
        self.organization_id = organization_id

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        return f"organizations/{self.organization_id}"

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        return [response.json()]


class Events(IncrementalBrellaStream):
    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        return f"organizations/{self.config['organization_id']}/events"


class Invites(BrellaSubStream):
    parent_stream_class: object = Events
    cursor_field = "id"
    slice_key = "event_id"
    data_field = "invites"
    nested_substream = "invites"

    def stream_slices(self, stream_state: Mapping[str, Any] = None, **kwargs) -> Iterable[Optional[Mapping[str, Any]]]:
        self.logger.info(f"Invite stream slices")
        parent_stream = self.parent_stream()
        parent_stream_state = stream_state.get(parent_stream.name) if stream_state else {}
        slice_yielded = False

        for record in parent_stream.read_records(stream_state=parent_stream_state, **kwargs):
            event = record["data"][0]
            yield {"event_id": event["id"] }
            slice_yielded = True

        if not slice_yielded:
            yield {}

    def path(
        self,
        stream_state: Mapping[str, Any] = None,
        stream_slice: Mapping[str, Any] = None,
        next_page_token: Mapping[str, Any] = None
    ) -> str:
        self.logger.info(stream_slice)
        return f"organizations/{self.config['organization_id']}/events/{stream_slice[self.slice_key]}/invites"
