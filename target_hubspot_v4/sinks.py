"""Hubspot-v4 target sink class, which handles writing streams."""

from target_hubspot_v4.client import HubspotSink
import ast
import json

class FallbackSink(HubspotSink):
    """Precoro target sink class."""

    @property
    def endpoint(self):
        return f"/{self.stream_name}"
    
    @property
    def name(self):
        return self.stream_name
    
    def parse_objs(self, obj):
        try:
            try:
                return ast.literal_eval(obj)
            except:
                return json.loads(obj)
        except:
            return obj

    def preprocess_record(self, record: dict, context: dict) -> None:
        """Process the record."""
        for key, value in record.items():
            record[key] = self.parse_objs(value)
        # wrap all in properties if properties is not in the payload
        if not record.get("properties"):
            new_record = {}
            new_record["properties"] = record
        return new_record
    

    def upsert_record(self, record: dict, context: dict):
        state_updates = dict()
        method = "POST"
        endpoint = self.endpoint
        pk = self.key_properties[0] if self.key_properties else "id"
        if record:
            # post or put record
            id = record.get(pk)
            if id:
                method = "PUT"
                endpoint = f"{endpoint}/{id}"
            response = self.request_api(method, endpoint=endpoint, request_data=record)
            id = response.json()[pk]
            return id, True, state_updates