"""Hubspot-v4 target sink class, which handles writing streams."""

from target_hubspot_v4.client import HubspotSink

class FallbackSink(HubspotSink):
    """Precoro target sink class."""

    @property
    def endpoint(self):
        return f"/{self.stream_name}"
    
    @property
    def name(self):
        return self.stream_name

    def preprocess_record(self, record: dict, context: dict) -> None:
        """Process the record."""
        if record.get("properties"):
            # some send record wrapped in properties key
            # denesting properties to parse all values inside properly
            record = record["properties"] 
        for key, value in record.items():
            record[key] = self.parse_objs(value)   
        return {"properties": record}
    
    def upsert_record(self, record: dict, context: dict):
        state_updates = dict()
        method = "POST"
        endpoint = self.endpoint
        pk = self.key_properties[0] if self.key_properties else "id"
        if record:
            # post or put record
            id = record['properties'].pop(pk, None) if record.get("properties") else record.pop(pk, None)
            if id:
                method = "PATCH"
                endpoint = f"{endpoint}/{id}"
            response = self.request_api(method, endpoint=endpoint, request_data=record)
            id = response.json()[pk]
            return id, True, state_updates