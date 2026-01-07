"""Hubspot-v4 target sink class, which handles writing streams."""

from target_hubspot_v4.client import HubspotSink
from target_hubspot_v4.utils import request_push, search_objects_by_property

class FallbackSink(HubspotSink):
    """Precoro target sink class."""

    @property
    def is_full_path(self):
        return '/' in self.stream_name

    @property
    def endpoint(self):
        if self.is_full_path and self.stream_name.startswith("/"):
            return self.stream_name

        return f"/{self.stream_name}"
    
    @property
    def name(self):
        return self.stream_name

    def perform_object_lookup(self, record: dict, lookup_fields):
        if len(lookup_fields) == 0:
            return []
        if len(lookup_fields) == 1:
            lookup_field = lookup_fields[0]
            if not record.get(lookup_field):
                return []

            return search_objects_by_property(
                dict(self.config), 
                self.name, 
                [{"property_name": lookup_field, "value": record[lookup_field]}]
            )
        else:
            if self.lookup_method == "sequential":
                for lookup_field in lookup_fields:
                    matches = self.perform_object_lookup(record, [lookup_field])
                    if matches and len(matches) == 1:
                        return [matches[0]]
                return []
            else:
                if not all(record.get(lookup_field) for lookup_field in lookup_fields):
                    return []
                return search_objects_by_property(
                    dict(self.config), 
                    self.name, 
                    [{"property_name": lookup_field, "value": record[lookup_field]} for lookup_field in lookup_fields]
                )

    def preprocess_record(self, record: dict, context: dict) -> None:
        """Process the record."""
        if self.is_full_path:
            return record

        associations = record.pop("associations", None)

        if record.get("properties"):
            # some send record wrapped in properties key
            # denesting properties to parse all values inside properly
            record = record["properties"] 
        for key, value in record.items():
            record[key] = self.parse_objs(value)

        
        if self.lookup_fields:
            self.logger.debug(f"Searching for object by {self.lookup_fields}")
            # look contact by email and update id if found
            existing_objects = self.perform_object_lookup(record, self.lookup_fields)
            if existing_objects and len(existing_objects) > 1:
                raise Exception(f"Multiple objects found for lookup fields {self.lookup_fields} on record {record}")
            if existing_objects and len(existing_objects) == 1:
                self.logger.info(f"Found object by {self.lookup_fields} with id '{existing_objects[0]['id']}'")
                record["id"] = existing_objects[0]["id"]

        payload = {"properties": record}
        if associations:
            payload["associations"] = associations
        return payload
    
    def upsert_record(self, record: dict, context: dict):
        state_updates = dict()
        method = "POST"
        endpoint = self.endpoint
        pk = self.key_properties[0] if self.key_properties else "id"

        if record is not None:
            # post or put record
            id = record.get('properties', {}).pop(pk, None) if record.get("properties") else record.pop(pk, None)

            associations = None
            if id:
                method = "PATCH"
                endpoint = f"{endpoint}/{id}"
                # Hubspot only supports including associations in POST object
                associations = record.pop("associations", None)

            if record.get("properties") or record.get("associations"):
                response = self.request_api(method, endpoint=endpoint, request_data=record)
                id = response.json()[pk]
            elif self.is_full_path:
                full_url = f"https://api.hubapi.com{self.endpoint}"
                response = request_push(dict(self.config), full_url, payload=record, method=method)
                id = response.json().get(pk)

            if associations:
                self.put_associations(id, associations)


            
            return id, True, state_updates

    def put_associations(self, id, associations):
        for association in associations:
            to_id = association.get("to", {}).get("id")
            to_object_name = association.get("to", {}).get("objectType")
            fully_qualified_object_name = association.get("from", {}).get("objectType")
            if not to_id:
                raise Exception(f"to id is required for {association}")

            if not to_object_name:
                raise Exception(f"to objectType is required for {association}")

            types = association.get("types", [])
            if not types:
                raise Exception(f"types is required for {association}")

            from_object_name = fully_qualified_object_name or self.name

            associations_url = f"https://api.hubapi.com/crm/v4/objects/{from_object_name}/{id}/associations/{to_object_name}/{to_id}"
            
            response = request_push(dict(self.config), associations_url, payload=types, method="PUT")
            self.validate_response(response)
