"""Hubspot-v4 target sink class, which handles writing streams."""

from target_hubspot_v4.client import HubspotSink
from target_hubspot_v4.utils import search_contact_by_email, request_push

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

        associations = record.pop("associations", None)

        if record.get("properties"):
            # some send record wrapped in properties key
            # denesting properties to parse all values inside properly
            record = record["properties"] 
        for key, value in record.items():
            record[key] = self.parse_objs(value)

        if self.name.lower() == "contacts" and record.get("email"):
            self.logger.info(f"Searching for contact by email = {record['email']}")
            # look contact by email and update id if found
            existing_contact = search_contact_by_email(dict(self.config), record["email"], list(record.keys()))
            if existing_contact:
                self.logger.info(f"Found contact by email with id '{existing_contact['id']}'")
                record["id"] = existing_contact["id"]

        payload = {"properties": record}
        if associations:
            payload["associations"] = associations
        return payload
    
    def upsert_record(self, record: dict, context: dict):
        state_updates = dict()
        method = "POST"
        endpoint = self.endpoint
        pk = self.key_properties[0] if self.key_properties else "id"

        if record:
            # post or put record
            id = record['properties'].pop(pk, None) if record.get("properties") else record.pop(pk, None)

            associations = None
            if id:
                method = "PATCH"
                endpoint = f"{endpoint}/{id}"
                # Hubspot only supports including associations in POST object
                associations = record.pop("associations", None)

            if record.get("properties") or record.get("associations"):
                response = self.request_api(method, endpoint=endpoint, request_data=record)
                id = response.json()[pk]

            if associations:
                self.put_associations(id, associations)


            
            return id, True, state_updates

    def put_associations(self, id, associations):
        for association in associations:
            to_id = association.get("to", {}).get("id")
            to_object_name = association.get("to", {}).get("objectType")
            if not to_id:
                raise Exception(f"to id is required for {association}")

            if not to_object_name:
                raise Exception(f"to objectType is required for {association}")

            types = association.get("types", [])
            if not types:
                raise Exception(f"types is required for {association}")

            from_object_name = self.name

            associations_url = f"https://api.hubapi.com/crm/v4/objects/{from_object_name}/{id}/associations/{to_object_name}/{to_id}"
            
            response = request_push(dict(self.config), associations_url, payload=types, method="PUT")
            self.validate_response(response)

