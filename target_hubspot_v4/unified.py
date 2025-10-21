"""HubspotV2 target sink class, which handles writing streams."""

import re
import json

from target_hotglue.client import HotglueSink

from target_hubspot_v4.utils import request_push, request, search_company_by_name, search_contact_by_email, map_country, search_call_by_id, search_deal_by_name, search_task_by_id
from singer_sdk.plugin_base import PluginBase
from typing import Dict, List, Optional

class UnifiedSink(HotglueSink):
    """UnifiedSink target sink class."""

    def __init__(
        self,
        target: PluginBase,
        stream_name: str,
        schema: Dict,
        key_properties: Optional[List[str]],
    ) -> None:
        self._state = dict(target._state)
        self._target = target
        super().__init__(target, stream_name, schema, key_properties)

    max_size = 10  # Max records to write in one batch
    contacts = []
    base_url = "https://api.hubapi.com/crm/v3/objects"

    @property
    def name(self):
        return self.stream_name
    
    def preprocess_record(self, record: dict, context: dict) -> dict:
        return record
    
    def process_record(self, record: dict, context: dict) -> None:
        if not self.latest_state:
            self.init_state()

        hash = self.build_record_hash(record)

        existing_state =  self.get_existing_state(hash)

        if existing_state:
            return self.update_state(existing_state, is_duplicate=True)

        state = {"hash": hash}

        id = None
        success = False
        state_updates = dict()

        external_id = record.pop("externalId", None)

        try:
            if self.stream_name.lower() in ["contacts", "contact", "customer", "customers"]:
                success, id, state_updates = self.process_contacts(record)
            if self.stream_name.lower() in ["activities", "activity"]:
                success, id, state_updates = self.process_activities(record)
            if self.stream_name.lower() in ["companies", "company"]:
                success, id, state_updates = self.upload_company(record)
            if self.stream_name.lower() in ["deals", "deal", "opportunities"]:
                success, id, state_updates = self.upload_deal(record)
            if self.stream_name.lower() in ["notes", "note"]:
                success, id, state_updates = self.process_notes(record)
        except Exception as e:
            self.logger.exception(f"Upsert record error {str(e)}")
            state_updates['error'] = str(e)

        if success:
            self.logger.info(f"{self.name} processed id: {id}")

        state["success"] = success

        if id:
            state["id"] = id

        if external_id:
            state["externalId"] = external_id

        if state_updates and isinstance(state_updates, dict):
            state = dict(state, **state_updates)

        self.update_state(state)


    def process_activities(self, record):
        res = None
        if record.get("type") == "call":
            res = self.process_call(record)
        if record.get("type") == "task":
            res = self.upload_task(record)
        if res:
            return True, res.get("id"), {}
        return False, None, {"error": f"Failed to process activity because type is not supported or was not found, type: {record.get('type')}"}

    def process_call(self, record):
        url = f"{self.base_url}/calls"
        call = {
            "properties": {
                "hs_timestamp": record.get("activity_datetime"),
                "hs_call_title": record.get("title"),
                "hubspot_owner_id": record.get("owner_id"),
                #   "hs_call_body": " Decision maker out, will call back tomorrow",
                "hs_call_duration": int(record.get("duration_seconds")) * 1000,
                #   "hs_call_from_number": "(857)Ï829 5489",
                #   "hs_call_to_number": "(509) 999 9999",
                "hs_call_direction": record.get("call_direction"),
                "hs_call_recording_url": record.get("recording_url"),
                "hs_call_status": "COMPLETED",
            }
        }

        if record.get("id"):
           
            if self.config.get("only_upsert_empty_fields", False):
                matched_call = search_call_by_id(dict(self.config), record.get("id"), properties=list(call["properties"].keys()))
                if matched_call:
                    for key in call["properties"].keys():
                        if matched_call["properties"].get(key, None) is not None:
                            call["properties"][key] = matched_call["properties"][key]

        resp = request_push(dict(self.config), url, call)
        data = resp.json()

        # Defining the association call -> contact

        callId = data.get("id")
        contactId = record.get("contact_id")

        url = f"{self.base_url}/calls/{callId}/associations/contact/{contactId}/call_to_contact"

        request_push(dict(self.config), url, {}, method="PUT")

        url = f"https://api.hubapi.com/crm/v4/objects/contacts/{contactId}/associations/deals"

        response = request(dict(self.config), url)

        response = response.json()
        # Defining the association call -> deal
        if response.get("results"):
            for deal in response.get("results"):
                dealId = deal.get("toObjectId")
                url = f"{self.base_url}/calls/{callId}/associations/deal/{dealId}/call_to_deal"
                request_push(dict(self.config), url, {}, method="PUT")
        return data


    def process_contacts(self, record):
        phone_numbers = record.get("phone_numbers")
        phone = None
        if phone_numbers:
            for phone_number in phone_numbers:
                if type(phone_number) is dict:
                    # if phone_number.get("type") in ["primary","mobile","home","fax","work"]: #Use this if we need to pick valid types only
                    if "number" in phone_number:
                        phone = phone_number.get("number")
                        break
                if type(phone_number) is str:
                    phone = phone_number
                    break
        else:
            phone = None

        row = {"properties": {}}
        
        if "first_name" in record:
            row["properties"]["firstname"] = record.get("first_name")
        if "last_name" in record:
            row["properties"]["lastname"] = record.get("last_name")
        if "email" in record:
            row["properties"]["email"] = record.get("email") 
        if "company_name" in record:
            row["properties"]["company"] = record.get("company_name")
        if "phone_numbers" in record:
            row["properties"]["phone"] = phone
        if "birthdate" in record:
            row["properties"]["date_of_birth"] = record.get("birthdate")
        if "industry" in record:
            row["properties"]["industry"] = record.get("industry")
        if "annual_revenue" in record:
            row["properties"]["annualrevenue"] = record.get("annual_revenue")
        if "salutation" in record:
            row["properties"]["salutation"] = record.get("salutation")
        if "title" in record:
            row["properties"]["jobtitle"] = record.get("title")

        # add address to customers
        addresses = record.get("addresses")
        if addresses:
            address = addresses[0]
            address_dict = {
                "address": address.get("line1"),
                "city": address.get("city"),
                "state": address.get("state"),
                "country": map_country(address.get("country")),
                "zip": address.get("postal_code"),
            }
            row["properties"].update(address_dict)

        if record.get("custom_fields"):
            custom_fields = self.process_contacts_custom_fields(
                record.get("custom_fields")
            )
            for field in custom_fields:

                if field.get("type") == "date":
                    row["properties"][field["name"].lower()] = self.check_time_value(
                        field["value"]
                    )
                elif field.get("type") == "bool":
                    row["properties"][field["name"].lower()] = field["value"] if isinstance(field["value"], bool) else field["value"].lower() == "true"
                else:
                    row["properties"][field["name"].lower()] = field["value"]

        if record.get("id"):
            row.update({"id": record.get("id")})

        contact_search = search_contact_by_email(dict(self.config), row["properties"].get("email"), properties=list(row["properties"].keys()))

        if "id" not in row and row["properties"].get("email"):    
            if contact_search:
                row.update({"id": contact_search.get("id")})


        if self.config.get("only_upsert_empty_fields", False) and contact_search:
            for key in row["properties"].keys():
                if contact_search.get("properties", {}).get(key, None) is not None:
                    row["properties"][key] = contact_search.get("properties", {}).get(key)
        # self.contacts.append(row)ƒ
        # for now process one contact at a time because if on contact is duplicate whole batch will fail
        self.logger.info(f"Uploading contact = {row}")
        try:
            res = self.contact_upload(row)
        except Exception as e:
            try:
                # In rare cases, the contact can be created right before we make this call and give a CONFLICT error
                # { "status": "error", "message": "Contact already exists. Existing ID: ...", "correlationId": "...", "category": "CONFLICT" }
                error_prefix = "Contact already exists. Existing ID: "
                if error_prefix in str(e) and '"category":"CONFLICT"' in str(e):
                    error = json.loads(str(e))
                    contact_id = error["message"].replace(error_prefix, "")
                    row.update({"id": contact_id})
                    self.logger.info(f"Reattempting uploading contact = {row}")
                    res = self.contact_upload(row)
                else:
                    raise e
            except Exception as e:
                raise e

        res = res.json()
        
        if record.get("lists"):
            should_subscribe = False if record.get("subscribe_status") == "unsubscribed" else True
            if should_subscribe:
                self.subscribe_to_lists(res.get("id"), record.get("lists"))
            else:
                self.unsubscribe_from_lists(res.get("id"), record.get("lists"))
        
        return True, res.get("id"), {}
    
    def subscribe_to_lists(self, contact_id, lists):
        """Subscribe a contact to multiple lists, creating lists if they don't exist."""
        for list_name in lists:
            list_exists, list_id = self.list_exists(list_name)
            if not list_exists:
                self.logger.info(f"Creating new list: {list_name}")
                list_id = self.create_list(list_name)
            if not self.is_contact_subscribed_to_list(contact_id, list_id):
                self.logger.info(f"Subscribing contact {contact_id} to list: {list_name} - id: {list_id}")
                self.subscribe_to_list(contact_id, list_id)
            self.logger.info(f"Contact {contact_id} subscribed to list: {list_name} - id: {list_id}")

    def unsubscribe_from_lists(self, contact_id, lists):
        """Unsubscribe a contact from multiple lists if they are subscribed."""
        for list_name in lists:
            list_exists, list_id = self.list_exists(list_name)
            if list_exists and self.is_contact_subscribed_to_list(contact_id, list_id):
                self.logger.info(f"Unsubscribing contact {contact_id} from list: {list_name} - id: {list_id}")
                self.unsubscribe_from_list(contact_id, list_id)
            
    def list_exists(self, list_name):
        """Check if a list exists in HubSpot."""
        url = f"https://api.hubapi.com/crm/v3/lists/object-type-id/0-1/name/{list_name}"
        try:
            response = request(dict(self.config), url)
            return response.status_code == 200, response.json().get("list",{}).get("listId")
        except Exception as e:
            self.logger.error(f"Error checking if list exists: {list_name} - {str(e)}")
            return False, None

    def create_list(self, list_name):
        """Create a new contact list in HubSpot."""
        url = "https://api.hubapi.com/crm/v3/lists"
        payload = {
            "name": list_name,
            "objectTypeId": "0-1",
            "processingType": "MANUAL"
        }
        try:
            response = request_push(dict(self.config), url, payload)
            if response.status_code not in [200, 201]:
                self.logger.error(f"Failed to create list {list_name}: {response.text}")
            return response.json().get("list",{}).get("listId")
        except Exception as e:
            self.logger.error(f"Error creating list {list_name}: {str(e)}")
            raise
    
    def subscribe_to_list(self, contact_id, list_id):
        """Add a contact to a specific list."""
        url = f"https://api.hubapi.com/crm/v3/lists/{list_id}/memberships/add-and-remove"
        payload = {
            "recordIdsToRemove": [],
            "recordIdsToAdd": [contact_id],
            "listId": list_id
        }
        try:
            response = request_push(dict(self.config), url, payload, method="PUT")
            if response.status_code not in [200, 201]:
                self.logger.error(f"Failed to subscribe contact {contact_id} to list {list_id}: {response.text}")
        except Exception as e:
            self.logger.error(f"Error subscribing contact {contact_id} to list {list_id}: {str(e)}")
            raise
    
    def unsubscribe_from_list(self, contact_id, list_id):
        """Remove a contact from a specific list."""
        url = f"https://api.hubapi.com/crm/v3/lists/{list_id}/memberships/add-and-remove"
        payload = {
            "recordIdsToRemove": [contact_id],
            "recordIdsToAdd": [],
            "listId": list_id
        }
        try:
            response = request_push(dict(self.config), url, payload, method="PUT")
            if response.status_code not in [200, 204]:
                self.logger.error(f"Failed to unsubscribe contact {contact_id} from list {list_id}: {response.text}")
        except Exception as e:
            self.logger.error(f"Error unsubscribing contact {contact_id} from list {list_id}: {str(e)}")
            raise
    
    def is_contact_subscribed_to_list(self, contact_id, list_id):
        """Check if a contact is subscribed to a specific list."""
        url = f"https://api.hubapi.com/crm/v3/lists/{list_id}/memberships/join-order"
        try:
            response = request(dict(self.config), url)
            response = response.json()
            members = [member.get("recordId") for member in response.get("results",[])]
            return contact_id in members
        except Exception as e:
            self.logger.error(f"Error checking if contact {contact_id} is subscribed to list {list_id}: {str(e)}")
            return False


    def process_contacts_custom_fields(self, custom_fields):
        url = "https://api.hubapi.com/crm/v3/properties/contacts"

        for field in custom_fields:
            payload = {
                "groupName": "contactinformation",
                "hidden": False,
                "name": field["name"].lower(),
                "label": (
                    field.get("label") if field.get("label") else field.get("name")
                ),
                "type": "string",
                "fieldType": "textarea",
            }
            if field.get("type"):  # check if type was passed in and assign
                payload["type"] = field.get("type")
                payload["fieldType"] = self.match_field_type_to_type(field.get("type"))

            response = request_push(dict(self.config), url, payload, "POST")
            if response.status_code == 409:
                self.logger.info(f"Custom field {field['name'].lower()} already exists")
            elif response.status_code == 201:
                self.logger.info(f"Custom field {field['name'].lower()} created")
            else:
                self.logger.error(
                    f"Error creating custom field {field['name'].lower()}"
                )
                custom_fields.remove(field)
                self.logger.error(response.json())

        return custom_fields

    def contact_upload(self, contact):
        method = "POST"
        url = f"{self.base_url}/contacts"
        if "id" in contact:
            url = f"{url}/{contact['id']}"
            del contact["id"]
            method = "PATCH"
        resp = request_push(dict(self.config), url, contact, None, method)
        if resp.status_code not in [200, 201, 204]:
            raise Exception(resp.text)
        return resp

    def contacts_batch_upload(self):
        url = f"{self.base_url}/contacts/batch/create"
        contacts = self.contacts
        request_push(dict(self.config), url, {"inputs": contacts})

    def process_batch(self, context: dict) -> None:
        if self.stream_name == "contacts":
            self.contacts_batch_upload()

    def upload_company(self, record):
        method = "POST"
        action = "created"
        mapping = {
            "name": record.get("name"),
            "domain": record.get("website"),
            "industry": record.get("industry"),
        }
        if "phone_numbers" in record:
            if len(record["phone_numbers"]) > 0:
                mapping.update({"phone": record["phone_numbers"][0]["number"]})
        if "addresses" in record:
            if len(record["addresses"]) > 0:
                mapping.update({"city": record["addresses"][0]["city"]})
                mapping.update({"state": record["addresses"][0]["state"]})
                mapping.update({"country": map_country(record["addresses"][0]["country"])})

        url = f"{self.base_url}/companies"
        if record.get("id"):
            url = f"{url}/{record.get('id')}"
            method = "PATCH"
            action = "updated"
        res = request_push(
            dict(self.config), url, {"properties": mapping}, None, method
        )
        res = res.json()
        if "id" in res:
            print(f"Comany id:{res['id']}, name:{mapping['name']}  {action}")
        return True, res.get("id"), {}
    

    def upload_deal(self, record):
        method = "POST"
        action = "created"
        mapping = {
            "dealname": record.get("title"),
            "amount": record.get("monetary_amount"),
            "pipeline": record.get("pipeline_id"),
            "priority": record.get("priority"),
        }

        close_date = record.get("close_date")
        if close_date:
            if close_date.endswith("Z"):
                mapping.update({"closedate": record.get("close_date")})
            else:
                mapping.update({"closedate": record.get("close_date") + "Z"})

        if record.get("owner_id"):
            mapping.update({"hubspot_owner_id": record.get("owner_id")})

        if record.get("status") in [
            "appointmentscheduled",
            "qualifiedtobuy",
            "presentationscheduled",
            "decisionmakerboughtin",
            "contractsent",
            "closedwon",
            "closedlost",
        ]:
            mapping["dealstage"] = record.get("status")


        url = f"{self.base_url}/deals"
        if record.get("id"):
            url = f"{url}/{record.get('id')}"
            method = "PATCH"
            action = "updated"
        res = request_push(
            dict(self.config), url, {"properties": mapping}, None, method
        )
        res = res.json()
        if "id" in res:
            self.logger.info(
                f"Deal id:{res['id']}, name:{mapping['dealname']}  {action}"
            )
            if "contact_email" in record:
                self.upload_deal_contact_association(
                    res["id"], None, record["contact_email"]
                )
            elif "contact_id" in record:
                self.upload_deal_contact_association(
                    res["id"], record["contact_id"], None
                )
        return True, res.get("id"), {}
    

    def upload_deal_contact_association(self, deal_id, contact_id, contact_email=None):
        if contact_email:
            contact_url = f"https://api.hubapi.com/crm/v3/objects/contacts/{contact_email}?idProperty=email"
            resp = request(dict(self.config), contact_url, None)
            if resp.status_code == 200:
                contact_id = resp.json()["id"]

        url = f"https://api.hubapi.com/crm/v4/objects/deals/{deal_id}/associations/contact/{contact_id}"
        url_labels = "https://api.hubapi.com/crm/v4/associations/deals/contacts/labels"
        res = request_push(dict(self.config), url_labels, None, None, "GET")
        res = res.json()
        label = res["results"][0]
        payload = [
            {
                "associationCategory": label["category"],
                "associationTypeId": label["typeId"],
            }
        ]
        res = request_push(dict(self.config), url, payload, None, "PUT")
        res = res.json()
        if res is not None:
            self.logger.info(
                f"Deal id:{deal_id} associated with contact id:{contact_id}"
            )
        else:
            self.logger.info(res.json())

    def upload_task(self, record):
        method = "POST"
        action = "created"

        mapping = {
            "hs_timestamp": record.get("end_datetime"),
            "hs_task_body": record.get("description"),
            "hs_task_subject": record.get("title"),
            "hs_task_status": record.get("status"),
            "hs_task_priority": record.get("priority"),
        }
        if record.get("owner_id"):
            mapping.update({"hubspot_owner_id": record.get("owner_id")})


        if record.get("id") and self.config.get("only_upsert_empty_fields", False):
                matched_task = search_task_by_id(dict(self.config), record.get("id"), properties=list(mapping.keys()))
                if matched_task:
                    for key in mapping.keys():
                        if matched_task["properties"].get(key, None) is not None:
                            mapping[key] = matched_task["properties"][key]

        url = f"{self.base_url}/tasks"
        if record.get("id"):
            url = f"{url}/{record.get('id')}"
            method = "PATCH"
            action = "updated"
        res = request_push(
            dict(self.config), url, {"properties": mapping}, None, method
        )
        res = res.json()
        if "id" in res:
            print(f"Task id:{res['id']}, name:{mapping['hs_task_subject']}  {action}")
        return res

    def process_notes(self, record):
        url = f"{self.base_url}/notes"

        mapping = {
            "hs_timestamp": int(record.get("created_at").timestamp()),
            "hs_note_body": record.get("content"),
            "hubspot_owner_id": record.get("customer_id")
        }

        mapping = {k: v for k, v in mapping.items() if v is not None}

        associations = []

        if record.get("company_id"):
            associations.append({
                "to": {"id": record.get("company_id")},
                "types": [
                    {
                        "associationCategory": "HUBSPOT_DEFINED",
                        "associationTypeId": 190
                    }
                ]
            })
        
        if record.get("company_name"):
            companies = search_company_by_name(dict(self.config), record.get("company_name"))
            if len(companies) == 1:
                company = companies[0]
                associations.append({
                    "to": {"id": company["id"]},
                    "types": [
                        {
                            "associationCategory": "HUBSPOT_DEFINED",
                            "associationTypeId": 190
                        }
                    ]
                })
            elif len(companies) > 1:
                return False, None, {"error": f"More than one company found for the provided company name"}
            else:
                return False, None, {"error": f"No company found for the provided company name"}

        if record.get("deal_id"):
            associations.append({
                "to": {"id": record.get("deal_id")},
                "types": [
                    {
                        "associationCategory": "HUBSPOT_DEFINED",
                        "associationTypeId": 214
                    }
                ]
            })
        
        if record.get("deal_name"):
            deals = search_deal_by_name(dict(self.config), record.get("deal_name"))
            if len(deals) == 1:
                deal = deals[0]
                associations.append({
                    "to": {"id": deal["id"]},
                    "types": [
                        {
                            "associationCategory": "HUBSPOT_DEFINED",
                            "associationTypeId": 214
                        }
                    ]
                })
            elif len(deals) > 1:
                return False, None, {"error": f"More than one deal found for the provided deal name"}
            else:
                return False, None, {"error": f"No deal found for the provided deal name"}

        payload = {"properties": mapping}
        if associations:
            payload["associations"] = associations

        if record.get("id"):
            url = f"{url}/{record.get('id')}"
            method = "PATCH"
            action = "updated"
        else:
            method = "POST"
            action = "created"

        res = request_push(
            dict(self.config), url, payload, None, method
        )
        res = res.json()
        if "id" in res:
            self.logger.info(
                f"Note id:{res['id']}, body:{mapping.get('hs_note_body', '')}  {action}"
            )
        return True, res.get("id"), {}

    def match_field_type_to_type(self, type):
        map_of_types = {
            "date": "date",
            "bool": "booleancheckbox",
            "enumeration": "select",
            "string": "textarea",
            "number": "number",
        }

        if type not in map_of_types:
            raise TypeError(
                f"Type: {type} provided does not match HubSpot accepted type values. Maybe you meant date, bool, enumeration, string or number?"
            )

        return map_of_types[type]

    def check_time_value(self, val):
        timestamp_regex = re.search("\d{4}[-]?\d{1,2}[-]?\d{1,2}", val)

        if timestamp_regex:
            return timestamp_regex.group()
        else:
            return val
