"""HubspotV2 target sink class, which handles writing streams."""

import re

from singer_sdk.sinks import BatchSink

from target_hubspot_v4.utils import request_push, request, search
from pendulum.parser import parse


class UnifiedSink(BatchSink):
    """UnifiedSink target sink class."""

    max_size = 10  # Max records to write in one batch
    contacts = []
    base_url = "https://api.hubapi.com/crm/v3/objects"

    def process_record(self, record: dict, context: dict) -> None:
        if self.stream_name.lower() in ["contacts", "contact", "customer", "customers"]:
            self.process_contacts(record)
        if self.stream_name.lower() in ["activities", "activity"]:
            self.process_activities(record)
        if self.stream_name.lower() in ["companies", "company"]:
            self.upload_company(record)
        if self.stream_name.lower() in ["deals", "deal", "opportunities"]:
            self.upload_deal(record)

    def process_activities(self, record):
        if record.get("type") == "call":
            self.process_call(record)
        if record.get("type") == "task":
            self.upload_task(record)

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

        row = {
            "properties": {
                "firstname": record.get("first_name"),
                "lastname": record.get("last_name"),
                "email": record.get("email"),
                # "hubspot_owner_id":record["owner_id"],
                "company": record.get("company_name"),
                "phone": phone,
            }
        }

        # add address to customers
        addresses = record.get("addresses")
        if addresses:
            address = addresses[0]
            address_dict = {
                "address": address.get("line1"),
                "city": address.get("city"),
                "state": address.get("state"),
                "country": address.get("country"),
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
                else:
                    row["properties"][field["name"].lower()] = field["value"]

        if record.get("id"):
            row.update({"id": record.get("id")})

        if "id" not in row and row["properties"].get("email"):
            contact_search = search(dict(self.config), row["properties"].get("email"))
            if contact_search:
                if len(contact_search) > 0:
                    if contact_search[0].get("id"):
                        row.update({"id": contact_search[0]["id"]})
        # self.contacts.append(row)
        # for now process one contact at a time because if on contact is duplicate whole batch will fail
        self.logger.info(f"Uploading contact = {row}")
        self.contact_upload(row)

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
        if resp.status_code in [400] or resp.status_code >= 500:
            raise Exception(resp.text)

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
                mapping.update({"country": record["addresses"][0]["country"]})

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
