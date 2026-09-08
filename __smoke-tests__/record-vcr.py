import os
import vcr
from hotglue_smoke_test.vcr.target import VCRTargetTestRunner
import json

class TargetHubspotv4TestRunner(VCRTargetTestRunner):

    SENSITIVE_KEYS = []

    def module(self) -> str:
        return "target_hubspot_v4"

    def launch(self):
        from target_hubspot_v4.target import TargetHubspotv4
        TargetHubspotv4.cli()

    def get_all_secrets_from_all_dirs(self):
        def is_secret(key):
            for secret_keyword in ['client_id', 'client_secret', 'refresh_token', 'access_token', 'client','secret','token','api_key','api_secret','password']:
                if secret_keyword in key.lower():
                    return True
            return False
        def deep_extract(obj, secrets_map):
            """Drills down into dicts and lists to find every single value."""
            if isinstance(obj, dict):
                for key, value in obj.items():
                    if isinstance(value, (str, int, float)) and value:
                        # We store the value as the key to find, 
                        # and the dict-key as part of the placeholder
                        if str(value) not in secrets_map and is_secret(key) and value not in ['client_id', 'client_secret', 'refresh_token', 'access_token', 'client','secret','token','api_key','api_secret','password']:
                            secrets_map[str(value)] = f"{key}"
                    else:
                        deep_extract(value, secrets_map)
            elif isinstance(obj, list):
                for item in obj:
                    deep_extract(item, secrets_map)
        secrets_map = {}
        # '.' starts the search from your current working directory
        for root, dirs, files in os.walk('.'):
            for filename in files:
                # Check for config files (adjust naming pattern if needed)
                if filename.endswith('.json') and 'config' in filename.lower():
                    file_path = os.path.join(root, filename)
                    try:
                        with open(file_path, 'r') as f:
                            data = json.load(f)
                            deep_extract(data, secrets_map)
                    except (json.JSONDecodeError, IOError):
                        continue
        #print(secrets_map.keys())
        self.SENSITIVE_KEYS = secrets_map.keys()
        print(self.SENSITIVE_KEYS)
        return secrets_map

    def scrub_interaction(self, interaction):
        """
        Handles both vcr.request.Request and response dictionaries.
        """
        placeholder= "<REDACTED>"
        for secret_value in self.SENSITIVE_KEYS:
            # 1. Handle Request (Attribute access)
            if hasattr(interaction, 'uri'):
                if secret_value in interaction.uri:
                    interaction.uri = interaction.uri.replace(secret_value, placeholder)
                
                # Request Headers
                for header, values in interaction.headers.items():
                    interaction.headers[header] = [v.replace(secret_value, placeholder) for v in values]
                
                # Request Body (bytes)
                if interaction.body:
                    s_bytes, p_bytes = secret_value.encode('utf-8'), placeholder.encode('utf-8')
                    interaction.body = interaction.body.replace(s_bytes, p_bytes)

            # 2. Handle Response (Usually passed as a dict in before_record_response)
            else:
                # Response Body
                if 'body' in interaction and 'string' in interaction['body']:
                    s_bytes, p_bytes = secret_value.encode('utf-8'), placeholder.encode('utf-8')
                    current_body = interaction['body']['string']
                    interaction['body']['string'] = current_body.replace(s_bytes, p_bytes)
                
                # Response Headers
                if 'headers' in interaction:
                    for header, values in interaction['headers'].items():
                        interaction['headers'][header] = [v.replace(secret_value, placeholder) for v in values]

                try:
                    token_keys = ['access_token', 'refresh_token']
                    body = interaction['body']['string'].decode('utf-8')

                    # Attempt to load the body as JSON
                    data = json.loads(body)

                    for key in token_keys:
                        if key in data:
                            # Replace the access token with a placeholder
                            data[key] = "<REDACTED>"
                            # Encode the modified data back to JSON and update the response
                            interaction['body']['string'] = json.dumps(data).encode('utf-8')
                    interaction['headers']['Content-Length'] = [str(len(interaction['body']['string']))]
                except json.JSONDecodeError:
                    # If the body is not JSON, do nothing
                    pass

        return interaction

    def vcr_use_cassette(self, filter_query_parameters):

        # decode_compressed_response=True is used to make sure responses are plain text, so we can scrub credentials.
        # before_record_request and before_record_response are used to scrub credentials from the request and response.
        self.get_all_secrets_from_all_dirs()


        my_vcr = vcr.VCR()

        return my_vcr.use_cassette(
            self.vcr_cassette_path,
            decode_compressed_response=True,
            before_record_request=self.scrub_interaction,
            before_record_response=self.scrub_interaction,
            filter_headers=['authorization'],
            filter_post_data_parameters=list(self.TOKEN_KEYS),
            filter_query_parameters=filter_query_parameters,
            match_on=["method", "scheme", "host", "port", "path", "query", "body"]
        )

if __name__ == "__main__":
    TargetHubspotv4TestRunner.main()
