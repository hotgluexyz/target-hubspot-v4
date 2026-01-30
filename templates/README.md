# target-hubspot-v4 Configuration

Configuration reference for the target-hubspot-v4 Singer target. Use these options in your `config.json` when running the target.

---

## Config options

### Authentication

Use either **API key** or **OAuth** credentials. If both are present, the API key takes precedence.

#### `hapikey` (string, optional)
HubSpot private app API key. Required when not using OAuth.
- **Example**: `"your-hubspot-api-key"`

#### `client_id` (string, optional)
OAuth app client ID. Required when using OAuth.
- **Example**: `"your-oauth-client-id"`

#### `client_secret` (string, optional)
OAuth app client secret. Required when using OAuth.
- **Example**: `"your-oauth-client-secret"`

#### `refresh_token` (string, optional)
OAuth refresh token. Required when using OAuth.
- **Example**: `"your-oauth-refresh-token"`

#### `redirect_uri` (string, optional)
OAuth redirect URI. Must match the app’s configured redirect URI. Required when using OAuth.
- **Example**: `"https://your-app.com/oauth/callback"`

### General

#### `user_agent` (string, optional)
Custom user agent string to include in HTTP requests to HubSpot.
- **Example**: `"YourApp/1.0 target-hubspot-v4"`

#### `unified_api_schema` (boolean, optional)
When `true`, uses the unified API sink for writing records.
- **Default**: `false`
- **Example**: `false` or `true`

#### `only_upsert_empty_fields` (boolean, optional)
When `true` (and unified schema is used), only fills in empty fields on existing records instead of overwriting.
- **Default**: `false`
- **Example**: `false` or `true`

#### `current_division` (string or number, optional)
HubSpot current division ID, when using multi-account or division features.
- **Example**: `null` or a division ID

#### `lookup_fields` (object, optional)
Per-stream field(s) used to look up existing records (e.g. for upserts). Keys are stream names (lowercase), values are a field name or list of field names.
- **Default**: `{}`
- **Example**: `{"contacts": "email", "companies": "name"}`

#### `lookup_method` (string, optional)
Strategy for lookups when matching existing records.
- **Default**: `"all"`
- **Example**: `"all"`

---

## Minimal config (API key)

Only the HubSpot API key is required when using private app authentication:

```json
{
  "hapikey": "your-hubspot-api-key"
}
```

---

## Minimal config (OAuth)

When using OAuth, these options are required (the target will obtain and persist `access_token` and `expires_in`):

```json
{
  "client_id": "your-oauth-client-id",
  "client_secret": "your-oauth-client-secret",
  "refresh_token": "your-oauth-refresh-token",
  "redirect_uri": "https://your-app.com/oauth/callback"
}
```

---

## Complete config example

Example with authentication, optional behavior, and lookup settings:

```json
{
  "client_id": "your-oauth-client-id",
  "client_secret": "your-oauth-client-secret",
  "refresh_token": "your-oauth-refresh-token",
  "redirect_uri": "https://your-app.com/oauth/callback",
  "user_agent": "YourApp/1.0 target-hubspot-v4",
  "unified_api_schema": true,
  "only_upsert_empty_fields": true,
  "current_division": null,
  "lookup_fields": {
    "contacts": "email",
    "companies": "name"
  },
  "lookup_method": "all"
}
```
