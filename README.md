# target-hubspot-v4

**target-hubspot-v4** is a Singer Target for writing data to Hubspot.

**target-hubspot-v4** can be run on [hotglue](https://hotglue.com), an embedded integration platform for running Singer Taps and Targets.

## Installation

- [ ] `Developer TODO:` Update the below as needed to correctly describe the install procedure. For instance, if you do not have a PyPi repo, or if you want users to directly install from your git repo, you can modify this step as appropriate.

```bash
pipx install target-hubspot-v4
```

## Configuration

### Accepted Config Options

- [ ] `Developer TODO:` Provide a list of config options accepted by the target.

A full list of supported settings and capabilities for this
target is available by running:

```bash
target-hubspot-v4 --about
```

### Configure using environment variables

This Singer target will automatically import any environment variables within the working directory's
`.env` if the `--config=ENV` is provided, such that config values will be considered if a matching
environment variable is set either in the terminal context or in the `.env` file.

### Source Authentication and Authorization

- [ ] `Developer TODO:` If your target requires special access on the source system, or any special authentication requirements, provide those here.

## Features
### Full API Paths (Advanced Usage)

For any HubSpot API endpoint not covered by standard CRM objects, use the **full API path** (including query parameters) as the stream name.

**How it works:**
- If your stream name contains `/`, it's treated as a full API path
- The target constructs the URL as: `https://api.hubapi.com{stream_name}`
- Your record data is sent directly without modification

**Example: Unsubscribe from All Email**

```json
{"type": "SCHEMA", "stream": "/communication-preferences/v4/statuses/batch/unsubscribe-all?channel=EMAIL", "schema": {"type": "object", "properties": {"inputs": {"type": "array", "items": {"type": "string"}}}}, "key_properties": []}
{"type": "RECORD", "stream": "/communication-preferences/v4/statuses/batch/unsubscribe-all?channel=EMAIL", "record": {"inputs": ["test1@hubspot.com", "test2@hubspot.com", "test3@hubspot.com"]}}
{"type": "STATE", "value": {}}
```

This flexibility allows you to call **any HubSpot API endpoint** by simply using its path as the stream name.

## Usage
You can easily run `target-hubspot-v4` by itself.

### Executing the Target Directly

```bash
target-hubspot-v4 --version
target-hubspot-v4 --help
# Test using the "Carbon Intensity" sample:
tap-carbon-intensity | target-hubspot-v4 --config /path/to/target-hubspot-v4-config.json
```

### Initialize your Development Environment

```bash
pipx install poetry
poetry install
```

### Create and Run Tests

Create tests within the `target_hubspot_v4/tests` subfolder and
  then run:

```bash
poetry run pytest
```

You can also test the `target-hubspot-v4` CLI interface directly using `poetry run`:

```bash
poetry run target-hubspot-v4 --help
```

### SDK Dev Guide

See the [dev guide](https://sdk.meltano.com/en/latest/dev_guide.html) for more instructions on how to use the SDK to
develop your own Singer taps and targets.
