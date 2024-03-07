# target-hubspot-v4

`target-hubspot-v4` is a Singer target for Hubspot-v4.


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
