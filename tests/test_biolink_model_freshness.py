"""Network test: keep the local Biolink Model fixture in sync with config.yaml.

The `tests/compendium/` package patches `bmt.Toolkit` to read the pinned
local files in `tests/fixtures/biolink-model/` instead of fetching them
from GitHub. That fixture must match the `biolink_version` declared in
`config.yaml`, otherwise the offline tests will be running against a
different version of the model than production.

This test fails loudly when the two diverge. To fix:

    uv run python tests/fixtures/biolink-model/refresh.py

then commit the updated fixture files alongside the `config.yaml` change.
"""

import hashlib
import urllib.request
from pathlib import Path

import pytest
import yaml

REPO_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = REPO_ROOT / "config.yaml"
FIXTURE_DIR = REPO_ROOT / "tests" / "fixtures" / "biolink-model"
VERSION_FILE = FIXTURE_DIR / "VERSION"

REMOTE_FILES = {
    "biolink-model.yaml": "{base}/biolink-model.yaml",
    "attributes.yaml": "{base}/attributes.yaml",
    "predicate_mapping.yaml": "{base}/predicate_mapping.yaml",
    "biolink_model_prefix_map.json": "{base}/project/prefixmap/biolink_model_prefix_map.json",
}

REFRESH_HINT = (
    "Run `uv run python tests/fixtures/biolink-model/refresh.py` and commit "
    "the updated fixture files."
)


def _config_version() -> str:
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)["biolink_version"]


def _fixture_version() -> str:
    return VERSION_FILE.read_text().strip()


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


@pytest.mark.unit
def test_biolink_fixture_version_matches_config():
    """The pinned VERSION file must equal `biolink_version` in config.yaml."""
    config_version = _config_version()
    fixture_version = _fixture_version()
    assert fixture_version == config_version, (
        f"Biolink Model fixture version ({fixture_version}) does not match "
        f"config.yaml biolink_version ({config_version}). {REFRESH_HINT}"
    )


@pytest.mark.network
@pytest.mark.parametrize("filename, url_template", list(REMOTE_FILES.items()))
def test_biolink_fixture_matches_upstream(filename, url_template):
    """Each pinned fixture file must byte-match the upstream file at its tag."""
    version = _fixture_version()
    url = url_template.format(base=f"https://raw.githubusercontent.com/biolink/biolink-model/v{version}")
    with urllib.request.urlopen(url) as resp:
        upstream = resp.read()
    local = (FIXTURE_DIR / filename).read_bytes()
    assert _sha256(local) == _sha256(upstream), (
        f"Local fixture {filename} (sha256 {_sha256(local)[:12]}…) does not match "
        f"{url} (sha256 {_sha256(upstream)[:12]}…). {REFRESH_HINT}"
    )
