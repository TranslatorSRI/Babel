"""Fixtures for near-end-to-end `write_compendium()` regression tests.

The tests in this package drive `write_compendium()` against curated cliques
and assert on the resulting JSONL `preferred_name` (and, in the future, other
output fields). Inputs live in checked-in files under
`tests/fixtures/compendium/babel_downloads/`; outputs land in `tmp_path`.

Two pieces of pipeline state are redirected:

1. `src.util.get_biolink_model_toolkit` and `src.util.get_biolink_prefix_map`
   are patched session-wide to read the pinned local copies in
   `tests/fixtures/biolink-model/`. This keeps the tests offline (`unit`).
   The companion `tests/test_biolink_model_freshness.py` test ensures the
   pinned copies stay in sync with `config.yaml`'s `biolink_version`.
2. `src.util.config_yaml` is patched per-test so factories see the
   fixture `download_directory` and a `tmp_path` `output_directory`.
"""

import copy
import dataclasses
import json
from pathlib import Path

import curies
import pytest
from bmt import Toolkit

import src.util as util_module

REPO_ROOT = Path(__file__).resolve().parents[2]
FIXTURES_DIR = REPO_ROOT / "tests" / "fixtures"
BIOLINK_FIXTURE_DIR = FIXTURES_DIR / "biolink-model"
COMPENDIUM_FIXTURE_DIR = FIXTURES_DIR / "compendium"
LOCAL_DOWNLOAD_DIR = COMPENDIUM_FIXTURE_DIR / "babel_downloads"
LOCAL_ICRDF = COMPENDIUM_FIXTURE_DIR / "icRDF.tsv"
LOCAL_BIOLINK_MODEL = BIOLINK_FIXTURE_DIR / "biolink-model.yaml"
LOCAL_BIOLINK_PREFIX_MAP = BIOLINK_FIXTURE_DIR / "biolink_model_prefix_map.json"


@pytest.fixture(scope="session", autouse=True)
def local_biolink_toolkit():
    """Redirect bmt + curies lookups to the pinned local Biolink Model files.

    Without this, `NodeFactory.__init__` and `InformationContentFactory.__init__`
    would each fetch from `raw.githubusercontent.com`, which would make every
    test in this package a `network` test. The local copies are refreshed via
    `tests/fixtures/biolink-model/refresh.py`; the freshness check lives in
    `tests/test_biolink_model_freshness.py` (marked `network`).
    """
    mp = pytest.MonkeyPatch()

    def _local_toolkit(_biolink_version):
        return Toolkit(str(LOCAL_BIOLINK_MODEL))

    def _local_prefix_map():
        with open(LOCAL_BIOLINK_PREFIX_MAP) as f:
            data = json.load(f)
        return curies.Converter.from_prefix_map(data)

    mp.setattr(util_module, "get_biolink_model_toolkit", _local_toolkit)
    mp.setattr(util_module, "get_biolink_prefix_map", _local_prefix_map)
    # Also patch the names that babel_utils / node imported at module load time.
    import src.babel_utils as babel_utils_module
    import src.node as node_module
    if hasattr(node_module, "get_biolink_model_toolkit"):
        mp.setattr(node_module, "get_biolink_model_toolkit", _local_toolkit)
    if hasattr(node_module, "get_biolink_prefix_map"):
        mp.setattr(node_module, "get_biolink_prefix_map", _local_prefix_map)
    if hasattr(babel_utils_module, "get_biolink_model_toolkit"):
        mp.setattr(babel_utils_module, "get_biolink_model_toolkit", _local_toolkit)

    yield
    mp.undo()


@dataclasses.dataclass
class CompendiumTestEnv:
    """Paths and helpers for a single near-end-to-end compendium test."""

    output_dir: Path
    icrdf_path: Path

    def compendium_path(self, ofname: str) -> Path:
        return self.output_dir / "compendia" / ofname

    def synonyms_path(self, ofname: str) -> Path:
        return self.output_dir / "synonyms" / ofname

    def read_records(self, ofname: str) -> list[dict]:
        """Return all JSONL records from the compendium output file."""
        with open(self.compendium_path(ofname)) as f:
            return [json.loads(line) for line in f if line.strip()]


@pytest.fixture
def babel_test_env(tmp_path, monkeypatch):
    """Configure the runtime so factories read the checked-in fixtures.

    - `download_directory` -> `tests/fixtures/compendium/babel_downloads/`
      (the checked-in input tree)
    - `output_directory`   -> `tmp_path` (test-local, discarded after the run)
    - `icrdf_filename`     -> `tests/fixtures/compendium/icRDF.tsv`
      (returned on the env so each test can pass it to `write_compendium`)

    The fixture does not create or modify any input files. To add a new
    regression case, append rows to the appropriate
    `tests/fixtures/compendium/babel_downloads/{PREFIX}/labels` file.
    """
    real_config = util_module.get_config()
    test_config = copy.deepcopy(real_config)
    test_config["download_directory"] = str(LOCAL_DOWNLOAD_DIR)
    test_config["output_directory"] = str(tmp_path)
    test_config["intermediate_directory"] = str(tmp_path / "intermediate")
    test_config["tmp_directory"] = str(tmp_path / "tmp")

    monkeypatch.setattr(util_module, "config_yaml", test_config)

    (tmp_path / "compendia").mkdir(exist_ok=True)
    (tmp_path / "synonyms").mkdir(exist_ok=True)

    return CompendiumTestEnv(output_dir=tmp_path, icrdf_path=LOCAL_ICRDF)


def assert_preferred_name(record: dict, expected: str) -> None:
    """Focused assertion for the `preferred_name` field of a compendium record.

    Sibling helpers (e.g. `assert_first_identifier`, `assert_descriptions`,
    `assert_taxa`, `assert_clique_membership`) can be added here as new
    fields come under regression testing.
    """
    assert record.get("preferred_name") == expected, (
        f"preferred_name mismatch:\n  expected: {expected!r}\n  got:      {record.get('preferred_name')!r}\n"
        f"  full record: {record}"
    )
