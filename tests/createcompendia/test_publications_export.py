"""Network checks that the pubmed2db export pinned in config.yaml still matches what Babel expects.

They are cheap (a directory listing, the validation report, a HEAD per shard): the pinned export should be
reachable, its report should be in the structure ``expected_shard_counts()`` reads, every shard it lists
should exist -- the contract in docs/sources/PubMed/README.md -- and it should still be the newest export,
which is the reminder to move config.yaml on when pubmed2db publishes another. The full download and parse
is ``tests/pipeline/test_publications.py``.

Run with:
    uv run pytest tests/createcompendia/test_publications_export.py --network -v
"""

import gzip
import re
from datetime import datetime

import pytest
import requests

from src.createcompendia import publications
from src.util import get_config

CONFIG = get_config()
BASE_URL = CONFIG["pubmed2db_base_url"]
VERSION = CONFIG["pubmed2db_version"]
EXPORT_URL = BASE_URL + VERSION + "/"


@pytest.fixture(scope="module")
def pinned_report_dir(tmp_path_factory):
    """Download only the pinned export's validation report, into a directory laid out like the pipeline's."""
    report_dir = tmp_path_factory.mktemp("pubmed2db-report")
    response = requests.get(EXPORT_URL + CONFIG["pubmed2db_validation_report"], timeout=60)
    response.raise_for_status()
    content = response.content
    if CONFIG["pubmed2db_validation_report"].endswith(".gz"):
        content = gzip.decompress(content)
    (report_dir / publications.VALIDATION_REPORT).write_bytes(content)
    return str(report_dir)


@pytest.mark.network
def test_pinned_export_report_lists_shards_that_exist(pinned_report_dir):
    """The pinned export's validation report should parse, and every shard it lists should exist on the
    server and be non-empty -- i.e. download_pubmed2db() would find everything it is told to fetch."""
    shard_counts = publications.expected_shard_counts(pinned_report_dir)
    for shard in shard_counts:
        response = requests.head(EXPORT_URL + shard, timeout=60)
        assert response.status_code == 200, f"{EXPORT_URL + shard} returned {response.status_code}"
        assert int(response.headers["Content-Length"]) > 0


@pytest.mark.network
def test_pinned_export_is_the_newest():
    """config.yaml should pin the newest pubmed2db export. When this fails, pubmed2db has published a newer
    one: move pubmed2db_version (and check pubmed2db_validation_report) to it, per docs/sources/PubMed/README.md.
    """
    response = requests.get(BASE_URL, timeout=60)
    response.raise_for_status()
    # Export directories are named like 2026sep26; manifests/ and anything else don't match.
    versions = set(re.findall(r'href="(\d{4}[a-z]{3}\d{1,2})/"', response.text))
    assert VERSION in versions, f"{VERSION} is not listed at {BASE_URL}: {sorted(versions)}"
    newest = max(versions, key=lambda version: datetime.strptime(version, "%Y%b%d"))
    assert VERSION == newest, f"config.yaml pins pubmed2db {VERSION}, but {BASE_URL} has {newest}."
