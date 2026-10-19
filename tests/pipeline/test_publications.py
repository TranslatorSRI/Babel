"""Download the whole pubmed2db export pinned in config.yaml and parse it.

The export (~17 GB) is cached in babel_downloads/PubMed2DB/<version>/, like the pipeline's own download.
``parse_pubmed2db_into_tsvs()`` checks every shard against the record count in its validation report; the
test then asserts that every row of the real titles.tsv has exactly two fields, which is what
``escape_tsv_field()`` is for. The cheap checks of the same export are
``tests/createcompendia/test_publications_export.py``.

Run with:
    uv run pytest tests/pipeline/test_publications.py --pipeline --network --no-cov -v
"""

import os
import shutil

import pytest

from src.createcompendia import publications
from src.util import get_config

CONFIG = get_config()
VERSION = CONFIG["pubmed2db_version"]
EXPORT_URL = CONFIG["pubmed2db_base_url"] + VERSION + "/"


@pytest.mark.pipeline
@pytest.mark.network
# `slow` for its hour-long timeout: a cold download of ~17 GB plus the parse can outrun `pipeline`'s 15 min.
@pytest.mark.slow
@pytest.mark.skipif(shutil.which("wget") is None, reason="wget is not installed")
def test_full_export_downloads_and_parses(tmp_path):
    """The whole pinned export should download and parse with every shard matching its record count, and
    every row of the resulting titles.tsv should have exactly two fields."""
    download_dir = os.path.join(CONFIG["download_directory"], "PubMed2DB", VERSION)
    done_file = os.path.join(download_dir, "downloaded")
    if not os.path.exists(done_file):
        publications.download_pubmed2db(EXPORT_URL, CONFIG["pubmed2db_validation_report"], download_dir, done_file)

    titles_file = tmp_path / "titles.tsv"
    publications.parse_pubmed2db_into_tsvs(
        download_dir,
        str(titles_file),
        str(tmp_path / "PMID"),
        str(tmp_path / "PMID_DOI"),
        str(tmp_path / "shared_identifiers.tsv"),
        str(tmp_path / "metadata.yaml"),
        EXPORT_URL,
        workers=os.cpu_count(),
    )

    with open(titles_file) as f:
        bad_rows = [line for line in f if line.count("\t") != 1]
    assert bad_rows == []
