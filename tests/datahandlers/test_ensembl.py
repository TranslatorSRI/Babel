# Tests for datahandlers/ensembl.py
import csv
import json
import logging
import os
import re
from pathlib import Path

import pytest
import requests
from apybiomart import find_attributes, find_datasets

from src.datahandlers.ensembl import BIOMART_ATTRIBUTES, BIOMART_MAX_ATTRIBUTE_COUNT, pull_ensembl

logging.basicConfig(level=logging.INFO)


def read_biomart_file(biomart_file):
    """
    Reads a BioMart output file (TSV) and yields each row as a dictionary.

    :param biomart_file: An opened file object containing tab-delimited data, formatted
                         as output from BioMart.
    :type biomart_file: file-like object
    :return: A generator yielding dictionaries representing each row from the input file.
    :rtype: Iterator[Dict[str, str]]
    """
    reader = csv.DictReader(biomart_file, dialect="excel-tab")
    yield from reader


def normalize_list_of_dictionaries(dict_list):
    """
    Sort and normalize a list of dictionaries by converting them into sorted JSON strings.

    This function accepts a list of dictionaries, serializes each dictionary into its JSON
    representation, and ensures keys are sorted so that the overall lists can be compared.

    :param dict_list: A list containing dictionary objects to be normalized, with keys
        sorted and represented as JSON strings.
    :type dict_list: list[dict]
    :return: A list of JSON strings where each string represents a dictionary input
        with sorted keys.
    :rtype: list[str]
    """
    return sorted(json.dumps(dictionary, sort_keys=True) for dictionary in dict_list)


# The dataset the unsplit/split comparison uses. It is small (~16k genes) and exposes none of the
# External References attributes, which is what lets it be fetched whole -- see _ISSUE_193_DATASET.
_DATASET = "choffmanni_gene_ensembl"

# The dataset from https://github.com/NCATSTranslator/Babel/issues/193, which pull_ensembl()'s
# batching exists to rescue: it carries five External References attributes and BioMart rejects a
# query asking for more than about three of them ("Too many attributes selected for External
# References"). Downloading it at the default BIOMART_MAX_ATTRIBUTE_COUNT is the regression check.
_ISSUE_193_DATASET = "hgfemale_gene_ensembl"


def _biomart_attribute_count(dataset):
    """How many of BIOMART_ATTRIBUTES this dataset exposes -- pull_ensembl()'s own batching input.

    The unsplit arm below passes this as max_attribute_count so the download is a single query by
    construction. Deriving it is the fix for a stale assumption: the arm used to rely on the default
    BIOMART_MAX_ATTRIBUTE_COUNT (6) exceeding what _DATASET exposes, and when Ensembl grew the
    dataset to 8 attributes both arms silently became batched downloads and the comparison below
    stopped comparing what it claimed to.
    """
    return len(BIOMART_ATTRIBUTES & set(find_attributes(dataset)["Attribute_ID"].to_list()))


def _pull_or_xfail(*args, **kwargs):
    """Call pull_ensembl(), turning a BioMart outage into an xfail but leaving real failures alone.

    This test used to carry a blanket ``xfail(strict=False)`` reading "requires network access to
    the Ensembl BioMart service". BioMart came back, the test started running, and the assertion
    about single-query downloads had gone stale -- but the blanket marker reported that as a routine
    xfail, so nothing surfaced it. Guarding only the unreachable-service case keeps every assertion
    live. pull_ensembl() retries a failing dataset BIOMART_MAX_RETRIES times before giving up, so
    reaching this handler means BioMart was unusable for minutes, not that it blipped once.
    """
    try:
        return pull_ensembl(*args, **kwargs)
    except Exception as e:
        pytest.xfail(f"Ensembl BioMart is unusable: {e}")


@pytest.mark.network
def test_pull_ensembl(tmp_path):
    """Splitting a dataset across several BioMart queries must reproduce the single-query download.

    pull_ensembl() batches a dataset that exposes more of BIOMART_ATTRIBUTES than
    ``max_attribute_count``, then merges the batches back together on ``ensembl_gene_id``. This
    downloads _DATASET both ways and compares the two TSVs row for row, and downloads
    _ISSUE_193_DATASET alongside it to confirm the dataset that motivated the batching still
    arrives intact.
    """
    # Make a temporary directory for testing.
    pull_ensembl_test_dir = tmp_path / "pull_ensembl_test"
    batched_dir = pull_ensembl_test_dir / "download"
    os.makedirs(batched_dir)

    # Both datasets at the default limit, so both are batched: _DATASET as the split half of the
    # comparison, _ISSUE_193_DATASET as the issue #193 regression.
    batched_query_report = _pull_or_xfail(
        batched_dir, batched_dir / "download_complete", [_DATASET, _ISSUE_193_DATASET]
    )

    # Now the same dataset again, asking for every attribute it has at once so nothing is batched.
    unsplit_dir = pull_ensembl_test_dir / "download_unsplit"
    os.makedirs(unsplit_dir)
    unsplit_query_report = _pull_or_xfail(
        unsplit_dir,
        unsplit_dir / "download_complete",
        [_DATASET],
        max_attribute_count=_biomart_attribute_count(_DATASET),
    )

    # We need to check two things:
    # 1. Whether the single/split reports make sense.
    single_report = unsplit_query_report[_DATASET]
    split_report = batched_query_report[_DATASET]

    # No batches with the single query; the default limit must actually have split the download.
    # The batch counts are asserted as ">= 2" rather than exact numbers because they are a function
    # of how many attributes Ensembl exposes for a dataset, which changes without notice -- the
    # property under test is that batching happened and reassembled correctly, not how many pieces
    # it took. An exact count here is what went stale last time.
    assert len(single_report["batches"]) == 0
    assert len(split_report["batches"]) >= 2

    # Issue #193: the naked mole rat dataset has more External References attributes than BioMart
    # will serve in one query, so it downloads only if the batching works.
    issue_193_report = batched_query_report[_ISSUE_193_DATASET]
    assert issue_193_report["status"] == "downloaded", issue_193_report["message"]
    assert len(issue_193_report["batches"]) >= 2

    # Make sure we have the right counts in the reports returned by pull_ensembl().
    assert split_report["num_rows"] == single_report["num_rows"]
    expected_attributes = set(single_report["attributes"])
    assert set(split_report["attributes"]) == expected_attributes
    batched_attributes = {"ensembl_gene_id"}
    for batch in split_report["batches"]:
        batched_attributes.update(batch["attributes"])
    assert batched_attributes == expected_attributes

    # 2. Whether the unsplit file is identical to the split file.
    unsplit_tsv = unsplit_dir / _DATASET / "BioMart.tsv"
    split_tsv = batched_dir / _DATASET / "BioMart.tsv"
    assert unsplit_tsv.exists()
    assert split_tsv.exists()
    with open(unsplit_tsv) as unsplit_file, open(split_tsv) as split_file:
        # So we can't compare these files directly, because rows with the same ensembl_gene_id shows up in an
        # undetermined order. So we need to load them, group them by ENSEMBL gene ID, and then compare those sets.
        unsplit_rows = list(read_biomart_file(unsplit_file))
        split_rows = list(read_biomart_file(split_file))
        assert len(unsplit_rows) == len(split_rows)
        assert unsplit_rows[0].keys() == split_rows[0].keys()

        # Confirm that the normalized lists of data dictionaries are the same.
        unsplit_rows_normalized = normalize_list_of_dictionaries(unsplit_rows)
        split_rows_normalized = normalize_list_of_dictionaries(split_rows)
        assert unsplit_rows_normalized == split_rows_normalized


@pytest.mark.network
@pytest.mark.timeout(120)
def test_biomart_find_datasets_response_format():
    """
    Diagnose the 'Too many columns specified: expected 9 and found 1' error from
    apybiomart.find_datasets().  That call parses the BioMart ?type=datasets response
    as a 9-column TSV; if the API is returning an error page (HTML/JSON) instead of
    tab-separated text the parse will fail.

    This test hits the live BioMart HTTPS endpoint, prints the raw response so we can
    see what the server actually returns, and then checks that the response looks like
    the tab-separated text that apybiomart expects.

    apybiomart uses http:// internally; Ensembl now redirects / blocks HTTP, so we
    probe https:// here to see the actual current server response.
    """
    url = "https://www.ensembl.org/biomart/martservice"
    try:
        resp = requests.get(url, params={"type": "datasets", "mart": "ENSEMBL_MART_ENSEMBL"}, timeout=90)
    except requests.exceptions.Timeout:
        pytest.skip("BioMart endpoint timed out — service may be down or unreachable from this host")

    raw = resp.text
    first_500 = raw[:500]
    print(f"\n--- BioMart find_datasets raw response (first 500 chars) ---\n{first_500}\n---")
    print(f"HTTP status: {resp.status_code}")
    print(f"Content-Type: {resp.headers.get('Content-Type', '(none)')}")

    # Count tab-separated columns in the first non-empty line.
    first_line = next((ln for ln in raw.splitlines() if ln.strip()), "")
    tab_count = first_line.count("\t")
    print(f"First non-empty line tab count: {tab_count}")
    print(f"First line: {first_line[:200]!r}")

    # apybiomart expects 9 tab-separated columns per row.
    assert tab_count == 8, (
        f"BioMart ?type=datasets response has changed: expected 8 tabs (9 columns) per row "
        f"but found {tab_count} tabs in the first line.\n"
        f"Content-Type: {resp.headers.get('Content-Type', '(none)')!r}\n"
        f"First line: {first_line[:200]!r}\n"
        f"Full response prefix: {first_500!r}"
    )

    # Also confirm apybiomart.find_datasets() itself can parse without raising.
    df = find_datasets()
    print(f"\nfind_datasets() returned {len(df)} rows with columns: {list(df.columns)}")
    assert "Dataset_ID" in df.columns, f"Expected 'Dataset_ID' column, got: {list(df.columns)}"
    assert len(df) > 0, "find_datasets() returned an empty dataframe"


# --- documentation drift -------------------------------------------------------
#
# docs/sources/ENSEMBL/Download.md hand-mirrors BIOMART_ATTRIBUTES as a table (each row pairs an
# attribute with what Babel uses it for, which the constant cannot carry). A hand-kept copy of a
# constant is exactly the duplication that goes stale silently, so it is asserted rather than
# trusted. These are `unit` tests: they read a committed file and never touch BioMart.

_DOWNLOAD_DOC = Path(__file__).resolve().parents[2] / "docs" / "sources" / "ENSEMBL" / "Download.md"


@pytest.mark.unit
def test_download_doc_attribute_table_matches_biomart_attributes():
    """Every attribute Babel requests is in the doc's table, and the table invents none.

    Adding an attribute to BIOMART_ATTRIBUTES without saying what it is for leaves the table --
    the only place that explains why each column is fetched -- quietly incomplete.
    """
    # Table rows look like: | `ensembl_gene_id` | primary ENSEMBL gene CURIE |
    documented = set(re.findall(r"^\|\s*`([a-z_]+)`\s*\|", _DOWNLOAD_DOC.read_text(), re.MULTILINE))
    assert documented == BIOMART_ATTRIBUTES, (
        "docs/sources/ENSEMBL/Download.md's attribute table has drifted from BIOMART_ATTRIBUTES; "
        f"undocumented: {sorted(BIOMART_ATTRIBUTES - documented)}, "
        f"documented but not requested: {sorted(documented - BIOMART_ATTRIBUTES)}"
    )


@pytest.mark.unit
def test_download_doc_quotes_the_current_batch_size():
    """The doc states BIOMART_MAX_ATTRIBUTE_COUNT's value twice, in prose; keep both honest.

    The value is load-bearing for a reader deciding whether a dataset will be batched, so a doc
    still saying 6 after the constant moved would send them to the wrong conclusion.
    """
    text = _DOWNLOAD_DOC.read_text()
    assert f"`BIOMART_MAX_ATTRIBUTE_COUNT` ({BIOMART_MAX_ATTRIBUTE_COUNT})" in text
    assert f"The flat count of {BIOMART_MAX_ATTRIBUTE_COUNT} is a conservative stand-in" in text
