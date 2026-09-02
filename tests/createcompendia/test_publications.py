"""Unit tests for building the Publication compendium from a pubmed2db NDJSON export.

The fixture in ``pubmed2db/`` is a three-shard miniature of a real export. Every record in shards
00000 and 00001 is copied verbatim from ``pubmed_metadata_00000.ndjson.gz`` of the 2026aug5 export
(https://stars.renci.org/var/babel_outputs/pubmed2db/2026aug5/):

- ``PMID:1`` — a DOI, the first record of the export.
- ``PMID:113`` — a PubMed Central id, spelled ``PMC:`` as that export does.
- ``PMID:27766828`` — no DOI or PMCID; a title containing a literal newline.
- ``PMID:9959305`` — an empty ``article_title``.
- ``PMID:4585992`` (shard 00000) and ``PMID:4576000`` (shard 00001) — share
  ``doi:10.1288/00005537-197309000-00014``; the higher PMID is deliberately in the shard parsed first.
- ``PMID:7935102`` — a DOI.

Shard 00002 holds one **synthetic** record, ``PMID:999999113``: ``PMID:113`` with its identifier
respelled ``PMCID:PMC1666769``, the form pubmed2db exports made after 2026aug5 use
(https://github.com/NCATSTranslator/Babel/issues/1044). ``validation_report.json.gz`` is cut down to
the ``records-present`` check the code reads.
"""

import gzip
import json
import os
import shutil

import pytest

import src.util
from src.categories import JOURNAL_ARTICLE, PUBLICATION
from src.createcompendia import publications
from src.util import ensure_parent_dir
from tests.conftest import assert_concordance_file_valid, assert_ids_file_valid, read_tsv

pytestmark = pytest.mark.unit

FIXTURE_DIR = os.path.join(os.path.dirname(__file__), "pubmed2db")
EXPORT_URL = "https://stars.renci.org/var/babel_outputs/pubmed2db/2026aug5/"
SHARED_DOI = "doi:10.1288/00005537-197309000-00014"


@pytest.fixture
def parsed(tmp_path):
    """Run parse_pubmed2db_into_tsvs() over the fixture export and return the output paths."""
    out = {
        "titles_file": str(tmp_path / "titles.tsv"),
        "pmid_id_file": str(tmp_path / "ids" / "PMID"),
        "concord_file": str(tmp_path / "concords" / "PMID_DOI"),
        "shared_ids_file": str(tmp_path / "concords" / "shared_identifiers.tsv"),
        "metadata_yaml": str(tmp_path / "concords" / "metadata.yaml"),
    }
    publications.parse_pubmed2db_into_tsvs(FIXTURE_DIR, **out, url=EXPORT_URL, workers=2)
    return out


# PARSING


def test_ids_titles_and_concords_keep_the_old_tsv_formats(parsed):
    """Every record should yield a JournalArticle id row, a concord row per DOI/PMC id with the bare `eq`
    relation, and a title row unless the title is empty; newlines in titles are escaped as `\\n`."""
    ids = assert_ids_file_valid(parsed["pmid_id_file"])
    assert [row[1] for row in ids] == [JOURNAL_ARTICLE] * 8
    assert [row[0] for row in ids][:2] == ["PMID:1", "PMID:113"]

    concords = assert_concordance_file_valid(parsed["concord_file"])
    assert {relation for _, relation, _ in concords} == {"eq"}
    assert ["PMID:1", "eq", "doi:10.1016/0006-2944(75)90147-7"] in concords
    assert ["PMID:113", "eq", "PMC:PMC1666769"] in concords
    assert ["PMID:999999113", "eq", "PMCID:PMC1666769"] in concords
    assert not any(pmid == "PMID:27766828" for pmid, _, _ in concords)

    titles = dict(read_tsv(parsed["titles_file"]))
    assert "PMID:9959305" not in titles, "an empty title should not be written"
    assert "\n" not in titles["PMID:27766828"] and "\\n" in titles["PMID:27766828"]
    assert titles["PMID:1"] == "Formate assay in body fluids: application in methanol poisoning."


def test_shared_identifier_is_assigned_to_the_lowest_pmid(parsed):
    """A DOI carried by two PMIDs should be listed once in shared_identifiers.tsv with the lowest PMID as
    its winner, even though the higher PMID's shard was parsed first."""
    assert read_tsv(parsed["shared_ids_file"]) == [[SHARED_DOI, "PMID:4576000", "2"]]


def test_metadata_records_the_export_url_and_counts(parsed):
    """The metadata YAML should name the pubmed2db URL as its source and carry the record counts."""
    import yaml

    with open(parsed["metadata_yaml"]) as f:
        metadata = yaml.safe_load(f)
    assert metadata["sources"][0]["url"] == EXPORT_URL
    assert metadata["counts"]["pmid_count"] == 8
    assert metadata["counts"]["shard_count"] == 3
    assert metadata["counts"]["shared_identifier_count"] == 1
    assert metadata["counts"]["concords"]["count_concords"] == 7


def test_record_count_mismatch_with_validation_report_raises(tmp_path):
    """An export whose shards hold fewer records than its validation report says should be refused — that
    is the only signal of a truncated or missing shard."""
    export = tmp_path / "export"
    shutil.copytree(FIXTURE_DIR, export)
    os.remove(export / "pubmed_metadata_00001.ndjson.gz")
    with pytest.raises(RuntimeError, match="validation report says 8"):
        publications.parse_pubmed2db_into_tsvs(
            str(export),
            str(tmp_path / "titles.tsv"),
            str(tmp_path / "PMID"),
            str(tmp_path / "PMID_DOI"),
            str(tmp_path / "shared.tsv"),
            str(tmp_path / "metadata.yaml"),
            EXPORT_URL,
            workers=1,
        )


def test_failed_validation_report_raises(tmp_path):
    """An export whose own validator reported `fail` should be refused outright."""
    export = tmp_path / "export"
    shutil.copytree(FIXTURE_DIR, export)
    with gzip.open(export / "validation_report.json.gz", "wt") as f:
        json.dump({"status": "fail", "errors": ["boom"], "checks_run": []}, f)
    with pytest.raises(RuntimeError, match="status 'fail'"):
        publications.expected_record_count(str(export))


# BUILDING THE COMPENDIUM


@pytest.fixture
def babel_config(tmp_path):
    """Point Babel's download and output directories at a temporary tree, with empty common-label files so
    write_compendium()'s factories need no UberGraph download."""
    config = dict(src.util.get_config())
    config["download_directory"] = str(tmp_path / "babel_downloads")
    config["output_directory"] = str(tmp_path / "babel_outputs")
    for common_files in config["common"].values():
        for common_file in common_files:
            path = os.path.join(config["download_directory"], "common", common_file)
            ensure_parent_dir(path)
            open(path, "w").close()
    original = src.util.config_yaml
    src.util.config_yaml = config
    yield config
    src.util.config_yaml = original


def test_compendium_streams_one_clique_per_record(parsed, babel_config, tmp_path):
    """generate_compendium() should write one Publication clique per record, with the title as the PMID's
    label, both PMC spellings kept alive, and a shared DOI present only in its winning clique."""
    icrdf = tmp_path / "icRDF.tsv"
    icrdf.write_text("")
    compendium = os.path.join(babel_config["output_directory"], "compendia", "Publication.txt")

    publications.generate_compendium(
        FIXTURE_DIR, parsed["shared_ids_file"], [parsed["metadata_yaml"]], compendium, str(icrdf), workers=2
    )

    with open(compendium) as f:
        cliques = [json.loads(line) for line in f]
    by_pmid = {clique["identifiers"][0]["i"]: clique for clique in cliques}
    assert len(cliques) == 8 and len(by_pmid) == 8
    assert {clique["type"] for clique in cliques} == {PUBLICATION}

    def curies(pmid):
        return [identifier["i"] for identifier in by_pmid[pmid]["identifiers"]]

    assert curies("PMID:1") == ["PMID:1", "doi:10.1016/0006-2944(75)90147-7"]
    assert (
        by_pmid["PMID:1"]["identifiers"][0]["l"] == "Formate assay in body fluids: application in methanol poisoning."
    )
    assert curies("PMID:27766828") == ["PMID:27766828"]
    # write_compendium() writes an unlabeled identifier with an empty "l" (pre-existing behaviour).
    assert by_pmid["PMID:9959305"]["identifiers"][0].get("l", "") == ""

    # PMC: is registered on biolink:Publication; PMCID: survives only via extra_prefixes (see #1044).
    assert curies("PMID:113") == ["PMID:113", "PMC:PMC1666769"]
    assert curies("PMID:999999113") == ["PMID:999999113", "PMCID:PMC1666769"]

    # The shared DOI lands on the lowest PMID only, so no identifier appears in two cliques.
    assert curies("PMID:4576000") == ["PMID:4576000", SHARED_DOI]
    assert curies("PMID:4585992") == ["PMID:4585992"]
    all_curies = [identifier["i"] for clique in cliques for identifier in clique["identifiers"]]
    assert len(all_curies) == len(set(all_curies))
