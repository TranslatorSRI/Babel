"""Unit tests for building the Publication compendium from a pubmed2db NDJSON export.

The fixture in ``pubmed2db/`` is a three-shard miniature of a real export, laid out the way Babel's
download leaves one (shards plus ``validation_report.json``). Every record in shards 0 and 1 is copied
verbatim from ``pubmed_metadata_0.ndjson.gz`` of the 2026sep26 export
(https://stars.renci.org/var/babel_outputs/pubmed2db/2026sep26/):

- ``PMID:5202740`` — the first record of the export; a PubMed Central id, spelled ``PMCID:``.
- ``PMID:30803192`` — a DOI and a PMCID; a title containing a newline.
- ``PMID:17072568`` — a DOI and an empty ``article_title``.
- ``PMID:7754983`` (shard 0) and ``PMID:7754980`` (shard 1) — share ``doi:10.1093/ajcn/61.6.1324S``; the
  higher PMID is deliberately in the shard parsed first.
- ``PMID:26225670`` — a title containing a backslash.
- ``PMID:9245294`` — a title with trailing whitespace.
- ``PMID:25303692`` — no DOI or PMCID; a title containing ``\x85``, a C1 control character (cp1252's
  ellipsis, mis-decoded).
- ``PMID:5202677`` — no DOI or PMCID.

Shard 2 holds one **synthetic** record, ``PMID:999999113``: ``PMID:113`` copied verbatim from
``pubmed_metadata_00000.ndjson.gz`` of the 2026aug5 export, which spelled its PubMed Central id
``PMC:PMC1666769`` (https://github.com/NCATSTranslator/Babel/issues/1044), with only its PMID changed (in
``id`` and ``identifiers[0]``). ``validation_report.json`` is cut down to the fields the code reads, in the
2026sep26 report's structure.
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
from tests.conftest import assert_concordance_file_valid, assert_ids_file_valid

pytestmark = pytest.mark.unit

FIXTURE_DIR = os.path.join(os.path.dirname(__file__), "pubmed2db")
EXPORT_URL = "https://stars.renci.org/var/babel_outputs/pubmed2db/2026sep26/"
SHARED_DOI = "doi:10.1093/ajcn/61.6.1324S"
RECORD_COUNT = 10


def copy_fixture(tmp_path):
    """A writable copy of the fixture export, for tests that damage it."""
    export = tmp_path / "export"
    shutil.copytree(FIXTURE_DIR, export)
    return export


def edit_report(report_file, edit):
    """Apply ``edit`` to a parsed validation report and write it back."""
    report = json.loads(report_file.read_text())
    edit(report)
    report_file.write_text(json.dumps(report))


def read_titles(titles_file):
    """titles.tsv as ``{pmid: escaped title}``, keeping each title's whitespace (read_tsv() strips it)."""
    with open(titles_file) as f:
        rows = [line.removesuffix("\n").split("\t") for line in f]
    assert all(len(row) == 2 for row in rows), "every titles.tsv row should have exactly two fields"
    return dict(rows)


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


def parse_into(tmp_path, export):
    """Run parse_pubmed2db_into_tsvs() over ``export`` with throwaway outputs."""
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


# PARSING


def test_ids_titles_and_concords(parsed):
    """Every record should yield a JournalArticle id row, a concord row per DOI/PMC id with the bare `eq`
    relation, and a title row unless the title is empty, in shard order."""
    ids = assert_ids_file_valid(parsed["pmid_id_file"])
    assert [row[1] for row in ids] == [JOURNAL_ARTICLE] * RECORD_COUNT
    assert [row[0] for row in ids][:2] == ["PMID:5202740", "PMID:7754983"]
    assert ids[-1][0] == "PMID:999999113"

    concords = assert_concordance_file_valid(parsed["concord_file"])
    assert {relation for _, relation, _ in concords} == {"eq"}
    assert ["PMID:5202740", "eq", "PMCID:PMC1712714"] in concords
    assert ["PMID:30803192", "eq", "doi:10.31557/APJCP.2019.20.2.345"] in concords
    assert ["PMID:999999113", "eq", "PMC:PMC1666769"] in concords
    assert not any(pmid == "PMID:5202677" for pmid, _, _ in concords)

    titles = read_titles(parsed["titles_file"])
    assert "PMID:17072568" not in titles, "an empty title should not be written"
    assert titles["PMID:5202740"] == "Renal tubular acidosis: an immunopathological study on four patients."


def test_titles_are_escaped_losslessly(parsed):
    """Newlines and backslashes in titles should be escaped in titles.tsv, so that every row is exactly
    `PMID<TAB>title`; other characters, including surrounding whitespace, should be kept as they are."""
    titles = read_titles(parsed["titles_file"])
    assert "Genotypes and\\nHaplotype" in titles["PMID:30803192"]
    assert "for \\\\Preoperative" in titles["PMID:26225670"]
    assert titles["PMID:9245294"] == "Identity as adaptation to social, cultural, and historical context "
    assert titles["PMID:25303692"] == "New challenges\x85."


@pytest.mark.parametrize(
    "title,escaped",
    [
        ("plain", "plain"),
        ("a\nb", "a\\nb"),
        ("a\tb\rc", "a\\tb\\rc"),
        # The backslash is escaped first, so a literal backslash-n stays distinguishable from a newline.
        ("a\\nb", "a\\\\nb"),
    ],
)
def test_escape_tsv_field(title, escaped):
    """escape_tsv_field() should escape backslash, tab, newline and carriage return, and nothing else."""
    assert publications.escape_tsv_field(title) == escaped


def test_shared_identifier_is_assigned_to_the_lowest_pmid(parsed):
    """A DOI carried by two PMIDs should be listed once in shared_identifiers.tsv with the lowest PMID as
    its winner, even though the higher PMID's shard was parsed first."""
    with open(parsed["shared_ids_file"]) as f:
        assert f.read() == f"{SHARED_DOI}\tPMID:7754980\t2\n"


def test_metadata_records_the_export_url_and_counts(parsed):
    """The metadata YAML should name the pubmed2db URL as its source and carry the record counts."""
    import yaml

    with open(parsed["metadata_yaml"]) as f:
        metadata = yaml.safe_load(f)
    assert metadata["sources"][0]["url"] == EXPORT_URL
    assert metadata["counts"]["pmid_count"] == RECORD_COUNT
    assert metadata["counts"]["shard_count"] == 3
    assert metadata["counts"]["shared_identifier_count"] == 1


def test_record_without_identifiers_raises_naming_the_shard(tmp_path):
    """A record whose `identifiers` list is empty should raise a ValueError naming its shard and PMID, not a
    bare IndexError."""
    export = copy_fixture(tmp_path)
    shard = export / "pubmed_metadata_2.ndjson.gz"
    with gzip.open(shard, "rt") as f:
        record = json.loads(f.readline())
    record["identifiers"] = []
    with gzip.open(shard, "wt") as f:
        f.write(json.dumps(record) + "\n")
    with pytest.raises(ValueError, match=r"pubmed_metadata_2\.ndjson\.gz: record PMID:999999113"):
        parse_into(tmp_path, export)


# VALIDATION REPORT


def test_expected_shard_counts_reads_the_structured_counts():
    """expected_shard_counts() should return each shard's record count from checks.structure, sorted."""
    counts = publications.expected_shard_counts(FIXTURE_DIR)
    assert list(counts) == [f"pubmed_metadata_{i}.ndjson.gz" for i in range(3)]
    assert sum(counts.values()) == RECORD_COUNT


def test_truncated_shard_is_refused_and_named(tmp_path):
    """A shard holding fewer records than the validation report says should be refused, naming the shard —
    that is the only signal of a truncated download."""
    export = copy_fixture(tmp_path)
    shard = export / "pubmed_metadata_1.ndjson.gz"
    with gzip.open(shard, "rt") as f:
        first_line = f.readline()
    with gzip.open(shard, "wt") as f:
        f.write(first_line)
    with pytest.raises(RuntimeError, match=r"pubmed_metadata_1\.ndjson\.gz: parsed 1, report says"):
        parse_into(tmp_path, export)


def test_failed_validation_report_raises(tmp_path):
    """An export whose own validator reported `fail` should be refused outright."""
    export = copy_fixture(tmp_path)
    edit_report(export / publications.VALIDATION_REPORT, lambda report: report.update(status="fail", errors=["boom"]))
    with pytest.raises(RuntimeError, match="status 'fail'"):
        publications.expected_shard_counts(str(export))


def test_report_without_structured_counts_raises(tmp_path):
    """A report missing checks.structure.records_by_shard (a format change) should raise a clear error rather
    than a KeyError."""
    export = copy_fixture(tmp_path)
    edit_report(
        export / publications.VALIDATION_REPORT, lambda report: report["checks"]["structure"].pop("records_by_shard")
    )
    with pytest.raises(RuntimeError, match="report format has changed"):
        publications.expected_shard_counts(str(export))


def test_report_whose_shard_counts_disagree_with_its_total_raises(tmp_path):
    """records_by_shard summing to something other than records_total means the report can't be trusted."""
    export = copy_fixture(tmp_path)
    edit_report(
        export / publications.VALIDATION_REPORT,
        lambda report: report["checks"]["structure"].update(records_total=RECORD_COUNT + 1),
    )
    with pytest.raises(RuntimeError, match="records_total"):
        publications.expected_shard_counts(str(export))


@pytest.mark.parametrize("name", ["../pubmed_metadata_0.ndjson.gz", "pubmed_metadata_0.ndjson", "pubmed.duckdb"])
def test_report_listing_an_unexpected_shard_name_raises(tmp_path, name):
    """A shard name that isn't a bare *.ndjson.gz filename should be refused, since it becomes both a URL and a
    local path."""
    export = copy_fixture(tmp_path)
    edit_report(
        export / publications.VALIDATION_REPORT,
        lambda report: report["checks"]["structure"]["records_by_shard"].update({name: 0}),
    )
    with pytest.raises(RuntimeError, match="unexpected shard name"):
        publications.expected_shard_counts(str(export))


# DOWNLOADING


@pytest.fixture
def fake_export(tmp_path, monkeypatch):
    """Lay the fixture out like a published export (shards, the report under manifests/, and pubmed2db's own
    database as a decoy), and swap pull_via_wget() for a copy from it. Yields (remote_dir, calls)."""
    remote = tmp_path / "remote"
    (remote / "manifests").mkdir(parents=True)
    for shard in publications.expected_shard_counts(FIXTURE_DIR):
        shutil.copy(os.path.join(FIXTURE_DIR, shard), remote / shard)
    shutil.copy(os.path.join(FIXTURE_DIR, publications.VALIDATION_REPORT), remote / "manifests" / "report.json")
    with gzip.open(remote / "manifests" / "report.json.gz", "wb") as f:
        f.write((remote / "manifests" / "report.json").read_bytes())
    (remote / "pubmed.duckdb").write_text("never download me")

    calls = []

    def fake_pull_via_wget(url_prefix, in_file_name, decompress, outpath, continue_incomplete):
        assert url_prefix == EXPORT_URL
        calls.append((in_file_name, continue_incomplete))
        ensure_parent_dir(outpath)
        shutil.copy(remote / in_file_name, outpath)
        if decompress:
            with gzip.open(outpath, "rb") as inf, open(outpath.removesuffix(".gz"), "wb") as outf:
                shutil.copyfileobj(inf, outf)
            os.remove(outpath)

    monkeypatch.setattr(publications, "pull_via_wget", fake_pull_via_wget)
    yield remote, calls


@pytest.mark.parametrize("report_path", ["manifests/report.json", "manifests/report.json.gz"])
def test_download_fetches_the_report_then_exactly_the_shards_it_lists(fake_export, tmp_path, report_path):
    """download_pubmed2db() should fetch the validation report (fresh, gunzipping it if needed), then each
    shard the report lists (resumably) -- and nothing else in the export directory -- then touch the marker."""
    _, calls = fake_export
    download_dir = tmp_path / "PubMed2DB" / "2026sep26"
    done_file = download_dir / "downloaded"

    publications.download_pubmed2db(EXPORT_URL, report_path, str(download_dir), str(done_file))

    shards = list(publications.expected_shard_counts(FIXTURE_DIR))
    assert calls == [(report_path, False)] + [(shard, True) for shard in shards]
    assert done_file.exists()
    assert publications.expected_shard_counts(str(download_dir)) == publications.expected_shard_counts(FIXTURE_DIR)
    assert not (download_dir / "pubmed.duckdb").exists()


def test_download_can_be_rerun_over_an_existing_download(fake_export, tmp_path):
    """Re-running download_pubmed2db() over a finished download (a retry, or a preloaded directory) should
    replace the report rather than fail on the old copy."""
    download_dir = tmp_path / "PubMed2DB" / "2026sep26"
    done_file = download_dir / "downloaded"
    for _ in range(2):
        publications.download_pubmed2db(EXPORT_URL, "manifests/report.json.gz", str(download_dir), str(done_file))
    assert publications.expected_shard_counts(str(download_dir)) == publications.expected_shard_counts(FIXTURE_DIR)


def test_download_of_a_failed_export_stops_before_the_shards(fake_export, tmp_path):
    """An export whose validation report says `fail` should not be downloaded, nor marked done."""
    remote, calls = fake_export
    edit_report(remote / "manifests" / "report.json", lambda report: report.update(status="fail"))
    download_dir = tmp_path / "PubMed2DB" / "2026sep26"
    with pytest.raises(RuntimeError, match="status 'fail'"):
        publications.download_pubmed2db(
            EXPORT_URL, "manifests/report.json", str(download_dir), str(download_dir / "downloaded")
        )
    assert calls == [("manifests/report.json", False)]
    assert not (download_dir / "downloaded").exists()


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
        FIXTURE_DIR, parsed["shared_ids_file"], [parsed["metadata_yaml"]], compendium, str(icrdf)
    )

    with open(compendium) as f:
        cliques = [json.loads(line) for line in f]
    by_pmid = {clique["identifiers"][0]["i"]: clique for clique in cliques}
    assert len(cliques) == RECORD_COUNT and len(by_pmid) == RECORD_COUNT
    assert {clique["type"] for clique in cliques} == {PUBLICATION}

    def curies(pmid):
        return [identifier["i"] for identifier in by_pmid[pmid]["identifiers"]]

    def label(pmid):
        # write_compendium() writes an unlabeled identifier with an empty "l" (pre-existing behaviour).
        return by_pmid[pmid]["identifiers"][0].get("l", "")

    assert curies("PMID:5202740") == ["PMID:5202740", "PMCID:PMC1712714"]
    assert label("PMID:5202740") == "Renal tubular acidosis: an immunopathological study on four patients."
    assert curies("PMID:5202677") == ["PMID:5202677"]
    assert curies("PMID:17072568") == ["PMID:17072568", "doi:10.1007/s00115-006-2195-7"]
    assert label("PMID:17072568") == ""

    # Labels are the exported titles exactly: the newline is a real character rather than `\n`, and
    # surrounding whitespace and control characters are kept (docs/sources/PubMed/README.md, "Titles").
    assert "Genotypes and\nHaplotype" in label("PMID:30803192")
    assert by_pmid["PMID:30803192"]["preferred_name"] == label("PMID:30803192")
    assert label("PMID:9245294") == "Identity as adaptation to social, cultural, and historical context "
    assert label("PMID:25303692") == "New challenges\x85."

    # PMC: is registered on biolink:Publication; PMCID: survives only via extra_prefixes (see #1044), which
    # create_node() places after every registered prefix.
    assert curies("PMID:30803192") == ["PMID:30803192", "doi:10.31557/APJCP.2019.20.2.345", "PMCID:PMC6897020"]
    assert curies("PMID:999999113") == ["PMID:999999113", "PMC:PMC1666769"]

    # The shared DOI lands on the lowest PMID only, so no identifier appears in two cliques.
    assert curies("PMID:7754980") == ["PMID:7754980", SHARED_DOI]
    assert curies("PMID:7754983") == ["PMID:7754983"]
    all_curies = [identifier["i"] for clique in cliques for identifier in clique["identifiers"]]
    assert len(all_curies) == len(set(all_curies))
