"""Unit tests for src/createcompendia/publications.py: download verification, then XML parsing.

verify_pubmed_downloads() is the backstop that makes it safe to carry PubMed files forward from a
previous run (see docs/RunningBabel.md, "Preloading PubMed downloads"): it MD5s every downloaded
`.gz` against the `.md5` file PubMed publishes alongside it, and re-downloads the ones that fail.

parse_pubmed_into_tsvs() turns those files into the ids/titles/statuses/concord outputs. Its tests
run against tests/data/pubmed_three_articles.xml.gz, three articles copied verbatim from baseline
file pubmed26n0001.xml.gz. The end-to-end version over freshly downloaded files lives in
tests/pipeline/test_publications.py.
"""

import gzip
import hashlib
import json
import shutil
from pathlib import Path

import pytest

import src.createcompendia.publications as publications


def write_pubmed_file(directory, name, content=b"pubmed article data"):
    """Write a fake PubMed download and the matching `.md5` file PubMed would publish for it."""
    path = directory / name
    path.write_bytes(content)
    md5 = hashlib.md5(content).hexdigest()
    (directory / f"{name}.md5").write_text(f"MD5({name})= {md5}\n")
    return path


@pytest.fixture
def baseline_dir(tmp_path):
    directory = tmp_path / "baseline"
    directory.mkdir()
    return directory


@pytest.fixture
def no_downloads(monkeypatch):
    """Fail loudly if verification tries to download anything, and record any attempt.

    Raising, rather than merely recording, matters: verify_pubmed_downloads() re-downloads in a
    `while not verified` loop, so a stub that returned without fixing the file would spin forever.
    """
    attempts = []

    def _fail(url_prefix, in_file_name, **kwargs):
        attempts.append(in_file_name)
        raise AssertionError(f"unexpected re-download of {in_file_name}")

    monkeypatch.setattr(publications, "pull_via_wget", _fail)
    return attempts


# VERIFYING A SINGLE FILE AGAINST ITS MD5


@pytest.mark.unit
def test_file_matching_its_md5_verifies(baseline_dir):
    """A file whose MD5 matches the published checksum should verify."""
    path = write_pubmed_file(baseline_dir, "pubmed26n0001.xml.gz")
    assert publications.verify_pubmed_download_against_md5(str(path), f"{path}.md5")


@pytest.mark.unit
def test_file_not_matching_its_md5_fails_verification(baseline_dir):
    """A file whose content no longer matches its published checksum — a corrupt or truncated
    download, or one carried over from a previous run and since revised — should fail."""
    path = write_pubmed_file(baseline_dir, "pubmed26n0001.xml.gz")
    path.write_bytes(b"corrupted data")
    assert not publications.verify_pubmed_download_against_md5(str(path), f"{path}.md5")


@pytest.mark.unit
def test_missing_file_fails_verification(baseline_dir):
    """A file that doesn't exist should fail verification rather than raising."""
    write_pubmed_file(baseline_dir, "pubmed26n0001.xml.gz")
    missing = baseline_dir / "pubmed26n0002.xml.gz"
    assert not publications.verify_pubmed_download_against_md5(str(missing), f"{missing}.md5")


@pytest.mark.unit
def test_zero_length_file_fails_verification(baseline_dir):
    """A zero-length file should fail verification: verify_pubmed_downloads() truncates a file it is
    about to re-download, so an empty file means an earlier re-download attempt didn't finish."""
    path = write_pubmed_file(baseline_dir, "pubmed26n0001.xml.gz")
    path.write_bytes(b"")
    assert not publications.verify_pubmed_download_against_md5(str(path), f"{path}.md5")


@pytest.mark.unit
def test_missing_md5_file_fails_verification(baseline_dir):
    """A download with no `.md5` alongside it should fail verification, so that it is re-downloaded
    together with its checksum rather than trusted unchecked."""
    path = baseline_dir / "pubmed26n0001.xml.gz"
    path.write_bytes(b"pubmed article data")
    assert not publications.verify_pubmed_download_against_md5(str(path), f"{path}.md5")


@pytest.mark.unit
def test_unreadable_md5_file_raises(baseline_dir):
    """An `.md5` file we can't parse is a format change upstream, not a bad download: raise rather
    than re-downloading the file forever."""
    path = write_pubmed_file(baseline_dir, "pubmed26n0001.xml.gz")
    (baseline_dir / "pubmed26n0001.xml.gz.md5").write_text("MD5(pubmed26n0001.xml.gz)= deadbeef\n")
    with pytest.raises(RuntimeError, match="could not read MD5 hash"):
        publications.verify_pubmed_download_against_md5(str(path), f"{path}.md5")


# VERIFYING A DIRECTORY OF DOWNLOADS


@pytest.mark.unit
def test_verifying_good_downloads_downloads_nothing_and_writes_the_done_file(baseline_dir, tmp_path, no_downloads):
    """Files that all match their checksums — including ones preloaded from a previous run — should
    be left alone, with nothing re-downloaded and the done marker written."""
    write_pubmed_file(baseline_dir, "pubmed26n0001.xml.gz")
    write_pubmed_file(baseline_dir, "pubmed26n0002.xml.gz")
    done_file = tmp_path / "verified"

    publications.verify_pubmed_downloads([str(baseline_dir)], str(done_file))

    assert no_downloads == []
    assert done_file.exists()


@pytest.mark.unit
def test_verifying_a_corrupt_download_redownloads_it_and_its_md5(baseline_dir, tmp_path, monkeypatch):
    """A file that fails its checksum should be re-downloaded along with its `.md5`, and the local
    copies truncated first so that an interrupted re-download can't leave a file that verifies."""
    good = write_pubmed_file(baseline_dir, "pubmed26n0001.xml.gz")
    corrupt = write_pubmed_file(baseline_dir, "pubmed26n0002.xml.gz")
    corrupt.write_bytes(b"corrupted data")

    downloaded = []

    def fake_pull_via_wget(url_prefix, in_file_name, **kwargs):
        # Both files are truncated before the re-download starts.
        if not downloaded:
            assert corrupt.stat().st_size == 0
            assert (baseline_dir / "pubmed26n0002.xml.gz.md5").stat().st_size == 0
        downloaded.append(in_file_name)
        # Serve a correct copy, as PubMed would, so verification converges.
        write_pubmed_file(baseline_dir, "pubmed26n0002.xml.gz")

    monkeypatch.setattr(publications, "pull_via_wget", fake_pull_via_wget)

    done_file = tmp_path / "verified"
    publications.verify_pubmed_downloads([str(baseline_dir)], str(done_file))

    assert downloaded == ["pubmed26n0002.xml.gz", "pubmed26n0002.xml.gz.md5"]
    assert publications.verify_pubmed_download_against_md5(str(corrupt), f"{corrupt}.md5")
    # The file that was fine was never touched.
    assert good.read_bytes() == b"pubmed article data"
    assert done_file.exists()


# PARSING PUBMED XML INTO TSVS


@pytest.fixture
def parsed_fixture(tmp_path):
    """Parse tests/data/pubmed_three_articles.xml.gz and return the output paths."""
    baseline = tmp_path / "baseline"
    baseline.mkdir()
    updatefiles = tmp_path / "updatefiles"
    updatefiles.mkdir()
    shutil.copy(
        Path(__file__).parent.parent / "data" / "pubmed_three_articles.xml.gz",
        baseline / "pubmed26n0001.xml.gz",
    )

    outputs = {name: tmp_path / name for name in ("titles.tsv", "statuses.jsonl.gz", "PMID", "PMID_DOI")}
    publications.parse_pubmed_into_tsvs(
        str(baseline),
        str(updatefiles),
        str(outputs["titles.tsv"]),
        str(outputs["statuses.jsonl.gz"]),
        str(outputs["PMID"]),
        str(outputs["PMID_DOI"]),
        str(tmp_path / "metadata.yaml"),
    )
    return outputs


@pytest.mark.unit
def test_parsing_emits_every_article_in_the_file(parsed_fixture):
    """Every PubmedArticle should reach the ids, titles and statuses files.

    This is the regression test for the parser releasing articles as it goes: the loop clears the
    parsed tree off the root after each PubmedArticle, and clearing one article too early would
    drop it (or its siblings) silently rather than crashing.
    """
    assert parsed_fixture["PMID"].read_text().splitlines() == [
        f"PMID:{pmid}\tbiolink:JournalArticle" for pmid in ("1", "114", "10")
    ]

    titles = dict(line.split("\t", 1) for line in parsed_fixture["titles.tsv"].read_text().splitlines())
    assert titles == {
        "PMID:1": "Formate assay in body fluids: application in methanol poisoning.",
        "PMID:114": "Proceedings: Central hypertensive action of histamine in rats.",
        "PMID:10": "Digitoxin metabolism by rat liver microsomes.",
    }

    with gzip.open(parsed_fixture["statuses.jsonl.gz"], "rt") as statusf:
        statuses = [json.loads(line) for line in statusf]
    assert statuses == [
        {"id": f"PMID:{pmid}", "statuses": ["entrez", "medline", "pubmed"]} for pmid in ("1", "114", "10")
    ]


@pytest.mark.unit
def test_parsing_writes_doi_and_pmc_concords(parsed_fixture):
    """DOIs and PMCIDs should become `eq` concords; an article with neither should emit none.

    The three fixture articles are copied verbatim from PubMed baseline file pubmed26n0001.xml.gz
    and chosen for exactly this split: PMID:1 carries a DOI, PMID:114 a PMCID, PMID:10 neither.
    """
    assert parsed_fixture["PMID_DOI"].read_text().splitlines() == [
        "PMID:1\teq\tdoi:10.1016/0006-2944(75)90147-7",
        "PMID:114\teq\tPMC:PMC1666842",
    ]
