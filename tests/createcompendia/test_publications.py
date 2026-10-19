"""Unit tests for src/createcompendia/publications.py: PubMed download verification and parsing.

verify_pubmed_downloads() is the backstop that makes it safe to carry PubMed files forward from a
previous run (see docs/RunningBabel.md, "Preloading PubMed downloads"): it MD5s every downloaded
`.gz` against the `.md5` file PubMed publishes alongside it, and re-downloads the ones that fail.
"""

import gzip
import hashlib
from pathlib import Path

import pytest
import yaml

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


# PARSING PUBMED INTO CONCORDS

# Two PubmedArticle records copied verbatim from the 2026 baseline file pubmed26n1280.xml.gz:
# PMID:39970118, whose DOI has a second identifier appended ("10.31857/S0026898424050076, EDN: HUNTKB"),
# and PMID:39970131, with a clean DOI and a PMCID.
PUBMED_EXCERPT = Path(__file__).parent.parent / "data" / "pubmed26n1280_malformed_doi_excerpt.xml"


@pytest.mark.unit
def test_parse_pubmed_skips_malformed_dois(tmp_path):
    """Should keep well-formed DOIs and PMCIDs, skip the malformed DOI that write_compendium() would
    reject (#1112), and count the skip in the concord metadata."""
    baseline = tmp_path / "baseline"
    updatefiles = tmp_path / "updatefiles"
    baseline.mkdir()
    updatefiles.mkdir()
    with gzip.open(baseline / "pubmed26n1280.xml.gz", "wb") as f:
        f.write(PUBMED_EXCERPT.read_bytes())
    concord = tmp_path / "PMID_DOI"
    metadata = tmp_path / "metadata.yaml"
    publications.parse_pubmed_into_tsvs(
        baseline,
        updatefiles,
        tmp_path / "titles",
        tmp_path / "statuses.jsonl.gz",
        tmp_path / "pmid_ids",
        concord,
        metadata,
    )
    assert concord.read_text().splitlines() == [
        "PMID:39970131\teq\tdoi:10.1371/journal.pone.0318168",
        "PMID:39970131\teq\tPMC:PMC11838910",
    ]
    assert "PMID:39970118" in (tmp_path / "pmid_ids").read_text()
    assert yaml.safe_load(metadata.read_text())["counts"]["skipped_malformed_article_ids"] == 1
