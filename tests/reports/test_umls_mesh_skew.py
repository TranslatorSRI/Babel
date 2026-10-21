"""Unit tests for the UMLS/MeSH version skew report (``src/reports/umls_mesh_skew.py``).

Fixtures are copied from real files: ``tests/data/mesh/MRCONSO_sample.RRF`` from UMLS 2026AA MRCONSO.RRF
and ``tests/data/mesh/mesh2026_sample.nt`` from mesh2026.nt (see the test docstrings for which records).
"""

import gzip
import json
import shutil

import pytest

from src.datahandlers.mesh import normalize_mesh_nt
from src.reports.umls_mesh_skew import (
    check_umls_mesh_skew,
    generate_umls_mesh_skew_report,
    read_labeled_mesh_ids,
    read_umls_mesh_codes,
)
from src.util import get_repo_root

MESH_TEST_DATA = get_repo_root() / "tests" / "data" / "mesh"


@pytest.fixture
def mesh_nt(tmp_path):
    """mesh2026_sample.nt (C000730045 foliglurax and D002839 Chromans) normalized the way pull_mesh() does it."""
    gz_path = tmp_path / "mesh2026.nt.gz"
    with open(MESH_TEST_DATA / "mesh2026_sample.nt", "rb") as inf, gzip.open(gz_path, "wb") as outf:
        shutil.copyfileobj(inf, outf)
    out = tmp_path / "mesh.nt"
    normalize_mesh_nt(gz_path, out, "2026")
    return out


# READERS


@pytest.mark.unit
def test_read_labeled_mesh_ids(mesh_nt):
    """Only records with an rdfs:label should be read, as bare IDs."""
    assert read_labeled_mesh_ids(mesh_nt) == {"C000730045", "D002839"}


@pytest.mark.unit
def test_read_umls_mesh_codes_only_reads_msh_rows():
    """MSH rows should be read by CODE; MSHCZE and NCI rows for the same CUIs should be ignored."""
    assert read_umls_mesh_codes(MESH_TEST_DATA / "MRCONSO_sample.RRF") == {
        "D002839": {"C0008542"},
        "C000730045": {"C5139670"},
        "U000002": {"C0002807"},
    }


# REPORT


@pytest.mark.unit
def test_report_counts_codes_missing_from_mesh(mesh_nt, tmp_path):
    """U000002 ("Anatomy (MeSH Category)") is a UMLS-only heading with no MeSH record, so it should be the one
    missing code, on one CUI that has no other MSH code; the report should be written as JSON."""
    outfile = tmp_path / "umls_mesh_skew.json"
    report = generate_umls_mesh_skew_report(MESH_TEST_DATA / "MRCONSO_sample.RRF", mesh_nt, "2026AA", "2026", outfile)
    assert report == {
        "umls_version": "2026AA",
        "mesh_year": "2026",
        "mesh_records_in_dump": 2,
        "umls_msh_codes": 3,
        "umls_msh_codes_missing_from_mesh": 1,
        "umls_msh_codes_missing_from_mesh_excluding_categories": 0,
        "missing_by_record_type": {"U": 1},
        "umls_cuis_with_msh_codes": 3,
        "umls_cuis_with_any_missing_msh_code": 1,
        "umls_cuis_with_only_missing_msh_codes": 1,
        "missing_sample": ["MESH:U000002"],
    }
    assert json.loads(outfile.read_text()) == report


# CHECK


def _write_report(tmp_path, missing_excluding_categories):
    report = tmp_path / "umls_mesh_skew.json"
    report.write_text(
        json.dumps(
            {
                "umls_version": "2026AA",
                "mesh_year": "2025",
                "umls_msh_codes_missing_from_mesh_excluding_categories": missing_excluding_categories,
            }
        )
    )
    return report


@pytest.mark.unit
def test_check_passes_at_the_limit(tmp_path):
    """A count equal to the limit should pass and write the checked file with a summary."""
    checked = tmp_path / "umls_mesh_skew.checked"
    check_umls_mesh_skew(_write_report(tmp_path, 100), 100, checked)
    assert "100 MSH codes in UMLS 2026AA have no record in MeSH 2025" in checked.read_text()


@pytest.mark.unit
def test_check_fails_over_the_limit(tmp_path):
    """A count over the limit should raise, naming mesh_year as the likely fix, and write no checked file."""
    checked = tmp_path / "umls_mesh_skew.checked"
    with pytest.raises(RuntimeError, match="mesh_year is probably behind umls_version"):
        check_umls_mesh_skew(_write_report(tmp_path, 101), 100, checked)
    assert not checked.exists()


@pytest.mark.unit
def test_check_ignores_category_headings(mesh_nt, tmp_path):
    """The U "(MeSH Category)" headings are missing from every MeSH dump, so a report whose only missing code is
    U000002 should pass even with a limit of zero."""
    report = tmp_path / "umls_mesh_skew.json"
    generate_umls_mesh_skew_report(MESH_TEST_DATA / "MRCONSO_sample.RRF", mesh_nt, "2026AA", "2026", report)
    checked = tmp_path / "umls_mesh_skew.checked"
    check_umls_mesh_skew(report, 0, checked)
    assert checked.exists()
