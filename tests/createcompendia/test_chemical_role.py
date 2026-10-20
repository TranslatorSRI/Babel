"""Tests for src/createcompendia/chemical_role.py."""

import pytest

from src.categories import CHEMICAL_ROLE
from src.createcompendia import chemical_role
from tests.conftest import assert_ids_file_valid


def _mrconso_row(cui, sab, code):
    """Build a synthetic 18-column English, unsuppressed MRCONSO.RRF row."""
    cols = [""] * 18
    cols[0], cols[1], cols[11], cols[13], cols[16] = cui, "ENG", sab, code, "N"
    return "|".join(cols) + "\n"


# UMLS IDS


@pytest.mark.unit
def test_write_umls_ids_follows_mesh_ids_and_substance_types(tmp_path):
    """UMLS ids should be the substance-typed, non-protein CUIs whose MSH atoms are all ChemicalRole MeSH ids."""
    mesh_ids = tmp_path / "MESH"
    mesh_ids.write_text(f"MESH:D000001\t{CHEMICAL_ROLE}\n")
    mrconso = tmp_path / "MRCONSO.RRF"
    mrconso.write_text(
        _mrconso_row("C9000001", "MSH", "D000001")  # role, Pharmacologic Substance -> kept
        + _mrconso_row("C9000002", "MSH", "D000001")  # also on a non-role descriptor -> dropped
        + _mrconso_row("C9000002", "MSH", "D000002")
        + _mrconso_row("C9000003", "MSH", "D000001")  # role, but a Physiologic Function -> dropped
        + _mrconso_row("C9000004", "MSH", "D000001")  # role, but also a protein -> dropped
    )
    # Synthetic MRSTY.RRF rows (CUI|TUI|STN|STY|ATUI|CVF|) using the real semantic type tree numbers.
    mrsty = tmp_path / "MRSTY.RRF"
    mrsty.write_text(
        "C9000001|T121|A1.4.1.1.1|Pharmacologic Substance|AT1|256|\n"
        "C9000002|T121|A1.4.1.1.1|Pharmacologic Substance|AT2|256|\n"
        "C9000003|T039|B2.2.1.2|Physiologic Function|AT3|256|\n"
        "C9000004|T121|A1.4.1.1.1|Pharmacologic Substance|AT4|256|\n"
        "C9000004|T116|A1.4.1.2.1.7|Amino Acid, Peptide, or Protein|AT5|256|\n"
    )
    outfile = tmp_path / "UMLS"
    chemical_role.write_umls_ids(str(mrconso), str(mrsty), str(mesh_ids), str(outfile))
    assert outfile.read_text() == f"UMLS:C9000001\t{CHEMICAL_ROLE}\n"


# CHEBI IDS


@pytest.mark.network
def test_write_chebi_ids(tmp_path):
    """ChEBI ids should include the role root and its descendants, and no chemical entities."""
    outfile = tmp_path / "CHEBI"
    chemical_role.write_chebi_ids(str(outfile), str(tmp_path / "metadata.yaml"))
    assert_ids_file_valid(outfile)
    ids = {line.split("\t")[0] for line in outfile.read_text().splitlines()}
    assert {"CHEBI:50906", "CHEBI:51086", "CHEBI:35469"} <= ids  # role, chemical role, antidepressant
    assert "CHEBI:15377" not in ids  # water


# WIKIDATA CONCORD


@pytest.mark.network
def test_build_wikidata_relationships_limited_to_id_lists(tmp_path):
    """Only Wikidata MeSH<->ChEBI pairs with both sides in the ChemicalRole id lists should be written."""
    chebi_ids = tmp_path / "CHEBI"
    chebi_ids.write_text(f"CHEBI:35469\t{CHEMICAL_ROLE}\n")
    mesh_ids = tmp_path / "MESH"
    mesh_ids.write_text(f"MESH:D000928\t{CHEMICAL_ROLE}\n")
    outfile = tmp_path / "wikidata_mesh_chebi"
    chemical_role.build_wikidata_relationships(str(chebi_ids), str(mesh_ids), str(outfile), str(tmp_path / "m.yaml"))
    assert outfile.read_text() == "MESH:D000928\txref\tCHEBI:35469\n"
