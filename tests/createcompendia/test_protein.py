"""Tests for the protein compendium's concord builders (src/createcompendia/protein.py)."""

import pytest

from src.createcompendia.protein import build_ncit_uniprot_relationships

# NCIT-SWISSPROT MAPPING

# Rows copied verbatim (CRLF line endings included) from NCIt-SwissProt_Mapping.txt, downloaded
# 2026-09-25 from https://evs.nci.nih.gov/ftp1/NCI_Thesaurus/Mappings/. The row's NCIt code is its ID.
NCIT_SWISSPROT_ROWS = (
    "NCIt Code\tSwissProt ID\tNCIt Preferred Name\r\n"
    "C150023\tQ9C0C9\t(E3-Independent) E2 Ubiquitin-Conjugating Enzyme\r\n"
    "C16375\tP0DP24|P0DP23|P0DP25\tCalmodulin\r\n"
    "C184960\tP0DTC1|P0DTD1\tSARS-CoV-2 3C-Like Proteinase Nsp5\r\n"
    "C71447\tU65002\tZinc Finger Protein PLAG1\r\n"
)


@pytest.mark.unit
def test_ncit_uniprot_keeps_only_single_uniprot_accessions(tmp_path):
    """Should map only rows with one UniProt accession, skipping the header, "|"-joined accession
    lists (C16375, C184960) and GenBank accessions (C71447)."""
    infile = tmp_path / "NCIt-SwissProt_Mapping.txt"
    infile.write_bytes(NCIT_SWISSPROT_ROWS.encode())
    outfile = tmp_path / "NCIT_UniProtKB"
    build_ncit_uniprot_relationships(infile, outfile, tmp_path / "metadata.yaml")
    assert outfile.read_text() == "NCIT:C150023\teq\tUniProtKB:Q9C0C9\n"


@pytest.mark.unit
def test_ncit_uniprot_rejects_unexpected_header(tmp_path):
    """Should raise rather than silently treat the first row as a header if NCIt drops the header."""
    infile = tmp_path / "NCIt-SwissProt_Mapping.txt"
    infile.write_bytes(NCIT_SWISSPROT_ROWS.split("\r\n", 1)[1].encode())
    with pytest.raises(ValueError, match="Unexpected header"):
        build_ncit_uniprot_relationships(infile, tmp_path / "out", tmp_path / "metadata.yaml")
