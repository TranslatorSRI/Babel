"""Unit tests for src.datahandlers.umls."""

import pytest

from src.datahandlers.umls import get_cuis_mapped_only_to_mesh

# MeSH-ARBITRATED CUIS


def _mrconso_row(cui, lang, sab, code, suppress="N"):
    """Build a synthetic 18-column MRCONSO.RRF row; only the columns get_cuis_mapped_only_to_mesh reads are set."""
    cols = [""] * 18
    cols[0], cols[1], cols[11], cols[13], cols[16] = cui, lang, sab, code, suppress
    return "|".join(cols) + "\n"


@pytest.mark.unit
def test_get_cuis_mapped_only_to_mesh(tmp_path):
    """Only CUIs whose usable MSH atoms are all in the given MeSH set should be returned."""
    mrconso = tmp_path / "MRCONSO.RRF"
    mrconso.write_text(
        "".join(
            [
                # Every MSH atom is a role descriptor -> returned.
                _mrconso_row("C9000001", "ENG", "MSH", "D000001"),
                _mrconso_row("C9000001", "ENG", "RXNORM", "12345"),
                # One MSH atom is outside the set -> not returned.
                _mrconso_row("C9000002", "ENG", "MSH", "D000001"),
                _mrconso_row("C9000002", "ENG", "MSH", "D000002"),
                # An SCR atom outside the set -> not returned.
                _mrconso_row("C9000003", "ENG", "MSH", "D000001"),
                _mrconso_row("C9000003", "ENG", "MSH", "C000003"),
                # No MSH atoms at all -> not returned.
                _mrconso_row("C9000004", "ENG", "RXNORM", "67890"),
                # The out-of-set atom is suppressed or non-English, so it is ignored -> returned.
                _mrconso_row("C9000005", "ENG", "MSH", "D000001"),
                _mrconso_row("C9000005", "ENG", "MSH", "D000002", suppress="O"),
                _mrconso_row("C9000005", "FRE", "MSH", "D000002"),
            ]
        )
    )
    assert get_cuis_mapped_only_to_mesh(str(mrconso), {"MESH:D000001"}) == {"C9000001", "C9000005"}
