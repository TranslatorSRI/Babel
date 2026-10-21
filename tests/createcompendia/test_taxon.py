"""Tests for building the OrganismTaxon concords (src/createcompendia/taxon.py)."""

import io
import tarfile

import pytest
import yaml

from src.createcompendia.taxon import build_taxon_umls_relationships, update_obsolete_ncbitaxon_ids
from src.datahandlers.ncbitaxon import read_obsolete_taxa
from tests.conftest import assert_concordance_file_valid

# MRCONSO.RRF rows copied verbatim from the UMLS 2026AA release.
MRCONSO_ROWS = [
    # C1110641 Armoracia rusticana: UMLS calls NCBI Taxonomy "NCBI", and its CODE is the bare taxon ID.
    "C1110641|ENG|P|L0514063|PF|S0592368|N|A0647575||3704||NCBI|SCN|3704|Armoracia rusticana|0|N||",
    # C0003062 Animals: only the MeSH main heading (MH) is used; the entry term (ET) is skipped.
    "C0003062|ENG|P|L0003043|PF|S0014166|N|A2782515||M0001219|D000818|MSH|MH|D000818|Animals|0|N||",
    "C0003062|ENG|P|L0003043|VO|S0014083|Y|A26622215||M0001219|D000818|MSH|ET|D000818|Animal|0|N||",
    "C0003062|ENG|S|L0161925|PF|S0226340|N|A2687514||33208||NCBI|SY|33208|Animalia|0|N||",
    # C1704307 Fly (organism): a SNOMED CT atom, which isn't a source we map taxa from.
    "C1704307|ENG|S|L0016237|PF|S0004196|Y|A2876298|17694013|10147004||SNOMEDCT_US|PT|10147004|Fly|9|N||",
    # C0010395 Crustacea: NCBI 6657 has since been merged into 197562 (see MERGED_DMP_ROWS).
    "C0010395|ENG|S|L0010395|PF|S0028997|N|A0043885||6657||NCBI|SCN|6657|Crustacea|0|N||",
    # C0331518 Paspalum notatum: NCBI 147272 has since been deleted (see DELNODES_DMP_ROWS).
    "C0331518|ENG|P|L0599004|PF|S0711248|N|A2688028||147272||NCBI|SCN|147272|Paspalum notatum|0|N||",
]
TAXON_CUIS = ["C1110641", "C0003062", "C1704307", "C0010395", "C0331518"]

# merged.dmp and delnodes.dmp rows copied verbatim from the NCBI Taxonomy taxdump of 2026-10-09.
MERGED_DMP_ROWS = [
    "6657\t|\t197562\t|",  # Crustacea -> Pancrustacea
    "650420\t|\t2761485\t|",  # Marasmius distantifolius
    "71877\t|\t206318\t|",  # Collybia dryophila -> Gymnopus dryophilus
]
DELNODES_DMP_ROWS = [
    "147272\t|",  # Paspalum notatum
]


def write_taxdump(path, merged_rows, delnodes_rows):
    """Write a taxdump.tar containing only merged.dmp and delnodes.dmp."""
    with tarfile.open(path, "w") as tar:
        for name, rows in [("merged.dmp", merged_rows), ("delnodes.dmp", delnodes_rows)]:
            data = "".join(row + "\n" for row in rows).encode("utf-8")
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    return str(path)


@pytest.fixture
def taxdump(tmp_path):
    return write_taxdump(tmp_path / "taxdump.tar", MERGED_DMP_ROWS, DELNODES_DMP_ROWS)


# OBSOLETE NCBITAXON IDS


@pytest.mark.unit
def test_read_obsolete_taxa(taxdump):
    """merged.dmp should be read as old CURIE -> new CURIE, and delnodes.dmp as a set of CURIEs."""
    merged, deleted = read_obsolete_taxa(taxdump)
    assert merged == {
        "NCBITaxon:6657": "NCBITaxon:197562",
        "NCBITaxon:650420": "NCBITaxon:2761485",
        "NCBITaxon:71877": "NCBITaxon:206318",
    }
    assert deleted == {"NCBITaxon:147272"}


@pytest.mark.unit
def test_read_obsolete_taxa_rejects_merge_chains(tmp_path):
    """A merge target that is itself merged should raise, since we only follow one step.

    NCBI flattens merge chains, so this taxdump is constructed rather than copied: it chains the two real
    merges above so that 650420 -> 2761485 -> 6657.
    """
    path = write_taxdump(tmp_path / "taxdump.tar", ["2761485\t|\t6657\t|", "650420\t|\t2761485\t|"], [])
    with pytest.raises(ValueError, match="merge targets are themselves obsolete"):
        read_obsolete_taxa(path)


@pytest.mark.unit
def test_update_obsolete_ncbitaxon_ids(tmp_path, taxdump):
    """Merged IDs should be replaced, rows with deleted IDs dropped, and rows that become duplicates removed."""
    concord = tmp_path / "concord"
    concord.write_text(
        "MESH:C000695935\txref\tNCBITaxon:650420\n"
        "MESH:D000818\txref\tNCBITaxon:33208\n"
        "UMLS:C0331518\teq\tNCBITaxon:147272\n"
        "UMLS:C0331518\teq\tMESH:C000695935\n"
    )

    counts = update_obsolete_ncbitaxon_ids(str(concord), taxdump)

    assert assert_concordance_file_valid(str(concord)) == [
        ["MESH:C000695935", "xref", "NCBITaxon:2761485"],
        ["MESH:D000818", "xref", "NCBITaxon:33208"],
        ["UMLS:C0331518", "eq", "MESH:C000695935"],
    ]
    assert counts == {
        "obsolete_ncbitaxon": {"remapped_merged": 1, "dropped_merged_superseded_by_direct": 0, "dropped_deleted": 1}
    }


@pytest.mark.unit
def test_update_obsolete_ncbitaxon_ids_prefers_direct_links(tmp_path, taxdump):
    """A remapped row should be dropped when another ID of the same prefix links to that taxon directly.

    These are the MeSH registry rows for Collybia dryophila (pointing at merged NCBITaxon:71877) and
    Gymnopus dryophilus (pointing at its replacement, NCBITaxon:206318). The remapped Collybia row must not
    compete with the direct Gymnopus row, whichever comes first in the file.
    """
    concord = tmp_path / "concord"
    concord.write_text("MESH:C000668355\txref\tNCBITaxon:71877\nMESH:C000672803\txref\tNCBITaxon:206318\n")

    counts = update_obsolete_ncbitaxon_ids(str(concord), taxdump)

    assert assert_concordance_file_valid(str(concord)) == [["MESH:C000672803", "xref", "NCBITaxon:206318"]]
    assert counts["obsolete_ncbitaxon"]["dropped_merged_superseded_by_direct"] == 1


@pytest.mark.unit
def test_update_obsolete_ncbitaxon_ids_dedupes_remapped_rows(tmp_path, taxdump):
    """A row that duplicates another after remapping should only be written once."""
    concord = tmp_path / "concord"
    concord.write_text("UMLS:C2783570\teq\tNCBITaxon:650420\nUMLS:C2783570\teq\tNCBITaxon:650420\n")

    update_obsolete_ncbitaxon_ids(str(concord), taxdump)

    assert assert_concordance_file_valid(str(concord)) == [["UMLS:C2783570", "eq", "NCBITaxon:2761485"]]


# UMLS CONCORD


def run_build_taxon_umls_relationships(tmp_path, taxdump, cuis):
    mrconso = tmp_path / "MRCONSO.RRF"
    mrconso.write_text("".join(row + "\n" for row in MRCONSO_ROWS))
    idfile = tmp_path / "ids"
    idfile.write_text("".join(f"UMLS:{cui}\tbiolink:OrganismTaxon\n" for cui in cuis))
    outfile = tmp_path / "concord"
    metadata = tmp_path / "metadata.yaml"
    build_taxon_umls_relationships(str(mrconso), str(idfile), taxdump, str(outfile), str(metadata))
    return outfile, metadata


@pytest.mark.unit
def test_build_taxon_umls_relationships(tmp_path, taxdump):
    """UMLS taxon CUIs should be mapped to MeSH and to current NCBITaxon IDs, and other sources ignored."""
    outfile, metadata = run_build_taxon_umls_relationships(tmp_path, taxdump, TAXON_CUIS)

    assert {tuple(row) for row in assert_concordance_file_valid(str(outfile))} == {
        ("UMLS:C1110641", "eq", "NCBITaxon:3704"),
        ("UMLS:C0003062", "eq", "MESH:D000818"),
        ("UMLS:C0003062", "eq", "NCBITaxon:33208"),
        # Merged into Pancrustacea.
        ("UMLS:C0010395", "eq", "NCBITaxon:197562"),
        # C0331518's only NCBI ID was deleted, so it has no row.
    }
    counts = yaml.safe_load(metadata.read_text())["counts"]
    assert counts["obsolete_ncbitaxon"] == {
        "remapped_merged": 1,
        "dropped_merged_superseded_by_direct": 0,
        "dropped_deleted": 1,
    }


@pytest.mark.unit
def test_build_taxon_umls_relationships_skips_cuis_not_in_id_list(tmp_path, taxdump):
    """A CUI with an NCBI atom should not be mapped if it isn't one of the taxon UMLS IDs."""
    outfile, _ = run_build_taxon_umls_relationships(tmp_path, taxdump, ["C0003062"])

    assert "UMLS:C1110641" not in outfile.read_text()
