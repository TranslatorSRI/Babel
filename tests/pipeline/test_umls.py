"""UMLS-specific pipeline tests (issue #675 extension).

The generic non-empty and mutual-exclusivity tests for all seven UMLS compendia
(chemicals, protein, anatomy, disease/phenotype, process/activity, taxon, gene)
are in test_vocabulary_partitioning.py.  This file contains only the
UMLS-specific targeted test that has no generic equivalent.

All tests require UMLS_API_KEY to be set for the initial download (or the
files to already be cached in babel_downloads/UMLS/).  They are skipped by
default.  Run with:
    uv run pytest tests/pipeline/test_umls.py --pipeline --no-cov -v
"""

import pytest

from tests.pipeline.conftest import get_curies_from_ids_file


@pytest.mark.pipeline
def test_chemicals_excludes_protein_semantic_tree(umls_pipeline_outputs):
    """Chemicals must not contain any UMLS IDs that the protein compendium claimed.

    This is the chemicals/protein edge of the mutual-exclusivity invariant, stated
    explicitly so that a failure message immediately names the semantic-tree involved
    (A1.4.1.2.1.7, Amino Acid/Peptide/Protein).  Unlike test_no_id_in_multiple_compendia,
    this test has no KNOWN_DUPLICATES carve-out — a chem/protein UMLS overlap is always
    a hard failure here, making it a stricter sentinel for this specific pair.

    Since MeSH ownership (umls.apply_mesh_ownership) T116 CUIs whose descriptor is a
    chemical are claimed by chemicals, not protein, so disjointness now holds *because*
    each such CUI moved, not because chemicals blocklists the whole semantic tree.
    """
    chem_ids = get_curies_from_ids_file(umls_pipeline_outputs["chemicals"])
    prot_ids = get_curies_from_ids_file(umls_pipeline_outputs["protein"])
    overlap = chem_ids & prot_ids
    assert len(overlap) == 0, (
        f"Found {len(overlap)} IDs in both chemicals and protein UMLS outputs: {sorted(overlap)[:10]}"
    )


@pytest.mark.pipeline
def test_cui_follows_its_mesh_descriptor(umls_pipeline_outputs):
    """A CUI whose semantic type and MeSH descriptor point at different pipelines should be claimed by
    the descriptor's pipeline only (issues #308 and #1123):

    - UMLS:C0242726 "Plant Roots" is T002 Plant, but MESH:D018517 is anatomy's (A18), so anatomy, not taxon.
    - UMLS:C0000608 "Aminocaproic Acid" is T116, but MESH:D015119 is a chemical D-tree descriptor, so
      chemicals, not protein.

    and a CUI the gene pipeline selects stays there, because gene never concords UMLS to MeSH:

    - UMLS:C1413234 "CD68 gene" is T028, and protein alone claims its MESH:D000097382 "CD68 Molecule", so
      gene, not protein.
    """
    anatomy_ids = get_curies_from_ids_file(umls_pipeline_outputs["anatomy"])
    taxon_ids = get_curies_from_ids_file(umls_pipeline_outputs["taxon"])
    chem_ids = get_curies_from_ids_file(umls_pipeline_outputs["chemicals"])
    prot_ids = get_curies_from_ids_file(umls_pipeline_outputs["protein"])
    assert "UMLS:C0242726" in anatomy_ids and "UMLS:C0242726" not in taxon_ids
    assert "UMLS:C0000608" in chem_ids and "UMLS:C0000608" not in prot_ids
    gene_ids = get_curies_from_ids_file(umls_pipeline_outputs["gene"])
    assert "UMLS:C1413234" in gene_ids and "UMLS:C1413234" not in prot_ids
