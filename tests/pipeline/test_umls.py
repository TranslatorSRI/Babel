"""UMLS-specific pipeline tests (issue #675 extension).

The generic non-empty and mutual-exclusivity tests for all eight UMLS compendia
(chemicals, chemical role, protein, anatomy, disease/phenotype, process/activity, taxon, gene)
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
    """
    chem_ids = get_curies_from_ids_file(umls_pipeline_outputs["chemicals"])
    prot_ids = get_curies_from_ids_file(umls_pipeline_outputs["protein"])
    overlap = chem_ids & prot_ids
    assert len(overlap) == 0, (
        f"Found {len(overlap)} IDs in both chemicals and protein UMLS outputs: {sorted(overlap)[:10]}"
    )
