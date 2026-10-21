"""Pipeline tests for the taxon concords, run against a real build.

Skipped by default unless pytest is run with --pipeline.  Run with:
    uv run pytest tests/pipeline/test_taxon.py --pipeline --no-cov -v

See docs/sources/NCBITaxon/README.md for why these hold.
"""

import os

import pytest

from src.prefixes import NCBITAXON
from tests.pipeline.conftest import _intermediate_concord_path, _intermediate_id_path

TAXON_CONCORDS_WITH_NCBITAXON = ["NCBI_MESH", "UMLS"]


def _built(path, rule):
    if not os.path.exists(path):
        pytest.skip(f"{path} not built; run `uv run snakemake -c all {rule}` first")
    return path


def _ncbitaxon_curies(concord_path):
    with open(concord_path) as inf:
        return [
            curie for line in inf for curie in line.rstrip("\n").split("\t")[0::2] if curie.startswith(f"{NCBITAXON}:")
        ]


@pytest.mark.pipeline
def test_umls_taxon_concord_maps_to_ncbitaxon():
    """The UMLS taxon concord should link UMLS CUIs to NCBITaxon, not just to MeSH.

    UMLS calls NCBI Taxonomy "NCBI" in MRCONSO; looking it up as "NCBITaxon" silently produced no rows at
    all, which left ~700,000 UMLS taxa as single-identifier cliques.
    """
    concord = _built(_intermediate_concord_path("taxon", "UMLS"), "get_taxon_umls_relationships")
    count = len(_ncbitaxon_curies(concord))
    assert count > 500_000, f"only {count:,} UMLS -> NCBITaxon rows in {concord}"


@pytest.mark.pipeline
@pytest.mark.parametrize("concord_name", TAXON_CONCORDS_WITH_NCBITAXON)
def test_taxon_concords_only_use_current_ncbitaxon_ids(concord_name):
    """Every NCBITaxon CURIE in a taxon concord should be a current NCBI Taxonomy ID.

    MeSH and UMLS both cite IDs NCBI has since merged or deleted; update_obsolete_ncbitaxon_ids() replaces
    or drops them. One that slipped through would become an unlabeled NCBITaxon identifier competing with
    the current ID for the same MeSH or UMLS identifier.
    """
    concord = _built(_intermediate_concord_path("taxon", concord_name), "taxon")
    ids_file = _built(_intermediate_id_path("taxon", NCBITAXON), "taxon_ncbi_ids")
    with open(ids_file) as inf:
        current = {line.split("\t")[0] for line in inf}

    stale = sorted(set(_ncbitaxon_curies(concord)) - current)
    assert not stale, f"{len(stale):,} non-current NCBITaxon IDs in {concord}, e.g. {stale[:10]}"
