"""Tests for src/createcompendia/anatomy.py."""

import pytest

from src.categories import ANATOMICAL_ENTITY, CELL, CELLULAR_COMPONENT, GROSS_ANATOMICAL_STRUCTURE
from src.createcompendia.anatomy import (
    ANATOMY_OBO_IGNORE_LIST,
    ANATOMY_OBO_SOURCES,
    UBERON_OBO_IGNORE_LIST,
    build_wikidata_cell_relationships,
    get_anatomy_extra_prefixes,
    get_anatomy_xref_prefix_map,
)
from src.prefixes import CL, EMAPA, FMA, GO, SNOMEDCT, UBERON, UMLS, WIKIDATA
from src.util import get_biolink_model_toolkit, get_config

# XREF PREFIX RENAMES AND IGNORE LISTS


@pytest.mark.unit
def test_uberon_sctid_xrefs_are_renamed_to_snomedct():
    """UBERON's SCTID xrefs should be renamed to SNOMEDCT, the spelling the UMLS concord uses.

    Without the rename no UBERON SNOMED xref can join a UMLS concept (docs/sources/UBERON/umls-joins/README.md).
    """
    assert get_anatomy_xref_prefix_map(UBERON) == {"SCTID": SNOMEDCT}


@pytest.mark.unit
def test_every_anatomy_obo_xref_source_has_a_rename_entry():
    """Every source whose xrefs build_anatomy_obo_relationships() writes should have an
    anatomy_xref_prefixes entry, so a missing one fails here rather than at build time."""
    for source in (UBERON, GO, EMAPA):
        assert source in ANATOMY_OBO_SOURCES
        assert isinstance(get_anatomy_xref_prefix_map(source), dict)


@pytest.mark.unit
def test_fma_xrefs_are_allowed_for_uberon_only():
    """UBERON's FMA xrefs should be kept while GO and EMAPA still ignore FMA, and the UBERON list
    should otherwise match the shared one."""
    assert FMA in ANATOMY_OBO_IGNORE_LIST
    assert FMA not in UBERON_OBO_IGNORE_LIST
    assert set(UBERON_OBO_IGNORE_LIST) == set(ANATOMY_OBO_IGNORE_LIST) - {FMA}


# EXTRA PREFIXES


@pytest.mark.unit
def test_fma_is_shipped_as_an_extra_prefix_for_gross_anatomical_structure_only():
    """FMA should be an extra prefix for GrossAnatomicalStructure and for no other anatomy class.

    Biolink registers FMA for AnatomicalEntity but not GrossAnatomicalStructure, so without the extra
    prefix an FMA CURIE in a clique that joins an UBERON gross anatomy term is dropped (#1134). The
    allowlist overrides Biolink, so pin it: update this test alongside the config, not instead of it."""
    assert get_config()["anatomy_extra_prefixes_by_biolink_class"] == {GROSS_ANATOMICAL_STRUCTURE: [FMA]}
    assert get_anatomy_extra_prefixes(GROSS_ANATOMICAL_STRUCTURE) == [FMA]
    for biotype in (ANATOMICAL_ENTITY, CELL, CELLULAR_COMPONENT):
        assert get_anatomy_extra_prefixes(biotype) == []


@pytest.mark.network
def test_fma_extra_prefix_is_still_needed():
    """The FMA extra prefix should be removed once Biolink registers FMA for GrossAnatomicalStructure.

    A stale entry keeps FMA from competing for the preferred identifier in its registered position, and
    nothing else would notice. When this fails, remove the entry from config.yaml:
    anatomy_extra_prefixes_by_biolink_class and close https://github.com/NCATSTranslator/Babel/issues/1134
    (biolink/biolink-model#1827)."""
    toolkit = get_biolink_model_toolkit(get_config()["biolink_version"])
    assert FMA in toolkit.get_element("anatomical entity").id_prefixes
    assert FMA not in toolkit.get_element("gross anatomical structure").id_prefixes


# WIKIDATA CELL RELATIONSHIPS


@pytest.mark.network
def test_build_wikidata_cell_relationships(tmp_path):
    """The Wikidata CL/UMLS SPARQL endpoint should still answer and yield usable concord rows."""
    build_wikidata_cell_relationships(str(tmp_path), str(tmp_path / "wikidata.yaml"))

    lines = (tmp_path / WIKIDATA).read_text().splitlines()
    assert len(lines) > 100, "Expected thousands of unique UMLS/CL pairs, got a near-empty concord"
    for line in lines:
        umls_curie, relation, cl_curie = line.split("\t")
        assert umls_curie.startswith(f"{UMLS}:")
        assert relation == "eq"
        assert cl_curie.startswith(f"{CL}:")
