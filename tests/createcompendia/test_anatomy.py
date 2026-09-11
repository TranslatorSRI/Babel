"""Tests for src/createcompendia/anatomy.py."""

import pytest

from src.createcompendia.anatomy import build_wikidata_cell_relationships
from src.prefixes import CL, UMLS, WIKIDATA

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
