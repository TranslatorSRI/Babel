"""Unit tests for src/createcompendia/processactivitypathway.py."""

import pytest

from src.createcompendia.processactivitypathway import _process_concord_pair_filter

# CONCORD PAIR FILTER


@pytest.mark.unit
@pytest.mark.parametrize("pair", [("GO:0034227", "EC:2.8.1.4"), ("EC:2.8.1.4", "GO:0034227")])
def test_concord_pair_filter_drops_bad_concords(pair):
    """A BAD_CONCORDS pair should be dropped whichever way round the concord writes it."""
    assert not _process_concord_pair_filter([pair[0], "xref", pair[1]], "concords/GO", {})


@pytest.mark.unit
def test_concord_pair_filter_keeps_other_pairs():
    """Any other non-UMLS pair should be kept, even when neither CURIE is in the clique state yet."""
    assert _process_concord_pair_filter(["GO:0140741", "xref", "EC:2.8.1.4"], "concords/GO", {})


@pytest.mark.unit
def test_concord_pair_filter_requires_known_umls_pair():
    """A UMLS concord pair should be kept only when both CURIEs are already in the clique state."""
    parts = ["UMLS:C1158785", "eq", "GO:0045943"]
    assert not _process_concord_pair_filter(parts, "concords/UMLS", {"UMLS:C1158785": set()})
    assert _process_concord_pair_filter(parts, "concords/UMLS", {"UMLS:C1158785": set(), "GO:0045943": set()})
