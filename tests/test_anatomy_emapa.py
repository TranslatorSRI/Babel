"""Unit tests for EMAPA ID typing, concord generation and clique typing in
``src.createcompendia.anatomy``.

All tests run offline (``unit``) by monkeypatching the UberGraph-facing calls.
"""

import pytest

import src.createcompendia.anatomy as anatomy
from src.babel_utils import read_badxrefs
from src.categories import ANATOMICAL_ENTITY, CELL, CELLULAR_COMPONENT, GROSS_ANATOMICAL_STRUCTURE
from src.prefixes import CL, EMAPA, GO, UBERON
from src.ubergraph import HIERARCHY_PART_OF

# Fixed part_of descendant closures keyed by root IRI. Mirrors UberGraph.get_subclasses_of,
# which returns the full transitive closure from the redundant graph.
_PART_OF_DESCENDANTS = {
    "EMAPA:0": ["EMAPA:35949", "EMAPA:35868", "EMAPA:100", "EMAPA:200", "EMAPA:300", "UBERON:9999"],
    "EMAPA:35949": ["EMAPA:100"],  # organ subtree
    "EMAPA:35868": ["EMAPA:200"],  # tissue subtree
}


class _FakeUberGraph:
    def get_subclasses_of(self, iri, hierarchy_predicate=None):
        if hierarchy_predicate == HIERARCHY_PART_OF:
            return [{"descendent": c} for c in _PART_OF_DESCENDANTS.get(iri, [])]
        # No is_a (subClassOf) links in this fixture.
        return []


# --- Per-CURIE biolink typing ---


@pytest.mark.unit
def test_write_emapa_ids_types_organ_and_tissue_as_gross(tmp_path, monkeypatch):
    monkeypatch.setattr(anatomy, "UberGraph", lambda *a, **k: _FakeUberGraph())
    outfile = tmp_path / "EMAPA"
    anatomy.write_emapa_ids(str(outfile))

    rows = [line.split("\t") for line in outfile.read_text().splitlines()]
    types = {curie: biolink for curie, biolink in rows}

    # Non-EMAPA descendants are filtered out.
    assert "UBERON:9999" not in types

    # Organ/tissue roots and their descendants are gross.
    for curie in ("EMAPA:35949", "EMAPA:35868", "EMAPA:100", "EMAPA:200"):
        assert types[curie] == "biolink:GrossAnatomicalStructure", curie

    # The root and unrelated terms default to AnatomicalEntity.
    assert types["EMAPA:0"] == "biolink:AnatomicalEntity"
    assert types["EMAPA:300"] == "biolink:AnatomicalEntity"

    # Output is sorted by CURIE for deterministic, clean diffs.
    curies = [curie for curie, _ in rows]
    assert curies == sorted(curies)


# --- Source registry ---


@pytest.mark.unit
def test_obo_id_roots_match_the_literals_they_replaced():
    """ANATOMY_OBO_SOURCES should resolve to exactly the root lists that used to be hardcoded.

    UBERON's GrossAnatomicalStructure branch and EMAPA's organ/tissue branches were once
    literals inside their write_*_ids() functions; they now live in the registry as
    ``subtype_roots``. Pinning the resolved lists is what makes that move checkable, and
    guards the registry as it grows to cover more of each ontology.
    """
    assert anatomy._obo_id_roots(UBERON) == [
        ("UBERON:0001062", ANATOMICAL_ENTITY),
        ("UBERON:0010000", GROSS_ANATOMICAL_STRUCTURE),
    ]
    assert anatomy._obo_id_roots(CL) == [("CL:0000000", CELL)]
    assert anatomy._obo_id_roots(GO) == [("GO:0005575", CELLULAR_COMPONENT)]
    assert anatomy._obo_id_roots(EMAPA) == [
        ("EMAPA:0", ANATOMICAL_ENTITY),
        ("EMAPA:35868", GROSS_ANATOMICAL_STRUCTURE),
        ("EMAPA:35949", GROSS_ANATOMICAL_STRUCTURE),
    ]


# --- Concord generation ---


@pytest.mark.unit
def test_build_emapa_obo_relationships_walks_part_of_with_ignore_list(monkeypatch):
    """build_emapa_obo_relationships() should walk part_of, not the default subClassOf.

    EMAPA is a partonomy, so a subClassOf walk finds only two terms. It must also apply
    ANATOMY_OBO_IGNORE_LIST, so the concord never picks up PMIDs, bare URLs or CL/GO
    xrefs. Both the real build and the EMAPA pipeline test fixture route through this
    function, so pinning its call keeps them from drifting apart.
    """
    captured = {}

    def _fake_build_sets(iri, concordfiles, set_type, **kwargs):
        captured["iri"] = iri
        captured["concordfiles"] = concordfiles
        captured["set_type"] = set_type
        captured.update(kwargs)

    monkeypatch.setattr(anatomy, "build_sets", _fake_build_sets)
    sentinel = object()
    anatomy.build_emapa_obo_relationships({EMAPA: sentinel})

    assert captured["iri"] == "EMAPA:0"
    assert captured["set_type"] == "xref"
    assert captured["hierarchy_predicate"] == HIERARCHY_PART_OF
    assert captured["ignore_list"] == anatomy.ANATOMY_OBO_IGNORE_LIST
    assert captured["concordfiles"] == {EMAPA: sentinel}


# --- Bad-xref filtering ---


@pytest.mark.unit
def test_anatomy_bad_xref_pairs_are_dropped_in_either_direction():
    """A pair listed in the bad-xrefs file should be dropped whichever way the concord writes it.

    The two shipped pairs come from different concords (UBERON writes
    ``UBERON:0001236 xref MESH:D019439``; UMLS writes ``UMLS:C0008503 eq GO:0042600``), so the
    filter must not assume an orientation. Matching only one direction would silently let the
    pair through if a source ever flipped it.
    """
    bad_pairs = {frozenset(("UBERON:0001236", "MESH:D019439"))}
    pair_filter = anatomy._make_anatomy_concord_pair_filter(bad_pairs)

    assert not pair_filter(["UBERON:0001236", "xref", "MESH:D019439"], "UBERON", {})
    assert not pair_filter(["MESH:D019439", "xref", "UBERON:0001236"], "UBERON", {})
    # An unlisted pair is kept.
    assert pair_filter(["UBERON:0001236", "xref", "MESH:D000313"], "UBERON", {})


@pytest.mark.unit
def test_anatomy_bad_xref_filter_still_applies_the_umls_go_rule():
    """Wrapping the UMLS<->GO rule must not disable it.

    UMLS<->GO pairs are only kept when both CURIEs are already in the clique state, so a pair
    whose members are absent from ``dicts`` is dropped even though it is not in the bad-xrefs
    file. Pinning this catches a refactor that returned True before delegating.
    """
    pair_filter = anatomy._make_anatomy_concord_pair_filter(set())

    assert not pair_filter(["UMLS:C0000001", "eq", "GO:0000001"], "UMLS", {})
    assert pair_filter(["UMLS:C0000001", "eq", "GO:0000001"], "UMLS", {"UMLS:C0000001": {}, "GO:0000001": {}})


@pytest.mark.unit
def test_badxrefs_path_is_threaded_through_to_clique_building(tmp_path):
    """A pair in the bad-xrefs file should stop the merge in ``compute_cliques_for_impact_report``.

    The filter is unit-tested above in isolation, but the value of the file depends on the whole
    chain — path argument, ``read_badxrefs``, frozenset conversion, the ``concord_pair_filter``
    hook, ``glom_from_files`` — actually being connected. Running the same two ids files and
    concord with and without the file is the cheapest assertion that it is: without, the two
    CURIEs land in one clique; with, they stay apart.
    """
    ids = tmp_path / "UBERON"
    ids.write_text(f"UBERON:0001236\t{GROSS_ANATOMICAL_STRUCTURE}\nMESH:D019439\t{CELL}\n")
    concord = tmp_path / "UBERON_concord"
    concord.write_text("UBERON:0001236\txref\tMESH:D019439\n")
    badxrefs = tmp_path / "badxrefs.txt"
    badxrefs.write_text("# drop it\nUBERON:0001236 MESH:D019439\n")

    # Passing "" disables the filter, so this is the un-suppressed baseline.
    merged, _ = anatomy.compute_cliques_for_impact_report([str(concord)], [str(ids)], badxrefs="")
    assert merged["UBERON:0001236"] == merged["MESH:D019439"], "expected the xref to merge them without the file"

    split, _ = anatomy.compute_cliques_for_impact_report([str(concord)], [str(ids)], badxrefs=str(badxrefs))
    assert split["UBERON:0001236"] != split["MESH:D019439"], "the bad-xrefs file should have blocked the merge"


@pytest.mark.unit
def test_read_badxrefs_rejects_a_line_that_is_not_a_pair(tmp_path):
    """A malformed entry should raise, not be skipped.

    Skipping it would mean a maintainer's suppression silently does nothing — the bad xref
    reappears in the compendia with nothing anywhere saying why. A tab instead of a space is
    the easy way to write one, so that is the case used here. Blank and comment lines must
    still be tolerated; the shipped anatomy file has both.
    """
    bad = tmp_path / "badxrefs.txt"
    bad.write_text("# fine\n\nUBERON:0001236\tMESH:D019439\n")

    with pytest.raises(ValueError, match="not a tab"):
        read_badxrefs(str(bad))


@pytest.mark.unit
def test_read_badxrefs_tolerates_repeated_spaces(tmp_path):
    """A stray double space should parse, not fail the build.

    Only tabs are rejected. A run of spaces is still unambiguously two CURIEs, so raising on it
    would fail a build over a harmless typo; ``split()`` collapses the run.
    """
    ok = tmp_path / "badxrefs.txt"
    ok.write_text("UBERON:0001236   MESH:D019439\n")

    assert read_badxrefs(str(ok)) == {("UBERON:0001236", "MESH:D019439")}


@pytest.mark.unit
def test_shipped_anatomy_badxrefs_file_parses_and_lists_the_known_pairs():
    """The committed bad-xrefs file should parse and contain the conflations it documents.

    Each entry exists to stop a gross anatomical structure being merged with a cell or cellular
    component; if either silently disappeared, the merge would come back.
    """
    pairs = {frozenset(pair) for pair in read_badxrefs(anatomy.ANATOMY_BAD_XREFS)}

    assert frozenset(("UBERON:0001236", "MESH:D019439")) in pairs
    assert frozenset(("UMLS:C0008503", "GO:0042600")) in pairs
    assert frozenset(("UBERON:0000125", "NCIT:C13197")) in pairs


# --- Clique typing ---


@pytest.mark.unit
def test_classify_anatomy_clique_trusts_emapa_when_no_other_ontology_is_typed():
    """EMAPA should decide a clique's type when no GO/CL/UBERON member carries one.

    ``classify_anatomy_clique()`` trusts source ontologies in the order GO, CL, UBERON,
    EMAPA. EMAPA is last, so it only speaks for cliques the three established ontologies
    are silent on -- typically an EMAPA term joined to untyped MESH/UMLS CURIEs.
    """
    equivalent_ids = ["MESH:D000001", "EMAPA:35949"]
    types = {"EMAPA:35949": GROSS_ANATOMICAL_STRUCTURE}

    assert anatomy.classify_anatomy_clique(equivalent_ids, types) == GROSS_ANATOMICAL_STRUCTURE


@pytest.mark.unit
def test_classify_anatomy_clique_prefers_uberon_over_emapa():
    """A typed UBERON member should outrank a typed EMAPA member.

    This is the property that keeps adding EMAPA from retyping established cliques: EMAPA
    sits behind UBERON in the precedence list, so it can only add typing, never override it.
    """
    equivalent_ids = ["UBERON:0001062", "EMAPA:35949"]
    types = {"UBERON:0001062": ANATOMICAL_ENTITY, "EMAPA:35949": GROSS_ANATOMICAL_STRUCTURE}

    assert anatomy.classify_anatomy_clique(equivalent_ids, types) == ANATOMICAL_ENTITY


@pytest.mark.unit
def test_classify_anatomy_clique_prefers_go_and_cl_over_emapa():
    """GO and CL should both outrank EMAPA, in that order."""
    equivalent_ids = ["CL:0000000", "EMAPA:35949"]
    types = {"CL:0000000": CELL, "EMAPA:35949": GROSS_ANATOMICAL_STRUCTURE}

    assert anatomy.classify_anatomy_clique(equivalent_ids, types) == CELL


@pytest.mark.unit
def test_classify_anatomy_clique_returns_none_when_nothing_is_typed():
    """A clique whose members all lack a declared type should classify as None."""
    assert anatomy.classify_anatomy_clique(["MESH:D000001", "EMAPA:35949"], {}) is None


@pytest.mark.unit
def test_anatomy_obo_ignore_list_entries_are_upper_case():
    """build_sets() matches ignore_list against Text.get_prefix_or_none(), which upper-cases.

    A lower-case entry would silently never match and the prefix it was meant to block
    would be written to the concord, so build_sets() rejects one outright.
    """
    assert all(prefix == prefix.upper() for prefix in anatomy.ANATOMY_OBO_IGNORE_LIST)
