"""Tests for ``src.createcompendia.anatomy``.

Covers the anatomy pipeline's per-CURIE typing, its concord generation (EMAPA's part_of walk and
the Wikidata cell mappings), the ids-file type maps, bad-xref filtering, clique typing, and an
end-to-end ``build_compendia()`` run. The EMAPA ids/concord extraction against live UberGraph data
is in ``tests/pipeline/test_emapa_pipeline.py``.

Everything here runs offline except the tests marked ``network``, which query the live FRINK and
UberGraph endpoints or fetch the Biolink Model.
"""

import json
import os
import zlib
from unittest.mock import MagicMock, patch

import pytest

import src.createcompendia.anatomy as anatomy
import src.util
from src.babel_utils import read_badxrefs
from src.categories import ANATOMICAL_ENTITY, CELL, CELLULAR_COMPONENT, GROSS_ANATOMICAL_STRUCTURE
from src.metadata.provenance import write_concord_metadata
from src.prefixes import CL, EMAPA, GO, MESH, NCIT, UBERON, UMLS, WIKIDATA
from src.ubergraph import HIERARCHY_PART_OF
from src.util import ensure_parent_dir, get_config

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


def _sparql_response(rows):
    """Stand in for the FRINK endpoint's JSON response carrying ``rows`` of (umls, cl) values."""
    response = MagicMock(ok=True)
    response.json.return_value = {
        "results": {
            "bindings": [
                {
                    "wd": {"value": f"http://www.wikidata.org/entity/Q{i}"},
                    "umls": {"value": umls_id},
                    "cl": {"value": f"http://purl.obolibrary.org/obo/CL_{cl_id}"},
                }
                for i, (umls_id, cl_id) in enumerate(rows)
            ]
        }
    }
    return response


def _read_concord(path):
    with open(path) as f:
        return [line.rstrip("\n").split("\t") for line in f]


# PER-CURIE BIOLINK TYPING


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


# SOURCE ROOTS


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


@pytest.mark.network
@pytest.mark.parametrize("prefix", [UBERON, CL, GO, EMAPA])
def test_anatomy_source_roots_still_have_descendants_in_ubergraph(ubergraph, prefix):
    """Every root in ANATOMY_OBO_SOURCES should still resolve to a populated subtree.

    An obsoleted or renumbered root does not error -- it returns nothing, and the source quietly
    contributes no identifiers to the build. Subtype roots are checked too, since one of those
    going missing would silently demote a whole branch to the source's default type.
    """
    from src.ubergraph import HIERARCHY_PART_OF, HIERARCHY_SUBCLASS_OF

    # EMAPA is a part_of partonomy; a subClassOf walk over it reaches almost nothing.
    predicate = HIERARCHY_PART_OF if prefix == EMAPA else HIERARCHY_SUBCLASS_OF
    for root, _biolink_type in anatomy._obo_id_roots(prefix):
        descendants = ubergraph.get_subclasses_of(root, hierarchy_predicate=predicate)
        assert descendants, f"{prefix} root {root} has no descendants in UberGraph"


# CONCORD GENERATION


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


# WIKIDATA CELL RELATIONSHIPS


@pytest.mark.network
def test_build_wikidata_cell_relationships(tmp_path):
    """The Wikidata CL/UMLS SPARQL endpoint should still answer and yield usable concord rows."""
    anatomy.build_wikidata_cell_relationships(str(tmp_path), str(tmp_path / "wikidata.yaml"))

    rows = _read_concord(tmp_path / WIKIDATA)
    assert len(rows) > 100, "Expected hundreds of unique UMLS/CL pairs, got a near-empty concord"
    for umls_curie, relation, cl_curie in rows:
        assert umls_curie.startswith(f"{UMLS}:")
        assert relation == "eq"
        assert cl_curie.startswith(f"{CL}:")


@pytest.mark.unit
def test_wikidata_cell_concord_keeps_only_one_to_one_pairs(tmp_path):
    """A UMLS CUI or CL term appearing in more than one pair should take both its pairs out.

    One Wikidata item carrying two CL ids (or two items carrying the same CL) would glom the two
    cells together through the shared UMLS CUI, so the whole ambiguous group is dropped rather
    than picking a winner. Only the unambiguous pair survives here.
    """
    rows = [
        ("C0000001", "0000001"),  # 1:1 -- kept
        ("C0000002", "0000002"),  # C0000002 also maps to CL:0000003 below
        ("C0000002", "0000003"),
        ("C0000004", "0000004"),  # CL:0000004 is also claimed by C0000005 below
        ("C0000005", "0000004"),
    ]
    with patch.object(anatomy.requests, "post", return_value=_sparql_response(rows)):
        anatomy.build_wikidata_cell_relationships(str(tmp_path), str(tmp_path / "wikidata.yaml"))

    assert _read_concord(tmp_path / WIKIDATA) == [[f"{UMLS}:C0000001", "eq", f"{CL}:0000001"]]


@pytest.mark.unit
def test_wikidata_cell_concord_raises_on_a_failed_query(tmp_path):
    """A non-OK response should raise rather than write an empty concord.

    An empty concord is indistinguishable from "Wikidata has no cell mappings" to every later
    rule, so the failure has to surface here or the build silently loses every UMLS<->CL edge.
    """
    with (
        patch.object(anatomy.requests, "post", return_value=MagicMock(ok=False, status_code=503, reason="Unavailable")),
        pytest.raises(RuntimeError, match="Could not query"),
    ):
        anatomy.build_wikidata_cell_relationships(str(tmp_path), str(tmp_path / "wikidata.yaml"))


@pytest.mark.unit
def test_wikidata_cell_concord_raises_on_an_unparseable_response(tmp_path):
    """A 200 response that isn't JSON should raise, naming the URL -- SPARQL endpoints answer
    HTML error pages with a 200 often enough that this is a real failure mode."""
    response = MagicMock(ok=True, content=b"<html>Gateway</html>")
    response.json.side_effect = ValueError("not json")
    with (
        patch.object(anatomy.requests, "post", return_value=response),
        pytest.raises(RuntimeError, match="Could not parse"),
    ):
        anatomy.build_wikidata_cell_relationships(str(tmp_path), str(tmp_path / "wikidata.yaml"))


# IDS-FILE TYPE MAPS


@pytest.mark.unit
def test_mesh_anatomy_tree_map_types_cells_and_cell_components():
    """MeSH anatomy trees A01-A20 should be anatomical entities, with A11 cells and A11.284
    cell components carved out -- the finer tree number has to win over its parent."""
    with patch.object(anatomy.mesh, "write_ids") as write_ids:
        anatomy.write_mesh_ids("/dev/null")

    meshmap = write_ids.call_args.args[0]
    # The 20 top-level trees, plus the A11.284 subtree carved out as cell components.
    assert set(meshmap) == {f"A{i:02d}" for i in range(1, 21)} | {"A11.284"}
    assert meshmap["A11"] == CELL
    assert meshmap["A11.284"] == CELLULAR_COMPONENT
    assert {meshmap[f"A{i:02d}"] for i in range(1, 21)} - {CELL} == {ANATOMICAL_ENTITY}
    assert "A21" not in meshmap and "A00" not in meshmap


@pytest.mark.unit
def test_umls_semantic_tree_map_types_cells_and_cell_components():
    """The UMLS semantic-type tree numbers anatomy claims should map to the three anatomy types.

    A1.2.3.3 (Cell) and A1.2.3.4 (Cell Component) are the carve-outs; everything else anatomy
    claims is an anatomical entity. A1.2.3 (Fully Formed Anatomical Structure) is deliberately
    absent -- it is the parent of the cell types, so claiming it would type cells as anatomical
    entities via their ancestor.
    """
    with patch.object(anatomy.umls, "write_umls_ids") as write_umls_ids:
        anatomy.write_umls_ids("mrsty.rrf", "/dev/null")

    umlsmap = write_umls_ids.call_args.args[1]
    assert umlsmap["A1.2.3.3"] == CELL
    assert umlsmap["A1.2.3.4"] == CELLULAR_COMPONENT
    assert "A1.2.3" not in umlsmap
    assert {t for stn, t in umlsmap.items() if stn not in ("A1.2.3.3", "A1.2.3.4")} == {ANATOMICAL_ENTITY}


@pytest.mark.unit
def test_ncit_ids_exclude_the_non_anatomy_branches():
    """NCIT's three anatomy roots should be taken, minus the branches that are not anatomy.

    Genomic loci, chromosome bands and protein domains all sit under NCIT's anatomy root but are
    not anatomical entities; C13717 ("Anatomic Site") is the site of a procedure rather than a
    structure. Each was added after it over-glommed something, so pin the whole exclusion list.
    """
    with patch.object(anatomy, "write_obo_ids") as write_obo_ids:
        anatomy.write_ncit_ids("/dev/null")

    roots, _outfile = write_obo_ids.call_args.args
    assert roots == [
        (f"{NCIT}:C12219", ANATOMICAL_ENTITY),
        (f"{NCIT}:C12508", CELL),
        (f"{NCIT}:C34070", CELLULAR_COMPONENT),
    ]
    assert set(write_obo_ids.call_args.kwargs["exclude"]) == {
        f"{NCIT}:C64389",
        f"{NCIT}:C13432",
        f"{NCIT}:C14134",
        f"{NCIT}:C122638",
        f"{NCIT}:C13377",
        f"{NCIT}:C13717",
    }


# BAD-XREF FILTERING


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
    """The committed bad-xrefs file should parse and contain the two conflations it documents.

    Both entries exist to stop a gross anatomical structure being merged with a cell or cellular
    component; if either silently disappeared, the merge would come back.
    """
    pairs = {frozenset(pair) for pair in read_badxrefs(anatomy.ANATOMY_BAD_XREFS)}

    assert frozenset(("UBERON:0001236", "MESH:D019439")) in pairs
    assert frozenset(("UMLS:C0008503", "GO:0042600")) in pairs


@pytest.mark.unit
def test_anatomy_obo_ignore_list_entries_are_upper_case():
    """build_sets() matches ignore_list against Text.get_prefix_or_none(), which upper-cases.

    A lower-case entry would silently never match and the prefix it was meant to block
    would be written to the concord, so build_sets() rejects one outright.
    """
    assert all(prefix == prefix.upper() for prefix in anatomy.ANATOMY_OBO_IGNORE_LIST)


# CLIQUE TYPING


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
def test_classify_anatomy_clique_takes_a_majority_vote_when_no_trusted_prefix_is_typed():
    """With no typed GO/CL/UBERON/EMAPA member, the most common declared type should win.

    MESH and UMLS carry types too, and a clique built only from them still has to be typed;
    the vote is what does it.
    """
    equivalent_ids = [f"{MESH}:D000001", f"{MESH}:D000002", f"{UMLS}:C0000001"]
    types = {
        f"{MESH}:D000001": CELL,
        f"{MESH}:D000002": CELL,
        f"{UMLS}:C0000001": ANATOMICAL_ENTITY,
    }

    assert anatomy.classify_anatomy_clique(equivalent_ids, types) == CELL


@pytest.mark.unit
def test_classify_anatomy_clique_breaks_a_tied_vote_towards_the_most_specific_type():
    """A tied vote should be broken by the CellularComponent/Cell/GrossAnatomicalStructure/
    AnatomicalEntity precedence, not by dict or set iteration order.

    Both candidates have one vote here, so an implementation that took whichever type it saw
    first would pass or fail depending on hash ordering. CellularComponent is the most specific
    of the two and must win every time.
    """
    equivalent_ids = [f"{MESH}:D000001", f"{UMLS}:C0000001"]
    types = {f"{MESH}:D000001": ANATOMICAL_ENTITY, f"{UMLS}:C0000001": CELLULAR_COMPONENT}

    assert anatomy.classify_anatomy_clique(equivalent_ids, types) == CELLULAR_COMPONENT
    # Same clique, opposite insertion order: the answer must not move.
    assert anatomy.classify_anatomy_clique(list(reversed(equivalent_ids)), dict(reversed(types.items()))) == (
        CELLULAR_COMPONENT
    )


@pytest.mark.unit
def test_classify_anatomy_clique_returns_none_when_nothing_is_typed():
    """A clique whose members all lack a declared type should classify as None."""
    assert anatomy.classify_anatomy_clique(["MESH:D000001", "EMAPA:35949"], {}) is None


@pytest.mark.unit
def test_create_typed_sets_groups_cliques_under_their_classified_type():
    """Each clique should land in the bucket its classification names, keyed by biolink type."""
    cell = frozenset([f"{CL}:0000001", f"{MESH}:D000001"])
    gross = frozenset([f"{UBERON}:0000001"])
    types = {f"{CL}:0000001": CELL, f"{UBERON}:0000001": GROSS_ANATOMICAL_STRUCTURE}

    typed_sets = anatomy.create_typed_sets({cell, gross}, types)

    assert typed_sets == {CELL: {cell}, GROSS_ANATOMICAL_STRUCTURE: {gross}}


@pytest.mark.unit
def test_create_typed_sets_raises_on_a_clique_no_member_types():
    """An untypable clique should raise, naming the clique.

    Anatomy deliberately differs from diseasephenotype here, which drops such a clique with a
    warning: every anatomy ids file writes an explicit type, so an untyped clique means an ids
    file lost its type column rather than a source legitimately having nothing to say. Invert
    this assertion if anatomy ever gains a source that cannot type its own identifiers.
    """
    with pytest.raises(RuntimeError, match="Cannot assign a biolink type"):
        anatomy.create_typed_sets({frozenset([f"{MESH}:D000001"])}, {})


# END-TO-END COMPENDIUM BUILD


@pytest.fixture
def anatomy_build_dir(tmp_path):
    """Point Babel's download and output directories at a temporary tree, with empty common files.

    write_compendium()'s factories fall back to the common (UberGraph) labels/synonyms for CURIEs
    with none of their own; building the real ones is a large download, so stand in empty files.
    """
    config = dict(get_config())
    config["download_directory"] = str(tmp_path / "babel_downloads")
    config["output_directory"] = str(tmp_path / "babel_outputs")
    for common_files in config["common"].values():
        for common_file in common_files:
            path = os.path.join(config["download_directory"], "common", common_file)
            ensure_parent_dir(path)
            if common_file.endswith(".gz"):
                with open(path, "wb") as f:
                    f.write(zlib.compress(b"", wbits=zlib.MAX_WBITS | 16))
            else:
                open(path, "w").close()

    original = src.util.config_yaml
    src.util.config_yaml = config
    yield tmp_path, config
    src.util.config_yaml = original


@pytest.mark.network
def test_build_compendia_writes_one_typed_clique_per_output(anatomy_build_dir):
    """A UBERON/MESH concord and a CL ids file should build into two typed compendium files.

    This is the whole back half of the pipeline in one call -- glom_from_files, the type
    precedence, create_typed_sets and write_compendium -- over files in the same formats the
    Snakemake rule passes. Marked network because write_compendium builds a NodeFactory, which
    fetches the Biolink Model.
    """
    tmp_path, config = anatomy_build_dir
    ids_dir, concord_dir = tmp_path / "ids", tmp_path / "concords"
    ids_dir.mkdir(), concord_dir.mkdir()

    # UBERON:0000955 "brain" and its MeSH equivalent, plus an unrelated cell.
    (ids_dir / UBERON).write_text(f"{UBERON}:0000955\t{GROSS_ANATOMICAL_STRUCTURE}\n")
    (ids_dir / MESH).write_text(f"{MESH}:D001921\t{ANATOMICAL_ENTITY}\n")
    (ids_dir / CL).write_text(f"{CL}:0000540\t{CELL}\n")
    (concord_dir / UBERON).write_text(f"{UBERON}:0000955\teq\t{MESH}:D001921\n")

    metadata_yaml = str(concord_dir / f"metadata-{UBERON}.yaml")
    write_concord_metadata(metadata_yaml, name="test", concord_filename=str(concord_dir / UBERON))
    icrdf = tmp_path / "icRDF.tsv"
    icrdf.write_text("")

    anatomy.build_compendia(
        [str(concord_dir / UBERON)],
        [metadata_yaml],
        [str(ids_dir / p) for p in (UBERON, MESH, CL)],
        str(icrdf),
        badxrefs="",
    )

    compendia = os.path.join(config["output_directory"], "compendia")
    with open(os.path.join(compendia, "GrossAnatomicalStructure.txt")) as f:
        gross = [json.loads(line) for line in f]
    with open(os.path.join(compendia, "Cell.txt")) as f:
        cells = [json.loads(line) for line in f]

    # The UBERON member decides the type for the whole merged clique, and MESH comes along with it.
    assert len(gross) == 1
    assert gross[0]["type"] == GROSS_ANATOMICAL_STRUCTURE
    assert {i["i"] for i in gross[0]["identifiers"]} == {f"{UBERON}:0000955", f"{MESH}:D001921"}

    assert len(cells) == 1
    assert {i["i"] for i in cells[0]["identifiers"]} == {f"{CL}:0000540"}
