"""Tests for src/createcompendia/anatomy.py.

EMAPA typing, the bad-xrefs filter and the GO/CL/UBERON/EMAPA typing precedence are covered in
``tests/test_anatomy_emapa.py``; the EMAPA ids/concord extraction in
``tests/pipeline/test_emapa_pipeline.py``. This file covers the rest of the module: the Wikidata
cell concord, the ids-file type maps, the parts of clique typing that fall through precedence, and
an end-to-end ``build_compendia()`` run.
"""

import json
import os
import zlib
from unittest.mock import MagicMock, patch

import pytest

import src.util
from src.categories import ANATOMICAL_ENTITY, CELL, CELLULAR_COMPONENT, GROSS_ANATOMICAL_STRUCTURE
from src.createcompendia import anatomy
from src.metadata.provenance import write_concord_metadata
from src.prefixes import CL, EMAPA, GO, MESH, NCIT, UBERON, UMLS, WIKIDATA
from src.util import ensure_parent_dir, get_config


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


# CLIQUE TYPING BEYOND THE PRECEDENCE LIST


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


# SOURCE ROOTS


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
