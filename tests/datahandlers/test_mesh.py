"""Unit tests for src/datahandlers/mesh.py

Tests cover:
  - write_ids() parameter validation
  - write_ids() SCR filtering logic (mock-based)
  - Mesh.get_scr_terms_mapped_to_trees() (inline pyoxigraph store)
  - normalize_mesh_nt() on per-year MeSH RDF (fixture copied from mesh2026.nt)
  - read_mesh_xml_registry() on MeSH XML (fixtures copied from desc2026.gz / supp2026.gz)
  - that the configured mesh_year is published on the NLM FTP site (network)
"""

import ftplib
import gzip
import shutil
from unittest.mock import MagicMock, patch

import pyoxigraph
import pytest

from src.categories import CHEMICAL_ENTITY, PROTEIN
from src.datahandlers.mesh import (
    MESH_FTP_SITE,
    MESH_XML_FILES,
    Mesh,
    mesh_rdf_location,
    mesh_xml_dir,
    normalize_mesh_nt,
    read_mesh_xml_registry,
    write_ids,
)
from src.util import get_config, get_repo_root
from tests.datahandlers.conftest import make_graph_from_store, nn, quad

_MESH_NS = "http://id.nlm.nih.gov/mesh/"
_MESHV_NS = "http://id.nlm.nih.gov/mesh/vocab#"


def _mesh(local: str) -> pyoxigraph.NamedNode:
    return nn(f"{_MESH_NS}{local}")


def _meshv(local: str) -> pyoxigraph.NamedNode:
    return nn(f"{_MESHV_NS}{local}")


def _make_test_store() -> pyoxigraph.Store:
    """Return an in-memory Store with a small set of known MeSH-like triples.

    Fixture summary
    ---------------
    C000001 --mappedTo--> D12345 --treeNumber--> D12.776.123
                                   D12.776.123 --parentTreeNumber--> D12.776

    C000002 --preferredMappedTo--> D05678 --treeNumber--> D05.500.123
                                    D05.500.123 --parentTreeNumber--> D05.500
                                    D05.500     --parentTreeNumber--> D05

    C000003 --mappedTo--> D23456 --treeNumber--> D23.123
                                   D23.123 --parentTreeNumber--> D23
    """
    store = pyoxigraph.Store()
    mapped_to = _meshv("mappedTo")
    preferred_mapped_to = _meshv("preferredMappedTo")
    tree_number = _meshv("treeNumber")
    parent_tree = _meshv("parentTreeNumber")

    triples = [
        # C000001 under D12.776 (one hop)
        quad(_mesh("C000001"), mapped_to, _mesh("D12345")),
        quad(_mesh("D12345"), tree_number, _mesh("D12.776.123")),
        quad(_mesh("D12.776.123"), parent_tree, _mesh("D12.776")),
        # C000002 under D05 (two hops via D05.500)
        quad(_mesh("C000002"), preferred_mapped_to, _mesh("D05678")),
        quad(_mesh("D05678"), tree_number, _mesh("D05.500.123")),
        quad(_mesh("D05.500.123"), parent_tree, _mesh("D05.500")),
        quad(_mesh("D05.500"), parent_tree, _mesh("D05")),
        # C000003 under D23 only (not protein)
        quad(_mesh("C000003"), mapped_to, _mesh("D23456")),
        quad(_mesh("D23456"), tree_number, _mesh("D23.123")),
        quad(_mesh("D23.123"), parent_tree, _mesh("D23")),
    ]
    for triple in triples:
        store.add(triple)
    return store


# ---------------------------------------------------------------------------
# Group 1: write_ids() parameter validation
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_write_ids_raises_when_both_scr_params_set(tmp_path):
    outfile = str(tmp_path / "out.txt")
    with pytest.raises(ValueError, match="cannot both be set"):
        write_ids(
            {},
            outfile,
            scr_exclude_trees=["D05"],
            scr_include_trees=["D12.776"],
        )


# ---------------------------------------------------------------------------
# Group 2: write_ids() filtering logic (Mesh mocked)
# ---------------------------------------------------------------------------


def _make_mock_mesh(
    tree_map: dict[str, list[str]],
    scr_chemical_terms: list[str],
    scr_mapped_to_trees: dict[tuple, set[str]] | None = None,
) -> MagicMock:
    """Return a configured MagicMock for the Mesh instance.

    tree_map: treenum → list of CURIE strings returned by get_terms_in_tree
    scr_chemical_terms: list returned by get_terms_with_type("SCR_Chemical")
    scr_mapped_to_trees: mapping of frozenset(top_treenums) → set of CURIEs
    """
    mock = MagicMock()
    mock.get_terms_in_tree.side_effect = lambda t: tree_map.get(t, [])
    mock.get_terms_with_type.return_value = scr_chemical_terms

    if scr_mapped_to_trees is not None:

        def _mapped(trees):
            return scr_mapped_to_trees.get(tuple(sorted(trees)), set())

        mock.get_scr_terms_mapped_to_trees.side_effect = _mapped

    return mock


@pytest.mark.unit
@patch("src.datahandlers.mesh.Mesh")
def test_write_ids_scr_exclude_trees_removes_protein_scrs(mock_cls, tmp_path):
    mock_mesh = _make_mock_mesh(
        tree_map={"D02": ["MESH:D000002"]},
        scr_chemical_terms=["MESH:C000001", "MESH:C000002"],
        scr_mapped_to_trees={("D05",): {"MESH:C000001"}},
    )
    mock_cls.return_value = mock_mesh
    outfile = str(tmp_path / "out.txt")

    write_ids(
        {"D02": CHEMICAL_ENTITY},
        outfile,
        order=["EXCLUDE", CHEMICAL_ENTITY],
        extra_vocab={"SCR_Chemical": CHEMICAL_ENTITY},
        scr_exclude_trees=["D05"],
    )

    content = open(outfile).read()
    assert "MESH:D000002" in content
    assert "MESH:C000002" in content
    assert "MESH:C000001" not in content


@pytest.mark.unit
@patch("src.datahandlers.mesh.Mesh")
def test_write_ids_scr_include_trees_keeps_only_protein_scrs(mock_cls, tmp_path):
    include_trees = ["D05", "D08", "D12.776"]
    mock_mesh = _make_mock_mesh(
        tree_map={"D12.776": ["MESH:D000001"]},
        scr_chemical_terms=["MESH:C000001", "MESH:C000002"],
        scr_mapped_to_trees={tuple(sorted(include_trees)): {"MESH:C000001"}},
    )
    mock_cls.return_value = mock_mesh
    outfile = str(tmp_path / "out.txt")

    write_ids(
        {"D12.776": PROTEIN},
        outfile,
        order=[PROTEIN],
        extra_vocab={"SCR_Chemical": PROTEIN},
        scr_include_trees=include_trees,
    )

    content = open(outfile).read()
    assert "MESH:D000001" in content
    assert "MESH:C000001" in content
    assert "MESH:C000002" not in content


@pytest.mark.unit
@patch("src.datahandlers.mesh.Mesh")
def test_write_ids_default_includes_all_scr_terms(mock_cls, tmp_path):
    mock_mesh = _make_mock_mesh(
        tree_map={"D02": ["MESH:D000002"]},
        scr_chemical_terms=["MESH:C000001", "MESH:C000002"],
    )
    mock_cls.return_value = mock_mesh
    outfile = str(tmp_path / "out.txt")

    write_ids(
        {"D02": CHEMICAL_ENTITY},
        outfile,
        order=[CHEMICAL_ENTITY],
        extra_vocab={"SCR_Chemical": CHEMICAL_ENTITY},
    )

    content = open(outfile).read()
    assert "MESH:D000002" in content
    assert "MESH:C000001" in content
    assert "MESH:C000002" in content
    mock_mesh.get_scr_terms_mapped_to_trees.assert_not_called()


@pytest.mark.unit
@patch("src.datahandlers.mesh.Mesh")
def test_write_ids_exclude_flag_suppresses_term_from_output(mock_cls, tmp_path):
    mock_mesh = _make_mock_mesh(
        tree_map={"D02": ["MESH:D000002"], "D05": ["MESH:D000005"]},
        scr_chemical_terms=["MESH:C000001", "MESH:C000002"],
        scr_mapped_to_trees={("D05",): {"MESH:C000001"}},
    )
    mock_cls.return_value = mock_mesh
    outfile = str(tmp_path / "out.txt")

    write_ids(
        {"D02": CHEMICAL_ENTITY, "D05": "EXCLUDE"},
        outfile,
        order=["EXCLUDE", CHEMICAL_ENTITY],
        extra_vocab={"SCR_Chemical": CHEMICAL_ENTITY},
        scr_exclude_trees=["D05"],
    )

    content = open(outfile).read()
    assert "MESH:D000002" in content
    assert "MESH:C000002" in content
    assert "MESH:D000005" not in content  # in D05 tree → EXCLUDE
    assert "MESH:C000001" not in content  # mapped to D05 → EXCLUDE


# ---------------------------------------------------------------------------
# Group 3: get_scr_terms_mapped_to_trees() with inline pyoxigraph store
# ---------------------------------------------------------------------------


@pytest.fixture(scope="module")
def mesh_with_test_store():
    return make_graph_from_store(Mesh, _make_test_store())


@pytest.mark.unit
def test_get_scr_terms_mapped_to_trees_direct_mapping(mesh_with_test_store):
    assert mesh_with_test_store.get_scr_terms_mapped_to_trees(["D12.776"]) == {"MESH:C000001"}


@pytest.mark.unit
def test_get_scr_terms_mapped_to_trees_transitive_parent(mesh_with_test_store):
    assert mesh_with_test_store.get_scr_terms_mapped_to_trees(["D05"]) == {"MESH:C000002"}


@pytest.mark.unit
def test_get_scr_terms_mapped_to_trees_preferred_mapped_to(mesh_with_test_store):
    # C000002 uses preferredMappedTo — covered by the D05 transitive test above,
    # but explicitly confirmed here for clarity.
    assert "MESH:C000002" in mesh_with_test_store.get_scr_terms_mapped_to_trees(["D05"])


@pytest.mark.unit
def test_get_scr_terms_mapped_to_trees_multiple_top_treenums(mesh_with_test_store):
    result = mesh_with_test_store.get_scr_terms_mapped_to_trees(["D12.776", "D05"])
    assert result == {"MESH:C000001", "MESH:C000002"}
    assert "MESH:C000003" not in result


@pytest.mark.unit
def test_get_scr_terms_mapped_to_trees_no_match(mesh_with_test_store):
    assert mesh_with_test_store.get_scr_terms_mapped_to_trees(["D01"]) == set()


# ---------------------------------------------------------------------------
# Group 4: normalize_mesh_nt() on a per-year RDF dump
# ---------------------------------------------------------------------------

MESH_TEST_DATA = get_repo_root() / "tests" / "data" / "mesh"


def _gzip_fixture(src, tmp_path):
    """Gzip a fixture file into tmp_path, the way NLM ships it."""
    gz_path = tmp_path / (src.name + ".gz")
    with open(src, "rb") as inf, gzip.open(gz_path, "wb") as outf:
        shutil.copyfileobj(inf, outf)
    return gz_path


@pytest.fixture
def normalized_mesh2026(tmp_path):
    """mesh2026_sample.nt (every triple for C000730045 foliglurax and D002839 Chromans, copied from
    mesh2026.nt) gzipped and run through normalize_mesh_nt()."""
    out = tmp_path / "mesh.nt"
    normalize_mesh_nt(_gzip_fixture(MESH_TEST_DATA / "mesh2026_sample.nt", tmp_path), out, "2026")
    return out


@pytest.mark.unit
def test_normalize_mesh_nt_strips_year_from_record_iris(normalized_mesh2026):
    """Record IRIs like <http://id.nlm.nih.gov/mesh/2026/D002839> should become <http://id.nlm.nih.gov/mesh/D002839>,
    leaving no year-versioned IRIs behind and the vocab# namespace untouched."""
    text = normalized_mesh2026.read_text()
    assert "/mesh/2026/" not in text
    assert (
        "<http://id.nlm.nih.gov/mesh/D002839> <http://id.nlm.nih.gov/mesh/vocab#treeNumber> <http://id.nlm.nih.gov/mesh/D03.383.663.283.240> ."
        in text
    )
    assert len(text.splitlines()) == len((MESH_TEST_DATA / "mesh2026_sample.nt").read_text().splitlines())


@pytest.mark.unit
def test_normalized_mesh_nt_works_with_mesh_queries(normalized_mesh2026):
    """The Mesh SPARQL helpers should find records in a normalized per-year dump (they would find nothing
    in the raw one, whose IRIs carry the year)."""
    store = pyoxigraph.Store()
    with open(normalized_mesh2026, "rb") as inf:
        store.bulk_load(input=inf, format=pyoxigraph.RdfFormat.N_TRIPLES)
    m = make_graph_from_store(Mesh, store)
    assert m.get_terms_with_type("SCR_Chemical") == ["MESH:C000730045"]
    assert m.get_tree_numbers("MESH:D002839") == ["D03.383.663.283.240", "D03.633.100.150.240"]


@pytest.mark.unit
def test_normalize_mesh_nt_rejects_wrong_year(tmp_path):
    """A 2026 dump normalized as if it were 2025 should raise rather than produce a file the queries can't use."""
    with pytest.raises(ValueError, match="mesh/2025/"):
        normalize_mesh_nt(_gzip_fixture(MESH_TEST_DATA / "mesh2026_sample.nt", tmp_path), tmp_path / "mesh.nt", "2025")


# ---------------------------------------------------------------------------
# Group 5: read_mesh_xml_registry() on MeSH XML
# ---------------------------------------------------------------------------


@pytest.mark.unit
def test_read_mesh_xml_registry_descriptor_uses_preferred_concept_only(tmp_path):
    """D000068877 (Imatinib Mesylate, copied from desc2026.gz) should yield only its preferred concept's
    RegistryNumber: not the RelatedRegistryNumbers, not the non-preferred concept's BKJ8M8G5HI, not the
    "0" placeholders, and not the DescriptorUIs nested in its pharmacological actions."""
    gz_path = _gzip_fixture(MESH_TEST_DATA / "desc2026_sample.xml", tmp_path)
    assert list(read_mesh_xml_registry(gz_path, "DescriptorRecord", "DescriptorUI")) == [
        ("MESH:D000068877", "8A1O1M485B")
    ]


@pytest.mark.unit
def test_read_mesh_xml_registry_scrs(tmp_path):
    """From supp2026.gz: C000730045 (foliglurax) has only the "0" placeholder and should be skipped;
    C000612320 should yield its NCBI Taxon registry number."""
    gz_path = _gzip_fixture(MESH_TEST_DATA / "supp2026_sample.xml", tmp_path)
    assert list(read_mesh_xml_registry(gz_path, "SupplementalRecord", "SupplementalRecordUI")) == [
        ("MESH:C000612320", "txid354090")
    ]


# ---------------------------------------------------------------------------
# Group 6: the configured mesh_year is published
# ---------------------------------------------------------------------------


@pytest.mark.network
def test_configured_mesh_year_is_published():
    """config.yaml's mesh_year should name a year with both an RDF dump and XML files on the NLM FTP site,
    so get_mesh and get_mesh_xml will not fail partway through a build."""
    mesh_year = get_config()["mesh_year"]
    rdf_dir, rdf_file = mesh_rdf_location(mesh_year)
    xml_dir = mesh_xml_dir(mesh_year)
    ftp = ftplib.FTP(MESH_FTP_SITE)
    ftp.login()
    ftp.voidcmd("TYPE I")  # NLM refuses SIZE in ASCII mode.
    try:
        assert ftp.size(f"{rdf_dir}/{rdf_file}") > 0
        for prefix, _, _ in MESH_XML_FILES:
            assert ftp.size(f"{xml_dir}/{prefix}{mesh_year}.gz") > 0
    finally:
        ftp.quit()
