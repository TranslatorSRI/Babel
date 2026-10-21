import pytest

import src.node as node_module
import src.prefixes as pref
from src import categories
from src.LabeledID import LabeledID
from src.node import NodeFactory
from src.util import get_config

BIOLINK_VERSION = get_config()["biolink_version"]

# Node schema (as of the current codebase):
#   {"identifiers": [{"identifier": CURIE, "label": str}, ...], "type": str}
# identifiers[0] is the preferred identifier (highest-priority prefix first).
# Labels stay with their own identifier; they are not promoted to the first entry.
# "type" is a single biolink type string, not a list of ancestors.


@pytest.mark.network
def test_get_ancestors(node_factory):
    """ """
    ancestors = node_factory.get_ancestors("biolink:ChemicalEntity")
    # biolink 4.3.6 adds several mixins compared to earlier versions
    assert len(ancestors) == 8
    assert "biolink:ChemicalEntity" in ancestors  # self is in ancestors
    assert "biolink:PhysicalEssence" in ancestors
    assert "biolink:NamedThing" in ancestors
    assert "biolink:Entity" in ancestors
    assert "biolink:PhysicalEssenceOrOccurrent" in ancestors  # mixins are in ancestors
    assert "biolink:ChemicalOrDrugOrTreatment" in ancestors
    assert "biolink:ChemicalEntityOrGeneOrGeneProduct" in ancestors
    assert "biolink:ChemicalEntityOrProteinOrPolypeptide" in ancestors


@pytest.mark.network
def test_prefixes(node_factory):
    prefixes = node_factory.get_prefixes("biolink:SmallMolecule")
    # Prefix order and membership reflect biolink 4.3.6; update this list when the
    # model version in config.yaml is bumped.
    expected_prefixes = [
        pref.CHEBI,
        pref.UNII,
        pref.PUBCHEMCOMPOUND,
        pref.CHEMBLCOMPOUND,
        pref.DRUGBANK,
        pref.MESH,
        pref.CAS,
        pref.DRUGCENTRAL,
        pref.GTOPDB,
        pref.HMDB,
        pref.KEGGCOMPOUND,
        pref.PHARMGKB_DRUG,
        pref.CHEMBANK,
        pref.PUBCHEMSUBSTANCE,
        pref.SIDERDRUG,
        pref.INCHI,
        pref.INCHIKEY,
        pref.BIGG_METABOLITE,
        pref.FOODB_COMPOUND,
        pref.KEGGGLYCAN,
        pref.KEGGDRUG,
        pref.KEGGENVIRON,
        pref.KEGG,
        pref.UMLS,
    ]
    assert prefixes == expected_prefixes


@pytest.mark.network
def test_taxon_prefixes(node_factory):
    """There was some churn in biolink around organism, so we had it specialcased for a while"""
    prefixes = node_factory.get_prefixes("biolink:OrganismTaxon")
    expected_prefixes = [pref.NCBITAXON, pref.MESH, pref.UMLS]
    assert prefixes == expected_prefixes


@pytest.mark.network
def test_biological_process_prefixes(node_factory):
    """Verify that the Biolink Model (version in config.yaml) natively includes UMLS as a
    valid prefix for biolink:BiologicalProcess.

    NodeFactory.get_prefixes() previously appended UMLS manually as a workaround for an
    older Biolink Model version that was missing it.  Issue #413
    (https://github.com/NCATSTranslator/Babel/issues/413) tracked the upstream fix.
    This test queries the Biolink Model Toolkit directly (bypassing NodeFactory post-
    processing) to confirm that UMLS is present in the model itself.
    """
    from src.categories import BIOLOGICAL_PROCESS

    # Query the toolkit directly, bypassing NodeFactory.get_prefixes() post-processing.
    raw_prefixes = node_factory.toolkit.get_element(BIOLOGICAL_PROCESS)["id_prefixes"]

    assert pref.UMLS in raw_prefixes, (
        f"UMLS is not in the biolink:BiologicalProcess id_prefixes in the current Biolink Model. "
        f"Full list: {raw_prefixes}. "
        f"See https://github.com/NCATSTranslator/Babel/issues/413"
    )


@pytest.mark.network
def test_normalization(node_factory):
    """Basic normalization - do we pick the right identifier?  Note that the identifiers are made up."""
    node = node_factory.create_node(["MESH:D012034", "CHEBI:1234"], "biolink:SmallMolecule")
    assert node["identifiers"][0]["identifier"] == "CHEBI:1234"  # CHEBI ranks above MESH
    assert node["id"]["identifier"] == "CHEBI:1234"  # node["id"] is an alias for node["identifiers"][0]
    assert len(node["identifiers"]) == 2
    ids = [x["identifier"] for x in node["identifiers"]]
    assert "MESH:D012034" in ids
    assert "CHEBI:1234" in ids
    assert node["type"] == "biolink:SmallMolecule"  # type is now a single string, not a list


@pytest.mark.network
def test_normalization_bad_prefix(node_factory):
    """When we include the prefix CHEMBL, it does not get added to the list of prefixes (it should be CHEMBL.COMPOUND)"""
    node = node_factory.create_node(["MESH:D012034", "CHEBI:1234", "CHEMBL:CHEMBL123"], "biolink:SmallMolecule")
    assert node["identifiers"][0]["identifier"] == "CHEBI:1234"
    assert node["id"]["identifier"] == "CHEBI:1234"  # node["id"] is an alias for node["identifiers"][0]
    assert len(node["identifiers"]) == 2  # CHEMBL: filtered out, only CHEBI + MESH remain
    ids = [x["identifier"] for x in node["identifiers"]]
    assert "MESH:D012034" in ids
    assert "CHEBI:1234" in ids
    assert node["type"] == "biolink:SmallMolecule"


@pytest.mark.network
def test_extra_prefix_does_not_duplicate_identifier(node_factory):
    """Make sure adding an extra prefix should not duplicate identifiers."""
    node = node_factory.create_node([f"{pref.UMLS}:C0000005"], "biolink:SmallMolecule", extra_prefixes=[pref.UMLS])
    assert node["identifiers"] == [{"identifier": f"{pref.UMLS}:C0000005"}]
    assert node["id"] == {"identifier": f"{pref.UMLS}:C0000005"}


@pytest.mark.network
def test_extra_prefix_does_not_duplicate_identifier_multi(node_factory):
    """Passing multiple extra_prefixes that overlap standard Biolink prefixes must not
    duplicate any identifier, including the highest-priority one, in a multi-identifier clique."""
    # CHEBI (highest priority) and MESH are both standard SmallMolecule prefixes.
    # Passing them again as extra_prefixes must not cause either to appear twice
    # or change their relative ordering.
    node = node_factory.create_node(
        [f"{pref.UMLS}:C0000005", f"{pref.MESH}:D012034", f"{pref.CHEBI}:1234"],
        categories.SMALL_MOLECULE,
        extra_prefixes=[pref.UMLS, pref.MESH, pref.CHEBI],
    )
    ids = [x["identifier"] for x in node["identifiers"]]
    assert len(ids) == len(set(ids)), f"duplicate identifiers in output: {ids}"
    assert ids.count(f"{pref.CHEBI}:1234") == 1
    assert ids.count(f"{pref.MESH}:D012034") == 1
    assert ids.count(f"{pref.UMLS}:C0000005") == 1
    # CHEBI must still be the preferred identifier despite also appearing in extra_prefixes
    assert node["identifiers"][0]["identifier"] == f"{pref.CHEBI}:1234"
    assert node["id"]["identifier"] == f"{pref.CHEBI}:1234"


@pytest.mark.network
def test_normalization_labeled_id(node_factory):
    """Make sure that the node creator can handle labels passed as a dict"""
    labels = {"CHEBI:1234": "name"}
    node = node_factory.create_node(["MESH:D012034", "CHEBI:1234", "CHEMBL:CHEMBL123"], "biolink:SmallMolecule", labels)
    assert node["identifiers"][0]["identifier"] == "CHEBI:1234"
    assert node["identifiers"][0]["label"] == "name"
    assert node["id"]["identifier"] == "CHEBI:1234"  # node["id"] is an alias for node["identifiers"][0]
    assert node["id"]["label"] == "name"
    assert len(node["identifiers"]) == 2
    assert "MESH:D012034" in [x["identifier"] for x in node["identifiers"]]
    assert node["type"] == "biolink:SmallMolecule"


@pytest.mark.network
def test_labeling_2(node_factory):
    """Labels remain on the identifier that owns them; they are not promoted to the preferred node.
    Here only the dictyBase identifier has a label."""
    node = node_factory.create_node(
        [f"{pref.ENSEMBL}:81239", f"{pref.NCBIGENE}:123", f"{pref.DICTYBASE}:1234"],
        "biolink:Gene",
        {f"{pref.DICTYBASE}:1234": "name"},
    )
    assert node["identifiers"][0]["identifier"] == f"{pref.NCBIGENE}:123"
    assert node["identifiers"][1]["identifier"] == f"{pref.ENSEMBL}:81239"
    assert node["identifiers"][2]["identifier"] == f"{pref.DICTYBASE}:1234"
    assert node["identifiers"][2]["label"] == "name"
    # The preferred identifier (NCBIGene) has no label because none was provided for it
    assert "label" not in node["identifiers"][0]
    assert node["id"]["identifier"] == f"{pref.NCBIGENE}:123"  # node["id"] is an alias for node["identifiers"][0]
    assert "label" not in node["id"]
    assert node["type"] == "biolink:Gene"


@pytest.mark.network
def test_clean_list(node_factory):
    input_ids = frozenset(
        {
            "UMLS:C1839767",
            "UMLS:C1853383",
            LabeledID("HP:0010804", "Tented upper lip vermilion"),
            "UMLS:C1850072",
            "HP:0010804",
        }
    )
    output = node_factory.clean_list(input_ids)
    assert len(output) == 4
    lidfound = False
    for x in output:
        if isinstance(x, LabeledID):
            lidfound = True
            assert x.identifier == "HP:0010804"
    assert lidfound


@pytest.mark.network
def test_losing_umls(node_factory):
    input_ids = frozenset({"HP:0010804", "UMLS:C1839767", "UMLS:C1853383", "HP:0010804", "UMLS:C1850072"})
    node = node_factory.create_node(
        input_ids, "biolink:PhenotypicFeature", {"HP:0010804": "Tented upper lip vermilion"}
    )
    assert node["identifiers"][0]["identifier"] == "HP:0010804"
    assert node["identifiers"][0]["label"] == "Tented upper lip vermilion"
    assert node["id"]["identifier"] == "HP:0010804"  # node["id"] is an alias for node["identifiers"][0]
    assert node["id"]["label"] == "Tented upper lip vermilion"
    assert len(node["identifiers"]) == 4  # HP + 3 UMLS
    assert node["type"] == "biolink:PhenotypicFeature"


@pytest.mark.network
def test_same_value_different_prefix(node_factory):
    input_ids = frozenset({"FB:FBgn0261954", "ENSEMBL:FBgn0261954", "NCBIGene:46006"})
    node = node_factory.create_node(input_ids, "biolink:Gene", {})
    assert len(node["identifiers"]) == 3
    assert len(set([x["identifier"] for x in node["identifiers"]])) == 3
    assert node["id"] == node["identifiers"][0]
    assert node["type"] == "biolink:Gene"


@pytest.mark.network
def test_pubchem_simple(node_factory):
    """When multiple PUBCHEM.COMPOUND identifiers exist, prefer the one whose label matches other
    identifiers in the clique.  In biolink 4.3.6 CHEBI ranks above PUBCHEM.COMPOUND, so CHEBI is
    the overall preferred identifier; the pubchem ordering is verified by checking position [1]."""
    node = node_factory.create_node(
        [f"{pref.PUBCHEMCOMPOUND}:999", f"{pref.PUBCHEMCOMPOUND}:111", f"{pref.CHEBI}:XYZ"],
        "biolink:SmallMolecule",
        {f"{pref.PUBCHEMCOMPOUND}:999": "water", f"{pref.PUBCHEMCOMPOUND}:111": "h", f"{pref.CHEBI}:XYZ": "WATER"},
    )
    assert node["identifiers"][0]["identifier"] == f"{pref.CHEBI}:XYZ"  # CHEBI wins overall
    assert node["id"]["identifier"] == f"{pref.CHEBI}:XYZ"  # node["id"] is an alias for node["identifiers"][0]
    assert node["id"]["label"] == "WATER"
    # Among the two PUBCHEMs, :999 ("water") matches the CHEBI label "WATER" → ranked first
    assert node["identifiers"][1]["identifier"] == f"{pref.PUBCHEMCOMPOUND}:999"
    assert node["identifiers"][1]["label"] == "water"
    assert node["type"] == "biolink:SmallMolecule"


@pytest.mark.network
def test_pubchem_no_match(node_factory):
    """When no PUBCHEM label matches other identifiers, prefer the one with the shortest label."""
    node = node_factory.create_node(
        [f"{pref.PUBCHEMCOMPOUND}:999", f"{pref.PUBCHEMCOMPOUND}:111", f"{pref.CHEBI}:XYZ"],
        "biolink:SmallMolecule",
        {f"{pref.PUBCHEMCOMPOUND}:999": "h", f"{pref.PUBCHEMCOMPOUND}:111": "water", f"{pref.CHEBI}:XYZ": "blahblah"},
    )
    assert node["identifiers"][0]["identifier"] == f"{pref.CHEBI}:XYZ"  # CHEBI wins overall
    assert node["id"]["identifier"] == f"{pref.CHEBI}:XYZ"  # node["id"] is an alias for node["identifiers"][0]
    assert node["id"]["label"] == "blahblah"
    # Among the two PUBCHEMs, :999 ("h") has the shortest label → ranked first
    assert node["identifiers"][1]["identifier"] == f"{pref.PUBCHEMCOMPOUND}:999"
    assert node["identifiers"][1]["label"] == "h"
    assert node["type"] == "biolink:SmallMolecule"


@pytest.mark.network
def test_pubchem_ignore_CID(node_factory):
    """When choosing the shortest PUBCHEM label, skip labels that start with 'CID'."""
    node = node_factory.create_node(
        [f"{pref.PUBCHEMCOMPOUND}:999", f"{pref.PUBCHEMCOMPOUND}:111", f"{pref.CHEBI}:XYZ"],
        "biolink:SmallMolecule",
        {
            f"{pref.PUBCHEMCOMPOUND}:999": "CID1",
            f"{pref.PUBCHEMCOMPOUND}:111": "longerlabel",
            f"{pref.CHEBI}:XYZ": "blahblah",
        },
    )
    assert node["identifiers"][0]["identifier"] == f"{pref.CHEBI}:XYZ"  # CHEBI wins overall
    assert node["id"]["identifier"] == f"{pref.CHEBI}:XYZ"  # node["id"] is an alias for node["identifiers"][0]
    assert node["id"]["label"] == "blahblah"
    # :999 has label "CID1" which is ignored; :111 ("longerlabel") is preferred instead
    assert node["identifiers"][1]["identifier"] == f"{pref.PUBCHEMCOMPOUND}:111"
    assert node["identifiers"][1]["label"] == "longerlabel"
    assert node["type"] == "biolink:SmallMolecule"


@pytest.mark.unit
def test_load_extra_labels_single_column(tmp_path):
    """load_extra_labels() must not raise on single-column lines (identifier with no label)."""
    label_dir = tmp_path / "CHEMBL.COMPOUND"
    label_dir.mkdir()
    (label_dir / "labels").write_text("CHEMBL.COMPOUND:CHEMBL1\tWater\nCHEMBL.COMPOUND:CHEMBL2\n")
    fac = NodeFactory(str(tmp_path), BIOLINK_VERSION)
    fac.common_labels = {}
    fac.load_extra_labels("CHEMBL.COMPOUND")
    assert fac.extra_labels["CHEMBL.COMPOUND"]["CHEMBL.COMPOUND:CHEMBL1"] == "Water"
    assert fac.extra_labels["CHEMBL.COMPOUND"]["CHEMBL.COMPOUND:CHEMBL2"] == ""


@pytest.mark.unit
def test_load_extra_labels_tab_in_label(tmp_path):
    """load_extra_labels() must preserve labels that themselves contain a tab (maxsplit=1)."""
    label_dir = tmp_path / "CHEMBL.COMPOUND"
    label_dir.mkdir()
    (label_dir / "labels").write_text(
        "CHEMBL.COMPOUND:CHEMBL1\tWater\nCHEMBL.COMPOUND:CHEMBL2\nCHEMBL.COMPOUND:CHEMBL3\tWater\tbottle\n"
    )
    fac = NodeFactory(str(tmp_path), BIOLINK_VERSION)
    fac.common_labels = {}
    fac.load_extra_labels("CHEMBL.COMPOUND")
    assert fac.extra_labels["CHEMBL.COMPOUND"]["CHEMBL.COMPOUND:CHEMBL3"] == "Water\tbottle"


@pytest.mark.unit
def test_load_extra_labels_applies_label_override(tmp_path):
    """CURIE-specific label overrides are applied when source label files are loaded."""
    label_dir = tmp_path / "DRUGBANK"
    label_dir.mkdir()
    (label_dir / "labels").write_text("DRUGBANK:DB10626\tTrout\n")
    fac = NodeFactory(str(tmp_path), BIOLINK_VERSION)
    fac.common_labels = {}
    fac.load_extra_labels("DRUGBANK")
    assert fac.extra_labels["DRUGBANK"]["DRUGBANK:DB10626"] == "Trout allergenic extract"


@pytest.mark.unit
def test_common_labels_apply_label_override(tmp_path, monkeypatch):
    """Common labels use CURIE-specific label overrides before fallback labeling."""
    common_dir = tmp_path / "common"
    common_dir.mkdir()
    (common_dir / "labels").write_text("DRUGBANK:DB10626\tTrout\n")
    config = dict(get_config())
    config["download_directory"] = str(tmp_path)
    config["common"] = {"labels": ["labels"], "synonyms": [], "descriptions": []}

    monkeypatch.setattr(node_module, "get_config", lambda: config)
    fac = NodeFactory(str(tmp_path / "labels"), BIOLINK_VERSION)

    labels = fac.apply_labels(["DRUGBANK:DB10626"], {})

    assert labels == [LabeledID("DRUGBANK:DB10626", "Trout allergenic extract")]


@pytest.mark.unit
def test_label_override_feeds_preferred_name_selection(tmp_path):
    """DRUGBANK:DB10626 is corrected before preferred-name selection sees the node."""
    label_dir = tmp_path / "DRUGBANK"
    label_dir.mkdir()
    (label_dir / "labels").write_text("DRUGBANK:DB10626\tTrout\n")
    fac = NodeFactory(str(tmp_path), BIOLINK_VERSION)
    fac.common_labels = {}

    node = fac.create_node(["DRUGBANK:DB10626", "UMLS:C2725895"], categories.CHEMICAL_ENTITY)

    assert node["identifiers"][0]["identifier"] == "DRUGBANK:DB10626"
    assert node["identifiers"][0]["label"] == "Trout allergenic extract"
