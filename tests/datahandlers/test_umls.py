"""Unit tests for src/datahandlers/umls.py: the "a CUI follows its MeSH descriptor" rule that keeps a UMLS CUI,
its MeSH descriptor and its DrugBank partners in one pipeline's ids (issues #276, #308, #1123).

The MRSTY and MRCONSO rows are copied verbatim from UMLS 2026AA; each block names its CUI.
"""

import pytest

from src.categories import ANATOMICAL_ENTITY, CHEMICAL_ENTITY, ORGANISM_TAXON, PROTEIN
from src.createcompendia import anatomy, chemicals, diseasephenotype, gene, processactivitypathway, protein, taxon
from src.datahandlers import umls
from src.util import get_config

# C0242726 "Plant Roots": T002 Plant (taxon), descriptor D018517 in anatomy's A18 tree (#1123).
# C0000608 "Aminocaproic Acid": T116 (protein) + T121, descriptor D015119 in a chemical D-tree (#308).
# C0000005 "(131)I-Macroaggregated Albumin": T116, but its only MSH atoms are PEP/ET, so build_sets() never
#   concords it to D012711 and MeSH ownership must leave it alone.
MRSTY = (
    "C0242726|T002|A1.1.3.3|Plant|AT17675273||\n"
    "C0000608|T116|A1.4.1.2.1.7|Amino Acid, Peptide, or Protein|AT17641842|4096|\n"
    "C0000608|T121|A1.4.1.1.1|Pharmacologic Substance|AT17567390|4096|\n"
    "C0000005|T116|A1.4.1.2.1.7|Amino Acid, Peptide, or Protein|AT17648347||\n"
    "C0000005|T121|A1.4.1.1.1|Pharmacologic Substance|AT17575038||\n"
    "C0000005|T130|A1.4.1.1.4|Indicator, Reagent, or Diagnostic Aid|AT17634323||\n"
)

MRCONSO = (
    # C0242726: a Czech MH atom (must be ignored: not ENG), the English MH atom, and ET/PM atoms.
    "C0242726|CZE|P|L11818128|PF|S14691612|Y|A24275078||M0027763|D018517|MSHCZE|MH|D018517|kořeny rostlin|3|N||\n"
    "C0242726|ENG|P|L0315401|PF|S0396396|N|A0447154||M0027763|D018517|MSH|MH|D018517|Plant Roots|0|N||\n"
    "C0242726|ENG|P|L0315401|VO|S0396393|Y|A26677612||M0027763|D018517|MSH|ET|D018517|Plant Root|0|N||\n"
    "C0242726|ENG|P|L0315401|VW|S11094902|Y|A16992050||M0027763|D018517|MSH|PM|D018517|Roots, Plant|0|N||\n"
    # C0000608: the MH atom among PM/ET atoms.
    "C0000608|ENG|P|L0000608|VC|S0008731|Y|A0017463||M0023260|D015119|MSH|PM|D015119|6 Aminocaproic Acid|0|N||\n"
    "C0000608|ENG|S|L0002546|VC|S0013139|Y|A22790800||M0023260|D015119|MSH|MH|D015119|Aminocaproic Acid|0|N||\n"
    "C0000608|ENG|S|L0014638|PF|S0038520|Y|A26677291||M0023260|D015119|MSH|ET|D015119|epsilon-Aminocaproic Acid|0|N||\n"
    # C0000005: PEP and ET atoms only.
    "C0000005|ENG|P|L0000005|PF|S0007492|Y|A26634265||M0019694|D012711|MSH|PEP|D012711|(131)I-Macroaggregated Albumin|0|N||\n"
    "C0000005|ENG|S|L0270109|PF|S0007491|Y|A26634266||M0019694|D012711|MSH|ET|D012711|(131)I-MAA|0|N||\n"
)

# Semantic-type maps in the shape the pipelines use (subsets of taxon/anatomy/protein/chemicals.write_umls_ids).
TAXON_MAP = {"A1.1.3.3": ORGANISM_TAXON}
ANATOMY_MAP = {"A1.2": ANATOMICAL_ENTITY}
PROTEIN_MAP = {"A1.4.1.2.1.7": PROTEIN}
CHEMICAL_MAP = {"A1.4.1.1.1.1": CHEMICAL_ENTITY, "A1.4.1.2.1": CHEMICAL_ENTITY}
PROTEIN_STY_TREES = {"A1.4.1.2.1.7"}


@pytest.fixture
def rrf(tmp_path):
    (tmp_path / "MRSTY.RRF").write_text(MRSTY)
    (tmp_path / "MRCONSO.RRF").write_text(MRCONSO)
    return tmp_path


def mesh_ids(tmp_path, name, rows):
    """Write an ids/MESH-style file (CURIE<tab>type per row) and return its path."""
    path = tmp_path / f"MESH_{name}"
    path.write_text("".join(f"{curie}\t{biolink_type}\n" for curie, biolink_type in rows))
    return str(path)


def read_ids(path):
    return dict(line.rstrip("\n").split("\t") for line in open(path) if line.strip())


def ownership(rrf, mesh_ids_by_pipeline, blocklists=None, non_mesh_cuis=()):
    """Write the umls_mesh_ownership file for these pipelines' MeSH claims and return its path.

    ``blocklists`` maps a pipeline to the bare CUIs of its blocklist file (badumls-style); ``non_mesh_cuis`` are
    the CUIs of a gene/process-style ids/UMLS file."""
    path = rrf / "mesh_ownership.tsv"
    files = {pipeline: mesh_ids(rrf, pipeline, rows) for pipeline, rows in mesh_ids_by_pipeline.items()}
    blocklist_files = {}
    for pipeline, cuis in (blocklists or {}).items():
        blocklist_files[pipeline] = str(rrf / f"blocklist_{pipeline}")
        (rrf / f"blocklist_{pipeline}").write_text("#a comment line\n" + "".join(f"{cui} label\n" for cui in cuis))
    (rrf / "non_mesh_UMLS").write_text("".join(f"{cui}\tbiolink:Gene\n" for cui in non_mesh_cuis))
    umls.write_mesh_ownership(
        str(rrf / "MRCONSO.RRF"),
        files,
        str(path),
        blocklist_files_by_pipeline=blocklist_files,
        non_mesh_umls_ids_files=[str(rrf / "non_mesh_UMLS")],
    )
    return str(path)


def run(rrf, category_map, mesh_ownership, pipeline, **kwargs):
    out = rrf / "UMLS"
    umls.write_umls_ids(
        str(rrf / "MRSTY.RRF"), category_map, str(out), mesh_ownership=mesh_ownership, pipeline=pipeline, **kwargs
    )
    return read_ids(out)


# READING MRCONSO


@pytest.mark.unit
def test_read_cui_to_mesh_descriptors_uses_concord_atoms_only(rrf):
    """Only English MH/NM/HT/QAB atoms should map a CUI to a descriptor, so C0000005 (PEP/ET only) is absent and
    the Czech MH atom for C0242726 is ignored."""
    assert umls.read_cui_to_mesh_descriptors(str(rrf / "MRCONSO.RRF")) == {
        "UMLS:C0242726": {"MESH:D018517"},
        "UMLS:C0000608": {"MESH:D015119"},
    }


@pytest.mark.unit
def test_read_mesh_ids_types_rejects_an_untyped_row(tmp_path):
    """An ids/MESH row with no type should raise, naming the CURIE, rather than give an owned CUI no type."""
    path = tmp_path / "MESH"
    path.write_text(f"MESH:D018517\t{ANATOMICAL_ENTITY}\nMESH:D015119\n")
    with pytest.raises(ValueError, match="MESH:D015119 has no Biolink type"):
        umls.read_mesh_ids_types([str(path)])


@pytest.mark.unit
def test_read_cui_to_mesh_descriptors_skips_suppressed_atoms(tmp_path):
    """An obsolete (SUPPRESS=O) MH atom should not map its CUI, just as check_mrconso_line() drops it for build_sets()."""
    row = "C0242726|ENG|P|L0315401|PF|S0396396|N|A0447154||M0027763|D018517|MSH|MH|D018517|Plant Roots|0|O||\n"
    (tmp_path / "MRCONSO.RRF").write_text(row)
    assert umls.read_cui_to_mesh_descriptors(str(tmp_path / "MRCONSO.RRF")) == {}


# One atom of each of the other three concorded term types, copied verbatim from UMLS 2026AA: C0000268
# "2-bromolysergic acid diethylamide" (NM, a supplementary record), C0002784 (HT, a MeSH category heading) and
# C0000769 (QAB, a qualifier abbreviation).
OTHER_CONCORD_TTY_ROWS = (
    "C0000268|ENG|P|L0000268|PF|S0349869|N|A0383097||M0224845|C084642|MSH|NM|C084642|2-bromolysergic acid diethylamide|0|N||\n"
    "C0002784|ENG|P|L0002784|PF|S0101154|Y|A0135345|||U000001|MSH|HT|U000001|Analytical, Diagnostic and Therapeutic Techniques and Equipment (MeSH Category)|0|N||\n"
    "C0000769|ENG|S|L0655322|PF|S0787633|Y|A12081474||M0030203|Q000002|MSH|QAB|Q000002|AB|0|Y||\n"
)


@pytest.mark.unit
def test_build_sets_concords_exactly_the_descriptors_ownership_reads(tmp_path):
    """build_sets() should concord each CUI to exactly the descriptors read_cui_to_mesh_descriptors() returns for
    it, over atoms of every MeSH term type (MH, NM, HT, QAB, and the PM/ET/PEP/Czech atoms neither may use).
    Otherwise MeSH ownership would move a CUI for a descriptor it is never concorded to, or leave one behind
    that its concord still drags."""
    mrconso = tmp_path / "MRCONSO.RRF"
    mrconso.write_text(MRCONSO + OTHER_CONCORD_TTY_ROWS)
    cuis = ["C0242726", "C0000608", "C0000005", "C0000268", "C0002784", "C0000769"]
    (tmp_path / "ids").write_text("".join(f"UMLS:{cui}\n" for cui in cuis))
    umls.build_sets(str(mrconso), str(tmp_path / "ids"), str(tmp_path / "concord"), {"MSH": "MESH"})

    concorded = {tuple(line.rstrip("\n").split("\teq\t")) for line in open(tmp_path / "concord")}
    read = {
        (cui, d) for cui, descriptors in umls.read_cui_to_mesh_descriptors(str(mrconso)).items() for d in descriptors
    }
    assert concorded == read
    assert len(read) == 5, "every concorded term type should be represented, and C0000005's PEP/ET atoms not"


# DECIDING OWNERSHIP


@pytest.mark.unit
def test_owner_is_the_only_pipeline_claiming_a_descriptor():
    """A CUI whose descriptors only one pipeline claims should be owned by it, typed as its first descriptor in
    sorted order; unclaimed descriptors do not count against it."""
    cui_to_mesh = {"UMLS:C1": {"MESH:D2", "MESH:D1", "MESH:D9"}}
    mesh_types = {"chemicals": {"MESH:D2": CHEMICAL_ENTITY, "MESH:D1": "biolink:Polypeptide"}, "anatomy": {}}
    assert umls.compute_mesh_owners(cui_to_mesh, mesh_types) == {
        "UMLS:C1": ("chemicals", "MESH:D1", "biolink:Polypeptide")
    }


@pytest.mark.unit
def test_owned_cui_takes_its_descriptors_type_before_a_supplementary_records():
    """UMLS:C0020388 "hydroxyproline" stands for descriptor D006909 (typed Polypeptide in 2026jul22's chemicals
    ids/MESH) and SCR C000716210 (typed ChemicalEntity). It should take the descriptor's type, although
    "MESH:C..." sorts first."""
    cui_to_mesh = {"UMLS:C0020388": {"MESH:D006909", "MESH:C000716210"}}
    mesh_types = {"chemicals": {"MESH:D006909": "biolink:Polypeptide", "MESH:C000716210": CHEMICAL_ENTITY}}
    assert umls.compute_mesh_owners(cui_to_mesh, mesh_types) == {
        "UMLS:C0020388": ("chemicals", "MESH:D006909", "biolink:Polypeptide")
    }


@pytest.mark.unit
def test_cui_has_no_owner_when_two_pipelines_claim_its_descriptors():
    """A descriptor cross-listed in two pipelines' ids/MESH, or two descriptors claimed by different pipelines,
    should leave the CUI without an owner, so that no pipeline drops it and none adds it."""
    cui_to_mesh = {"UMLS:C1": {"MESH:D1"}, "UMLS:C2": {"MESH:D2", "MESH:D3"}}
    mesh_types = {
        "anatomy": {"MESH:D1": ANATOMICAL_ENTITY, "MESH:D2": ANATOMICAL_ENTITY},
        "protein": {"MESH:D1": PROTEIN, "MESH:D3": PROTEIN},
    }
    assert umls.compute_mesh_owners(cui_to_mesh, mesh_types) == {}


@pytest.mark.unit
def test_cui_has_no_owner_when_its_only_claimant_refuses_it():
    """A CUI on its only claimant's never_add list (disease's badumls) should have no owner, so that the pipeline
    whose semantic types selected it keeps it."""
    mesh_types = {"disease": {"MESH:D1": "biolink:Disease"}}
    assert umls.compute_mesh_owners({"UMLS:C1": {"MESH:D1"}}, mesh_types, {"disease": {"UMLS:C1"}}) == {}


@pytest.mark.unit
def test_mesh_ownership_file_round_trips(rrf):
    """write_mesh_ownership() should write one row per owned CUI that read_mesh_ownership() reads back."""
    path = ownership(
        rrf, {"anatomy": [("MESH:D018517", ANATOMICAL_ENTITY)], "chemicals": [("MESH:D015119", CHEMICAL_ENTITY)]}
    )
    assert umls.read_mesh_ownership(path) == {
        "UMLS:C0242726": ("anatomy", "MESH:D018517", ANATOMICAL_ENTITY),
        "UMLS:C0000608": ("chemicals", "MESH:D015119", CHEMICAL_ENTITY),
    }


# APPLYING OWNERSHIP


@pytest.mark.unit
def test_cui_dropped_when_another_pipeline_owns_it(rrf):
    """Taxon selects C0242726 by T002, but anatomy alone claims D018517, so taxon should not claim it."""
    path = ownership(rrf, {"anatomy": [("MESH:D018517", ANATOMICAL_ENTITY)], "taxon": []})
    assert run(rrf, TAXON_MAP, path, "taxon") == {}


@pytest.mark.unit
def test_cui_added_to_the_pipeline_that_owns_it(rrf):
    """Anatomy's semantic types never select a T002 CUI, but it alone claims D018517, so it should claim C0242726
    typed as that descriptor."""
    path = ownership(rrf, {"anatomy": [("MESH:D018517", ANATOMICAL_ENTITY)], "taxon": []})
    assert run(rrf, ANATOMY_MAP, path, "anatomy") == {"UMLS:C0242726": ANATOMICAL_ENTITY}


@pytest.mark.unit
def test_cui_kept_when_both_pipelines_claim_its_descriptor(rrf):
    """A descriptor cross-listed in both pipelines' MeSH ids decides nothing: the semantic types stand, so
    taxon keeps C0242726 and anatomy does not add it (that would recreate the duplicate)."""
    path = ownership(
        rrf, {"anatomy": [("MESH:D018517", ANATOMICAL_ENTITY)], "taxon": [("MESH:D018517", ORGANISM_TAXON)]}
    )
    assert run(rrf, TAXON_MAP, path, "taxon") == {"UMLS:C0242726": ORGANISM_TAXON}
    assert run(rrf, ANATOMY_MAP, path, "anatomy") == {}


@pytest.mark.unit
def test_cui_kept_when_two_other_pipelines_claim_its_descriptor(rrf):
    """Taxon selects C0242726; if anatomy *and* chemicals both claim D018517, neither will add it, so taxon must
    keep it rather than drop it into the leftover compendium."""
    path = ownership(
        rrf,
        {
            "anatomy": [("MESH:D018517", ANATOMICAL_ENTITY)],
            "chemicals": [("MESH:D018517", CHEMICAL_ENTITY)],
            "taxon": [],
        },
    )
    assert run(rrf, TAXON_MAP, path, "taxon") == {"UMLS:C0242726": ORGANISM_TAXON}
    assert run(rrf, ANATOMY_MAP, path, "anatomy") == {}


@pytest.mark.unit
def test_cui_kept_when_nobody_claims_its_descriptor(rrf):
    """A descriptor in a tree no pipeline ingests leaves the CUI where its semantic types put it."""
    path = ownership(rrf, {"anatomy": [], "taxon": []})
    assert run(rrf, TAXON_MAP, path, "taxon") == {"UMLS:C0242726": ORGANISM_TAXON}


@pytest.mark.unit
def test_t116_cui_with_chemical_descriptor_moves_from_protein_to_chemicals(rrf):
    """C0000608 is T116 so protein selects it and chemicals' blocklist removes it; because D015119 is a chemical
    descriptor, protein should drop it and chemicals should re-admit it typed ChemicalEntity (#308)."""
    path = ownership(rrf, {"protein": [], "chemicals": [("MESH:D015119", CHEMICAL_ENTITY)]})
    assert run(rrf, PROTEIN_MAP, path, "protein") == {"UMLS:C0000005": PROTEIN}
    assert run(rrf, CHEMICAL_MAP, path, "chemicals", blocklist_umls_semantic_type_tree=PROTEIN_STY_TREES) == {
        "UMLS:C0000608": CHEMICAL_ENTITY
    }


@pytest.mark.unit
def test_cui_with_entry_term_atoms_only_is_untouched(rrf):
    """C0000005 maps to D012711 only through PEP/ET atoms, which build_sets() ignores, so even a chemical claim on
    D012711 should neither drop it from protein nor add it to chemicals."""
    path = ownership(rrf, {"protein": [], "chemicals": [("MESH:D012711", CHEMICAL_ENTITY)]})
    assert "UMLS:C0000005" in run(rrf, PROTEIN_MAP, path, "protein")
    assert "UMLS:C0000005" not in run(
        rrf, CHEMICAL_MAP, path, "chemicals", blocklist_umls_semantic_type_tree=PROTEIN_STY_TREES
    )


@pytest.mark.unit
def test_cui_the_owner_refuses_stays_where_its_semantic_types_put_it(rrf):
    """If the only claimant of D018517 blocks C0242726 (as disease's badumls does), taxon should keep it and the
    claimant should not get it -- not drop it from taxon and leave it nowhere."""
    path = ownership(rrf, {"disease": [("MESH:D018517", "biolink:Disease")], "taxon": []}, {"disease": ["C0242726"]})
    assert run(rrf, TAXON_MAP, path, "taxon") == {"UMLS:C0242726": ORGANISM_TAXON}
    assert run(rrf, ANATOMY_MAP, path, "disease", blocklist_umls_ids={"C0242726"}) == {}


@pytest.mark.unit
def test_ownership_computed_without_the_blocklist_raises(rrf):
    """An ownership file that gives a pipeline a CUI on its own blocklist was built from a different blocklist;
    write_umls_ids() should raise rather than add a CUI the pipeline refuses."""
    path = ownership(rrf, {"disease": [("MESH:D018517", "biolink:Disease")]})
    with pytest.raises(ValueError, match="blocks"):
        run(rrf, ANATOMY_MAP, path, "disease", blocklist_umls_ids={"C0242726"})


@pytest.mark.unit
def test_mesh_ownership_needs_the_pipeline(rrf):
    """Passing an ownership file without naming the pipeline it applies to should raise rather than guess."""
    with pytest.raises(ValueError):
        umls.write_umls_ids(str(rrf / "MRSTY.RRF"), TAXON_MAP, str(rrf / "UMLS"), mesh_ownership=str(rrf / "x"))


@pytest.mark.unit
def test_without_mesh_ownership_semantic_types_alone_decide(rrf):
    """The rule is optional: the classic call writes exactly what the category map selects."""
    out = rrf / "UMLS"
    umls.write_umls_ids(str(rrf / "MRSTY.RRF"), PROTEIN_MAP, str(out))
    assert read_ids(out) == {"UMLS:C0000608": PROTEIN, "UMLS:C0000005": PROTEIN}


# PIPELINE WRAPPERS

# C0000722 "Abbreviated Injury Scale": T170 Intellectual Product, which no pipeline selects by semantic type.
UNSELECTED_MRSTY = "C0000722|T170|A2.4|Intellectual Product|AT17737160||\n"


@pytest.mark.unit
@pytest.mark.parametrize(
    "pipeline,write",
    [
        ("anatomy", lambda mrsty, out, own, badumls: anatomy.write_umls_ids(mrsty, out, mesh_ownership=own)),
        ("chemicals", lambda mrsty, out, own, badumls: chemicals.write_umls_ids(mrsty, out, mesh_ownership=own)),
        (
            "disease",
            lambda mrsty, out, own, badumls: diseasephenotype.write_umls_ids(mrsty, out, badumls, mesh_ownership=own),
        ),
        ("protein", lambda mrsty, out, own, badumls: protein.write_umls_ids(mrsty, out, mesh_ownership=own)),
        ("taxon", lambda mrsty, out, own, badumls: taxon.write_umls_ids(mrsty, out, mesh_ownership=own)),
    ],
)
def test_each_mesh_owning_wrapper_applies_ownership_under_its_config_name(tmp_path, pipeline, write):
    """Each MeSH-owning pipeline's write_umls_ids() should add a CUI the ownership file gives to its
    ``umls_mesh_owning_pipelines`` name. A wrapper passing a different name would silently treat its own CUIs
    as another pipeline's and drop them."""
    assert pipeline in get_config()["umls_mesh_owning_pipelines"]
    (tmp_path / "MRSTY.RRF").write_text(UNSELECTED_MRSTY)
    (tmp_path / "badumls").write_text("#nothing blocked\n")
    (tmp_path / "own").write_text(f"UMLS:C0000722\t{pipeline}\tMESH:D015998\tbiolink:Thing\n")
    write(str(tmp_path / "MRSTY.RRF"), str(tmp_path / "UMLS"), str(tmp_path / "own"), str(tmp_path / "badumls"))
    assert read_ids(tmp_path / "UMLS") == {"UMLS:C0000722": "biolink:Thing"}


# PIPELINES WITHOUT MESH (gene and process)

# C0004372 "Autolysis": T043 Cell Function, which the process pipeline selects, but its descriptor D001329 is in
# MeSH's C23 tree, which the disease pipeline claims.
PROCESS_MRSTY = "C0004372|T043|B2.2.1.1.3|Cell Function|AT265805073||\n"
PROCESS_MRCONSO = "C0004372|ENG|P|L0004372|PF|S0016981|Y|A0028052||M0001990|D001329|MSH|MH|D001329|Autolysis|0|N||\n"

# C1413234 "CD68 gene": T028 Gene or Genome, which the gene pipeline selects, but UMLS also gives it the MeSH
# descriptor D000097382, which the protein pipeline claims.
GENE_MRSTY = "C1413234|T028|A1.2.3.5|Gene or Genome|AT40051042||\n"
GENE_MRCONSO = (
    "C1413234|ENG|P|L5135306|PF|S5868440|N|A20787987||HGNC:1693||HGNC|MTH_ACR|HGNC:1693|CD68 gene|0|N||\n"
    "C1413234|ENG|S|L6387821|PF|S22528022|Y|A35665631||M000763930|D000097382|MSH|MH|D000097382|CD68 Molecule|0|N||\n"
)


@pytest.mark.unit
def test_gene_cui_is_not_given_to_the_descriptors_owner(tmp_path):
    """Gene concords UMLS only to HGNC, so C1413234 "CD68 gene" drags no descriptor: with the gene pipeline's
    ids/UMLS passed to the ownership rule, protein should not add it although it alone claims D000097382, and
    gene's own output should be unchanged."""
    (tmp_path / "MRSTY.RRF").write_text(GENE_MRSTY)
    (tmp_path / "MRCONSO.RRF").write_text(GENE_MRCONSO)
    gene.write_umls_ids(str(tmp_path / "MRCONSO.RRF"), str(tmp_path / "MRSTY.RRF"), str(tmp_path / "gene_UMLS"))
    assert read_ids(tmp_path / "gene_UMLS") == {"UMLS:C1413234": "biolink:Gene"}

    claims = {"protein": [("MESH:D000097382", PROTEIN)]}
    assert umls.read_mesh_ownership(ownership(tmp_path, claims, non_mesh_cuis=["UMLS:C1413234"])) == {}
    # Without the gene pipeline's CUIs, protein would own C1413234 and add it: the duplicate this prevents.
    assert umls.read_mesh_ownership(ownership(tmp_path, claims)) == {
        "UMLS:C1413234": ("protein", "MESH:D000097382", PROTEIN)
    }


@pytest.mark.unit
def test_process_cui_is_not_given_to_the_descriptors_owner(tmp_path):
    """Process concords UMLS only to GO, so C0004372 "Autolysis" should not go to disease, the sole claimant of
    D001329, once the process pipeline's CUIs are passed to the ownership rule."""
    (tmp_path / "MRSTY.RRF").write_text(PROCESS_MRSTY)
    (tmp_path / "MRCONSO.RRF").write_text(PROCESS_MRCONSO)
    processactivitypathway.write_umls_ids(str(tmp_path / "MRSTY.RRF"), str(tmp_path / "process_UMLS"))
    assert read_ids(tmp_path / "process_UMLS") == {"UMLS:C0004372": "biolink:BiologicalProcess"}

    claims = {"disease": [("MESH:D001329", "biolink:PhenotypicFeature")]}
    assert umls.read_mesh_ownership(ownership(tmp_path, claims, non_mesh_cuis=["UMLS:C0004372"])) == {}
