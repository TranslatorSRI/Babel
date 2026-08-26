import io
from collections import defaultdict
from os import path

import src.datahandlers.doid as doid
import src.datahandlers.efo as efo
import src.datahandlers.mesh as mesh
import src.datahandlers.obo as obo
import src.datahandlers.umls as umls
from src.babel_utils import (
    get_prefixes,
    glom,
    read_badxrefs,
    read_identifier_file,
    remove_overused_xrefs,
    write_compendium,
)
from src.categories import DISEASE, PHENOTYPIC_FEATURE
from src.datahandlers.gard import normalize_gard_curie
from src.metadata.provenance import write_concord_metadata
from src.prefixes import (
    DOID,
    GARD,
    HP,
    ICD0,
    ICD9,
    ICD10,
    ICD11,
    MEDDRA,
    MESH,
    MGI,
    MONDO,
    MP,
    NCIT,
    OMIM,
    ORPHANET,
    SNOMEDCT,
    UMLS,
)
from src.ubergraph import build_sets
from src.util import Text, get_config, get_logger, get_repo_root

logger = get_logger(__name__)

DISEASE_OBO_SOURCES = {
    MP: {"root": f"{MP}:0000001", "type": PHENOTYPIC_FEATURE},
}

# Renames whose target prefix depends on the CURIE's *local id*, not just its source prefix, keyed
# by the Babel prefix `config.yaml: disease_xref_prefixes` names. YAML cannot say "OMIM, unless the
# id starts with PS, in which case OMIM.PS", so the config states the common case and the exception
# lives here; norm() accepts either a replacement prefix or a callable taking the whole CURIE.
#
# GARD is a rename of the local id, not the prefix: sources write the registry's zero-padded form
# (GARD:0006038) and Babel standardizes on the unpadded one (GARD:6038). Every source that emits GARD
# xrefs must list `GARD: GARD` in its disease_xref_prefixes block to pick this up; a source that does
# not would reintroduce the padded/unpadded split (see src/datahandlers/gard.py, "Local-id form").
LOCAL_ID_DEPENDENT_RENAMES = {
    OMIM: lambda curie: Text.omim_curie(Text.un_curie(curie)),
    GARD: normalize_gard_curie,
}


def get_xref_prefix_map(source):
    """Return `config.yaml: disease_xref_prefixes[source]` as an `other_prefixes` map for `norm()`.

    Every target prefix is checked against src/prefixes.py (via `Text.prefixmap`, which is built
    from it) so a typo fails the build here, rather than renaming CURIEs into a namespace no ids
    file carries -- which `norm()` cannot detect and `glom()` happily merges through. A value listed
    in `LOCAL_ID_DEPENDENT_RENAMES` is swapped for its callable.
    """
    mapping = get_config()["disease_xref_prefixes"][source]
    known = set(Text.prefixmap.values())
    unknown = sorted({v for v in mapping.values() if v not in known})
    if unknown:
        raise ValueError(
            f"config.yaml: disease_xref_prefixes[{source}] renames to {unknown}, which src/prefixes.py "
            f"does not define. Add the constant there first, so the rename lands on a prefix Babel uses."
        )
    return {key: LOCAL_ID_DEPENDENT_RENAMES.get(value, value) for key, value in mapping.items()}


# Prefixes glom() rejects a *same-prefix* merge for: if a clique would end up holding two
# distinct identifiers of one of these prefixes (e.g. two different MONDO ids, or two
# different HP/MP ids), the merge is refused rather than silently accepted. MONDO, HP, and MP
# are each meant to be the single authoritative identifier of their kind within a clique, so
# an accidental same-prefix collision (e.g. via a bad transitive MESH/SNOMED bridge) is a
# data-quality signal worth catching rather than merging through.
DISEASE_UNIQUE_PREFIXES = [MONDO, HP, MP]

# Prefix groups that must never co-occur in a single clique. After glom, any clique that
# holds identifiers from two or more prefixes within a group is split: the first-listed
# prefix (and every identifier whose prefix is not in the group) stays in the clique, and
# each subsequent prefix's identifiers are peeled into their own clique. HP (human) and MP
# (mammalian) phenotypes are kept disjoint at the SMEs' request: a human phenotype clique
# must not absorb a mouse-phenotype identifier (and vice versa), even though UberGraph xrefs
# (e.g. HP↔MP, EFO↔MP, or transitively via MESH/SNOMED/MONDO) would otherwise merge them.
# HP is listed first so the existing human/disease clique is preserved and MP is the part
# that splits off. MP may still merge with non-HP disease ids (MONDO/MESH); only HP triggers
# a split. See docs/sources/MP/disjointness.md.
MUTUALLY_EXCLUSIVE_PREFIX_GROUPS = [[HP, MP]]

# EFO's direct xrefs to MP are dropped when building concords/EFO. EFO is a species-agnostic /
# human-leaning ontology, so an EFO phenotype term xref'd to an MP term is ambiguous: it may be
# human-scoped (which, like HP, must stay disjoint from MP) or genuinely mammalian, and we have
# no reliable signal to tell them apart. Per the SMEs' MP/HP disjointness request we therefore do
# not trust EFO->MP xrefs and remove them at the source. This is the EFO-source complement to the
# [HP, MP] post-glom split above (HP->MP is handled there); it is applied via
# efo.make_concords(..., excluded_target_prefixes=...). See docs/sources/MP/disjointness.md.
EFO_EXCLUDED_XREF_PREFIXES = [MP]

# Target prefixes we accept from MP's UberGraph xrefs; every other target is dropped. MP uses
# oboInOwl:hasDbXref to mean "this phenotype is *about* that thing", not "is equivalent to it",
# so most of its xref targets are category errors: CL/MA/FMA are the cell or anatomical structure
# the abnormality occurs in (MP:0009873 "abnormal aorta tunica media morphology" -> MA:0002903
# "aorta tunica media"), GO is the process the phenotype perturbs (MP:0002998 "abnormal bone
# remodeling" -> GO:0046849 "bone remodeling"), NBO is a behavior, NLX a cell type, and
# Fyler/PMID/http(s) are registry codes, citations and Wikipedia links. What survives:
#   HP    - genuine phenotype equivalences (the [HP, MP] split above still separates them).
#   MGI   - MGI phenotype-slim terms, the same kind of thing as the MP term.
#   MPATH - mouse pathology lesions, phenotype-shaped.
#   UMLS  - phenotype concepts, and a prefix the disease compendia actually carry.
# This is an allowlist rather than an ignore_list so that a namespace MP newly starts emitting is
# rejected by default and surfaces as a review decision instead of a silent regression. Matched
# against Text.get_prefix_or_none(), which upper-cases, hence "MPATH" (no prefixes.py constant
# exists for it; Babel does not ingest MPATH). See docs/sources/MP/mappings.md.
MP_XREF_ALLOWED_PREFIXES = [HP, MGI, "MPATH", UMLS]

# DOID's ICD target prefixes. An ICD code names a disease *family*, never one disease: DOID:2476
# "hereditary spastic paraplegia" carries ICD10:G11.4, and so does every one of its 60 subtypes, so
# glom() fused 61 mutually exclusive diseases into one 223-identifier clique. These are the
# post-norm() spellings (ICD10, not ICD10CM); DOID emits 5 ICD11 rows.
#
# They are NOT excluded categorically. Of 6,425 ICD rows only 1,584 sit on a target claimed by 2+
# DOID terms; the other 4,841 are 1:1 and many are plainly right (ICD10:A01.0 "Typhoid fever" ->
# DOID:13258 "typhoid fever"). Dropping the namespace outright deleted all 4,841 to suppress the
# 1,584. Instead OVERUSE_FILTERED_CONCORDS scopes remove_overused_xrefs to exactly these prefixes:
# the four merge engines go (ICD10:H90.3 with 134 subjects, H35.5 with 107, G11.4 with 60, G60.0
# with 58) and the 1:1 rows stay. The objection that a 1:1 code today could fuse a subtype DOID adds
# tomorrow is handled by construction -- the filter recounts every build, so the row goes the day a
# second subtype cites it. See docs/sources/DOID/mappings.md.
DOID_ICD_XREF_PREFIXES = [ICD10, ICD9, ICD0, ICD11]

# Concord file basenames whose pair stream is filtered through remove_overused_xrefs before glom,
# mapped to the target prefixes the filter may drop (None = every namespace, the original
# behaviour). Other concord sources are trusted as-is. MONDO/HP/EFO/MP are unscoped because their
# UberGraph xrefs have the "one xref target claimed by many source ids" failure mode across the
# board; DOID is scoped to ICD and GARD because those are the only namespaces of its xrefs where
# being claimed twice reliably means the mapping is wrong -- MESH:D010195 "Pancreatitis" is
# legitimately claimed by both DOID:4989 "pancreatitis" and DOID:2913 "acute pancreatitis", and an
# unscoped filter would discard the correct mapping along with the too-broad one.
#
# GARD joins DOID's list because a registry term names one rare disease, so two DOID terms citing
# it is the ICD family-code failure in miniature. 12 of DOID's 2,196 distinct GARD targets are
# claimed twice: GARD:625 fuses DOID:0051080 "Alport syndrome 3B" with DOID:0110033 "Alport
# syndrome 2", and GARD:7674 pulls DOID:0060160 "childhood spinal muscular atrophy" out of its
# MONDO clique. MONDO's own mapping (MONDO_GARD) still places every one of the 12 in its proper
# clique, so the filter costs nothing -- and it makes DOID:0061030 "hemophilia"'s typo GARD:0418 ->
# GARD:418 "Essential pentosuria" (claimed by DOID:0111258 too) a no-op; see src/datahandlers/gard.py.
#
# MONDO_GARD is MONDO's hasDbXref mapping to GARD, taken without a bad-xrefs file. It is 1:1 today
# (tests/pipeline/test_mondo_gard.py), but MONDO is a unique prefix, so a future release mapping one
# GARD id to two MONDO terms would make glom() hand the id to whichever row sorts first with no
# error. Routing it through the filter turns that into "neither claims it" -- the id then reaches
# its clique only through DOID, or stays a single-identifier clique -- instead of a silent wrong
# merge. The file is deduplicated when written (build_disease_obo_relationships), which matters
# here: the filter counts rows, so a pair written once per MONDO root would count as overused.
OVERUSE_FILTERED_CONCORDS = {
    "MONDO": None,
    "HP": None,
    "EFO": None,
    "MP": None,
    "DOID": DOID_ICD_XREF_PREFIXES + [GARD],
    f"{MONDO}_{GARD}": [GARD],
}

# Per-source bad-xref files used when build_compendium is called without explicit
# badxrefs (e.g. by the source-impact report CLI). The Snakemake call site still
# passes its own explicit dict so production behaviour is unchanged. Anchored at the repo
# root because the CLI, unlike Snakemake, need not run from there.
#
# A new prefix's key must be added HERE and in the `disease_compendia` rule's explicit dict
# (src/snakefiles/diseasephenotype.snakefile). A prefix missing from whichever dict was actually
# passed in simply never filters anything; a key that matches no concord basename (a typo, or the
# two dicts drifting apart) is caught by the guard in compute_cliques_for_impact_report().
DEFAULT_BAD_XREFS = {
    "HP": str(get_repo_root() / "input_data/badHPx.txt"),
    "MONDO": str(get_repo_root() / "input_data/mondo_badxrefs.txt"),
    "MP": str(get_repo_root() / "input_data/mp_badxrefs.txt"),
    "UMLS": str(get_repo_root() / "input_data/umls_badxrefs.txt"),
}

# MONDO_close lives in the same intermediate concords/ directory as ordinary concord
# files but is fed to glom() as `close={MONDO: ...}` rather than as a pair stream.
# When discovering inputs from disk (impact-report CLI) we have to recognise it by
# name and pull it out of the iterated list.
MONDO_CLOSE_BASENAME = "MONDO_close"


def write_obo_ids(irisandtypes, outfile, exclude=[]):
    order = [DISEASE, PHENOTYPIC_FEATURE]
    obo.write_obo_ids(irisandtypes, outfile, order, exclude=[])


def write_ncit_ids(outfile):
    disease_id = f"{NCIT}:C2991"
    phenotypic_feature_id = f"{NCIT}:C3367"
    write_obo_ids([(disease_id, DISEASE), (phenotypic_feature_id, PHENOTYPIC_FEATURE)], outfile, exclude=[])


def write_mondo_ids(outfile):
    disease_id = f"{MONDO}:0000001"
    disease_sus_id = f"{MONDO}:0042489"
    write_obo_ids([(disease_id, DISEASE), (disease_sus_id, DISEASE)], outfile)


def write_efo_ids(owlfile, outfile):
    disease_id = "EFO:0000408"
    phenotype_id = "EFO:0000651"
    measurement_id = "EFO:0001444"
    efos = [(disease_id, DISEASE), (phenotype_id, PHENOTYPIC_FEATURE), (measurement_id, PHENOTYPIC_FEATURE)]
    efo.make_ids(efos, owlfile, outfile)


def write_hp_ids(outfile):
    # Phenotype
    phenotype_id = "HP:0000118"
    write_obo_ids([(phenotype_id, PHENOTYPIC_FEATURE)], outfile)


def write_mp_ids(outfile):
    """Collect MP (Mammalian Phenotype Ontology) identifiers from UberGraph.

    MP is a standard rdfs:subClassOf hierarchy rooted at MP:0000001, so the default
    subClassOf walk (write_obo_ids, same as write_hp_ids) reaches every term. Every MP
    term is typed as PhenotypicFeature.
    """
    root = DISEASE_OBO_SOURCES[MP]["root"]
    biolink_type = DISEASE_OBO_SOURCES[MP]["type"]
    write_obo_ids([(root, biolink_type)], outfile)


def write_phenotype_taxa(idfile, taxon, outfile):
    """Write a ``babel_downloads/<PREFIX>/taxa`` file assigning a fixed taxon to every
    identifier in a phenotype ontology's ids file.

    HP and MP terms are inherently taxon-scoped: every HP term we ingest describes a human
    (NCBITaxon:9606) phenotype and every MP term a mammalian (NCBITaxon:40674) phenotype.
    Rather than re-walk the ontology, we read the already-built ids file (the authoritative
    set of identifiers Babel ingests for that prefix, ``CURIE\\tbiolink:Type`` per row) so the
    taxa file covers exactly those CURIEs and never drifts from what lands in the compendia.
    TaxonFactory then reads this file to populate each identifier's ``t`` field; a clique that
    mixes HP and MP members ends up with both taxa via the per-clique union in write_compendium.

    :param idfile: path to the prefix's ids file (``CURIE\\tbiolink:Type`` rows)
    :param taxon: the NCBITaxon CURIE to assign to every identifier (e.g. ``"NCBITaxon:9606"``)
    :param outfile: path to the taxa file to write (``CURIE\\tNCBITaxon:NNNN`` rows)
    """
    if not taxon.startswith("NCBITaxon:"):
        raise ValueError(f"Phenotype taxon must be an NCBITaxon CURIE, got {taxon!r}")
    with open(idfile) as inf, open(outfile, "w") as outf:
        for line in inf:
            stripped = line.strip()
            if not stripped:
                continue
            curie = stripped.split("\t", maxsplit=1)[0]
            outf.write(f"{curie}\t{taxon}\n")


def write_omim_ids(infile, outfile):
    with open(infile) as inf, open(outfile, "w") as outf:
        for line in inf:
            if line.startswith("#"):
                continue
            chunks = line.split("\t")
            if "phenotype" in chunks[1]:
                outf.write(f"{OMIM}:{chunks[0]}\t{DISEASE}\n")


def write_mesh_ids(outfile):
    # NOTE: C02 (Virus Diseases) and C03 (Parasitic Diseases) are intentionally absent
    # below.  Before adding them, verify that MESH terms in those trees have adequate
    # cross-references to MONDO (our primary disease source) and/or HPO (our primary
    # phenotype source) so that the resulting cliques are well-connected.  Without those
    # mappings the MESH IDs would appear as isolated singletons in the disease compendium.
    dcodes = [
        "C01",
        "C04",
        "C05",
        "C06",
        "C07",
        "C08",
        "C09",
        "C10",
        "C11",
        "C12",
        "C13",
        "C14",
        "C15",
        "C16",
        "C17",
        "C18",
        "C19",
        "C20",
        "C21",
        "C22",
        "C24",
        "C25",
        "C26",
    ]
    meshmap = {i: DISEASE for i in dcodes}
    meshmap["C23"] = PHENOTYPIC_FEATURE
    mesh.write_ids(meshmap, outfile, order=[DISEASE, PHENOTYPIC_FEATURE])


def write_umls_ids(mrsty, outfile, badumlsfile):
    badumls = set()
    with open(badumlsfile) as inf:
        for line in inf:
            if line.startswith("#"):
                continue
            umlscui = line.split()[0]
            badumls.add(umlscui)
    # Disease
    # B2.2.1.2.1 Disease or Syndrome
    # A1.2.2.1 Congenital Abnormality
    # A1.2.2.2 Acquired Abnormality
    # B2.3 Injury or Poisoning
    # B2.2.1.2 Pathologic Function
    # B2.2.1.2.1.1 Mental or Behavioral Dysfunction
    # B2.2.1.2.2 Cell or Molecular Dysfunction
    # A1.2.2 Anatomical Abnormality
    # B2.2.1.2.1.2 Neoplastic Process
    umlsmap = {
        x: DISEASE
        for x in [
            "B2.2.1.2.1",
            "A1.2.2.1",
            "A1.2.2.2",
            "B2.3",
            "B2.2.1.2",
            "B2.2.1.2.1.1",
            "B2.2.1.2.2",
            "A1.2.2",
            "B2.2.1.2.1.2",
        ]
    }
    # A2.2 Finding (T033) and A2.2.1 Laboratory or Test Result (T034) are deliberately NOT mapped
    # here. "Finding" is far too broad to be a phenotypic feature -- it globs in things like
    # "Negative" and pulls nonsense into e.g. the Alzheimer node -- and a lab/test result is a
    # clinical finding rather than a generic phenotype. Instead of claiming them as
    # PhenotypicFeature, we let any such concept that is not otherwise connected to a disease or
    # phenotype clique fall through to the leftover UMLS compendium, where STY_OVERRIDES re-types
    # it (T033 -> biolink:Phenomenon, T034 -> biolink:ClinicalFinding). See
    # src/createcompendia/leftover_umls.py and https://github.com/NCATSTranslator/Babel/issues/569.
    #
    # A2.2.2 Sign or Symptom (T184) genuinely is a phenotypic feature, so it stays.
    umlsmap["A2.2.2"] = PHENOTYPIC_FEATURE
    # A2.3 Organism Attribute
    # Includes things like "Age" which will merge with EFOs
    umlsmap["A2.3"] = PHENOTYPIC_FEATURE
    umls.write_umls_ids(mrsty, umlsmap, outfile, blocklist_umls_ids=badumls)


def build_disease_obo_relationships(outdir, metadata_yamls):
    # Create the equivalence pairs
    with open(f"{outdir}/{HP}", "w") as outfile:
        other_prefixes = get_xref_prefix_map(HP)
        build_sets(f"{HP}:0000118", {HP: outfile}, ignore_list=["ICD"], other_prefixes=other_prefixes, set_type="xref")

    write_concord_metadata(
        metadata_yamls["HP"],
        name="build_disease_obo_relationships()",
        sources=[{"type": "UberGraph", "name": "HP"}],
        description=f"ubergraph.build_sets() of {HP}:0000118 with other_prefixes {other_prefixes}",
        concord_filename=f"{outdir}/{HP}",
    )

    mondo_prefixes = get_xref_prefix_map(MONDO)
    with open(f"{outdir}/{MONDO}", "w") as outfile:
        build_sets("MONDO:0000001", {MONDO: outfile}, set_type="exact", other_prefixes=mondo_prefixes)
        build_sets("MONDO:0042489", {MONDO: outfile}, set_type="exact", other_prefixes=mondo_prefixes)

    write_concord_metadata(
        metadata_yamls["MONDO"],
        name="build_disease_obo_relationships()",
        sources=[{"type": "UberGraph", "name": "MONDO"}],
        description=f"ubergraph.build_sets() (exact) of {MONDO}:0000001 and {MONDO}:0042489, including ORPHANET prefixes",
        concord_filename=f"{outdir}/{MONDO}",
    )

    with open(f"{outdir}/{MONDO}_close", "w") as outfile:
        build_sets("MONDO:0000001", {MONDO: outfile}, set_type="close", other_prefixes=mondo_prefixes)
        build_sets("MONDO:0042489", {MONDO: outfile}, set_type="close", other_prefixes=mondo_prefixes)

    write_concord_metadata(
        metadata_yamls["MONDO_close"],
        name="build_disease_obo_relationships()",
        sources=[{"type": "UberGraph", "name": "MONDO"}],
        description=f"ubergraph.build_sets() (close matches) of {MONDO}:0000001 and {MONDO}:0042489, including ORPHANET prefixes",
        concord_filename=f"{outdir}/{MONDO}_close",
    )

    # MONDO's GARD mappings are the one hasDbXref exception in this build. MONDO asserts all
    # ~15,930 of them as oboInOwl:hasDbXref and *none* as skos:exactMatch/closeMatch, so the two
    # concords above see none of them -- and GARD, being a registry rather than an ontology,
    # asserts no mappings of its own. Without this file 98% of GARD lands in single-identifier
    # cliques that duplicate concepts MONDO already names, which is exactly the split-clique
    # outcome an ingest is supposed to avoid (see docs/AddingNewSources.md, "Prefer joining an
    # existing clique").
    #
    # It gets its own concord rather than being folded into MONDO's, and the allowlist is
    # fail-closed at exactly one prefix, because MONDO's *other* hasDbXref targets are emphatically
    # not equivalences -- an unrestricted pass would merge on ICD/MEDDRA family codes the way
    # DOID's did (#1031). Keeping it separate puts the exception in `disease_concords` where a
    # reviewer sees it, and lets it be filtered or dropped without touching MONDO's exact matches.
    # The mapping is 1:1 in both directions today (tests/pipeline/test_mondo_gard.py checks the
    # real concord); OVERUSE_FILTERED_CONCORDS drops any GARD id a future MONDO release maps
    # twice, so that property is enforced at build time and not only in a pipeline test.
    #
    # MONDO writes GARD in the registry's zero-padded form (GARD:0022702); `mondo_prefixes` carries
    # the GARD -> normalize_gard_curie rename that strips it (LOCAL_ID_DEPENDENT_RENAMES), without
    # which these would not join DOID's unpadded xrefs or GARD's own ids file. See
    # docs/sources/MONDO/README.md.
    #
    # Terms under both roots would be written once per root; the rows are deduplicated because
    # remove_overused_xrefs counts rows, and a doubled pair would read as a GARD id claimed twice.
    buffer = io.StringIO()
    for mondo_root in ("MONDO:0000001", "MONDO:0042489"):
        build_sets(mondo_root, {MONDO: buffer}, set_type="xref", other_prefixes=mondo_prefixes, allowed_prefixes={GARD})
    with open(f"{outdir}/{MONDO}_{GARD}", "w") as outfile:
        outfile.writelines(sorted(set(buffer.getvalue().splitlines(keepends=True))))

    write_concord_metadata(
        metadata_yamls[f"{MONDO}_{GARD}"],
        name="build_disease_obo_relationships()",
        sources=[{"type": "UberGraph", "name": "MONDO"}],
        description=(
            "ubergraph.build_sets() (oboInOwl:hasDbXref) of MONDO:0000001 and MONDO:0042489, "
            f"restricted to target prefix {GARD} and unpadded by gard.normalize_gard_curie(). "
            "MONDO asserts its GARD mappings only as hasDbXref, never as skos:exactMatch, so they "
            "are invisible to the MONDO/MONDO_close concords; every other hasDbXref target is "
            "deliberately excluded. Rationale and the excluded namespaces: "
            "docs/sources/MONDO/README.md"
        ),
        url="https://github.com/NCATSTranslator/Babel/blob/main/docs/sources/MONDO/README.md",
        concord_filename=f"{outdir}/{MONDO}_{GARD}",
    )

    # MP cross-references. Standard subClassOf walk from the MP root. Most of MP's declared
    # xrefs point at anatomy, processes or citations rather than equivalent phenotypes, so only
    # the MP_XREF_ALLOWED_PREFIXES targets are kept. SSSOM-derived MP↔HP mappings are
    # intentionally not loaded here — see docs/sources/MP/mappings.md.
    mp_root = DISEASE_OBO_SOURCES[MP]["root"]
    with open(f"{outdir}/{MP}", "w") as outfile:
        build_sets(mp_root, {MP: outfile}, set_type="xref", allowed_prefixes=MP_XREF_ALLOWED_PREFIXES)

    write_concord_metadata(
        metadata_yamls[MP],
        name="build_disease_obo_relationships()",
        sources=[{"type": "UberGraph", "name": MP}],
        description=f"ubergraph.build_sets() (xref) of {mp_root}, "
        f"restricted to target prefixes {MP_XREF_ALLOWED_PREFIXES}",
        concord_filename=f"{outdir}/{MP}",
    )


def build_disease_efo_relationships(owlfile, idfile, outfile, metadata_yaml):
    efo.make_concords(
        owlfile,
        idfile,
        outfile,
        provenance_metadata=metadata_yaml,
        excluded_target_prefixes=EFO_EXCLUDED_XREF_PREFIXES,
    )


def build_disease_umls_relationships(mrconso, idfile, outfile, omimfile, ncitfile, metadata_yaml):
    # UMLS contains xrefs between a disease UMLS and a gene OMIM. So here we are saying: if you are going to link to
    # an omim identifier, make sure it's a disease omim, not some other thing.
    good_ids = {}
    for prefix, prefixidfile in [(OMIM, omimfile), (NCIT, ncitfile)]:
        good_ids[prefix] = set()
        with open(prefixidfile) as inf:
            for line in inf:
                x = line.split()[0]
                good_ids[prefix].add(x)
    umls.build_sets(
        mrconso,
        idfile,
        outfile,
        {"SNOMEDCT_US": SNOMEDCT, "MSH": MESH, "NCI": NCIT, "HPO": HP, "MDR": MEDDRA, "OMIM": OMIM},
        acceptable_identifiers=good_ids,
        provenance_metadata_yaml=metadata_yaml,
    )


def build_disease_doid_relationships(idfile, outfile, metadata_yaml):
    other_prefixes = get_xref_prefix_map(DOID)
    # No excluded_target_prefixes: DOID's ICD rows stay in the concord and are filtered at glom
    # time by OVERUSE_FILTERED_CONCORDS, which drops only the codes claimed by 2+ DOID terms. That
    # keeps the audit trail -- babel-overused-xrefs can still see every row that was considered.
    doid.build_xrefs(idfile, outfile, other_prefixes=other_prefixes)
    write_concord_metadata(
        metadata_yaml,
        name="build_disease_doid_relationships()",
        description=f"build_disease_doid_relationships() using the DOID ID file {idfile} and other_prefixes "
        f"{other_prefixes}; ICD and GARD targets ({DOID_ICD_XREF_PREFIXES + [GARD]}) are kept here and overuse-filtered at glom",
        concord_filename=outfile,
        sources=[{"type": "DOID", "name": "doid.build_xrefs"}],
    )


def normalize_label_for_matching(label):
    """Fold a label to the key `build_gard_label_concord()` matches on.

    Case-insensitive: a source that writes "Genu varum" and one that writes "Genu Varum" mean the
    same disease, and requiring identical casing does not merely lose those matches -- measured
    against the GARD ids MONDO and DOID already place, case-sensitive matching is ten times *worse*
    (2.64% wrong against 0.21%), because it skips past the right clique on a capital letter and then
    matches some other clique that happens to be capitalized the same way.
    """
    return label.strip().lower()


def build_gard_label_concord(
    gard_labels,
    match_ids_files,
    match_labels_files,
    all_ids_files,
    other_concords,
    mondoclose,
    badxrefs,
    outfile,
    metadata_yaml,
):
    """Link GARD registry terms to identically labelled identifiers already in this pipeline.

    GARD is a registry, not an ontology: it publishes no cross-references at all, so its terms reach
    cliques only through MONDO's and DOID's inbound xrefs (the MONDO_GARD and DOID concords). ~277
    registry terms are mapped by neither and ship as single-identifier cliques that duplicate a
    concept Babel already names -- `GARD:27461` "Odontogenic Carcinoma" sits alone beside the
    existing `UMLS:C5401355` + `NCIT:C173720` clique carrying the identical label. No source Babel
    ingests connects them: the registry CSV has no xref column, MRCONSO has no GARD source
    vocabulary, and NCIt asserts no GARD provenance. The label is the only signal there is, and it
    measures well -- see docs/sources/GARD/label-matches.md for the numbers and how to reproduce
    them.

    The rule, for each GARD id **no other disease concord mentions**: walk `match_ids_files` in order
    (the order of `config.yaml: disease_gard_label_match_prefixes`) and stop at the first vocabulary
    holding an identifier with the same normalized label. Emit one row if that vocabulary holds
    exactly one such identifier; skip the term otherwise.

    Three guards carry the safety of this concord, and none is optional:

    1. **Skip GARD ids another concord already places.** Those already sit in a curated clique, and
       re-deciding them by label would be a licence to fuse. Run over them as a held-out test, the
       rule disagrees with the xref-derived clique 33 times in 15,370 -- harmless as an error rate,
       catastrophic as 33 attempted merges of curated MONDO cliques.

    2. **At most one row per GARD id.** Because guard 1 leaves only GARD ids that appear in no other
       concord, each is a single-identifier clique, so one pair can only union `{GARD:x}` into the
       target's clique. This concord is therefore *structurally incapable* of merging two
       pre-existing cliques -- the invariant is enforced here, by construction, rather than detected
       downstream by a warning nobody reads (AGENTS.md, "A log warning is not a control"). Emitting
       every matching identifier instead would have put 475 existing clique pairs at risk of fusion.

    3. **Skip a target whose clique is not `biolink:Disease`.** `write_compendium`'s per-class prefix
       filter keeps GARD alive in Disease.txt only (`config.yaml: disease_extra_prefixes` is a
       Disease-only allowlist, and Biolink registers GARD for no class at all), so a GARD id that
       joins a phenotype clique is not moved to PhenotypicFeature.txt -- it is dropped from the
       build entirely, trading a working single-identifier clique for a vanished identifier. Five
       registry terms are in that position today (Cementoblastoma, Ileal Atresia, Phocomelia of the
       Lower Limb, Chilblains, Myokymia): Babel holds each as a phenotype because HP names it, while
       GARD calls it a rare disease. They stay as they are until GARD is registered in the Biolink
       Model, which is https://github.com/NCATSTranslator/Babel/issues/1051.

       This is the one guard that cannot be decided from labels: the clique that makes those five
       phenotypes is formed through UMLS, two hops from the target, so no amount of label or
       ids-file inspection sees it. So this glommed the other concords -- the same
       `compute_cliques_for_impact_report()` the build itself calls, and the same
       `classify_disease_clique()` that assigns the type -- and asks the question directly. It costs
       about five seconds and is exact; a hand-rolled proxy was tried first and was not.

    HP and MP are absent from `disease_gard_label_match_prefixes` for a related but separate reason:
    `split_mutually_exclusive_cliques()` keeps phenotype and disease cliques disjoint on purpose and
    `disease_gard_ids` types every registry term `biolink:Disease`, so matching a GARD term directly
    onto an HP or MP term asserts an identity this pipeline is built to refuse.

    There is no OVERUSE_FILTERED_CONCORDS entry. An identifier claimed by two GARD ids means the
    registry carries the same label twice; both joining the same clique is the right outcome, not an
    overuse to filter.

    :param gard_labels: `babel_downloads/GARD/labels`, the registry's labels (unpadded CURIEs).
    :param match_ids_files: this pipeline's ids files for the match pool, in priority order. The ids
        file, not the labels file, is the authoritative identifier set: `babel_downloads/NCIT/labels`
        carries every NCIt term, chemicals and anatomy included, and matching against those would
        link a rare disease to whatever else happens to share its name.
    :param match_labels_files: the corresponding `babel_downloads/<PREFIX>/labels`, same order.
    :param all_ids_files: every disease ids file, for the guard-3 glom. Passing only the match pool
        would rebuild a different clique structure than the build's, and the guard would answer for
        cliques that do not exist.
    :param other_concords: every other disease concord. Any GARD CURIE they name is skipped (guard
        1), and they are the input to the guard-3 glom.
    :param mondoclose: the MONDO_close concord, passed through to the guard-3 glom.
    :param badxrefs: the bad-xrefs map, passed through to the guard-3 glom.
    """
    if len(match_ids_files) != len(match_labels_files):
        raise ValueError(
            f"match_ids_files ({len(match_ids_files)}) and match_labels_files ({len(match_labels_files)}) "
            "must be parallel lists in the same priority order"
        )

    # Guard 1. Read the GARD ids from the concord files themselves rather than from a hardcoded
    # (MONDO_GARD, DOID) pair, so a third source that starts emitting GARD xrefs cannot silently
    # escape this.
    claimed = set()
    for concord in other_concords:
        with open(concord) as inf:
            for line in inf:
                for curie in line.rstrip("\n").split("\t"):
                    if Text.get_prefix_or_none(curie) == GARD:
                        claimed.add(curie)

    # normalized label -> CURIEs, one dict per pool vocabulary, in priority order.
    labels_by_vocabulary = []
    for idsfile, labelsfile in zip(match_ids_files, match_labels_files):
        with open(idsfile) as inf:
            known_ids = {line.split("\t")[0] for line in inf}
        matchable = defaultdict(set)
        with open(labelsfile) as inf:
            for line in inf:
                curie, _, label = line.rstrip("\n").partition("\t")
                if curie in known_ids and label.strip():
                    matchable[normalize_label_for_matching(label)].add(curie)
        labels_by_vocabulary.append(matchable)
        logger.info(f"build_gard_label_concord(): {len(matchable)} matchable labels from {labelsfile} ({idsfile})")

    # Guard 2: one candidate row per GARD id, at most.
    candidates = []
    count_skipped_claimed = count_skipped_ambiguous = count_unmatched = 0
    with open(gard_labels) as inf:
        for line in inf:
            gard_curie, _, gard_label = line.rstrip("\n").partition("\t")
            if gard_curie in claimed:
                count_skipped_claimed += 1
                continue
            key = normalize_label_for_matching(gard_label)
            for matchable in labels_by_vocabulary:
                if key not in matchable:
                    continue
                if len(matchable[key]) == 1:
                    candidates.append((gard_curie, next(iter(matchable[key]))))
                else:
                    # Two identifiers of one vocabulary share this label: the vocabulary itself does
                    # not say which concept the registry means, so neither can we.
                    count_skipped_ambiguous += 1
                break
            else:
                count_unmatched += 1

    # Guard 3: rebuild the cliques the other concords make, and drop any candidate whose target
    # clique the build would not type biolink:Disease.
    dicts, types = compute_cliques_for_impact_report(
        other_concords, all_ids_files, mondoclose=mondoclose, badxrefs=badxrefs
    )
    count_skipped_not_disease = 0
    with open(outfile, "w") as outf:
        for gard_curie, target in candidates:
            joined = set(dicts.get(target, {target})) | {gard_curie}
            clique_type = classify_disease_clique(joined, {**types, gard_curie: DISEASE})
            if clique_type != DISEASE:
                logger.info(
                    f"build_gard_label_concord(): not linking {gard_curie} to {target}, whose clique is "
                    f"{clique_type} and would drop the GARD identifier (see guard 3 in the docstring)"
                )
                count_skipped_not_disease += 1
                continue
            outf.write(f"{gard_curie}\txref\t{target}\n")

    logger.info(
        f"build_gard_label_concord(): wrote {len(candidates) - count_skipped_not_disease} rows to {outfile}; "
        f"skipped {count_skipped_claimed} GARD ids already placed by another concord, "
        f"{count_skipped_ambiguous} ambiguous within a vocabulary, {count_unmatched} with no label match, "
        f"and {count_skipped_not_disease} whose target clique is not biolink:Disease"
    )

    write_concord_metadata(
        metadata_yaml,
        name="build_gard_label_concord()",
        sources=[{"type": "GARD", "name": "GARD registry labels"}],
        description=(
            "build_gard_label_concord() links each GARD term that no other disease concord places to the one "
            "identically labelled identifier (case-insensitive) in the first of "
            f"{[path.basename(f) for f in match_ids_files]} that holds exactly one, skipping targets whose clique "
            "is not biolink:Disease. GARD publishes no cross-references, so a label match is the only signal "
            "available; one row per GARD id means this concord can join a GARD id to an existing clique but never "
            "merge two of them. Measured precision and the full row list: docs/sources/GARD/label-matches.md"
        ),
        url="https://github.com/NCATSTranslator/Babel/blob/main/docs/sources/GARD/label-matches.md",
        concord_filename=outfile,
    )


def compute_cliques_for_impact_report(
    concordances,
    identifiers,
    excluded_sources=(),
    *,
    mondoclose=None,
    badxrefs=None,
):
    """Load disease/phenotype identifier and concord files and return the union-find
    clique state without writing compendia.

    Production build_compendium() calls this once with everything included; the
    source-impact report CLI calls it twice (once with the new source's basename in
    `excluded_sources`, once without) to compute a before/after diff.

    Unlike the anatomy version, this has to be aware of two disease-specific quirks:

    * `MONDO_close` lives alongside regular concord files but is fed to glom() as
      ``close={MONDO: ...}`` rather than as a pair stream. If `mondoclose` is None we
      look for a path in `concordances` whose basename matches MONDO_CLOSE_BASENAME
      and pull it out of the iterated list. If absent the close map is empty (which
      preserves the CLI's ability to run before all intermediates are built).
    * A concord named ``A_B`` (``MONDO_close``, ``MONDO_GARD``) is excluded when either
      ``A`` or ``B`` is in `excluded_sources`, so ``--source GARD`` and ``--source MONDO``
      both see a "before" state without MONDO's GARD mappings.
    * Per-source bad-xrefs filtering (``DEFAULT_BAD_XREFS`` when `badxrefs` is None) and
      `remove_overused_xrefs` scoped per ``OVERUSE_FILTERED_CONCORDS``.

    :param concordances: list of paths to concord files
    :param identifiers: list of paths to ids files
    :param excluded_sources: set of source basenames (e.g. ``{"MP"}``) to skip
    :param mondoclose: explicit MONDO_close path; otherwise discovered by basename
    :param badxrefs: explicit basename->path dict; otherwise DEFAULT_BAD_XREFS
    :returns: ``(dicts, types)`` — glom dict-of-sets and the CURIE->biolink-type map
    """
    excluded = set(excluded_sources)
    if badxrefs is None:
        badxrefs = DEFAULT_BAD_XREFS

    # A bad-xrefs key is looked up by concord basename (`pref in badxrefs`, below), so a key that
    # matches no concord silently never filters anything -- the two-place-registration footgun the
    # DEFAULT_BAD_XREFS docstring warns about (a typo, or a key added to one dict but not the
    # other). Fail loud here instead. Validated against the full concordances list rather than the
    # post-exclusion subset, so a `--source X` impact-report before-run (which keeps X's concord in
    # the list but skips it inside the loop) doesn't trip on X's own bad-xrefs entry.
    concord_basenames = {path.basename(c) for c in concordances}
    unknown_badxrefs = set(badxrefs) - concord_basenames
    if unknown_badxrefs:
        raise ValueError(
            f"bad-xrefs keys match no concord basename: {sorted(unknown_badxrefs)} "
            f"(known concords: {sorted(concord_basenames)}). Fix the key or register the concord."
        )

    # A concord named `A_B` (MONDO_close, MONDO_GARD) is A's data about B, so it must be skipped
    # whenever *either* side is excluded: a `--source GARD` impact-report "before" run that kept
    # MONDO_GARD would already hold every GARD CURIE and report that adding GARD merged nothing,
    # and a `--source MONDO` run that kept it would under-count MONDO by ~15.9k joins.
    def is_excluded(concord_path):
        return bool(excluded & set(path.basename(concord_path).split("_")))

    # MONDO_close is not a regular concord; pull it out of the iterated list so it
    # isn't double-loaded. Production already passes it as a separate `mondoclose`
    # argument and doesn't include it in `concordances`; this branch only fires when
    # the impact-report CLI auto-discovered concord files from disk.
    iterated_concords = []
    discovered_mondoclose = None
    for c in concordances:
        if path.basename(c) == MONDO_CLOSE_BASENAME:
            if not is_excluded(c):
                discovered_mondoclose = c
        else:
            iterated_concords.append(c)
    if mondoclose is None:
        mondoclose = discovered_mondoclose
    elif MONDO in excluded:
        mondoclose = None

    dicts = {}
    types = {}
    for ifile in identifiers:
        if path.basename(ifile) in excluded:
            continue
        logger.info("Reading identifiers from %s", ifile)
        new_identifiers, new_types = read_identifier_file(ifile)
        glom(dicts, new_identifiers, unique_prefixes=DISEASE_UNIQUE_PREFIXES)
        types.update(new_types)

    close_mondos = defaultdict(set)
    if mondoclose is not None:
        with open(mondoclose) as inf:
            for line in inf:
                stripped = line.strip()
                if not stripped:
                    continue
                # MONDO_close is a 3-column concord (subject, predicate, object), written by
                # ubergraph.build_sets() exactly like the regular concords below.
                #
                # NOTE: this intentionally preserves the long-standing behaviour from `main` of
                # keying on x[1] (the predicate, e.g. "oio:closeMatch") rather than x[2] (the
                # close-match object). Because no clique ever contains the literal predicate
                # string, glom()'s `close=` guard never matches and has effectively been a no-op.
                # Fixing it to x[2] activates the guard and changes disease clique merging
                # broadly (it drops ~1,219 MEDDRA identifiers from Disease.txt), so it is deferred
                # to a dedicated follow-up PR (#888, `fix-mondo-close-guard`) with its own
                # before/after impact analysis rather than riding along with the MP addition.
                x = tuple(stripped.split("\t"))
                if len(x) != 3:
                    raise RuntimeError(f'Line "{stripped}" is not a valid MONDO_close entry: {x}')
                close_mondos[x[0]].add(x[1])

    for infile in iterated_concords:
        if is_excluded(infile):
            continue
        logger.info("Reading concords from %s", infile)
        pairs = []
        pref = path.basename(infile)
        if pref in badxrefs:
            logger.info("Reading bad xrefs for %s", pref)
            bad_pairs = read_badxrefs(badxrefs[pref])
        else:
            logger.info("No bad xrefs configured for %s", pref)
            bad_pairs = set()
        with open(infile) as inf:
            for line in inf:
                stuff = line.strip().split("\t")
                if len(stuff) != 3:
                    raise RuntimeError('Line "', line.strip(), '" is not a valid concord: ', stuff)
                x = tuple([stuff[0].strip(), stuff[2].strip()])
                if x not in bad_pairs:
                    pairs.append(x)
        if pref in OVERUSE_FILTERED_CONCORDS:
            newpairs = remove_overused_xrefs(pairs, target_prefixes=OVERUSE_FILTERED_CONCORDS[pref])
        else:
            newpairs = pairs
        glom(dicts, newpairs, unique_prefixes=DISEASE_UNIQUE_PREFIXES, close={MONDO: close_mondos})

    # Enforce HP/MP (and any other configured) disjointness as the final step, so BOTH the
    # real build (build_compendium) and the source-impact report (which diffs these dicts)
    # see identical, already-split cliques. Must stay the last statement before the return.
    split_mutually_exclusive_cliques(dicts)
    return dicts, types


def split_mutually_exclusive_cliques(dicts, exclusive_prefix_groups=None):
    """Split glommed cliques so no clique holds identifiers from two prefixes in one group.

    ``dicts`` is glom's dict-of-sets: every clique member maps to the *same* set object
    (its clique). For each clique containing identifiers from two or more prefixes within a
    group in ``exclusive_prefix_groups``, the group's earliest-listed *occupied* prefix stays
    put (along with every identifier whose prefix is outside the group), and each subsequent
    occupied prefix's identifiers are peeled into a clique of their own. Mutates ``dicts`` in
    place (re-pointing each affected member's key to a fresh set object) and returns it.

    Order within a group is significant: ``[[HP, MP]]`` keeps the HP-bearing clique intact and
    pulls MP out. It is the earliest *occupied* prefix that is kept, not ``group[0]``
    unconditionally: for a group ``[A, B, C]`` and a clique holding only B, C and out-of-group
    identifiers, B keeps the out-of-group members and C is peeled off. (Peeling every prefix
    after ``group[0]`` instead would strand the out-of-group members in a clique of their own.)
    No empty clique is ever produced.

    Multiple groups are supported. Each group is applied to what the previous group left behind,
    and the cliques peeled off along the way need no further checking: a peeled clique holds the
    identifiers of exactly one prefix, so it can never hold two prefixes of a later group.

    :param exclusive_prefix_groups: defaults to ``MUTUALLY_EXCLUSIVE_PREFIX_GROUPS``. Not
        given a mutable default directly, since a shared mutable default is re-used across
        every call and any accidental in-place edit (e.g. ``exclusive_prefix_groups.append``
        by a caller) would leak into all other callers for the lifetime of the process.
        Prefixes are matched case-insensitively, so a lower-case constant (prefixes.ORPHANET
        is ``"orphanet"``) works as a group member.
    """
    if exclusive_prefix_groups is None:
        exclusive_prefix_groups = MUTUALLY_EXCLUSIVE_PREFIX_GROUPS

    # Text.get_prefix_or_none() upper-cases, so compare everything in upper case. Group order is
    # preserved, since `occupied` below relies on it to decide which prefix keeps the remainder.
    upper_groups = [[prefix.upper() for prefix in group] for group in exclusive_prefix_groups]

    # dicts spans the whole disease/phenotype build (MONDO/DOID/Orphanet/HP/MP/MESH/NCIT/
    # UMLS/OMIM/EFO), but only cliques touching a group prefix (HP/MP) can possibly need
    # splitting. Find just those cliques with a single pass over dicts' keys, rather than
    # dedupe-by-identity over every clique in the whole dict and then re-scan every member of
    # every clique (most of which are pure MONDO/UMLS/MESH/etc. and can never match).
    group_prefixes = {prefix for group in upper_groups for prefix in group}
    candidate_cliques = {}
    for curie, clique in dicts.items():
        if Text.get_prefix_or_none(curie) in group_prefixes:
            candidate_cliques[id(clique)] = clique

    for clique in candidate_cliques.values():
        for group in upper_groups:
            members_by_prefix = {prefix: set() for prefix in group}
            for curie in clique:
                prefix = Text.get_prefix_or_none(curie)
                if prefix in members_by_prefix:
                    members_by_prefix[prefix].add(curie)
            occupied = [prefix for prefix in group if members_by_prefix[prefix]]
            if len(occupied) < 2:
                continue  # at most one of the group's prefixes is present; nothing to split
            # Keep the earliest occupied prefix with the remainder; peel every later one out.
            for peel_prefix in occupied[1:]:
                peel_ids = members_by_prefix[peel_prefix]
                rest_ids = clique - peel_ids
                for curie in peel_ids:
                    dicts[curie] = peel_ids
                for curie in rest_ids:
                    dicts[curie] = rest_ids
                clique = rest_ids  # later peels, and later groups, see only the remainder
    return dicts


def build_compendium(concordances, metadata_yamls, identifiers, mondoclose, badxrefs, icrdf_filename):
    """:concordances: a list of files from which to read relationships
    :identifiers: a list of files from which to read identifiers and optional categories"""
    dicts, types = compute_cliques_for_impact_report(
        concordances,
        identifiers,
        mondoclose=mondoclose,
        badxrefs=badxrefs,
    )
    typed_sets = create_typed_sets(set([frozenset(x) for x in dicts.values()]), types)
    # Prefixes Biolink does not register for biolink:Disease but that we ship anyway; without this
    # write_compendium() drops them silently, after they have already merged cliques in glom(). See
    # config.yaml: disease_extra_prefixes for which, and why each one is there.
    #
    # extra_prefixes is a per-class allowlist, so it is applied to Disease ONLY. A GARD or ICD10CM
    # CURIE that ends up in a phenotype clique must still face PhenotypicFeature's own prefix
    # filter rather than inheriting an exemption granted on disease grounds.
    extra_prefixes = get_config()["disease_extra_prefixes"]
    for biotype, sets in typed_sets.items():
        baretype = biotype.split(":")[-1]
        write_compendium(
            metadata_yamls,
            sets,
            f"{baretype}.txt",
            biotype,
            {},
            extra_prefixes=extra_prefixes if biotype == DISEASE else [],
            icrdf_filename=icrdf_filename,
        )


def classify_disease_clique(equivalent_ids, types):
    """Pick a biolink type for one disease/phenotype clique using the same precedence as
    ``create_typed_sets``: trust MONDO, then HP, then MP (MP is always PhenotypicFeature),
    then fall back to a majority vote over the declared types of the clique's members,
    breaking ties by most-specific type.

    Returns the biolink type string (e.g. ``"biolink:Disease"``) or ``None`` if no member
    of the clique has any declared type.

    Used both by ``create_typed_sets`` (the real build) and by the source-impact report,
    via the ``clique_classifier`` hook in ``PIPELINE_CONFIG``, so the report labels each
    clique with the same type and preferred CURIE the build would assign.
    """
    order = [DISEASE, PHENOTYPIC_FEATURE]
    prefixes = get_prefixes(equivalent_ids)
    for prefix in [MONDO, HP, MP]:
        if prefix in prefixes:
            try:
                return types[prefixes[prefix][0]]
            except KeyError:
                # This can happen if the concords are out of sync. Typically, e.g. there might be an HP
                # that doesn't exist anymore but is still in UMLS. Fall through to the next trusted prefix.
                pass
    typecounts = defaultdict(int)
    for eid in equivalent_ids:
        if eid in types:
            typecounts[types[eid]] += 1
    if not typecounts:
        return None
    if len(typecounts) == 1:
        return next(iter(typecounts.keys()))
    # First attempt is majority vote, and after that by most specific
    otypes = [(-c, order.index(t), t) for t, c in typecounts.items()]
    otypes.sort()
    return otypes[0][2]


def create_typed_sets(eqsets, types):
    """Given a set of sets of equivalent identifiers, we want to type each one into
    being either a disease or a phenotypic feature.  Or something else, that we may want to
    chuck out here.
    Current rules: If it has MONDO trust the MONDO's type
                  If it has a HP trust the HP's type
                  If it has an MP trust the MP's type (always PhenotypicFeature)
    After that, check the types dict to see if we know anything.
    """
    typed_sets = defaultdict(set)
    dropped = 0
    for equivalent_ids in eqsets:
        t = classify_disease_clique(equivalent_ids, types)
        if t is None:
            # No member carries a declared type, so we can't assign a Biolink type and can't
            # emit the clique. This is normally an empty set in practice, but the HP/MP split
            # can strand a lone identifier that is referenced in a concord yet absent from any
            # ids file (e.g. an obsolete MP). Drop it with a warning rather than aborting the
            # whole build over one untypeable stray.
            dropped += 1
            logger.warning(
                "Dropping untypeable disease/phenotype clique (no member has a declared type): %s",
                sorted(equivalent_ids),
            )
            continue
        typed_sets[t].add(equivalent_ids)
    if dropped:
        # A one-line total, so a mass drop (e.g. an ids file that silently failed to build) is
        # visible in the Snakemake log without counting individual warnings.
        logger.warning("Dropped %d untypeable disease/phenotype cliques out of %d.", dropped, len(eqsets))
    return typed_sets


# def load_diseases_and_phenotypes(concords, idlists, badhpos, badhpoxrefs, icrdf_filename):
#     metadata_yamls = []
#     # print('disease/phenotype')
#     # print('get and write hp sets')
#     # bad_mappings = read_bad_hp_mappings(badhpos)
#     # more_bad_mappings = read_badxrefs(badhpoxrefs)
#     # for h,m in more_bad_mappings.items():
#     #    bad_mappings[h].update(m)
#     # hpo_sets,labels = build_sets('HP:0000118', ignore_list = ['ICD','NCIT'], bad_mappings = bad_mappings)
#     # print('filter')
#     hpo_sets = filter_out_non_unique_ids(hpo_sets)
#     # print('ok')
#     # dump_sets(hpo_sets,'hpo_sets.txt')
#     print("get and write mondo sets")
#     # MONDO has disease, and its sister disease susceptibility.  I'm putting both in disease.  Biolink q
#     # But! this is a problem right now because there are some things that go in both, and they are getting filtered out
#     bad_mondo_mappings = read_badxrefs("mondo")
#     mondo_sets_1, labels_1 = build_exact_sets("MONDO:0000001", bad_mondo_mappings)
#     mondo_sets_2, labels_2 = build_exact_sets("MONDO:0042489", bad_mondo_mappings)
#     mondo_close = get_close_matches("MONDO:0000001")
#     mondo_close2 = get_close_matches("MONDO:0042489")
#     for k, v in mondo_close2.items():
#         mondo_close[k] = v
#     dump_sets(mondo_sets_1, "mondo1.txt")
#     dump_sets(mondo_sets_2, "mondo2.txt")
#     labels.update(labels_1)
#     labels.update(labels_2)
#     # if we just add these together, then any mondo in both lists will get filtered out in the next step.
#     # so we need to put them into a set.  You can't put sets directly into a set, you have to freeze them first
#     mondo_sets = combine_id_sets(mondo_sets_1, mondo_sets_2)
#     mondo_sets = filter_out_non_unique_ids(mondo_sets)
#     dump_sets(mondo_sets, "mondo_sets.txt")
#     print("get and write umls sets")
#     bad_umls = read_badxrefs("umls")
#     meddra_umls, secondary_meddra_umls = read_meddra(bad_umls)
#     meddra_umls = filter_umls(meddra_umls, mondo_sets + hpo_sets, "filtered.txt")
#     secondary_meddra_umls = filter_umls(secondary_meddra_umls, mondo_sets + hpo_sets, "filtered_secondary.txt")
#     # Now, if we just use all the secondary links, things get too agglommed.
#     # So instead, lets filter these again.
#     meddra_umls += filter_secondaries(secondary_meddra_umls, "double_filter.txt")
#     dump_sets(meddra_umls, "meddra_umls_sets.txt")
#     dicts = {}
#     # EFO has 3 parts that we want here:
#     # Disease
#     efo_sets_1, l = build_exact_sets("EFO:0000408")
#     labels.update(l)
#     # phenotype
#     efo_sets_2, l = build_exact_sets("EFO:0000651")
#     labels.update(l)
#     # measurement
#     efo_sets_3, l = build_exact_sets("EFO:0001444")
#     labels.update(l)
#     efo_sets_a = combine_id_sets(efo_sets_1, efo_sets_2)
#     efo_sets = combine_id_sets(efo_sets_a, efo_sets_3)
#     efo_sets = filter_out_non_unique_ids(efo_sets)
#     dump_sets(efo_sets, "efo_sets.txt")
#     print("put it all together")
#     print("mondo")
#     glom(dicts, mondo_sets, unique_prefixes=["MONDO"])
#     dump_dicts(dicts, "mondo_dicts.txt")
#     print("hpo")
#     glom(dicts, hpo_sets, unique_prefixes=["MONDO"], pref="HP")
#     dump_dicts(dicts, "mondo_hpo_dicts.txt")
#     print("umls")
#     glom(dicts, meddra_umls, unique_prefixes=["MONDO", "HP"], pref="UMLS", close={"MONDO": mondo_close})
#     dump_dicts(dicts, "mondo_hpo_meddra_dicts.txt")
#     print("efo")
#     glom(dicts, efo_sets, unique_prefixes=["MONDO", "HP"], pref="EFO")
#     dump_dicts(dicts, "mondo_hpo_meddra_efo_dicts.txt")
#     print("dump it")
#     fs = set([frozenset(x) for x in dicts.values()])
#     diseases, phenotypes = create_typed_sets(fs)
#     write_compendium(metadata_yamls, diseases, "disease.txt", "biolink:Disease", labels, icrdf_filename=icrdf_filename)
#     write_compendium(metadata_yamls, phenotypes, "phenotypes.txt", "biolink:PhenotypicFeature", labels, icrdf_filename=icrdf_filename)


if __name__ == "__main__":
    with open("crapfile", "w") as crapfile:
        build_sets("MONDO:0000001", {MONDO: crapfile}, set_type="exact", other_prefixes={"Orphanet": ORPHANET})
