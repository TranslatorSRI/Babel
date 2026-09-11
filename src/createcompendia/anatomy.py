from collections import defaultdict

import requests

import src.datahandlers.mesh as mesh
import src.datahandlers.obo as obo
import src.datahandlers.umls as umls
from src.babel_utils import get_prefixes, read_badxrefs, remove_overused_xrefs, write_compendium
from src.categories import ANATOMICAL_ENTITY, CELL, CELLULAR_COMPONENT, GROSS_ANATOMICAL_STRUCTURE
from src.metadata.provenance import write_concord_metadata
from src.model.cliques import glom_from_files
from src.prefixes import CL, EMAPA, FMA, GO, MESH, NCIT, SNOMEDCT, UBERON, UMLS, WIKIDATA
from src.ubergraph import HIERARCHY_PART_OF, UberGraph, build_sets
from src.util import Text, get_config, get_logger, get_repo_root

logger = get_logger(__name__)

# Individually wrong xref pairs to drop, for cases where the target prefix is legitimate in general
# (MESH and GO really are anatomy xref targets) so ANATOMY_OBO_IGNORE_LIST cannot help. Unlike
# diseasephenotype's DEFAULT_BAD_XREFS this is a single file applied to every anatomy concord
# rather than a per-concord mapping: a pair names both of its CURIEs, so there is nothing for a
# per-concord key to disambiguate, and one file avoids the two-place-registration footgun described
# in docs/sources/CLAUDE.md. See the file itself for why each pair is listed.
#
# Two consumers resolve this file: the real build, where the anatomy_compendia rule declares it as
# an input (so an edit re-triggers the rule) and passes the path in, and the source-impact report,
# which takes this default. The snakefile references this constant rather than repeating the
# literal, so there is one path and the report cannot drift from the build. Absolute, because the
# report CLI can be invoked from any directory.
ANATOMY_BAD_XREFS = str(get_repo_root() / "input_data/anatomy_badxrefs.txt")

# Which parts of each source ontology the anatomy pipeline takes, and as what Biolink type.
#
# "root" is the traversal root: the term whose descendants define the source's contribution, and
# the type every term below it gets by default. "subtype_roots" names the subtrees that override
# that default — UBERON's dedicated gross-anatomy branch, EMAPA's organ and tissue branches. A term
# at or below a subtype root takes the subtype's type; the subtype root itself is included.
#
# This is meant to be the one place to look for "which parts of this ontology does anatomy take,
# and as what". MESH and UMLS are not here: they span many Biolink types and are keyed by tree
# number and semantic type rather than by a CURIE root, so they still carry their own maps in
# write_mesh_ids() and write_umls_ids(). Folding those in is the natural next step.
ANATOMY_OBO_SOURCES = {
    UBERON: {
        "root": f"{UBERON}:0001062",  # "anatomical entity"
        "type": ANATOMICAL_ENTITY,
        "subtype_roots": {f"{UBERON}:0010000": GROSS_ANATOMICAL_STRUCTURE},  # "anatomical structure"
    },
    CL: {"root": f"{CL}:0000000", "type": CELL, "subtype_roots": {}},  # "cell"
    GO: {"root": f"{GO}:0005575", "type": CELLULAR_COMPONENT, "subtype_roots": {}},  # "cellular_component"
    EMAPA: {
        "root": f"{EMAPA}:0",  # "anatomical structure"
        "type": ANATOMICAL_ENTITY,
        "subtype_roots": {
            f"{EMAPA}:35949": GROSS_ANATOMICAL_STRUCTURE,  # "organ"
            f"{EMAPA}:35868": GROSS_ANATOMICAL_STRUCTURE,  # "tissue"
        },
    },
}


def _obo_id_roots(prefix):
    """Return the ``[(root CURIE, biolink type)]`` list :func:`write_obo_ids` takes for one source.

    The source's own root first, then its subtype roots sorted by CURIE. Order does not decide
    typing — ``write_obo_ids`` resolves an overlap through its ``order`` precedence list — but
    sorting keeps the ids file byte-identical between runs.
    """
    source = ANATOMY_OBO_SOURCES[prefix]
    return [(source["root"], source["type"]), *sorted(source["subtype_roots"].items())]


def write_obo_ids(irisandtypes, outfile, exclude=[]):
    order = [CELLULAR_COMPONENT, CELL, GROSS_ANATOMICAL_STRUCTURE, ANATOMICAL_ENTITY]
    obo.write_obo_ids(irisandtypes, outfile, order, exclude=[])


def write_ncit_ids(outfile):
    # For NCIT, there are some branches of the subhiearrchy that we don't want, like this one for genomic locus
    anatomy_id = f"{NCIT}:C12219"
    cell_id = f"{NCIT}:C12508"
    component_id = f"{NCIT}:C34070"
    genomic_location_id = f"{NCIT}:C64389"
    chromosome_band_id = f"{NCIT}:C13432"
    macromolecular_structure_id = f"{NCIT}:C14134"  # protein domains
    ostomy_site_id = f"{NCIT}:C122638"
    chromosome_structure_id = f"{NCIT}:C13377"
    anatomic_site_id = f"{NCIT}:C13717"  # the site of procedures like injections etc
    write_obo_ids(
        [(anatomy_id, ANATOMICAL_ENTITY), (cell_id, CELL), (component_id, CELLULAR_COMPONENT)],
        outfile,
        exclude=[
            genomic_location_id,
            chromosome_band_id,
            macromolecular_structure_id,
            ostomy_site_id,
            chromosome_structure_id,
            anatomic_site_id,
        ],
    )


def write_uberon_ids(outfile):
    write_obo_ids(_obo_id_roots(UBERON), outfile)


def write_cl_ids(outfile):
    write_obo_ids(_obo_id_roots(CL), outfile)


def _emapa_descendants(uber, root):
    """Return the set of EMAPA-prefixed CURIEs reachable from ``root`` in UberGraph.

    EMAPA is a part_of partonomy, not an rdfs:subClassOf hierarchy, so we union the
    part_of closure (the bulk of the structure) with the subClassOf closure (the few
    is_a links). ``get_subclasses_of`` queries the redundant graph, so each call returns
    the full transitive closure under its predicate. The ``root`` itself is not included.
    """
    found = set()
    for term in uber.get_subclasses_of(root, hierarchy_predicate=HIERARCHY_PART_OF) + uber.get_subclasses_of(root):
        curie = term["descendent"]
        if curie.startswith(f"{EMAPA}:"):
            found.add(curie)
    return found


def write_emapa_ids(outfile):
    """Collect EMAPA anatomy identifiers from UberGraph and assign each a biolink type.

    EMAPA is a part_of partonomy, not an rdfs:subClassOf hierarchy, so it cannot be
    collected with write_obo_ids() the way UBERON/GO/CL are — a subClassOf walk from
    the EMAPA root reaches only a handful of terms. We walk part_of from the root instead
    (plus the few is_a links), and keep every EMAPA-prefixed term.

    Biolink typing rule (one type per CURIE, written in column 2 of the ids file): terms at or
    below one of EMAPA's ``subtype_roots`` take that subtree's type, everything else takes the
    source's default ``type``. Both come from ANATOMY_OBO_SOURCES, so the branches this source
    splits on are declared in one place rather than here — today that is EMAPA:35949 "organ" and
    EMAPA:35868 "tissue" as biolink:GrossAnatomicalStructure, over a biolink:AnatomicalEntity
    default. This is the partonomy equivalent of UBERON's gross-branch override, which
    write_obo_ids() applies through its ``order`` precedence list.

    Both types survive write_compendium(): EMAPA is registered as an id_prefix for
    biolink:AnatomicalEntity and biolink:GrossAnatomicalStructure in the Biolink Model
    version this build uses. If a future version drops either registration, gross-typed
    EMAPA CURIEs would start being silently dropped, and the source-impact report's
    survival columns are what would catch it.
    """
    source = ANATOMY_OBO_SOURCES[EMAPA]
    uber = UberGraph()
    curies = {source["root"]} | _emapa_descendants(uber, source["root"])
    # Each subtype root's own subtree overrides the default type. Sorted so that if two subtype
    # roots ever overlap with *different* types, which one wins is at least reproducible; today
    # both EMAPA subtrees are GrossAnatomicalStructure, so the case does not arise.
    subtypes = {}
    for subtype_root, subtype in sorted(source["subtype_roots"].items()):
        for curie in {subtype_root} | _emapa_descendants(uber, subtype_root):
            subtypes[curie] = subtype
    with open(outfile, "w") as idfile:
        for curie in sorted(curies):
            idfile.write(f"{curie}\t{subtypes.get(curie, source['type'])}\n")


def write_go_ids(outfile):
    write_obo_ids(_obo_id_roots(GO), outfile)


def write_mesh_ids(outfile):
    meshmap = {f"A{str(i).zfill(2)}": ANATOMICAL_ENTITY for i in range(1, 21)}
    meshmap["A11"] = CELL
    meshmap["A11.284"] = CELLULAR_COMPONENT
    mesh.write_ids(meshmap, outfile)


def write_umls_ids(mrsty, outfile):
    # UMLS categories:
    # A1.2 Anatomical Structure
    # A1.2.1 Embryonic Structure
    # A1.2.3 Fully Formed Anatomical Structure
    # A1.2.3.1 Body Part, Organ, or Organ Component
    # A1.2.3.2 Tissue
    # A1.2.3.3 Cell
    # A1.2.3.4 Cell Component
    # A2.1.4.1 Body System
    # A2.1.5.1 Body Space or Junction
    # A2.1.5.2 Body Location or Region
    umlsmap = {
        x: ANATOMICAL_ENTITY for x in ["A1.2", "A1.2.1", "A1.2.3.1", "A1.2.3.2", "A2.1.4.1", "A2.1.5.1", "A2.1.5.2"]
    }
    umlsmap["A1.2.3.3"] = CELL
    umlsmap["A1.2.3.4"] = CELLULAR_COMPONENT
    umls.write_umls_ids(mrsty, umlsmap, outfile)


# Ignore list notes:
# The BTO and BAMs and HTTP (braininfo) identifiers promote over-glommed nodes
# FMA is a specific problem where in CL they use FMA xref to mean 'part of'
# CALOHA is a specific problem where in CL they use FMA xref to mean 'part of'
# GOC is a specific problem where in CL they use FMA xref to mean 'part of'
# wikipedia.en is a specific problem where in CL they use FMA xref to mean 'part of'
# NIF_Subcellular leads to a weird mashup between a GO term and a bunch of other stuff.
# CL only shows up as an xref once in uberon, and it's a mistake.  It doesn't show up in anything else.
# GO only shows up as an xref once in uberon, and it's a mistake.  It doesn't show up in anything else.
# PMID is just wrong
# Target prefixes never written to an anatomy OBO concord: citations (PMID), ontologies Babel
# does not ingest for anatomy (BTO, BAMS, FMA, CALOHA, OPENCYC, NIF_SUBCELLULAR), bare URLs
# (HTTP, WIKIPEDIA.EN), provenance annotations (GOC), and CL/GO, whose cliques are built from
# their own roots rather than from a cross-reference. Matched against Text.get_prefix_or_none(),
# which upper-cases, so every entry must be upper-case.
ANATOMY_OBO_IGNORE_LIST = [
    "PMID",
    "BTO",
    "BAMS",
    "FMA",
    "CALOHA",
    "GOC",
    "WIKIPEDIA.EN",
    "CL",
    "GO",
    "NIF_SUBCELLULAR",
    "HTTP",
    "OPENCYC",
]


def build_emapa_obo_relationships(concordfiles):
    """Write EMAPA's xref concords into ``concordfiles`` (a {prefix: open file} mapping).

    EMAPA is a part_of partonomy, not an is_a hierarchy, so its xref concords must be
    collected by walking part_of rather than rdfs:subClassOf; a subClassOf walk reaches
    only two terms.

    This exists as its own function so that the real build and the EMAPA pipeline test
    fixture issue the identical call. Both write to the same intermediate concord path, so
    a fixture that dropped ``hierarchy_predicate`` or ``ignore_list`` would leave a
    degenerate or unfiltered file that a later Snakemake run would treat as up to date.
    """
    build_sets(
        ANATOMY_OBO_SOURCES[EMAPA]["root"],
        concordfiles,
        "xref",
        ignore_list=ANATOMY_OBO_IGNORE_LIST,
        hierarchy_predicate=HIERARCHY_PART_OF,
    )


def build_anatomy_obo_relationships(outdir, metadata_yamls):
    # Create the equivalence pairs
    with (
        open(f"{outdir}/{UBERON}", "w") as uberon,
        open(f"{outdir}/{GO}", "w") as go,
        open(f"{outdir}/{CL}", "w") as cl,
        open(f"{outdir}/{EMAPA}", "w") as emapa,
    ):
        source_to_concord = {UBERON: uberon, GO: go, CL: cl, EMAPA: emapa}
        for source_prefix in [UBERON, GO]:
            build_sets(
                ANATOMY_OBO_SOURCES[source_prefix]["root"],
                source_to_concord,
                "xref",
                ignore_list=ANATOMY_OBO_IGNORE_LIST,
            )
        build_emapa_obo_relationships(source_to_concord)
        # CL is now being handled by Wikidata (build_wikidata_cell_relationships), so we can probably remove it from here.

    # Write out metadata.
    for metadata_name in [UBERON, GO, CL, EMAPA]:
        write_concord_metadata(
            metadata_yamls[metadata_name],
            name="build_anatomy_obo_relationships()",
            sources=[
                {"type": "UberGraph", "name": "UBERON"},
                {"type": "UberGraph", "name": "GO"},
                {"type": "UberGraph", "name": "CL"},
                {"type": "UberGraph", "name": "EMAPA"},
            ],
            description=(
                "get_subclasses_and_xrefs() of "
                f"{ANATOMY_OBO_SOURCES[UBERON]['root']}, "
                f"{ANATOMY_OBO_SOURCES[GO]['root']}, and "
                f"{ANATOMY_OBO_SOURCES[EMAPA]['root']}"
            ),
            concord_filename=f"{outdir}/{metadata_name}",
        )


def build_wikidata_cell_relationships(outdir, metadata_yaml):
    # This sparql returns all the wikidata items that have a UMLS identifier and a CL identifier
    sparql = """PREFIX wdt: <http://www.wikidata.org/prop/direct/>
        PREFIX wdtn: <http://www.wikidata.org/prop/direct-normalized/>
        SELECT * WHERE {
          ?wd wdtn:P7963 ?cl .
          ?wd wdt:P2892 ?umls .
        }"""
    frink_wikidata_url = "https://apps.okn.us/federation/sparql"
    response = requests.post(frink_wikidata_url, data={"query": sparql})
    if not response.ok:
        raise RuntimeError(f"Could not query {frink_wikidata_url}: {response.status_code} {response.reason}")
    try:
        results = response.json()
    except Exception as e:
        raise RuntimeError(
            f"Could not parse {frink_wikidata_url}: {e} raised when parsing response {response.content}."
        )
    rows = results["results"]["bindings"]
    # If one wikidata entry has either more than one CL or more than one UMLS, then we end up with problems
    # (It could also be possible that the same CL is on more than one wikidata entry, but haven't seen that yet)
    # Loop over the rows, transform each row into curies, and filter out any wikidata entry that occurs more than once.
    # Double check that the UMLS and CL are unique.  Then write out the now-unique UMLS/CL mappings
    counts = defaultdict(int)
    pairs = []
    for row in rows:
        umls_curie = f"{UMLS}:{row['umls']['value']}"
        # wd_curie = f"{WIKIDATA}:{row['wd']['value']}"
        cl_curie = Text.obo_to_curie(row["cl"]["value"])
        pairs.append((umls_curie, cl_curie))
        counts[umls_curie] += 1
        counts[cl_curie] += 1
    with open(f"{outdir}/{WIKIDATA}", "w") as wd:
        for pair in pairs:
            if (counts[pair[0]] == 1) and (counts[pair[1]] == 1):
                wd.write(f"{pair[0]}\teq\t{pair[1]}\n")
            else:
                print(f"Pair {pair} is not unique {counts[pair[0]]} {counts[pair[1]]}")

    # Write out metadata
    write_concord_metadata(
        metadata_yaml,
        name="build_wikidata_cell_relationships()",
        sources=[{"type": "Frink", "name": "Frink Direct Normalized Graph via SPARQL"}],
        description='wd:P7963 ("Cell Ontology ID") and wd:P2892 ("UMLS CUI") from Wikidata',
        concord_filename=f"{outdir}/{WIKIDATA}",
    )


def build_anatomy_umls_relationships(mrconso, idfile, outfile, umls_metadata):
    umls.build_sets(
        mrconso,
        idfile,
        outfile,
        {"SNOMEDCT_US": SNOMEDCT, "MSH": MESH, "NCI": NCIT, "GO": GO, "FMA": FMA},
        provenance_metadata_yaml=umls_metadata,
    )


def _make_anatomy_concord_pair_filter(bad_pairs):
    """Build the ``(parts, infile, dicts) -> bool`` hook glom_from_files applies to every pair.

    ``bad_pairs`` is a set of frozensets, so a listed pair is dropped whichever way round the
    concord happens to write it. It is captured in a closure because the hook runs once per
    concord row and the file must not be re-read that many times.
    """

    def _filter(parts, infile, dicts):
        if frozenset((parts[0], parts[2])) in bad_pairs:
            logger.debug("Skipping bad xref pair %s from %s (see %s)", parts, infile, ANATOMY_BAD_XREFS)
            return False
        return _anatomy_concord_pair_filter(parts, infile, dicts)

    return _filter


def _anatomy_concord_pair_filter(parts, infile, dicts):
    """Drop UMLS<->GO concord pairs unless both CURIEs are already in the clique state.

    UMLS includes obsolete GO terms we don't want to add, so we limit UMLS<->GO concords
    to terms that are already in ``dicts``. This is ONLY for the UMLS/GO combination — we
    trust the other concords to retrieve decent identifiers. Returns True to keep the pair.
    """
    bs = frozenset([UMLS, GO])
    prefixes = frozenset(xi.split(":")[0] for xi in parts[0:3:2])  # leave out the predicate
    if prefixes != bs:
        return True
    for xi in (parts[0], parts[2]):
        if xi not in dicts:
            logger.debug(
                "Skipping pair %s from %s: terms with prefixes %s are skipped unless they are already in the concords.",
                parts,
                infile,
                bs,
            )
            return False
    return True


def compute_cliques_for_impact_report(concordances, identifiers, excluded_sources=(), badxrefs=None):
    """Load anatomy identifier and concord files and return the union-find clique state
    without writing compendia.

    Thin wrapper over :func:`src.model.cliques.glom_from_files` supplying
    anatomy's hooks (unique prefixes, the UMLS<->GO pair filter, and overused-xref
    removal). ``build_compendia`` calls this too, so the source-impact report's reglom
    uses the same code path as the real build.

    The source-impact report CLI calls this twice — once with the new source's files
    excluded, once with everything — to compute a before/after diff.

    :param concordances: list of paths to concord files
    :param identifiers: list of paths to ids files
    :param excluded_sources: set of source names (file basenames) to skip; used to compute
        the "before-new-source" state for the impact report
    :param badxrefs: path to a bad-xrefs file; defaults to ``ANATOMY_BAD_XREFS``. Pass an
        empty string to disable the filter (used by tests).
    :returns: (dicts, types) where dicts is the glom dict-of-sets and types maps CURIE
        to its declared biolink type
    """
    if badxrefs is None:
        badxrefs = ANATOMY_BAD_XREFS
    bad_pairs = {frozenset(pair) for pair in read_badxrefs(badxrefs)} if badxrefs else set()
    logger.info("Loaded %d bad xref pair(s) from %s", len(bad_pairs), badxrefs or "(disabled)")
    return glom_from_files(
        concordances,
        identifiers,
        unique_prefixes=get_config()["anatomy_unique_prefixes"],
        concord_pair_filter=_make_anatomy_concord_pair_filter(bad_pairs),
        overused_xref_remover=lambda pairs, infile: remove_overused_xrefs(pairs),
        excluded_sources=excluded_sources,
    )


def build_compendia(concordances, metadata_yamls, identifiers, icrdf_filename, badxrefs=None):
    """:concordances: a list of files from which to read relationships
    :identifiers: a list of files from which to read identifiers and optional categories
    :badxrefs: path to a bad-xrefs file; defaults to ``ANATOMY_BAD_XREFS``"""
    dicts, types = compute_cliques_for_impact_report(concordances, identifiers, badxrefs=badxrefs)
    typed_sets = create_typed_sets(set([frozenset(x) for x in dicts.values()]), types)
    for biotype, sets in typed_sets.items():
        baretype = biotype.split(":")[-1]
        write_compendium(metadata_yamls, sets, f"{baretype}.txt", biotype, {}, icrdf_filename=icrdf_filename)


def classify_anatomy_clique(equivalent_ids, types):
    """Pick a biolink type for one anatomy clique using the same precedence as
    ``create_typed_sets``: trust GO/CL/UBERON/EMAPA in that order, then fall back to a
    majority vote over the declared types of the clique's members, breaking ties by
    most-specific type.

    Returns the biolink type string (e.g. ``"biolink:AnatomicalEntity"``) or ``None``
    if no member of the clique has any declared type.
    """
    order = [CELLULAR_COMPONENT, CELL, GROSS_ANATOMICAL_STRUCTURE, ANATOMICAL_ENTITY]
    prefixes = get_prefixes(equivalent_ids)
    for prefix in [GO, CL, UBERON, EMAPA]:
        if prefix in prefixes and prefixes[prefix][0] in types:
            return types[prefixes[prefix][0]]
    typecounts = defaultdict(int)
    for eid in equivalent_ids:
        if eid in types:
            typecounts[types[eid]] += 1
    if not typecounts:
        return None
    if len(typecounts) == 1:
        return next(iter(typecounts.keys()))
    otypes = [(-c, order.index(t), t) for t, c in typecounts.items()]
    otypes.sort()
    return otypes[0][2]


def create_typed_sets(eqsets, types):
    """Given a set of sets of equivalent identifiers, we want to type each one into
    being either a disease or a phenotypic feature.  Or something else, that we may want to
    chuck out here.
    Current rules: If it has GO trust the GO's type
                   If it has a CL trust the CL's type
                   If it has an UBERON trust the UBERON's type
    After that, check the types dict to see if we know anything.
    """
    typed_sets = defaultdict(set)
    for equivalent_ids in eqsets:
        t = classify_anatomy_clique(equivalent_ids, types)
        if t is None:
            raise RuntimeError(
                f"Cannot assign a biolink type to anatomy clique {equivalent_ids}: no member CURIE has a declared type."
            )
        typed_sets[t].add(equivalent_ids)
    return typed_sets
