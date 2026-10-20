from collections import defaultdict

import src.datahandlers.ec as ec
import src.datahandlers.obo as obo
import src.datahandlers.reactome as reactome
import src.datahandlers.rhea as rhea
import src.datahandlers.umls as umls
from src.babel_utils import get_prefixes, remove_overused_xrefs, write_compendium
from src.categories import BIOLOGICAL_PROCESS, MOLECULAR_ACTIVITY, PATHWAY
from src.metadata.provenance import write_concord_metadata
from src.model.cliques import glom_from_files
from src.prefixes import GO, REACT, TCDB, WIKIPATHWAYS
from src.ubergraph import build_sets


def write_obo_ids(irisandtypes, outfile, exclude=[]):
    order = [PATHWAY, BIOLOGICAL_PROCESS, MOLECULAR_ACTIVITY]
    obo.write_obo_ids(irisandtypes, outfile, order, exclude=[])


def write_go_ids(outfile):
    # Disease
    pathway_id = "GO:0007165"
    process_id = "GO:0008150"
    activity_id = "GO:0003674"
    gos = [(pathway_id, PATHWAY), (process_id, BIOLOGICAL_PROCESS), (activity_id, MOLECULAR_ACTIVITY)]
    write_obo_ids(gos, outfile)


def write_react_ids(infile, outfile):
    reactome.write_ids(infile, outfile)


def write_ec_ids(infile, outfile):
    ec.make_ids(infile, outfile)


def write_umls_ids(mrsty, outfile):
    umlsmap = {
        "B2.2.1.1.4": MOLECULAR_ACTIVITY,  # Molecular Function
        "B2.2.1.1": BIOLOGICAL_PROCESS,  # Physiologic Function
        "B2.2.1.1.1": BIOLOGICAL_PROCESS,  # Organism Function
        "B2.2.1.1.2": BIOLOGICAL_PROCESS,  # Organ or Tissue Function
        "B2.2.1.1.3": BIOLOGICAL_PROCESS,  #  Cell Function
        "B2.2.1.1.4.1": BIOLOGICAL_PROCESS,  # Genetic Function
    }
    umls.write_umls_ids(mrsty, umlsmap, outfile)


def build_process_umls_relationships(mrconso, idfile, outfile, metadata_yaml):
    umls.build_sets(
        mrconso,
        idfile,
        outfile,
        {"GO": GO},
        provenance_metadata_yaml=metadata_yaml,
        acceptable_ttys={**umls.DEFAULT_ACCEPTABLE_TTYS, "GO": umls.GO_PREFERRED_TTYS},
    )


def build_process_obo_relationships(outdir, metadata_yaml):
    # Create the equivalence pairs
    # op={'MSH':MESH,'SNOMEDCT_US':SNOMEDCT,'SNOMED_CT': SNOMEDCT, 'ORPHANET':ORPHANET, 'ICD-9':ICD9, 'ICD-10':ICD10, 'ICD-0':ICD0, 'ICD-O':ICD0 }
    op = {"WIKIPEDIA": WIKIPATHWAYS, "REACTOME": REACT, "TC": TCDB}
    with open(f"{outdir}/{GO}", "w") as outfile:
        build_sets(f"{GO}:0007165", {GO: outfile}, set_type="xref", other_prefixes=op)
        build_sets(f"{GO}:0008150", {GO: outfile}, set_type="xref", other_prefixes=op)
        build_sets(f"{GO}:0003674", {GO: outfile}, set_type="xref", other_prefixes=op)

    write_concord_metadata(
        metadata_yaml,
        name="build_process_obo_relationships()",
        description=f"Extract GO-GO relationships from UberGraph with get_subclasses_and_xrefs() from {GO}:0007165, {GO}:0008150 and {GO}:0003674,"
        f"with other_prefixes {op.values()}",
        sources=[
            {
                "type": "UberGraph",
                "name": "GO-GO relationships from UberGraph",
            }
        ],
        concord_filename=f"{outdir}/{GO}",
    )


def build_process_rhea_relationships(outfile, metadata_yaml):
    rhea.make_concord(outfile, metadata_yaml)


# Concord pairs that cause problems and are dropped before glom.
# GO:0034227/EC:2.8.1.4 is because that go term is a biological process, but EC is not a valid prefix for that,
#  leading to a loss of the EC term (and a unified RHEA) on output.
# A set *of* frozensets: set(frozenset([...])) would be a set of the two CURIE strings, which no pair matches.
BAD_CONCORDS = {frozenset(["GO:0034227", "EC:2.8.1.4"])}


def _process_concord_pair_filter(parts, infile, dicts):
    """Drop BAD_CONCORDS pairs, and UMLS concord pairs unless both CURIEs are already in the clique state.

    UMLS includes GO terms that are obsolete, so the UMLS concord may only join identifiers that the ids files (or
    earlier concords) already contain. We trust the other concords to retrieve decent identifiers.
    """
    if frozenset([parts[0], parts[2]]) in BAD_CONCORDS:
        return False
    return not infile.endswith("UMLS") or (parts[0] in dicts and parts[2] in dicts)


def compute_cliques_for_impact_report(concordances, identifiers, excluded_sources=()):
    """Load process/activity/pathway identifier and concord files and return the clique state without writing
    compendia.

    Thin wrapper over :func:`src.model.cliques.glom_from_files`; ``build_compendia`` calls it too, so a replay over a
    build's intermediate files uses the same code path as the real build.

    :returns: (dicts, types) where dicts is the glom dict-of-sets and types maps CURIE to its declared biolink type
    """
    return glom_from_files(
        concordances,
        identifiers,
        unique_prefixes=[GO],
        concord_pair_filter=_process_concord_pair_filter,
        # One kind of error is that GO->Reactome xrefs are frequently more like subclass relations: GO:0004674
        # (protein serine/threonine kinase) has over 400 Reactome xrefs. remove_overused_xrefs() drops pairs whose
        # second element is overused, but here it's the first, so we use bothways.
        overused_xref_remover=lambda pairs, infile: remove_overused_xrefs(pairs, bothways=True),
        excluded_sources=excluded_sources,
    )


def build_compendia(concordances, metadata_yamls, identifiers, icrdf_filename):
    """:concordances: a list of files from which to read relationships
    :identifiers: a list of files from which to read identifiers and optional categories"""
    dicts, types = compute_cliques_for_impact_report(concordances, identifiers)
    typed_sets = create_typed_sets(set([frozenset(x) for x in dicts.values()]), types)
    for biotype, sets in typed_sets.items():
        baretype = biotype.split(":")[-1]
        write_compendium(metadata_yamls, sets, f"{baretype}.txt", biotype, {}, icrdf_filename=icrdf_filename)


def create_typed_sets(eqsets, types):
    """Given a set of sets of equivalent identifiers, we want to type each one into
    being either a disease or a phenotypic feature.  Or something else, that we may want to
    chuck out here.
    Current rules: If it has GO trust the GO's type
    After that, check the types dict to see if we know anything.
    """
    order = [PATHWAY, BIOLOGICAL_PROCESS, MOLECULAR_ACTIVITY]
    typed_sets = defaultdict(set)
    for equivalent_ids in eqsets:
        # prefixes = set([ Text.get_curie(x) for x in equivalent_ids])
        prefixes = get_prefixes(equivalent_ids)
        found = False
        for prefix in [GO]:
            if prefix in prefixes and not found:
                mytype = types[prefixes[prefix][0]]
                typed_sets[mytype].add(equivalent_ids)
                found = True
        if not found:
            typecounts = defaultdict(int)
            for eid in equivalent_ids:
                if eid in types:
                    typecounts[types[eid]] += 1
            if len(typecounts) == 0:
                print("how did we not get any types?")
                print(equivalent_ids)
                exit()
            elif len(typecounts) == 1:
                t = list(typecounts.keys())[0]
                typed_sets[t].add(equivalent_ids)
            else:
                # First attempt is majority vote, and after that by most specific
                otypes = [(-c, order.index(t), t) for t, c in typecounts.items()]
                otypes.sort()
                t = otypes[0][2]
                typed_sets[t].add(equivalent_ids)
    return typed_sets
