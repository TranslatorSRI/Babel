import logging
import os
import re
import shutil
from collections import defaultdict
from zipfile import ZipFile

import requests

from src.babel_utils import make_local_name
from src.categories import CHEMICAL_ENTITY, DRUG, MOLECULAR_MIXTURE
from src.metadata.provenance import write_concord_metadata, write_download_metadata
from src.predicates import HAS_EXACT_SYNONYM
from src.prefixes import MESH, RXCUI, UMLS
from src.util import get_logger

logger = get_logger(__name__)


def check_mrconso_line(line):
    """
    This function can be used to filter lines from MRCONSO.RRF with a
    standard set of criteria that we apply across all of Babel:
    1. We only use English terms (this skips terms that don't have any English labels).
    2. We skip obsolete content on the basis of the SUPPRESS flag.

    :param line: A line from MRCONSO.RRF.
    :return: True if the line passes checks and should be tested, false if the line fails checks and should be skipped.
    """

    x = line.strip().split("|")
    lang = x[1]

    # Note that this skips terms that don't have any English labels.
    # Only keep english terms
    if lang != "ENG":
        return False

    # Use the SUPPRESS flag (https://www.ncbi.nlm.nih.gov/books/NBK9685/table/ch03.T.concept_names_and_sources_file_mr/)
    # - O:  All obsolete content, whether they are obsolesced by the source or by NLM. These will include all atoms
    #       having obsolete TTYs, and other atoms becoming obsolete that have not acquired an obsolete TTY (e.g. RxNorm
    #       SCDs no longer associated with current drugs, LNC atoms derived from obsolete LNC concepts).
    # - E:  Non-obsolete content marked suppressible by an editor. These do not have a suppressible SAB/TTY combination.
    # only keep unsuppressed rows
    suppress = x[16]
    if suppress == "O" or suppress == "E":
        return False

    return True


# The MeSH term types build_sets() concords a CUI to: every MeSH descriptor or supplementary record has exactly
# one of these "main heading" atoms per CUI, so they identify *the* descriptor a CUI stands for, unlike the
# entry-term (ET), permuted (PM) and pharmacological-entry (PEP) atoms. read_cui_to_mesh_descriptors() uses
# the same set so that MeSH ownership (below) moves a CUI exactly when build_sets() would drag its descriptor.
MESH_CONCORD_TTYS = frozenset({"MH", "NM", "HT", "QAB"})


def read_cui_to_mesh_descriptors(mrconso, prefix=UMLS):
    """Map every CUI to the MeSH descriptors it stands for, using the same MRCONSO atoms as build_sets().

    :param mrconso: The file path of MRCONSO.RRF.
    :param prefix: The prefix for the CUIs (defaults to UMLS).
    :return: A dict of ``UMLS:C...`` -> set of ``MESH:D...``/``MESH:C...`` CURIEs. Only English, unsuppressed
        ``SAB=MSH`` atoms with a term type in MESH_CONCORD_TTYS count; a CUI with only entry-term atoms (e.g.
        ``C0000005`` -> ``D012711`` via PEP/ET) is absent, just as build_sets() never concords it.
    """
    cui_to_mesh = defaultdict(set)
    with open(mrconso) as inf:
        for line in inf:
            if not check_mrconso_line(line):
                continue
            x = line.split("|")
            if x[11] != "MSH" or x[12] not in MESH_CONCORD_TTYS:
                continue
            cui_to_mesh[f"{prefix}:{x[0]}"].add(f"{MESH}:{x[13]}")
    return dict(cui_to_mesh)


def read_mesh_ids_types(mesh_ids_files):
    """Read one or more ``ids/MESH`` files (``CURIE<tab>biolink:Type``) into a dict of CURIE -> type.

    :param mesh_ids_files: An iterable of ids-file paths.
    :return: A dict mapping each MeSH CURIE to the Biolink type its ids file assigns it (the last file wins if
        a CURIE appears in several).
    """
    types = {}
    for mesh_ids_file in mesh_ids_files:
        with open(mesh_ids_file) as inf:
            for line in inf:
                parts = line.rstrip("\n").split("\t")
                if parts[0]:
                    types[parts[0]] = parts[1] if len(parts) > 1 else None
    return types


def compute_mesh_owners(cui_to_mesh, mesh_types_by_pipeline, never_add=None):
    """Decide, once for every pipeline, which pipeline owns each CUI -- "a CUI follows its MeSH descriptor".

    Each pipeline types CUIs by UMLS semantic type (``write_umls_ids``'s category_map) and MeSH descriptors by
    tree number (``mesh.write_ids``), independently. When the two disagree about a CUI -- say a CUI typed T116
    "Amino Acid, Peptide, or Protein" whose descriptor sits in a chemical D-tree -- the pipeline that claims the
    CUI would concord it to the descriptor (``build_sets``) and drag the other pipeline's descriptor and its
    DrugBank partners into its own clique, so the same identifiers ended up in two compendia
    (https://github.com/NCATSTranslator/Babel/issues/276, #308, #1123). MeSH placement is the tie-breaker: a
    CUI is *owned* by a pipeline when that pipeline is the only one whose ``ids/MESH`` claims any of the CUI's
    descriptors, and that pipeline does not refuse the CUI (``never_add``). apply_mesh_ownership() then adds an
    owned CUI to its owner and drops it from every other pipeline.

    A CUI with no owner -- its descriptors claimed by nobody, or by two or more pipelines (a descriptor
    cross-listed in two trees, e.g. ``MESH:D013171`` "Spores, Bacterial" in B05 and A11, or one descriptor on
    each side), or by a single pipeline that refuses it -- is left wherever its semantic types put it. Deciding
    ownership here, in one place, is what makes "drop" safe: a pipeline only drops a CUI that this map hands to
    an owner that will add it. An earlier version let each pipeline decide from its own ``ids/MESH`` and the
    union of the others', so a CUI whose descriptors two *other* pipelines claimed was dropped by the pipeline
    that selected it and added by neither, and a CUI the owner refused (disease's badumls) was dropped and
    never added; both fell to the leftover compendium, cut off from their descriptor's clique.

    :param cui_to_mesh: The output of read_cui_to_mesh_descriptors().
    :param mesh_types_by_pipeline: pipeline -> {MeSH CURIE -> Biolink type}, one entry per pipeline in
        config.yaml's ``umls_mesh_owning_pipelines`` (each the output of read_mesh_ids_types()).
    :param never_add: pipeline -> set of CUIs (``UMLS:C...``) that pipeline must not be given, because its
        ``write_umls_ids`` blocks them (disease's badumls). A CUI whose only claimant refuses it has no owner.
    :return: A dict of CUI -> ``(pipeline, descriptor, biolink_type)``: the owner, and the owner's first claimed
        descriptor in sorted order, whose type the CUI takes when it is added.
    """
    never_add = never_add or {}
    owners = {}
    for cui, descriptors in cui_to_mesh.items():
        claims = {}
        for pipeline, mesh_types in mesh_types_by_pipeline.items():
            claimed = sorted(d for d in descriptors if d in mesh_types)
            if claimed:
                claims[pipeline] = claimed
        if len(claims) != 1:
            continue
        ((pipeline, claimed),) = claims.items()
        if cui in never_add.get(pipeline, ()):
            continue
        owners[cui] = (pipeline, claimed[0], mesh_types_by_pipeline[pipeline][claimed[0]])
    return owners


def write_mesh_ownership(mrconso, mesh_ids_files_by_pipeline, outfile, never_add=None):
    """Write the CUI -> owning pipeline map (compute_mesh_owners) that every ``*_umls_ids`` rule reads.

    MRCONSO is parsed once here rather than once per pipeline, and every pipeline reads the same decision, so
    no CUI can be dropped by one pipeline and not added by another.

    :param mrconso: The file path of MRCONSO.RRF.
    :param mesh_ids_files_by_pipeline: pipeline -> that pipeline's ``ids/MESH`` file, for every pipeline in
        config.yaml's ``umls_mesh_owning_pipelines``.
    :param outfile: The TSV to write: ``CUI<tab>pipeline<tab>descriptor<tab>biolink_type``, sorted by CUI.
    :param never_add: See compute_mesh_owners().
    """
    owners = compute_mesh_owners(
        read_cui_to_mesh_descriptors(mrconso),
        {pipeline: read_mesh_ids_types([path]) for pipeline, path in mesh_ids_files_by_pipeline.items()},
        never_add,
    )
    with open(outfile, "w") as outf:
        for cui in sorted(owners):
            outf.write("\t".join((cui,) + owners[cui]) + "\n")
    counts = defaultdict(int)
    for pipeline, _, _ in owners.values():
        counts[pipeline] += 1
    logger.info(f"MeSH ownership: {len(owners)} CUIs have an owning pipeline ({dict(sorted(counts.items()))}).")


def read_mesh_ownership(path):
    """Read a write_mesh_ownership() file back into compute_mesh_owners()'s dict."""
    owners = {}
    with open(path) as inf:
        for line in inf:
            cui, pipeline, descriptor, biolink_type = line.rstrip("\n").split("\t")
            owners[cui] = (pipeline, descriptor, biolink_type)
    return owners


def apply_mesh_ownership(output_lines, mesh_owners, pipeline, blocklist=frozenset()):
    """Make one pipeline's UMLS ids follow the owners compute_mesh_owners() chose.

    Every CUI this pipeline owns is **added** (typed as its owned descriptor) if the semantic types did not
    already select it, whether they put it elsewhere or nowhere, so the CUI, its descriptor and its DrugBank
    partners stay together. Every CUI another pipeline owns is **dropped** here. A CUI with no owner is left
    as the semantic types decided.

    :param output_lines: The dict of CURIE -> [types] about to be written; mutated in place.
    :param mesh_owners: The output of compute_mesh_owners() or read_mesh_ownership().
    :param pipeline: This pipeline's directory name (an entry of ``umls_mesh_owning_pipelines``, or ``gene`` or
        ``process``, which own no MeSH and so only ever drop).
    :param blocklist: CUIs this pipeline must never claim. The ownership map was computed with them in
        ``never_add``, so being handed one means it was built from a different blocklist: raise rather than
        quietly add a CUI the pipeline refuses.
    :return: ``(dropped, added)``: dropped maps each removed CUI to ``(owner, descriptor)``; added maps each new
        CUI to ``(descriptor, type)``.
    """
    refused = sorted(cui for cui in blocklist if mesh_owners.get(cui, (None,))[0] == pipeline)
    if refused:
        raise ValueError(
            f"The MeSH ownership map gives {pipeline} {len(refused)} CUIs it blocks (e.g. {refused[:5]}); it was "
            f"computed without {pipeline}'s blocklist in never_add."
        )
    dropped = {}
    added = {}
    for cui, (owner, descriptor, biolink_type) in mesh_owners.items():
        if owner == pipeline:
            if cui not in output_lines:
                output_lines[cui] = [biolink_type]
                added[cui] = (descriptor, biolink_type)
        elif cui in output_lines:
            dropped[cui] = (owner, descriptor)
            del output_lines[cui]
    logger.info(
        f"MeSH ownership for {pipeline}: dropped {len(dropped)} CUIs another pipeline owns "
        f"(e.g. {dict(list(dropped.items())[:5])}) and added {len(added)} CUIs this pipeline owns "
        f"(e.g. {dict(list(added.items())[:5])})."
    )
    return dropped, added


def write_umls_ids(
    mrsty,
    category_map,
    umls_output,
    prefix=UMLS,
    blocklist_umls_ids=None,
    blocklist_umls_semantic_type_tree=None,
    *,
    mesh_ownership=None,
    pipeline=None,
):
    """
    Write out UMLS IDs and categories (as per a category map) to a file.

    :param mrsty: The file path of the MRSTY.RRF file from the UMLS download.
    :param category_map: A dictionary mapping UMLS semantic types to Biolink types.
    :param umls_output: The file path of the output file.
    :param prefix: The prefix to use for the UMLS IDs. Defaults to UMLS.
    :param blocklist_umls_ids: A set of individual UMLS IDs to block. Defaults to None.
    :param blocklist_umls_semantic_type_tree: A set of UMLS semantic type trees to block. Defaults to None.
        Note that we strictly filter out the semantic type trees listed here: if e.g. A1.2.3 is on the blocklist,
        UMLS IDs with a semantic type tree of A1.2.3.4 will be allowed -- only UMLS IDs with a type of A1.2.3 will
        be blocked.
    :param mesh_ownership: The write_mesh_ownership() file. Given (with ``pipeline``), the selected CUIs
        are adjusted by apply_mesh_ownership() -- "a CUI follows its MeSH descriptor" -- after the semantic
        type blocklist, so a CUI the blocklist removed can re-enter through a descriptor this pipeline owns.
        See ``umls_mesh_owning_pipelines`` in config.yaml.
    :param pipeline: This pipeline's directory name under intermediate_directory, as the ownership file names it.
    :return: None.
    """

    if blocklist_umls_ids is None:
        blocklist_umls_ids = set()
    if blocklist_umls_semantic_type_tree is None:
        blocklist_umls_semantic_type_tree = set()
    if mesh_ownership is not None and pipeline is None:
        raise ValueError("mesh_ownership needs the pipeline it is being applied to")

    # Fun fact: MRSTY has duplicate records for entities that have multiple types, e.g.
    #   CUI | TUI | STN | STY | ATUI | CVF
    #   C0000005|T116|A1.4.1.2.1.7|Amino Acid, Peptide, or Protein|AT17648347|256|
    #   C0000005|T121|A1.4.1.1.1|Pharmacologic Substance|AT17575038|256|
    #   C0000005|T130|A1.4.1.1.4|Indicator, Reagent, or Diagnostic Aid|AT17634323|256|
    #   C0000039|T109|A1.4.1.2.1|Organic Chemical|AT45562015|256|
    # (see https://github.com/NCATSTranslator/Babel/issues/200#issuecomment-1789550364 for another example and
    #  https://www.ncbi.nlm.nih.gov/books/NBK9685/table/ch03.Tf/ for column information.)
    #
    # This means that we can't blacklist UMLS types by just skipping those lines: instead, we will need to load
    # the type information on the selected CUI, and later filter out the blacklisted UMLS type trees.

    output_lines = defaultdict(list)
    semantic_type_trees = defaultdict(set)
    tree_names = defaultdict(set)
    with open(mrsty) as inf, open(umls_output, "w") as outf:
        for line in inf:
            x = line.strip().split("|")
            cat = x[2]
            cat_name = x[3]

            # Is this on the UMLS ID blocklist? If so, skip it!
            if x[0] in blocklist_umls_ids:
                continue

            curie = f"{prefix}:{x[0]}"

            tree_names[cat].add(cat_name)
            semantic_type_trees[curie].add(cat)

            # Do we know what Biolink type (i.e. category) to assign this to?
            if cat in category_map:
                output_lines[curie].append(category_map[cat])

        if blocklist_umls_semantic_type_tree:
            # If we need to blacklist by UMLS semantic type trees, delete CURIEs whose semantic type trees overlap
            # with the blocklist.
            selected_curies = list(output_lines.keys())
            for curie in selected_curies:
                if semantic_type_trees[curie] & blocklist_umls_semantic_type_tree:
                    # Note that this only works if the UMLS semantic tree type is exactly identical to the semantic
                    # tree type on the blocklist: so if you try to block "A1.2.3", then UMLS IDs with a semantic tree
                    # type of "A1.2.3.4" will NOT be blocked.

                    # Write out a log message.
                    sty_trees_with_names = ", ".join(
                        map(lambda sty_tree: f"{sty_tree}={tree_names[sty_tree]}", semantic_type_trees[curie])
                    )
                    blocklist_sty_trees_with_names = ", ".join(
                        map(lambda sty_tree: f"{sty_tree}={tree_names[sty_tree]}", blocklist_umls_semantic_type_tree)
                    )
                    logger.info(
                        f"Deleted {curie} from UMLS IDs because its types ({sty_trees_with_names}) overlapped with the blocklist ({blocklist_sty_trees_with_names})."
                    )

                    # Delete this CURIE from the output.
                    del output_lines[curie]

        if mesh_ownership is not None:
            apply_mesh_ownership(
                output_lines,
                read_mesh_ownership(mesh_ownership),
                pipeline,
                blocklist={f"{prefix}:{cui}" for cui in blocklist_umls_ids},
            )

        for curie in output_lines:
            # We only write out the first type we found for this UMLS ID.
            types = output_lines[curie]
            outf.write(f"{curie}\t{types[0]}\n")


def write_rxnorm_ids(
    category_map, bad_categories, infile, outfile, prefix=RXCUI, styfile="RXNSTY.RRF", blacklist=set()
):
    """It's surprising, but not everything in here has an RXCUI.
    Just because there's a row and it has an id in the first column, it doesn't mean pretty much anything.
    It's only ones that have an RXNORM in their row somewhere that count.   They are the ones that show up
    in MRCONSO.RRF.  It's not yet clear if there are relations that go through them though. So first we gotta
    go through RXNCONSO to find the ones that have an RXNORM, then back through STY to get the categories.

    Here is a response from the RXNORM folks about this (slightly reformatted):
    ---
    You are correct that some RXCUIs do not appear in the UMLS MRCONSO file.
    If there isn't a SAB='RXNORM' row in RxNorm for a particular RXCUI, there won't be one in the UMLS MRCONSO file.
    But you can still link RxNorm to the UMLS Metathesaurus in this case.
    Join RXNCONSO and MRCONSO on the the SAB fields (12th column in both RXNCONSO and MRCONSO) and CODE fields (14th column in both).
    So for your example of RXCUI 3, you can link RXNCONSO and MRCONSO on the SNOMEDCT_US code.

    Or you can also look up the RXCUI in the UMLS MRSAT file.
    For example, if you want to look up RXCUI 3, find rows where ATN (9th column) = 'RXCUI' and ATV (11th column) = '3'.

    Note that the UMLS is only updated twice a year, so new content in RxNorm will not appear in the UMLS until it is updated in May and November.

    Many RXCUIs do not have a SAB='RXNORM' row. There are a few reasons why an RXCUI would not have an associated RxNorm normalized name.
    Out of scope – Some information provided by source vocabularies is out of scope for RxNorm. While this information is grouped into concepts and given RXCUIs, RxNorm normalized names are not created for this information.
    Ambiguous – Some information provided by source vocabularies are too vague and for a specific RxNorm normalized name to be assigned.
    Base atom – A Base atom often lacks an RxNorm normalized name because a Base atom contains NDCs representing several different drug products. For more information about Base atoms, see Section 9: Duplicating Source Asserted Atoms (with NDC conflicts) of the RxNorm Technical Documentation.
    See our FAQ for more information: https://www.nlm.nih.gov/research/umls/rxnorm/faq.html
    ---
    Based on this response, I think that there are probably good reasons to leave out the RXCUIs without an RXNORM row.
    From what I can tell, RxNav also leaves these out.  So I'll leave them out for the moment.  If we find that we need
    them later, we can add them back in following the instructions above.

    After gorking around with STY for a while, I've realized that the best way to get the types is from RXNCONSO.
    If there is an IN or PIN TTY, then it's a ChemicalEntity, otherwise a Drug.
    """
    rxnconso = infile  # os.path.join('input_data', 'private', "RXNCONSO.RRF")
    with open(rxnconso) as inf, open(outfile, "w") as outf:
        current_id = None
        current_ttys = set()
        has_rxnorm = False
        for line in inf:
            # THis will remove obsolete and non-english lines
            if not check_mrconso_line(line):
                continue
            x = line.strip().split("|")
            if x[0] in blacklist:
                continue
            if x[0] != current_id:
                if (current_id is not None) and has_rxnorm:
                    if "DF" in current_ttys:
                        # These are dose forms.  Things like "Bottle" and "Tablet". Leads to all sorts of overglomming.
                        # Following is a hard pass.
                        pass
                    elif "IN" in current_ttys or "PIN" in current_ttys:
                        outf.write(f"{prefix}:{current_id}\t{CHEMICAL_ENTITY}\n")
                    elif "MIN" in current_ttys:
                        outf.write(f"{prefix}:{current_id}\t{MOLECULAR_MIXTURE}\n")
                    else:
                        outf.write(f"{prefix}:{current_id}\t{DRUG}\n")
                current_id = x[0]
                current_ttys = set()
                has_rxnorm = False
            if x[11] == "RXNORM":
                has_rxnorm = True
                current_ttys.add(x[12])
        if has_rxnorm:
            if "DF" in current_ttys:
                # These are dose forms.  Things like "Bottle" and "Tablet". Leads to all sorts of overglomming.
                # Following is a hard pass.
                pass
            elif "IN" in current_ttys or "PIN" in current_ttys:
                outf.write(f"{prefix}:{current_id}\t{CHEMICAL_ENTITY}\n")
            elif "MIN" in current_ttys:
                outf.write(f"{prefix}:{current_id}\t{MOLECULAR_MIXTURE}\n")
            else:
                outf.write(f"{prefix}:{current_id}\t{DRUG}\n")


# I've made this more complicated than it ought to be for 2 reasons:
# One is to keep from having to pass through the umls file more than once, but that's a bad reason
# The second is because I want to use the UMLS as a source for some terminologies (SNOMED) even if there's another
#  way.  I'm going to modify this to do one thing at a time, and if it takes a little longer, then so be it.
def build_sets(
    mrconso,
    umls_input,
    umls_output,
    other_prefixes,
    bad_mappings=defaultdict(set),
    acceptable_identifiers={},
    cui_prefix=UMLS,
    provenance_metadata_yaml=None,
):
    """Given a list of umls identifiers we want to generate all the concordances
    between UMLS and that other entity"""
    # On UMLS / MESH: we have been getting all UMLS / MESH relationships.   This has led to some clear mistakes
    # and logical impossibilities such as cyclical subclasses.   On further review, we can sharpen these relationships
    # by choosing the best match UMLS for each MESH.  We will make use of the TTY column (column 12) in MRCONSO.
    # This column can have a lot of values, but every MESH has one of (and only one of): MH, NM, HT, QAB.  These
    # will be the ones that we pull, as they correspond to the "main" name or heading of the mesh entry.
    # Because drugbank IDs are for active ingredients, we only want the UMLS IDs that map to a TTY of IN (ingredient)
    # Otherwise, you get the same DBID mapping to multiple UMLS IDs in a loose way.
    umls_ids = set()
    with open(umls_input) as inf:
        for line in inf:
            u = line.strip().split("\t")[0].split(":")[1]
            umls_ids.add(u)
    lookfor = set(other_prefixes.keys())
    acceptable_mesh_tty = MESH_CONCORD_TTYS
    acceptable_drugbank_tty = set(["IN", "PIN", "MIN"])
    pairs = set()
    # test_cui = 'C0026827'
    with open(mrconso) as inf, open(umls_output, "w") as concordfile:
        for line in inf:
            if not check_mrconso_line(line):
                continue

            x = line.strip().split("|")
            cui = x[0]
            if cui not in umls_ids:
                continue

            # only keep sources we're looking for
            source = x[11]
            if source not in lookfor:
                continue
            tty = x[12]
            if (source == "MSH") and (tty not in acceptable_mesh_tty):
                continue
            if (source == "DRUGBANK") and (tty not in acceptable_drugbank_tty):
                continue
            # For some dippy reason, in the id column they say "HGNC:76"
            pref = other_prefixes[source]
            if ":" in x[13]:
                other_id = f"{pref}:{x[13].split(':')[-1]}"
            else:
                other_id = f"{pref}:{x[13]}"
            # I don't know why this is in here, but it is not an identifier equivalent to anything
            if other_id == "NCIT:TCGA":
                continue
            tup = (f"{cui_prefix}:{cui}", other_id)
            # Don't include bad mappings or bad ids
            if tup[1] in bad_mappings[tup[0]]:
                continue
            if (pref in acceptable_identifiers) and (tup[1] not in acceptable_identifiers[pref]):
                continue
            if tup not in pairs:
                concordfile.write(f"{tup[0]}\teq\t{tup[1]}\n")
                pairs.add(tup)

    # Write provenance for this build_sets() call.
    if provenance_metadata_yaml is not None:
        write_concord_metadata(
            provenance_metadata_yaml,
            name="umls.build_sets()",
            sources=[{"type": "UMLS", "name": "MRCONSO"}],
            description=f"umls.build_sets() using UMLS MRCONSO with prefixes: {other_prefixes} with cui_prefix set to {cui_prefix}",
            concord_filename=umls_output,
        )


def read_umls_priority():
    mrp = os.path.join("input_data", "umls_precedence.txt")
    pris = []
    with open(mrp) as inf:
        _header = inf.readline()
        for line in inf:
            x = line.strip().split()
            if x[2] == "No":
                pris.append((x[0], x[1], "N"))
            elif x[2] == "Yes":
                pris.append((x[0], x[1], "Y"))
            else:
                pass
    prid = {x: i for i, x in enumerate(pris)}
    return prid


def download_umls(umls_version, umls_subset, download_dir):
    """
    Download the latest UMLS into the specified download directory. In addition to downloading
    and unzipping UMLS, this will move the files we use into the main directory.

    :param umls_version: The version of UMLS to download (e.g. `2023AA`).
    :param umls_subset: The subset of UMLS to download (e.g. `full`, `level-0`).
    :param download_dir: The directory to download UMLS to (e.g. `babel_downloads/UMLS`)
    """
    umls_api_key = os.environ.get("UMLS_API_KEY")
    if not umls_api_key:
        raise RuntimeError(
            "The environment variable UMLS_API_KEY needs to be set to a valid UMLS API key.\nSee instructions at https://documentation.uts.nlm.nih.gov/rest/authentication.html"
        )

    # Check umls_subset.
    if umls_subset not in ["full", "level-0"]:
        raise ValueError(f"The umls_subset parameter must be one of 'full' or 'level-0', but got: {umls_subset}.")

    # Download umls-{umls_version}-metathesaurus-full.zip
    # As described at https://documentation.uts.nlm.nih.gov/automating-downloads.html
    umls_url = "https://uts-ws.nlm.nih.gov/download"
    filename = f"umls-{umls_version}-metathesaurus-{umls_subset}.zip"
    umls_full_url = f"https://download.nlm.nih.gov/umls/kss/{umls_version}/{filename}"
    req = requests.get(
        umls_url,
        {"url": umls_full_url, "apiKey": umls_api_key},
        stream=True,
    )
    if not req.ok:
        raise RuntimeError(f"Unable to download UMLS from {umls_full_url}: {req}")

    # Write file to {download_dir}/umls-{umls_version}-metathesaurus-full.zip
    logging.info(f"Downloading {filename} to {download_dir}")
    os.makedirs(download_dir, exist_ok=True)
    umls_download_zip = os.path.join(download_dir, filename)
    with open(umls_download_zip, "wb") as fd:
        for chunk in req.iter_content(chunk_size=128):
            fd.write(chunk)

    # Unzip file.
    logging.info(f"Uncompressing {umls_download_zip}")
    with ZipFile(umls_download_zip, "r") as zipObj:
        zipObj.extractall(download_dir)

    # Move files we use to the main download directory.
    # - MRCONSO.RRF
    shutil.copy2(os.path.join(download_dir, umls_version, "META", "MRCONSO.RRF"), download_dir)
    # - MRSTY.RRF
    shutil.copy2(os.path.join(download_dir, umls_version, "META", "MRSTY.RRF"), download_dir)
    # - MRREL.RRF
    shutil.copy2(os.path.join(download_dir, umls_version, "META", "MRREL.RRF"), download_dir)

    # Create a metadata.yaml file for UMLS.
    metadata_yaml = os.path.join(download_dir, "UMLS.metadata.yaml")
    write_download_metadata(
        metadata_yaml,
        name="UMLS Metathesaurus",
        url=umls_full_url,
        description=f"UMLS Metathesaurus {umls_version} ({umls_subset} subset), downloaded via the UTS download API.",
        sources=[
            {
                "name": "UMLS Metathesaurus MRCONSO.RRF, MRSTY.RRF and MRREL.RRF",
                "version": umls_version,
                "subset": umls_subset,
                "url": umls_full_url,
            }
        ],
    )


def download_rxnorm(rxnorm_version, download_dir):
    """
    Download the specified RxNorm version into the specified download directory. In addition to
    downloading and unzipping RxNorm, this will move the files we use into the main directory.

    :param rxnorm_version: The version of RxNorm to download (e.g. `07032023`).
        Look for the latest download at https://www.nlm.nih.gov/research/umls/rxnorm/docs/rxnormfiles.html
    :param download_dir: The directory to download UMLS to (e.g. `babel_downloads/RxNORM`)
    """
    umls_api_key = os.environ.get("UMLS_API_KEY")
    if not umls_api_key:
        raise RuntimeError(
            "The environment variable UMLS_API_KEY needs to be set to a valid UMLS API key.\nSee instructions at https://documentation.uts.nlm.nih.gov/rest/authentication.html"
        )

    # Download RxNorm_full_{rxnorm_version}.zip
    # As described at https://documentation.uts.nlm.nih.gov/automating-downloads.html
    rxnorm_url = "https://uts-ws.nlm.nih.gov/download"
    req = requests.get(
        rxnorm_url,
        {
            "url": f"https://download.nlm.nih.gov/umls/kss/rxnorm/RxNorm_full_{rxnorm_version}.zip",
            "apiKey": umls_api_key,
        },
        stream=True,
    )
    if not req.ok:
        raise RuntimeError(f"Unable to download RxNorm from {rxnorm_url}: {req}")

    # Write file to {download_dir}/RxNorm_full_{rxnorm_version}.zip
    logging.info(f"Downloading RxNorm_full_{rxnorm_version}.zip to {download_dir}")
    os.makedirs(download_dir, exist_ok=True)
    rxnorm_download_zip = os.path.join(download_dir, f"RxNorm_full_{rxnorm_version}.zip")
    with open(rxnorm_download_zip, "wb") as fd:
        for chunk in req.iter_content(chunk_size=128):
            fd.write(chunk)

    # Unzip file.
    logging.info(f"Uncompressing {rxnorm_download_zip}")
    with ZipFile(rxnorm_download_zip, "r") as zipObj:
        zipObj.extractall(download_dir)

    # Move files we use to the main download directory.
    # - RXNCONSO.RRF
    shutil.copy2(os.path.join(download_dir, "rrf", "RXNCONSO.RRF"), download_dir)
    # - RXNREL.RRF
    shutil.copy2(os.path.join(download_dir, "rrf", "RXNREL.RRF"), download_dir)


def pull_umls(mrconso):
    """Run through MRCONSO.RRF creating label and synonym files for UMLS and SNOMEDCT"""
    rows = defaultdict(list)
    priority = read_umls_priority()
    snomed_label_name = make_local_name("labels", subpath="SNOMEDCT")
    snomed_syn_name = make_local_name("synonyms", subpath="SNOMEDCT")
    with open(mrconso) as inf, open(snomed_label_name, "w") as snolabels, open(snomed_syn_name, "w") as snosyns:
        for line in inf:
            if not check_mrconso_line(line):
                continue

            x = line.strip().split("|")
            cui = x[0]
            _lang = x[1]
            suppress = x[16]
            source = x[11]
            termtype = x[12]
            term = x[14]
            # While we're here, if this thing is snomed, lets get it
            if source == "SNOMEDCT_US":
                snomed_id = f"SNOMEDCT:{x[15]}"
                if termtype == "PT":
                    snolabels.write(f"{snomed_id}\t{term}\n")
                snosyns.write(f"{snomed_id}\t{HAS_EXACT_SYNONYM}\t{term}\n")
            # UMLS is a collection of sources. They pick one of the names from these sources for a concept,
            # and that's based on a priority that they define. Here we get the priority for terms so we
            # can get the right one for the label
            pkey = (source, termtype, suppress)
            try:
                pri = priority[pkey]
            except Exception:
                logger.warning(f"Priority not found for key {pkey}. Defaulting to high priority (1000000).")
                # print(pkey)
                pri = 1000000
            rows[cui].append((pri, term, line))
    lname = make_local_name("labels", subpath="UMLS")
    sname = make_local_name("synonyms", subpath="UMLS")
    re_numerical = re.compile(r"^\s*[+-]*[\d\.]+\s*$")
    with open(lname, "w") as labels, open(sname, "w") as synonyms:
        for cui, crows in rows.items():
            crows.sort()
            labels.write(f"{UMLS}:{cui}\t{crows[0][1]}\n")
            syns = set([crow[1] for crow in crows])
            for s in syns:
                # Skip any synonyms that are purely numerical, since those are unlikely to be useful.
                if re_numerical.fullmatch(s):
                    logging.debug(f"Found numerical synonym '{s}' in UMLS, skipping")
                    continue
                synonyms.write(f"{UMLS}:{cui}\t{HAS_EXACT_SYNONYM}\t{s}\n")
