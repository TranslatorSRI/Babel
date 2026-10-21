import csv
import json
import logging
import sys
import time
from collections import defaultdict

import jsonlines
from humanfriendly import format_timespan

from src.babel_utils import get_numerical_curie_suffix, glom
from src.categories import CHEMICAL_ENTITY
from src.metadata.provenance import write_combined_metadata, write_concord_metadata
from src.node import InformationContentFactory
from src.prefixes import PUBCHEMCOMPOUND, RXCUI, UMLS
from src.util import LoggingUtil, Text, get_biolink_model_toolkit, get_config, get_memory_usage_summary

logger = LoggingUtil.init_logging(__name__, level=logging.INFO)

# This module used to carry a commented-out PREFERRED_CONFLATION_TYPE_ORDER for ordering cliques
# within a conflation. That ranking now lives -- and is used -- as config.yaml: chemical_type_order,
# where create_typed_sets() reads it to break ties in the chemical clique type vote (issue #935).

# RXNORM has lots of relationships.
# RXNREL contains both directions of each relationship, just to make the file bigger
# Here's the list:
#   54 reformulated_to
#   54 reformulation_of
#  132 entry_version_of
#  132 has_entry_version
#  255 has_sort_version
#  255 sort_version_of
# 1551 has_product_monograph_title
# 1551 product_monograph_title_of
# 1667 mapped_to
# 1668 mapped_from
# 1932 has_modification
# 1932 is_modification_of
# 2886 has_permuted_term
# 2886 permuted_term_of
# 3367 form_of
# 3367 has_form
# 5372 has_member
# 5372 member_of
# 5894 contained_in
# 5894 contains
# 5937 has_quantified_form
# 5937 quantified_form_of
# 6215 included_in
# 6215 includes
# 9112 basis_of_strength_substance_of
# 9112 has_basis_of_strength_substance
# 9112 has_precise_active_ingredient
# 9112 precise_active_ingredient_of
# 10389 has_part
# 10389 part_of
# 11323 has_precise_ingredient
# 11323 precise_ingredient_of
# 11562 has_ingredients
# 11562 ingredients_of
# 29427 has_print_name
# 29427 print_name_of
# 35466 doseformgroup_of
# 35466 has_doseformgroup
# 101449 has_tradename
# 101449 tradename_of
# 111137 consists_of
# 111137 constitutes
# 128330 dose_form_of
# 128330 has_dose_form
# 251454 inverse_isa
# 251454 isa
# 335789 has_ingredient
# 335789 ingredient_of
# 352829 active_moiety_of
# 352829 has_active_moiety
# 374599 active_ingredient_of
# 374599 has_active_ingredient
# 561937
# 1640618 has_inactive_ingredient
# 1640618 inactive_ingredient_of

# Note that there are a bunch that are blank
# There's a reasonable picture explaining a lot of these here:
# https://www.nlm.nih.gov/research/umls/rxnorm/RxNorm_Drug_Relationships.png

# We're going to choose one of the two directions for each relationship from that picture.
# We're going to ignore the others because they seem freaky - for instance reformulation seems to have
# a bunch (all?) where the subject is not in RXNCONSO anywhere...

useful_relationships = [
    "has_form",
    "has_precise_active_ingredient",
    "has_precise_ingredient",
    "tradename_of",
    "consists_of",
    "has_ingredient",
    "has_active_ingredient",
]


def get_aui_to_cui(consofile):
    """Get a mapping from AUI to CUI"""
    aui_to_cui = {}
    sdui_to_cui = defaultdict(set)
    # consofile = os.path.join('input_data', 'private', "RXNCONSO.RRF")
    with open(consofile) as inf:
        for line in inf:
            x = line.strip().split("|")
            aui = x[7]
            cui = x[0]
            sdui = (x[11], x[7])
            if aui in aui_to_cui:
                print("What the all time fuck?")
                print(aui, cui)
                print(aui_to_cui[aui])
                raise RuntimeError("Something has gone very wrong")
            aui_to_cui[aui] = cui
            if sdui[1] == "":
                continue
            sdui_to_cui[sdui].add(cui)
    return aui_to_cui, sdui_to_cui


def get_cui(x, indicator_column, cui_column, aui_column, aui_to_cui, sdui_to_cui):
    relation_column = 7
    source_column = 10
    if x[relation_column] in useful_relationships:
        if x[indicator_column] == "CUI":
            return x[cui_column]
        elif x[indicator_column] == "AUI":
            try:
                return aui_to_cui[x[aui_column]]
            except Exception:
                # this really shouldn't happen.  But it seems to occur for the UMLS files?
                return None
        elif x[indicator_column] == "SDUI":
            cuis = sdui_to_cui[(x[source_column], x[aui_column])]
            if len(cuis) == 1:
                return list(cuis)[0]
            print("sdui garbage hell")
            print(x)
            print(cuis)
            raise RuntimeError("Something has gone very wrong with SDUI")
        elif x[indicator_column] == "SCUI":
            # SCUI is source cui, i.e. what the source calls it.  We might be able to pull this out of CONSO if we have to.
            return None
        print("cmon man")
        print(x)
        raise RuntimeError("Something has gone very wrong with CUI")


def build_rxnorm_relationships(conso, relfile, outfile, metadata_yaml):
    """RXNREL is a lousy file.
    The subject and object can sometimes be a CUI and sometimes an AUI and you have to use
    CONSO to figure out how to go back and forth.
    Some of them are using SDUIs are you joking?

    Another issue: there are things like this:
    RXCUI:214199	has_active_ingredient	RXCUI:435
    RXCUI:214199	has_active_ingredient	RXCUI:7213
    In this case, what we have is a single drug that has two active ingredients.
    We don't want to glom in this case, because it unifies the two ingredients at the conflation level,
    which leads to everything is everything.
    So we're going to need to collect has_active_ingredients as we go and only export ones that are singular

    What's more, the same thing happens with has_precise_active_ingredient, and maybe has_ingredient.
    Also, the same subject and object will have the more general term as well.  so both a has_precise_active_ingredient
    and a has_ingredient will be between the same set of subject and object.  Also there's consists of, which will hav
    similar issues. So, we're going to do a lot of catching here

    has_tradename is even worse - it needs to be 1:1 to be useable

    Also, the same (cui) subject/object/predicate triple can be on multiple lines, obscured
    by the fact that auis and sduis are used in the file.  This happens when the effective triple comes from multiple
    sources. That's why the collections below need to be sets rather than lists
    """
    # This is maybe relying on convention a bit too much.
    if outfile == "UMLS":
        prefix = UMLS
        sources = [
            {"type": "UMLS", "name": "MRCONSO", "filename": conso},
            {"type": "UMLS", "name": "MRREL", "filename": relfile},
        ]
    else:
        prefix = RXCUI
        sources = [
            {"type": "RXNORM", "name": "RXNCONSO", "filename": conso},
            {"type": "RXNOM", "name": "RXNREL", "filename": relfile},
        ]
    aui_to_cui, sdui_to_cui = get_aui_to_cui(conso)
    # relfile = os.path.join('input_data', 'private', "RXNREL.RRF")
    single_use_relations = {
        "has_active_ingredient": defaultdict(set),
        "has_precise_active_ingredient": defaultdict(set),
        "has_precise_ingredient": defaultdict(set),
        "has_ingredient": defaultdict(set),
        "tradename_of": defaultdict(set),
        "consists_of": defaultdict(set),
    }
    one_to_one_relations = {}
    # one_to_one_relations = {"has_tradename": {"subject": defaultdict(set),
    #                                          "object": defaultdict(set)}}
    with open(relfile) as inf, open(outfile, "w") as outf:
        for line in inf:
            x = line.strip().split("|")
            # UMLS always has the CUI in it, while RXNORM does not.
            if outfile == "UMLS":
                object_cui = x[0]
                subject_cui = x[4]
            else:
                object_cui = get_cui(x, 2, 0, 1, aui_to_cui, sdui_to_cui)
                subject_cui = get_cui(x, 6, 4, 5, aui_to_cui, sdui_to_cui)
            if (subject_cui is not None) and (object_cui is not None):
                if subject_cui == object_cui:
                    continue
                predicate = x[7]
                if predicate in single_use_relations:
                    single_use_relations[predicate][subject_cui].add(object_cui)
                elif predicate in one_to_one_relations:
                    one_to_one_relations[predicate]["subject"][subject_cui].add(object_cui)
                    one_to_one_relations[predicate]["object"][object_cui].add(subject_cui)
                else:
                    outf.write(f"{prefix}:{subject_cui}\t{predicate}\t{prefix}:{object_cui}\n")
        for predicate in single_use_relations:
            for subject_cui, objects in single_use_relations[predicate].items():
                if len(objects) > 1:
                    continue
                outf.write(f"{prefix}:{subject_cui}\t{predicate}\t{prefix}:{next(iter(objects))}\n")
        for predicate in one_to_one_relations:
            for subject_cui, objects in one_to_one_relations[predicate]["subject"].items():
                if len(objects) > 1:
                    continue
                if len(one_to_one_relations[predicate]["object"][next(iter(objects))]) > 1:
                    continue
                outf.write(f"{prefix}:{subject_cui}\t{predicate}\t{prefix}:{next(iter(objects))}\n")

    write_concord_metadata(
        metadata_yaml,
        name="build_rxnorm_relationships()",
        description=f"Builds relationships between RxCUI and other identifiers from a CONSO ({conso}) and a REL ({relfile}).",
        sources=sources,
        concord_filename=outfile,
    )


def load_cliques_containing_rxcui(compendium):
    rx_to_clique = {}
    with open(compendium) as infile:
        for line in infile:
            if RXCUI not in line:
                continue
            j = json.loads(line)
            clique = j["identifiers"][0]["i"]
            for terms in j["identifiers"]:
                if terms["i"].startswith(RXCUI):
                    rx_to_clique[terms["i"]] = clique
    return rx_to_clique


def build_pubchem_relationships(infile, outfile, metadata_yaml):
    with open(infile) as inf:
        document = json.load(inf)
    with open(outfile, "w") as outf:
        for annotation in document["Annotations"]["Annotation"]:
            rxnid = annotation["SourceID"]
            cids = annotation.get("LinkedRecords", {}).get("CID", [])
            for cid in cids:
                outf.write(f"{RXCUI}:{rxnid}\tlinked\t{PUBCHEMCOMPOUND}:{cid}\n")

    write_concord_metadata(
        metadata_yaml,
        name="build_pubchem_relationships()",
        description=f"Builds relationships between RxCUI and PubChem Compound identifiers from a PubChem annotations file ({infile}.",
        sources=[
            {
                "type": "PubChem",
                "name": "PubChem RxNorm annotations",
                "description": "PubChem RxNorm mappings generated by pubchem.pull_rxnorm_annotations()",
                "filename": infile,
            }
        ],
        concord_filename=outfile,
    )


def _validate_and_apply_manual_concords(
    manual_concords: list[tuple[str, str]],
    preferred_curie_for_curie: dict[str, str],
    pairs: list[tuple[str, str]],
    manual_concord_filename: str,
) -> int:
    """Validate each manual concord pair against the chemical compendia and append passing pairs to *pairs*.

    Both CURIEs in a pair must appear in *preferred_curie_for_curie*; if either is absent a warning is
    emitted for that CURIE, the whole pair is skipped, and the skip count returned by this function is
    incremented. When both are absent, a warning is emitted for each before the pair is skipped.
    Passing CURIEs are normalised to their preferred form before being appended. If both CURIEs normalise
    to the same preferred CURIE, a warning is emitted and the self-pair is skipped.

    Returns (skipped, applied_curies) where skipped is the number of skipped pairs and applied_curies
    is the set of preferred CURIEs that were actually added to pairs.
    """
    skipped = 0
    applied_curies: set[str] = set()
    for subject_curie, object_curie in manual_concords:
        subject_ok = subject_curie in preferred_curie_for_curie
        object_ok = object_curie in preferred_curie_for_curie
        if not subject_ok:
            logger.warning(
                f"Manual concord subject {subject_curie} (paired with {object_curie}) is not in any chemical compendium — "
                f"it may have been reclassified (e.g. as a protein). "
                f"If so, remove it from {manual_concord_filename}."
            )
        if not object_ok:
            logger.warning(
                f"Manual concord object {object_curie} (paired with {subject_curie}) is not in any chemical compendium — "
                f"it may have been reclassified (e.g. as a protein). "
                f"If so, remove it from {manual_concord_filename}."
            )
        if not subject_ok or not object_ok:
            skipped += 1
            continue
        norm_subject = preferred_curie_for_curie[subject_curie]
        norm_object = preferred_curie_for_curie[object_curie]
        if norm_subject == norm_object:
            logger.warning(
                f"Manual concord pair ({subject_curie}, {object_curie}) normalizes to the same preferred CURIE "
                f"({norm_subject}); skipping self-pair."
            )
            skipped += 1
            continue
        pairs.append((norm_subject, norm_object))
        applied_curies.add(norm_subject)
        applied_curies.add(norm_object)
    return skipped, applied_curies


def build_conflation(
    manual_concord_filename,
    rxn_concord,
    umls_concord,
    pubchem_rxn_concord,
    drug_compendium,
    chemical_compendia,
    icrdf_filename,
    outfilename,
    input_metadata_yamls,
    output_metadata_yaml,
):
    """RXN_concord contains relationshps between rxcuis that can be used to conflate
    Now we don't want all of them.  We want the ones that are between drugs and chemicals,
    and the ones between drugs and drugs.
    To determine which those are, we're going to have to dig around in all the compendia.
    We also want to get all the clique leaders as well.  For those, we only need to worry if there are RXCUIs
    in the clique."""

    config = get_config()

    logger.info("Loading information content values...")
    ic_factory = InformationContentFactory(icrdf_filename)

    logger.info("Loading manual concords ...")
    manual_concords = []
    manual_concords_curies = set()
    manual_concords_predicate_counts = defaultdict(int)
    manual_concords_curie_prefix_counts = defaultdict(int)
    with open(manual_concord_filename) as manualf:
        csv_reader = csv.DictReader(manualf, dialect=csv.excel_tab)
        for row in csv_reader:
            # We're only interested in two fields, so you can add additional files ('comment', 'notes', etc.) as needed.
            if "subject" not in row or "object" not in row:
                raise RuntimeError(f"Missing subject or object fields in {manual_concord_filename}: {row}")
            subject_curie = row["subject"].strip()
            object_curie = row["object"].strip()
            if subject_curie == "" or object_curie == "":
                raise RuntimeError(f"Empty subject or object fields in {manual_concord_filename}: {row}")
            manual_concords.append((subject_curie, object_curie))
            manual_concords_predicate_counts[row["predicate"]] += 1
            manual_concords_curies.add(subject_curie)
            manual_concords_curies.add(object_curie)

            sorted_curies = sorted([subject_curie, object_curie])
            prefix_count_label = row["predicate"] + "(" + (" ,".join(sorted_curies)) + ")"
            manual_concords_curie_prefix_counts[prefix_count_label] += 1
    logger.info(f"{len(manual_concords)} manual concords loaded.")

    logger.info("load all chemical compendia so we can normalize identifiers")
    preferred_curie_for_curie = {}
    type_for_preferred_curie = {}
    clique_for_preferred_curie = {}
    for chemical_compendium in chemical_compendia:
        with open(chemical_compendium) as compendiumf:
            logger.info(f"Loading {chemical_compendium}: {get_memory_usage_summary()}")
            for line in compendiumf:
                clique = json.loads(line)
                preferred_id = clique["identifiers"][0]["i"]
                clique_for_preferred_curie[preferred_id] = list(map(lambda ident: ident["i"], clique["identifiers"]))
                type_for_preferred_curie[preferred_id] = clique["type"]
                for ident in clique["identifiers"]:
                    id = ident["i"]
                    preferred_curie_for_curie[id] = preferred_id

    logger.info(
        f"Loaded preferred CURIEs for {len(preferred_curie_for_curie)} CURIEs from the chemical compendia: {get_memory_usage_summary()}"
    )

    logger.info("load drugs")
    drug_rxcui_to_clique = load_cliques_containing_rxcui(drug_compendium)
    chemical_rxcui_to_clique = {}
    for chemical_compendium in chemical_compendia:
        if chemical_compendium == drug_compendium:
            continue
        logger.info(f"load {chemical_compendium}: {get_memory_usage_summary()}")
        chemical_rxcui_to_clique.update(load_cliques_containing_rxcui(chemical_compendium))

    pairs = []
    for concfile in [rxn_concord, umls_concord]:
        with open(concfile) as infile:
            for line in infile:
                x = line.strip().split("\t")
                subject_curie = x[0]
                object_curie = x[2]

                # While we do this, we will also normalize all chemicals to their preferred clique IDs.
                if subject_curie in drug_rxcui_to_clique and object_curie in chemical_rxcui_to_clique:
                    subject_curie = drug_rxcui_to_clique[subject_curie]
                    object_curie = chemical_rxcui_to_clique[object_curie]
                    pairs.append((subject_curie, object_curie))
                elif subject_curie in chemical_rxcui_to_clique and object_curie in drug_rxcui_to_clique:
                    subject_curie = chemical_rxcui_to_clique[subject_curie]
                    object_curie = drug_rxcui_to_clique[object_curie]
                    pairs.append((subject_curie, object_curie))
                # OK, this is possible, and it's OK, as long as we get real clique leaders
                elif subject_curie in drug_rxcui_to_clique and object_curie in drug_rxcui_to_clique:
                    subject_curie = drug_rxcui_to_clique[subject_curie]
                    object_curie = drug_rxcui_to_clique[object_curie]
                    pairs.append((subject_curie, object_curie))
                elif subject_curie in chemical_rxcui_to_clique and object_curie in chemical_rxcui_to_clique:
                    subject_curie = chemical_rxcui_to_clique[subject_curie]
                    object_curie = chemical_rxcui_to_clique[object_curie]
                    pairs.append((subject_curie, object_curie))

    # Add the manual concords, normalizing CURIEs to their preferred form.
    manual_concords_skipped, manual_concords_applied_curies = _validate_and_apply_manual_concords(
        manual_concords, preferred_curie_for_curie, pairs, manual_concord_filename
    )

    # We've had some issues with non-chemical types getting conflated, so we filter those out here.
    biolink_model_toolkit = get_biolink_model_toolkit(config["biolink_version"])
    biolink_chemical_types = set(
        biolink_model_toolkit.get_descendants(
            CHEMICAL_ENTITY,
            reflexive=True,
            formatted=True,
            mixin=True,
        )
    )
    logger.info(f"Filtering RxCUI pairs to those in these Biolink chemical types: {sorted(biolink_chemical_types)}")
    with open(pubchem_rxn_concord) as infile:
        for line in infile:
            x = line.strip().split("\t")
            subject_curie = x[0]
            object_curie = x[2]

            if subject_curie in drug_rxcui_to_clique:
                subject_curie = drug_rxcui_to_clique[subject_curie]
            elif subject_curie in chemical_rxcui_to_clique:
                subject_curie = chemical_rxcui_to_clique[subject_curie]
            else:
                logger.warning(
                    f"Subject in subject-object pair ({subject_curie}, {object_curie}) isn't mapped to a RxCUI, skipping."
                )
                continue
                # raise RuntimeError(f"Unknown identifier in drugchemical conflation as subject: {subject_curie}")

            if object_curie in drug_rxcui_to_clique:
                object_curie = drug_rxcui_to_clique[object_curie]
            elif object_curie in chemical_rxcui_to_clique:
                object_curie = chemical_rxcui_to_clique[object_curie]
            else:
                logger.warning(
                    f"Object in subject-object pair ({subject_curie}, {object_curie}) isn't mapped to a RxCUI, skipping."
                )
                # raise RuntimeError(f"Unknown identifier in drugchemical conflation as object: {object_curie}")
                continue

            # Normalize both the subject and object, otherwise skip them.
            if subject_curie not in preferred_curie_for_curie:
                logger.warning(
                    f"Subject in subject-object pair ({subject_curie}, {object_curie}) has no preferred CURIE, skipping."
                )
                continue
            subject_curie = preferred_curie_for_curie[subject_curie]

            if object_curie not in preferred_curie_for_curie:
                logger.warning(
                    f"Object in subject-object pair ({subject_curie}, {object_curie}) has no preferred CURIE, skipping."
                )
                continue
            object_curie = preferred_curie_for_curie[object_curie]

            if subject_curie == object_curie:
                logger.warning(
                    f"Subject and object in subject-object pair ({subject_curie}, {object_curie}) normalize to the same identifier ({subject_curie}), skipping."
                )
                continue

            # Either the subject or the object might not be a chemical -- for example, MESH:C415772 shows up here,
            # but it's a gene, not a chemical.
            subject_type = type_for_preferred_curie[subject_curie]
            if subject_type not in biolink_chemical_types:
                logger.warning(
                    f"Subject in subject-object pair ({subject_curie}, {object_curie}) has type {subject_type}, which is is not a chemical type, skipping."
                )
                continue

            object_type = type_for_preferred_curie[object_curie]
            if object_type not in biolink_chemical_types:
                logger.warning(
                    f"Object in subject-object pair ({subject_curie}, {object_curie}) has type {object_type}, which is is not a chemical type, skipping."
                )
                continue

            pairs.append((subject_curie, object_curie))

    # Glommin' time
    logger.info(f"glom: {get_memory_usage_summary()}")
    gloms = {}
    glom(gloms, pairs)

    # Set up the preferred conflation type order.
    # preferred_conflation_type_order = PREFERRED_CONFLATION_TYPE_ORDER
    # logger.info(f"Using preferred_conflation_type_order: {json.dumps(preferred_conflation_type_order, indent=2)}")

    # Grouping conflation IDs by type is a great idea, and almost works! Unfortunately, we're currently
    # identifying too many things as ChemicalEntity for this to work properly -- non-ideal concepts like
    # CHEBI:5931 "insulin human" get placed further down in the conflation list than lots of other identifiers,
    # including UNII:AVT680JB39 "Insulin pork", which is NOT good.
    #
    # So, instead, I'm going to group them by prefix and then to sort it using the ChemicalEntity
    # prefix sort order.
    biolink_model_toolkit = get_biolink_model_toolkit(config["biolink_version"])
    biolink_chemical_entity = biolink_model_toolkit.get_element(CHEMICAL_ENTITY)
    conflation_prefix_order = biolink_chemical_entity["id_prefixes"]
    if not conflation_prefix_order:
        raise RuntimeError(
            f"Biolink model {config['biolink_version']} doesn't have a ChemicalEntity prefix order: {biolink_chemical_entity}"
        )

    # Remove RXCUI from the prefix order if it is present.
    conflation_prefix_order.remove("RXCUI")

    # ... and add it to the bottom.
    conflation_prefix_order.append("RXCUI")

    # Turn it into a sort order.
    conflation_prefix_sort_order = {}
    for i, prefix in enumerate(conflation_prefix_order):
        conflation_prefix_sort_order[prefix] = i

    logger.info(f"Using prefix sort order: {json.dumps(conflation_prefix_sort_order, indent=2)}")

    # Write out all the resulting cliques.
    written = set()
    with jsonlines.open(outfilename, "w") as outf:
        cliques = list(gloms.values())
        total_clique_count = len(gloms)
        clique_count = 0
        start_time = time.time_ns()
        for clique in cliques:
            # 0. Provide ongoing tracking of this task. There are only ~10K conflations, but
            # it's useful to know how quickly they are being processed.
            clique_count += 1
            if (clique_count == 1) or (clique_count % 1000 == 0):
                time_elapsed_seconds = (time.time_ns() - start_time) / 1e9
                if time_elapsed_seconds < 0.001:
                    # We don't want to divide by zero.
                    time_elapsed_seconds = 0.001
                remaining_cliques = total_clique_count - clique_count
                logger.info(
                    f"Generating DrugChemical conflations currently at {clique_count:,} out of {total_clique_count:,} ({clique_count / total_clique_count * 100:.2f}%) in {format_timespan(time_elapsed_seconds)}: {get_memory_usage_summary()}"
                )
                logger.info(
                    f" - Current rate: {clique_count / time_elapsed_seconds:.2f} cliques/second or {time_elapsed_seconds / clique_count:.6f} seconds/clique."
                )

                time_remaining_seconds = time_elapsed_seconds / clique_count * remaining_cliques
                logger.info(f" - Estimated time remaining: {format_timespan(time_remaining_seconds)}")

            # 1. Prepare a list of identifiers so we can iterate over them.
            fs = frozenset(clique)
            if fs in written:
                continue
            conflation_id_list = list(clique)

            # 2. Group identifiers by Biolink type, preserving the order of the clique members.
            # conflation_ids_by_type = defaultdict(list)
            conflation_ids_by_prefix = defaultdict(list)
            normalized_conflation_id_list = list()
            for iid in conflation_id_list:
                # Normalization shouldn't be needed here, because they're all clique leaders, but just in case.
                if iid not in preferred_curie_for_curie:
                    raise RuntimeError(
                        f"Conflation clique member {iid} (in clique {conflation_id_list}) is not in any chemical "
                        f"compendium. This is an internal logic error: all CURIEs entering glom() should have been "
                        f"validated against the compendia beforehand. Check the RXN/UMLS concord processing paths "
                        f"above, as manual concord entries from {manual_concord_filename} are already validated by "
                        f"_validate_and_apply_manual_concords."
                    )
                preferred_curie = preferred_curie_for_curie[iid]
                if preferred_curie != iid:
                    logger.warning(
                        f"Conflation leader {iid} should have been normalized to {preferred_curie}, normalizing now."
                    )
                if preferred_curie not in normalized_conflation_id_list:
                    normalized_conflation_id_list.append(preferred_curie)

                # Add it to the dictionary of types in the order of the clique members.
                # At the moment, we get these from glomming, so the order should not actually be significant.
                # But maybe in the future it will be if that changes? And it doesn't cost us much to maintain
                # insertion order.
                # preferred_curie_type = type_for_preferred_curie[preferred_curie]
                # if preferred_curie not in conflation_ids_by_type[preferred_curie_type]:
                #    # Don't add duplicates!
                #    conflation_ids_by_type[preferred_curie_type].append(preferred_curie)

                # We will use the preferred CURIE prefix to sort instead.
                preferred_curie_prefix = Text.get_prefix(preferred_curie)
                if preferred_curie not in conflation_ids_by_prefix[preferred_curie_prefix]:
                    conflation_ids_by_prefix[preferred_curie_prefix].append(preferred_curie)

            # After all the normalization, it's possible that we'll end up with a conflation that only has a
            # single identifier in it. If so, we don't need to add it to the conflation list, because it won't
            # do anything there.
            if len(normalized_conflation_id_list) == 1:
                logger.debug(
                    f"Found a DrugChemical conflation with a single identifier, skipping: {normalized_conflation_id_list}."
                )
                continue

            # Within each of those groups, we want to sort by:
            #   - information_content (lowest to highest, so that more general concepts are front-loaded)
            #   - clique size (largest to smallest, so that larger cliques are front-loaded)
            #   - numerical suffix (lowest to highest)
            # Note that this does NOT include prefix order for the Biolink type. I think mixing that with multiple
            # Biolink types will just make the output lists more confusing. Most people will only care about the
            # clique conflation leader.
            final_conflation_id_list = []
            clique_ics = []

            # If we want to put the biolink type order back, you can generate it with:
            #   grouped_by_conflation_type = sorted(conflation_ids_by_type.items(), key=lambda bt: preferred_conflation_type_order.get(bt[0], 100))
            # If you do that, please remember to sort these identifiers in the prefix order for that type,
            # which I forgot to do in the previous implementation!

            for prefix, ids in sorted(
                conflation_ids_by_prefix.items(), key=lambda bt: conflation_prefix_sort_order.get(bt[0], 100)
            ):
                # Is this Biolink type a chemical type? If not, ignore it.
                # if biolink_type not in biolink_chemical_types:
                #     logger.warning(f"Skipping Biolink type {biolink_type} because it's not a chemical type, with IDs: {ids}")
                #     continue

                # To sort the identifiers, we'll need to calculate a tuple for each identifier to sort on.
                sorted_ids = {}
                for curie in ids:
                    clique_for_id = clique_for_preferred_curie[curie]

                    # Criteria 1: the information content of the clique represented by this identifier (lowest -> highest).
                    clique_ic = ic_factory.get_ic(
                        {"identifiers": list(map(lambda c: {"identifier": c}, clique_for_id))}
                    )
                    clique_ics.append(clique_ic)
                    if clique_ic is None:
                        clique_ic = 100.0

                    # Criteria 2: the size of the clique represented by this identifier (highest -> lowest)
                    clique_size = len(clique_for_id)

                    # Criteria 3: the numerical suffix of the identifier (lowest -> highest)
                    numerical_suffix = get_numerical_curie_suffix(curie)
                    if numerical_suffix is None:
                        numerical_suffix = sys.maxsize

                    # Put all that information into a tuple for sorting.
                    sorted_ids[curie] = (
                        clique_ic,  # clique_ic (smallest -> largest)
                        -clique_size,  # clique_size DESC (largest -> smallest)
                        numerical_suffix,  # numerical_suffix ASC (smallest -> largest)
                    )

                sorted_ids = sorted(ids, key=sorted_ids.get)
                final_conflation_id_list.extend(sorted_ids)

            # The final conflation list won't match the initial list only if some of the Biolink types weren't
            # chemical types, and so were skipped that way.
            if set(final_conflation_id_list) != set(normalized_conflation_id_list):
                logger.warning(
                    "Final conflation ID list does not match the normalized conflation ID list:\n"
                    + f" - Final conflation ID list: {sorted(final_conflation_id_list)}\n"
                    + f" - Normalized conflation ID list: {sorted(normalized_conflation_id_list)}"
                )

            # Write out all the identifiers.
            logger.info(f"Ordered DrugChemical conflation {final_conflation_id_list} with IC values {clique_ics}.")
            outf.write(final_conflation_id_list)
            written.add(fs)

    # Write out metadata.yaml
    write_combined_metadata(
        output_metadata_yaml,
        typ="conflation",
        name="drugchemical.build_conflation()",
        description="Build DrugChemical conflation.",
        combined_from_filenames=input_metadata_yamls,
        also_combined_from={
            "Manual": {
                "name": "DrugChemical Manual",
                "filename": manual_concord_filename,
                "counts": {
                    "count_concords": len(manual_concords),
                    "count_concords_skipped": manual_concords_skipped,
                    "count_concords_applied": len(manual_concords) - manual_concords_skipped,
                    "count_distinct_curies": len(manual_concords_curies),
                    "count_distinct_curies_applied": len(manual_concords_applied_curies),
                    "predicates": dict(manual_concords_predicate_counts),
                    "prefix_counts": dict(manual_concords_curie_prefix_counts),
                },
            }
        },
    )


def sort_by_curie_suffix(curie):
    """
    Sort function to sort by curie suffix. We can't just use get_curie_suffix() because it returns None for CURIEs
    without a suffix. However, we can return a tuple with either a True or False as the first value to sort Nones
    after non-None values. As suggested by https://stackoverflow.com/a/72138073/27310

    :param curie: The CURIE to sort.
    :return: A tuple of either (False, None) if the CURIE doesn't have a numerical suffix or (True, suffix) if it does.
    """
    suffix = get_numerical_curie_suffix(curie)
    return suffix is None, suffix
