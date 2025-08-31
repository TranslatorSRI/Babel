import csv
import sys
import time

import jsonlines
from humanfriendly import format_timespan

from src.metadata.provenance import write_combined_metadata, write_concord_metadata
from src.node import NodeFactory, InformationContentFactory
from src.prefixes import RXCUI, PUBCHEMCOMPOUND, UMLS
from src.categories import (CHEMICAL_ENTITY, DRUG, MOLECULAR_MIXTURE, FOOD, COMPLEX_MOLECULAR_MIXTURE,
                            SMALL_MOLECULE, NUCLEIC_ACID_ENTITY, MOLECULAR_ENTITY, FOOD_ADDITIVE,
                            ENVIRONMENTAL_FOOD_CONTAMINANT, PROCESSED_MATERIAL, CHEMICAL_MIXTURE, POLYPEPTIDE)
from src.babel_utils import glom, get_numerical_curie_suffix
from collections import defaultdict
import os,json

import logging
from src.util import LoggingUtil, get_config, get_memory_usage_summary

logger = LoggingUtil.init_logging(__name__, level=logging.INFO)

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
"has_active_ingredient"]

def get_aui_to_cui(consofile):
    """Get a mapping from AUI to CUI"""
    aui_to_cui = {}
    sdui_to_cui = defaultdict(set)
    # consofile = os.path.join('input_data', 'private', "RXNCONSO.RRF")
    with open(consofile, 'r') as inf:
        for line in inf:
            x = line.strip().split('|')
            aui = x[7]
            cui = x[0]
            sdui = (x[11],x[7])
            if aui in aui_to_cui:
                print("What the all time fuck?")
                print(aui,cui)
                print(aui_to_cui[aui])
                raise RuntimeError("Something has gone very wrong")
            aui_to_cui[aui] = cui
            if sdui[1]=="":
                continue
            sdui_to_cui[sdui].add(cui)
    return aui_to_cui, sdui_to_cui

def get_cui(x,indicator_column,cui_column,aui_column,aui_to_cui,sdui_to_cui):
    relation_column = 7
    source_column = 10
    if x[relation_column] in useful_relationships:
        if x[indicator_column] == "CUI":
            return x[cui_column]
        elif x[indicator_column] == "AUI":
            try:
                return aui_to_cui[x[aui_column]]
            except:
                #this really shouldn't happen.  But it seems to occur for the UMLS files?
                return None
        elif x[indicator_column] == "SDUI":
            cuis = sdui_to_cui[(x[source_column],x[aui_column])]
            if len(cuis) == 1:
                return list(cuis)[0]
            print("sdui garbage hell")
            print(x)
            print(cuis)
            raise RuntimeError("Something has gone very wrong with SDUI")
        elif x[indicator_column] == "SCUI":
            #SCUI is source cui, i.e. what the source calls it.  We might be able to pull this out of CONSO if we have to.
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
    #This is maybe relying on convention a bit too much.
    if outfile == "UMLS":
        prefix = UMLS
        sources = [
            {
                'type': 'UMLS',
                'name': 'MRCONSO',
                'filename': conso
            },
            {
                'type': 'UMLS',
                'name': 'MRREL',
                'filename': relfile
            }
        ]
    else:
        prefix = RXCUI
        sources = [
            {
                'type': 'RXNORM',
                'name': 'RXNCONSO',
                'filename': conso
            },
            {
                'type': 'RXNOM',
                'name': 'RXNREL',
                'filename': relfile
            }
        ]
    aui_to_cui, sdui_to_cui = get_aui_to_cui(conso)
    # relfile = os.path.join('input_data', 'private', "RXNREL.RRF")
    single_use_relations = {"has_active_ingredient": defaultdict(set),
                            "has_precise_active_ingredient": defaultdict(set),
                            "has_precise_ingredient": defaultdict(set),
                            "has_ingredient": defaultdict(set),
                            "tradename_of": defaultdict(set),
                            "consists_of": defaultdict(set)}
    one_to_one_relations = {}
    #one_to_one_relations = {"has_tradename": {"subject": defaultdict(set),
    #                                          "object": defaultdict(set)}}
    with open(relfile, 'r') as inf, open(outfile, 'w') as outf:
        for line in inf:
            x = line.strip().split('|')
            #UMLS always has the CUI in it, while RXNORM does not.
            if outfile == "UMLS":
                object = x[0]
                subject = x[4]
            else:
                object = get_cui(x,2,0,1,aui_to_cui,sdui_to_cui)
                subject = get_cui(x,6,4,5,aui_to_cui,sdui_to_cui)
            if (subject is not None) and (object is not None):
                if subject == object:
                    continue
                predicate = x[7]
                if predicate in single_use_relations:
                    single_use_relations[predicate][subject].add(object)
                elif predicate in one_to_one_relations:
                    one_to_one_relations[predicate]["subject"][subject].add(object)
                    one_to_one_relations[predicate]["object"][object].add(subject)
                else:
                    outf.write(f"{prefix}:{subject}\t{predicate}\t{prefix}:{object}\n")
        for predicate in single_use_relations:
            for subject,objects in single_use_relations[predicate].items():
                if len(objects) > 1:
                    continue
                outf.write(f"{prefix}:{subject}\t{predicate}\t{prefix}:{next(iter(objects))}\n")
        for predicate in one_to_one_relations:
            for subject,objects in one_to_one_relations[predicate]["subject"].items():
                if len(objects) > 1:
                    continue
                if len(one_to_one_relations[predicate]["object"][next(iter(objects))]) > 1:
                    continue
                outf.write(f"{prefix}:{subject}\t{predicate}\t{prefix}:{next(iter(objects))}\n")

    write_concord_metadata(
        metadata_yaml,
        name='build_rxnorm_relationships()',
        description=f'Builds relationships between RxCUI and other identifiers from a CONSO ({conso}) and a REL ({relfile}).',
        sources=sources,
        concord_filename=outfile,
    )


def load_cliques(compendium):
    rx_to_clique = {}
    with open(compendium,"r") as infile:
        for line in infile:
            if RXCUI not in line:
                continue
            j = json.loads(line)
            clique = j["identifiers"][0]["i"]
            for terms in j["identifiers"]:
               if terms["i"].startswith(RXCUI):
                   rx_to_clique[terms["i"]] = clique
    return rx_to_clique

def build_pubchem_relationships(infile,outfile, metadata_yaml):
    with open(infile,"r") as inf:
        document = json.load(inf)
    with open(outfile,"w") as outf:
        for annotation in document["Annotations"]["Annotation"]:
            rxnid = annotation["SourceID"]
            cids = annotation.get("LinkedRecords",{}).get("CID",[])
            for cid in cids:
                outf.write(f"{RXCUI}:{rxnid}\tlinked\t{PUBCHEMCOMPOUND}:{cid}\n")

    write_concord_metadata(
        metadata_yaml,
        name='build_pubchem_relationships()',
        description=f'Builds relationships between RxCUI and PubChem Compound identifiers from a PubChem annotations file ({infile}.',
        sources=[{
            'type': 'PubChem',
            'name': 'PubChem RxNorm annotations',
            'description': 'PubChem RxNorm mappings generated by pubchem.pull_rxnorm_annotations()',
            'filename': infile
        }],
        concord_filename=outfile,
    )

def build_conflation(manual_concord_filename, rxn_concord, umls_concord, pubchem_rxn_concord, drug_compendium, chemical_compendia, icrdf_filename, outfilename, input_metadata_yamls, output_metadata_yaml):
    """RXN_concord contains relationshps between rxcuis that can be used to conflate
    Now we don't want all of them.  We want the ones that are between drugs and chemicals,
    and the ones between drugs and drugs.
    To determine which those are, we're going to have to dig around in all the compendia.
    We also want to get all the clique leaders as well.  For those, we only need to worry if there are RXCUIs
    in the clique."""

    logger.info("Loading information content values...")
    ic_factory = InformationContentFactory(icrdf_filename)

    logger.info("Loading manual concords ...")
    manual_concords = []
    manual_concords_curies = set()
    manual_concords_predicate_counts = defaultdict(int)
    manual_concords_curie_prefix_counts = defaultdict(int)
    with open(manual_concord_filename,"r") as manualf:
        csv_reader = csv.DictReader(manualf, dialect=csv.excel_tab)
        for row in csv_reader:
            # We're only interested in two fields, so you can add additional files ('comment', 'notes', etc.) as needed.
            if 'subject' not in row or 'object' not in row:
                raise RuntimeError(f"Missing subject or object fields in {manual_concord_filename}: {row}")
            if row['subject'].strip() == '' or row['object'].strip() == '':
                raise RuntimeError(f"Empty subject or object fields in {manual_concord_filename}: {row}")
            manual_concords.append((row['subject'], row['object']))
            manual_concords_predicate_counts[row['predicate']] += 1
            manual_concords_curies.add(row['subject'])
            manual_concords_curies.add(row['object'])

            sorted_curies = sorted([row['subject'], row['object']])
            prefix_count_label = row['predicate'] + '(' + (' ,'.join(sorted_curies)) + ')'
            manual_concords_curie_prefix_counts[prefix_count_label] += 1
    logger.info(f"{len(manual_concords)} manual concords loaded.")

    logger.info("load all chemical conflations so we can normalize identifiers")
    preferred_curie_for_curie = {}
    type_for_preferred_curie = {}
    clique_for_preferred_curie = {}
    for chemical_compendium in chemical_compendia:
        with open(chemical_compendium, 'r') as compendiumf:
            logger.info(f"Loading {chemical_compendium}: {get_memory_usage_summary()}")
            for line in compendiumf:
                clique = json.loads(line)
                preferred_id = clique['identifiers'][0]['i']
                clique_for_preferred_curie[preferred_id] = list(map(lambda ident: ident['i'], clique['identifiers']))
                type_for_preferred_curie[preferred_id] = clique['type']
                for ident in clique['identifiers']:
                    id = ident['i']
                    preferred_curie_for_curie[id] = preferred_id

    logger.info(f"Loaded preferred CURIEs for {len(preferred_curie_for_curie)} CURIEs from the chemical compendia: {get_memory_usage_summary()}")

    logger.info("load drugs")
    drug_rxcui_to_clique = load_cliques(drug_compendium)
    chemical_rxcui_to_clique = {}
    for chemical_compendium in chemical_compendia:
        if chemical_compendium == drug_compendium:
            continue
        logger.info(f"load {chemical_compendium}: {get_memory_usage_summary()}")
        chemical_rxcui_to_clique.update(load_cliques(chemical_compendium))

    pairs = []
    for concfile in [rxn_concord,umls_concord]:
        with open(concfile,"r") as infile:
            for line in infile:
                x = line.strip().split('\t')
                subject = x[0]
                object = x[2]

                # While we do this, we will also normalize all chemicals to their preferred clique IDs.
                if subject in drug_rxcui_to_clique and object in chemical_rxcui_to_clique:
                    subject = drug_rxcui_to_clique[subject]
                    object = chemical_rxcui_to_clique[object]
                    pairs.append( (subject,object) )
                elif subject in chemical_rxcui_to_clique and object in drug_rxcui_to_clique:
                    subject = chemical_rxcui_to_clique[subject]
                    object = drug_rxcui_to_clique[object]
                    pairs.append( (subject,object) )
                # OK, this is possible, and it's OK, as long as we get real clique leaders
                elif subject in drug_rxcui_to_clique and object in drug_rxcui_to_clique:
                    subject = drug_rxcui_to_clique[subject]
                    object = drug_rxcui_to_clique[object]
                    pairs.append( (subject,object) )
                elif subject in chemical_rxcui_to_clique and object in chemical_rxcui_to_clique:
                    subject = chemical_rxcui_to_clique[subject]
                    object = chemical_rxcui_to_clique[object]
                    pairs.append( (subject,object) )
    with open(pubchem_rxn_concord,"r") as infile:
        for line in infile:
            x = line.strip().split('\t')
            subject = x[0]
            object = x[2]

            if subject in drug_rxcui_to_clique:
                subject = drug_rxcui_to_clique[subject]
            elif subject in chemical_rxcui_to_clique:
                subject = chemical_rxcui_to_clique[subject]
            else:
                logger.warning(
                    f"Subject in subject-object pair ({subject}, {object}) isn't mapped to a RxCUI, skipping."
                )
                continue
                # raise RuntimeError(f"Unknown identifier in drugchemical conflation as subject: {subject}")

            if object in drug_rxcui_to_clique:
                object = drug_rxcui_to_clique[object]
            elif object in chemical_rxcui_to_clique:
                object = chemical_rxcui_to_clique[object]
            else:
                logger.warning(
                    f"Object in subject-object pair ({subject}, {object}) isn't mapped to a RxCUI"
                )
                # raise RuntimeError(f"Unknown identifier in drugchemical conflation as object: {object}")

            pairs.append((subject, object))

    # Normalize the pairs to be glommed. We need to do this here because it may be that multiple conflations will be
    # merged together because they share a normalized identifier. We can do this by adding pairs to indicate that every
    # subject and object is associated with its normalized identifier.
    pairs_to_be_glommed = []
    pairs.extend(manual_concords)
    for (subj, obj) in pairs:
        # If either the subject or the object cannot be normalized, skip this pair entirely.
        #
        # This appears to happen very rarely when we have a PUBCHEM.COMPOUND that is referenced from RxNorm but
        # hasn't made it into wherever we get PUBCHEM.COMPOUND IDs from. Not super-surprising since RxNorm is
        # updated every month, but still, it's only happened to be once that I've noticed.
        if subj not in preferred_curie_for_curie:
            logger.warning(f"Pair ({subj}, {obj}) has a subject that cannot be normalized, skipping pair.")
            continue

        if obj not in preferred_curie_for_curie:
            logger.warning(f"Pair ({subj}, {obj}) has an object that cannot be normalized, skipping pair.")
            continue

        # Add this tuple to the pairs to be glommed.
        pairs_to_be_glommed.append((subj, obj))

        # If the subject is not normalized, add a pair indicating the normalized ID.
        if preferred_curie_for_curie[subj] != subj:
            pairs_to_be_glommed.append((subj, preferred_curie_for_curie[subj]))
        # If the object is not normalized, add a pair indicating the normalized ID.
        if preferred_curie_for_curie[obj] != obj:
            pairs_to_be_glommed.append((obj, preferred_curie_for_curie[obj]))

    # Glommin' time
    logger.info(f"glom: {get_memory_usage_summary()}")
    gloms = {}
    glom(gloms, pairs_to_be_glommed)

    # Set up a NodeFactory.
    nodefactory = NodeFactory('', get_config()['biolink_version'])

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
            if (clique_count == 1) or (clique_count % 1000 == 0):
                time_elapsed_seconds = (time.time_ns() - start_time) / 1E9
                if time_elapsed_seconds < 0.001:
                    # We don't want to divide by zero.
                    time_elapsed_seconds = 0.001
                remaining_cliques = total_clique_count - clique_count
                logger.info(f"Generating DrugChemical conflations currently at {clique_count:,} out of {total_clique_count:,} ({clique_count/total_clique_count*100:.2f}%) in {format_timespan(time_elapsed_seconds)}: {get_memory_usage_summary()}")
                logger.info(f" - Current rate: {clique_count/time_elapsed_seconds:.2f} cliques/second or {time_elapsed_seconds/clique_count:.6f} seconds/clique.")

                time_remaining_seconds = (time_elapsed_seconds / clique_count * remaining_cliques)
                logger.info(f" - Estimated time remaining: {format_timespan(time_remaining_seconds)}")

            # 1. Prepare a list of identifiers so we can iterate over them.
            fs = frozenset(clique)
            if fs in written:
                continue
            conflation_id_list = list(clique)

            # 2. Group identifiers by Biolink type, preserving the order of the clique members.
            conflation_ids_by_type = defaultdict(list)
            normalized_conflation_id_list = list()
            for iid in conflation_id_list:
                # Normalization shouldn't be needed here, because they're all clique leaders, but just in case.
                preferred_curie = preferred_curie_for_curie[iid]
                if preferred_curie != iid:
                    logger.warning(f"Conflation leader {iid} should have been normalized to {preferred_curie}, normalizing now.")
                if preferred_curie not in normalized_conflation_id_list:
                    normalized_conflation_id_list.append(preferred_curie)

                # Add it to the dictionary of types in the order of the clique members.
                # At the moment, we get these from glomming, so the order should not actually be significant.
                # But maybe in the future it will be if that changes? And it doesn't cost us much to maintain
                # insertion order.
                conflation_ids_by_type[type_for_preferred_curie[preferred_curie]].append(preferred_curie)

            # 3. There's a particular order we'd like to arrange the conflation in.
            # I've also listed the number of entities as of 2024mar24 to give an idea of how common these are.
            PREFERRED_CONFLATION_TYPE_ORDER = {
                SMALL_MOLECULE: 1,                      # 107,459,280 cliques
                POLYPEPTIDE: 2,                         # 622 cliques
                NUCLEIC_ACID_ENTITY: 3,                 # N/A
                MOLECULAR_ENTITY: 4,                    # N/A
                COMPLEX_MOLECULAR_MIXTURE: 5,           # 177 cliques
                CHEMICAL_MIXTURE: 6,                    # 498 cliques
                MOLECULAR_MIXTURE: 7,                   # 10,371,847 cliques
                PROCESSED_MATERIAL: 8,                  # N/A
                DRUG: 9,                                # 145,677 cliques
                FOOD_ADDITIVE: 10,                      # N/A
                FOOD: 11,                               # N/A
                ENVIRONMENTAL_FOOD_CONTAMINANT: 12,     # N/A
                CHEMICAL_ENTITY: 13,                    # 7,398,124 cliques
            }

            # Within each of those classes, we want to sort by:
            #   - information_content (lowest to highest, so that more general concepts are front-loaded)
            #   - clique size (largest to smallest, so that larger cliques are front-loaded)
            #   - numerical suffix (lowest to highest)
            # Note that this does NOT include prefix order for the Biolink type. I think mixing that with multiple
            # Biolink types will just make the output lists more confusing. Most people will only care about the
            # clique conflation leader.
            final_conflation_id_list = []
            for biolink_type, ids in sorted(conflation_ids_by_type.items(), key=lambda x: PREFERRED_CONFLATION_TYPE_ORDER.get(x[0], 100)):
                # To sort the identifiers, we'll need to calculate a tuple for each identifier to sort on.
                sorted_ids = {}
                for curie in ids:
                    clique_for_id = clique_for_preferred_curie[curie]

                    # Criteria 1: the information content of the clique represented by this identifier (lowest -> highest).
                    clique_ic = ic_factory.get_ic({
                        'identifiers': list(map(lambda c: {'identifier': c}, clique_for_id))
                    })
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
                        clique_ic,                  # clique_ic (smallest -> largest)
                        -clique_size,               # clique_size DESC (largest -> smallest)
                        numerical_suffix            # numerical_suffix ASC (smallest -> largest)
                    )

                sorted_ids = sorted(ids, key=sorted_ids.get)
                final_conflation_id_list.extend(sorted_ids)

            # This should account for every type (including the ones not included in the PREFERRED_CONFLATION_TYPE_ORDER),
            # but just out of paranoia, we'll double-check that here.
            assert set(final_conflation_id_list) == set(normalized_conflation_id_list)

            # Write out all the identifiers.
            logger.info(f"Ordered DrugChemical conflation {final_conflation_id_list}")
            outf.write(final_conflation_id_list)
            written.add(fs)

    # Write out metadata.yaml
    write_combined_metadata(
        output_metadata_yaml,
        typ='conflation',
        name='drugchemical.build_conflation()',
        description='Build DrugChemical conflation.',
        combined_from_filenames=input_metadata_yamls,
        also_combined_from={
            'Manual': {
                'name': 'DrugChemical Manual',
                'filename': manual_concord_filename,
                'counts': {
                    'count_concords': len(manual_concords),
                    'count_distinct_curies': len(manual_concords_curies),
                    'predicates': dict(manual_concords_predicate_counts),
                    'prefix_counts': dict(manual_concords_curie_prefix_counts),
                }
            }
        }
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
