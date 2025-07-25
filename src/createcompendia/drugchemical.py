import csv

from src.node import NodeFactory, InformationContentFactory
from src.prefixes import RXCUI, PUBCHEMCOMPOUND, UMLS
from src.categories import (CHEMICAL_ENTITY, DRUG, MOLECULAR_MIXTURE, FOOD, COMPLEX_MOLECULAR_MIXTURE,
                            SMALL_MOLECULE, NUCLEIC_ACID_ENTITY, MOLECULAR_ENTITY, FOOD_ADDITIVE,
                            ENVIRONMENTAL_FOOD_CONTAMINANT, PROCESSED_MATERIAL, CHEMICAL_MIXTURE, POLYPEPTIDE)
from src.babel_utils import glom, get_numerical_curie_suffix
from collections import defaultdict
import os,json

import logging
from src.util import LoggingUtil, get_config

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
                exit()
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
            exit()
        elif x[indicator_column] == "SCUI":
            #SCUI is source cui, i.e. what the source calls it.  We might be able to pull this out of CONSO if we have to.
            return None
        print("cmon man")
        print(x)
        exit()

def build_rxnorm_relationships(conso, relfile, outfile):
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
    else:
        prefix = RXCUI
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

def build_pubchem_relationships(infile,outfile):
    with open(infile,"r") as inf:
        document = json.load(inf)
    with open(outfile,"w") as outf:
        for annotation in document["Annotations"]["Annotation"]:
            rxnid = annotation["SourceID"]
            cids = annotation.get("LinkedRecords",{}).get("CID",[])
            for cid in cids:
                outf.write(f"{RXCUI}:{rxnid}\tlinked\t{PUBCHEMCOMPOUND}:{cid}\n")

def build_conflation(manual_concord_filename, rxn_concord, umls_concord, pubchem_rxn_concord, drug_compendium, chemical_compendia, icrdf_filename, outfilename):
    """RXN_concord contains relationshps between rxcuis that can be used to conflate
    Now we don't want all of them.  We want the ones that are between drugs and chemicals,
    and the ones between drugs and drugs.
    To determine which those are, we're going to have to dig around in all the compendia.
    We also want to get all the clique leaders as well.  For those, we only need to worry if there are RXCUIs
    in the clique."""

    print("Loading information content values...")
    ic_factory = InformationContentFactory(icrdf_filename)

    print("Loading manual concords ...")
    manual_concords = []
    with open(manual_concord_filename,"r") as manualf:
        csv_reader = csv.DictReader(manualf, dialect=csv.excel_tab)
        for row in csv_reader:
            # We're only interested in two fields, so you can add additional files ('comment', 'notes', etc.) as needed.
            if 'subject' not in row or 'object' not in row:
                raise RuntimeError(f"Missing subject or object fields in {manual_concord_filename}: {row}")
            if row['subject'].strip() == '' or row['object'].strip() == '':
                raise RuntimeError(f"Empty subject or object fields in {manual_concord_filename}: {row}")
            manual_concords.append((row['subject'], row['object']))
    print(f"{len(manual_concords)} manual concords loaded.")

    print("load all chemical conflations so we can normalize identifiers")
    preferred_curie_for_curie = {}
    type_for_preferred_curie = {}
    clique_for_preferred_curie = {}
    for chemical_compendium in chemical_compendia:
        with open(chemical_compendium, 'r') as compendiumf:
            logger.info(f"Loading {chemical_compendium}")
            for line in compendiumf:
                clique = json.loads(line)
                preferred_id = clique['identifiers'][0]['i']
                clique_for_preferred_curie[preferred_id] = list(map(lambda ident: ident['i'], clique['identifiers']))
                type_for_preferred_curie[preferred_id] = clique['type']
                for ident in clique['identifiers']:
                    id = ident['i']
                    preferred_curie_for_curie[id] = preferred_id

    print(f"Loaded preferred CURIEs for {len(preferred_curie_for_curie)} CURIEs from the chemical compendia.")

    print("load drugs")
    drug_rxcui_to_clique = load_cliques(drug_compendium)
    chemical_rxcui_to_clique = {}
    for chemical_compendium in chemical_compendia:
        if chemical_compendium == drug_compendium:
            continue
        print(f"load {chemical_compendium}")
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
    print("glom")
    gloms = {}
    glom(gloms, pairs_to_be_glommed)

    # Set up a NodeFactory.
    nodefactory = NodeFactory('', get_config()['biolink_version'])

    # Write out all the resulting cliques.
    written = set()
    with open(outfilename,"w") as outfile:
        for clique_member,clique in gloms.items():
            fs = frozenset(clique)
            if fs in written:
                continue
            conflation_id_list = list(clique)

            # Now we need to figure out the type of this conflation. One possibility would be to use the
            # clique size (number of IDs in each clique) to determine this, but this approach might fail
            # if a conflation has one oversized clique that pulls us away from the right path. Instead,
            # we determine a preference order of Biolink types and follow that to choose a type for each
            # conflation.
            #
            # To do this is a two-step process:
            # 1. Figure out all the possible types (of the remaining IDs).
            conflation_possible_types = map(
                lambda id: type_for_preferred_curie[preferred_curie_for_curie[id]],
                conflation_id_list
            )
            # 2. Sort possible types in our preferred order of types.
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
            sorted_possible_types = sorted(conflation_possible_types,
                                           key=lambda typ: PREFERRED_CONFLATION_TYPE_ORDER.get(typ, 100))
            if len(sorted_possible_types) > 0:
                conflation_type = sorted_possible_types[0]
            else:
                logger.warning(f"Could not determine type for {conflation_id_list} with " +
                               f"conflation possible types: {conflation_possible_types}, defaulting to {CHEMICAL_ENTITY}.")
                conflation_type = CHEMICAL_ENTITY

            # Determine the prefixes to be used for this conflation list based on the prefixes from the NodeFactory
            # (which gets them from Biolink Model).
            prefixes_for_type = nodefactory.get_prefixes(conflation_type)
            logger.info(f"Conflation {conflation_id_list} determined to have conflation type {conflation_type} " +
                        f"with prefixes: {prefixes_for_type}")

            # Normalize all the identifiers. Any IDs that couldn't be normalized will show up as None.
            normalized_conflation_id_list = [preferred_curie_for_curie.get(id) for id in conflation_id_list]

            # Turn the conflation CURIE list into a prefix map, which maps prefixes to lists of CURIEs.
            # This allows us to sort each prefix separately.
            # Skip CURIE prefixes that aren't good conflation list leaders and ignore duplicates.
            prefix_map = defaultdict(list)
            ids_already_added = set()
            for index, curie in enumerate(normalized_conflation_id_list):
                # Remove Nones, which are IDs that could not be normalized.
                if curie is None:
                    logger.warning(f"Could not normalize CURIE {conflation_id_list[index]} in conflation {conflation_id_list}, skipping.")
                    continue

                # Remove duplicates
                if curie in ids_already_added:
                    continue

                # Group by prefix.
                curie_prefix = curie.split(':')[0]
                if curie_prefix == RXCUI:
                    # Drug has RXCUI rated highly as a prefix, but that's not a good ID for Babel, so let's skip
                    # this for now.
                    continue

                if curie_prefix == UMLS:
                    # UMLS is a particularly bad identifier for us because we tend not to conflate on it, so let's
                    # skip this for now.
                    continue

                prefix_map[curie_prefix].append(curie)
                ids_already_added.add(curie)

            # Produce a final conflation list in the prefix order specified for the type of the conflation leader.
            final_conflation_id_list = []
            ids_already_added = set()
            for prefix in prefixes_for_type:
                if prefix in prefix_map:
                    ids_to_add = []
                    for id in prefix_map[prefix]:
                        ids_already_added.add(id)
                        ids_to_add.append(id)

                    # Sort this set of CURIEs from the numerically smallest CURIE suffix to the largest, with
                    # non-numerical CURIE suffixes sorted to the end.
                    final_conflation_id_list.extend(list(sorted(ids_to_add, key=sort_by_curie_suffix)))

            # Add any identifiers that weren't in the prefix_map in the original order (which is not significant).
            ids_to_add = []
            for id in normalized_conflation_id_list:
                if id not in ids_already_added:
                    ids_to_add.append(id)

            # Sort this final set of CURIEs from the numerically smallest CURIE suffix to the largest, with
            # non-numerical CURIE suffixes sorted to the end.
            final_conflation_id_list.extend(list(sorted(ids_to_add, key=sort_by_curie_suffix)))

            # At this point, final_conflation_id_list is a list of all the identifiers for this conflation
            # arranged in two ways:
            #   - This is sorted by prefix in the prefix order specified for the type we've come up with for this
            #     conflation (conflation_type).
            #   - Within each prefix, we've sorted identifiers by CURIE suffix, so that the smallest identifier goes
            #     first.
            # This generally gives us the right identifier for the conflation, but there are a few cases where we can
            # improve this:
            #   - We might end up with a conflation clique leader that's not the right type.
            #   - We might end up with a conflation clique leader that's a more complex chemical than the simplest
            #     one (e.g. the conflated clique for CHEBI:45783 "imanitib" is currently lead by
            #     CHEBI:31690 "imatinib methanesulfonate", just because it's numerically smaller).
            #     - See https://github.com/TranslatorSRI/Babel/issues/341 for examples.
            #   - We might end up with a conflation clique leader that has a higher information content
            # To work around this, we take this chance to pick an alternate conflation clique leader.
            conflation_clique_leader = final_conflation_id_list[0]
            conflation_clique_leader_prefix = conflation_clique_leader.split(':')[0]
            conflation_clique_leader_ic = ic_factory.get_ic({
                'identifiers': list(map(lambda curie: {'identifier': curie}, clique_for_preferred_curie[conflation_clique_leader]))
            })
            if conflation_clique_leader_ic is None:
                conflation_clique_leader_ic = float(100.0)
            else:
                conflation_clique_leader_ic = float(conflation_clique_leader_ic)

            for curie in final_conflation_id_list:
                curie_prefix = curie.split(':')[0]
                if curie_prefix != conflation_clique_leader_prefix:
                    # Let's stick will the same prefix as the first entry.
                    continue

                # Note that this works because curie is always a clique leader here.
                curie_type = type_for_preferred_curie[curie]
                if curie_type != conflation_type:
                    # Only consider clique leaders that are of the calculated type.
                    continue

                # Is this a lower information content value? If so, prefer this CURIE.
                curie_ic = ic_factory.get_ic({
                    'identifiers': list(map(lambda curie: {'identifier': curie}, clique_for_preferred_curie[curie]))
                })
                if curie_ic is not None and float(curie_ic) < float(conflation_clique_leader_ic):
                    logging.info(f"Found better IC with CURIE {curie} (IC {curie_ic}) than previous conflation clique "
                                 f"leader {final_conflation_id_list[0]} (IC {conflation_clique_leader_ic}).")
                    conflation_clique_leader = curie
                    conflation_clique_leader_ic = float(curie_ic)

                # Is this a shorter label? If so, we would like to prefer this
                # CURIE, but loading all the labels into memory would take a
                # lot of memory. So let's see how good we can do with just the
                # information content values.

            # If we've picked a new clique leader, move it to the front of the list.
            if conflation_clique_leader != final_conflation_id_list[0]:
                logging.info(f"Replacing conflation clique leader {final_conflation_id_list[0]} with improved "
                             f"conflation clique leader {conflation_clique_leader}")
                final_conflation_id_list.remove(conflation_clique_leader)
                final_conflation_id_list.insert(0, conflation_clique_leader)

                # Write out all the identifiers.
            logger.info(f"Ordered DrugChemical conflation {final_conflation_id_list}")

            outfile.write(f"{json.dumps(final_conflation_id_list)}\n")
            written.add(fs)


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
