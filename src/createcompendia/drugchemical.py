from src.node import NodeFactory, get_config
from src.prefixes import RXCUI, PUBCHEMCOMPOUND, CHEMBLCOMPOUND, UNII, DRUGBANK, MESH, UMLS, CHEBI
from src.babel_utils import glom
from collections import defaultdict
import os,json

import logging
from src.util import LoggingUtil
logger = LoggingUtil.init_logging(__name__, level=logging.ERROR)

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
"has_tradename",
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
                            "consists_of": defaultdict(set)}
    one_to_one_relations = {"has_tradename": {"subject": defaultdict(set),
                                              "object": defaultdict(set)}}
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

def build_conflation(rxn_concord,umls_concord,pubchem_rxn_concord,drug_compendium,chemical_compendia,outfilename):
    """RXN_concord contains relationshps between rxcuis that can be used to conflate
    Now we don't want all of them.  We want the ones that are between drugs and chemicals,
    and the ones between drugs and drugs.
    To determine which those are, we're going to have to dig around in all the compendia.
    We also want to get all the clique leaders as well.  For those, we only need to worry if there are RXCUIs
    in the clique."""

    print("load all chemical conflations so we can normalize identifiers")
    preferred_curie_for_curie = {}
    type_for_preferred_curie = {}
    for chemical_compendium in chemical_compendia:
        with open(chemical_compendium, 'r') as compendiumf:
            for line in compendiumf:
                clique = json.loads(line)
                preferred_id = clique['identifiers'][0]['i']
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
                    subject = preferred_curie_for_curie[drug_rxcui_to_clique[subject]]
                    object = preferred_curie_for_curie[chemical_rxcui_to_clique[object]]
                    pairs.append( (subject,object) )
                elif subject in chemical_rxcui_to_clique and object in drug_rxcui_to_clique:
                    subject = preferred_curie_for_curie[chemical_rxcui_to_clique[subject]]
                    object = preferred_curie_for_curie[drug_rxcui_to_clique[object]]
                    pairs.append( (subject,object) )
                # OK, this is possible, and it's OK, as long as we get real clique leaders
                elif subject in drug_rxcui_to_clique and object in drug_rxcui_to_clique:
                    subject = preferred_curie_for_curie[drug_rxcui_to_clique[subject]]
                    object = preferred_curie_for_curie[drug_rxcui_to_clique[object]]
                    pairs.append( (subject,object) )
                elif subject in chemical_rxcui_to_clique and object in chemical_rxcui_to_clique:
                    subject = preferred_curie_for_curie[chemical_rxcui_to_clique[subject]]
                    object = preferred_curie_for_curie[chemical_rxcui_to_clique[object]]
                    pairs.append( (subject,object) )
    with open(pubchem_rxn_concord,"r") as infile:
        for line in infile:
            x = line.strip().split('\t')
            subject = x[0]
            object = x[2]

            if subject in drug_rxcui_to_clique:
                subject = preferred_curie_for_curie[drug_rxcui_to_clique[subject]]
            elif subject in chemical_rxcui_to_clique:
                subject = preferred_curie_for_curie[chemical_rxcui_to_clique[subject]]
            else:
                raise RuntimeError(f"Unknown identifier in drugchemical conflation as subject: {subject}")

            if object in drug_rxcui_to_clique:
                object = preferred_curie_for_curie[drug_rxcui_to_clique[object]]
            elif object in chemical_rxcui_to_clique:
                object = preferred_curie_for_curie[chemical_rxcui_to_clique[object]]
            else:
                logging.warning(
                    f"Object in subject-object pair ({subject}, {object}) isn't mapped to a RxCUI"
                )
                # raise RuntimeError(f"Unknown identifier in drugchemical conflation as object: {object}")

            pairs.append((subject, object))

    # Set up a NodeFactory.
    nodefactory = NodeFactory('', get_config()['biolink_version'])

    # Glommin' time
    print("glom")
    gloms = {}
    glom(gloms,pairs)
    written = set()
    with open(outfilename,"w") as outfile:
        for clique_member,clique in gloms.items():
            fs = frozenset(clique)
            if fs in written:
                continue
            conflation_id_list = list(clique)

            # Turn the conflation CURIE list into a prefix map.
            prefix_map = defaultdict(list)
            for curie in conflation_id_list:
                curie_prefix = curie.split(':')[0].upper()
                prefix_map[curie_prefix].append(curie_prefix)

            # Go through the prefixes for this type, and use it to order the identifiers in this conflation ID list.
            final_conflation_id_list = []
            type_for_leading_id = type_for_preferred_curie[conflation_id_list[0]]
            prefixes_for_type = nodefactory.get_prefixes(type_for_leading_id)
            ids_already_added = set()
            for prefix in prefixes_for_type:
                if prefix in prefix_map:
                    for id in prefix_map[prefix]:
                        # Just for kicks, let's make sure that this ID is normalized.
                        if preferred_curie_for_curie[id] != id:
                            logger.error(f"CURIE {id} in conflation list is not normalized: {preferred_curie_for_curie[id]} should be used instead.")

                        ids_already_added.add(id)
                        final_conflation_id_list.append(id)

            # Add any identifiers that weren't in the prefix_map.
            for id in conflation_id_list:
                if id not in ids_already_added:
                    final_conflation_id_list.append(id)

            outfile.write(f"{json.dumps(final_conflation_id_list)}\n")
            written.add(fs)
