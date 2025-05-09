from datetime import datetime
import json
import jsonlines
from pathlib import Path

from snakemake.logging import Logger
from bmt import Toolkit

from src.node import NodeFactory
from src.datahandlers import umls
from src.prefixes import UMLS
from src.categories import ACTIVITY, AGENT, DEVICE, DRUG, FOOD, SMALL_MOLECULE, PHYSICAL_ENTITY, PUBLICATION, PROCEDURE


def write_leftover_umls(compendia, umls_labels_filename, mrconso, mrsty, synonyms, umls_compendium, umls_synonyms, report, biolink_version):
    """
    Search for "leftover" UMLS concepts, i.e. those that are defined and valid in MRCONSO but are not
    mapped to a concept in Babel.

    As described in https://github.com/TranslatorSRI/NodeNormalization/issues/119#issuecomment-1154751451

    :param compendia: A list of compendia to collect.
    :param umls_labels_filename: The filename of the UMLS labels file to use for this compendium (e.g. 'babel_downloads/UMLS/labels').
    :param mrconso: MRCONSO.RRF file path
    :param mrsty: MRSTY.RRF file path
    :param synonyms: synonyms file for UMLS
    :param umls_compendium: The UMLS compendium file to write out.
    :param umls_synonyms: The synonyms file to generate for this compendium.
    :param report: The report file to write out.
    :return: Nothing.
    """

    logging = Logger()
    logging.info(f"write_leftover_umls({compendia}, {umls_labels_filename}, {mrconso}, {mrsty}, {synonyms}, {umls_compendium}, {umls_synonyms}, {report}, {biolink_version})")

    # For now, we have many more UMLS entities in MRCONSO than in the compendia, so
    # we'll make an in-memory list of those first. Once that flips, this should be
    # switched to the other way around (or perhaps written into an in-memory database
    # of some sort).
    umls_ids_in_other_compendia = set()

    # If we were interested in keeping all UMLS labels, we would create an identifier file as described in
    # babel_utils.read_identifier_file() and then glom them with babel_utils.glom(). However, for this initial
    # run, it's probably okay to just pick the first label for each code.
    umls_ids_in_this_compendium = set()

    # Write something to the compendium file so that Snakemake knows we've started.
    Path(umls_compendium).touch()

    with open(umls_compendium, 'w') as compendiumf, open(report, 'w') as reportf:

        umls_type_by_id = dict()
        preferred_name_by_id = dict()

        for compendium in compendia:
            logging.info(f"Starting compendium: {compendium}")
            umls_ids = set()

            with open(compendium, 'r') as f:
                for row in f:
                    cluster = json.loads(row)
                    for id in cluster['identifiers']:
                        if id['i'].startswith(UMLS + ':'):
                            umls_ids.add(id['i'])

            logging.info(f"Completed compendium {compendium} with {len(umls_ids)} UMLS IDs")
            umls_ids_in_other_compendia.update(umls_ids)

        logging.info(f"Completed all compendia with {len(umls_ids_in_other_compendia)} UMLS IDs.")
        reportf.write(f"Completed all compendia with {len(umls_ids_in_other_compendia)} UMLS IDs.\n")
        # print(umls_ids_in_other_compendia)

        umls_to_biolink = umls.UMLSToBiolinkTypeConverter(mrsty)
        logging.info(f"Completed loading {len(umls_to_biolink.types_by_id.keys())} UMLS IDs from MRSTY.RRF.")
        reportf.write(f"Completed loading {len(umls_to_biolink.types_by_id.keys())} UMLS IDs from MRSTY.RRF.\n")

        with open('babel_outputs/reports/umls-types.tsv', 'w') as outf:
            for tui in sorted(umls_to_biolink.types_by_tui.keys()):
                for sty in sorted(list(umls_to_biolink.types_by_tui[tui])):
                    outf.write(f"{tui}\t{sty}\n")

        # Create a compendium that consists solely of all MRCONSO entries that haven't been referenced.
        count_no_umls_type = 0
        count_multiple_umls_type = 0
        with open(mrconso, 'r') as inf:
            for line in inf:
                if not umls.check_mrconso_line(line):
                    continue

                x = line.strip().split('|')
                cui = x[0]
                umls_id = f"{UMLS}:{cui}"
                if umls_id in umls_ids_in_other_compendia:
                    logging.debug(f"UMLS ID {umls_id} is in another compendium, skipping.")
                    continue
                if umls_id in umls_ids_in_this_compendium:
                    logging.debug(f"UMLS ID {umls_id} has already been included in this compendium, skipping.")
                    continue

                # The STR value should be the label.
                label = x[14]

                biolink_types = umls_to_biolink.get_biolink_types(umls_id)
                if len(biolink_types) > 1:
                    count_multiple_umls_type += 1
                biolink_type = umls_to_biolink.choose_single_biolink_type(umls_id, biolink_types)

                if biolink_type is None:
                    umls_type_results = umls_to_biolink.types_by_id.get(umls_id, {'biolink:NamedThing': {'Named thing'}})
                    logging.debug(f"No UMLS type found for {umls_id}: {umls_type_results} -> {biolink_types}, skipping")
                    reportf.write(f"NO_UMLS_TYPE [{umls_id}]: {umls_type_results} -> {biolink_types}\n")
                    count_no_umls_type += 1

                    # Default to it being a biolink:NamedThing.
                    biolink_type = 'biolink:NamedThing'

                umls_type_by_id[umls_id] = biolink_type
                preferred_name_by_id[umls_id] = label

                # Write this UMLS term to UMLS.txt as a single-identifier term.
                cluster = {
                    'type': biolink_type,
                    'ic': None,
                    'preferred_name': label,
                    'taxa': [],
                    'identifiers': [{
                        'i': umls_id,
                        'l': label,
                    }]
                }
                compendiumf.write(json.dumps(cluster) + "\n")
                umls_ids_in_this_compendium.add(umls_id)
                logging.debug(f"Writing {cluster} to {compendiumf}")

        logging.info(f"Wrote out {len(umls_ids_in_this_compendium)} UMLS IDs into the leftover UMLS compendium.")
        reportf.write(f"Wrote out {len(umls_ids_in_this_compendium)} UMLS IDs into the leftover UMLS compendium.\n")

        logging.info(f"Found {count_no_umls_type} UMLS IDs without UMLS types and {count_multiple_umls_type} UMLS IDs with multiple UMLS types.")
        reportf.write(f"Found {count_no_umls_type} UMLS IDs without UMLS types and {count_multiple_umls_type} UMLS IDs with multiple UMLS types.\n")

        # Collected synonyms for all IDs in this compendium.
        synonyms_by_id = dict()
        with open(synonyms, 'r') as synonymsf:
            for line in synonymsf:
                id, relation, synonym = line.rstrip().split('\t')
                if id in umls_ids_in_this_compendium:
                    # Add this synonym to the set of synonyms for this identifier.
                    if id not in synonyms_by_id:
                        synonyms_by_id[id] = set()
                    synonyms_by_id[id].add(synonym)

                    # We don't record the synonym relation (https://github.com/TranslatorSRI/Babel/pull/113#issuecomment-1516450124),
                    # so we don't need to write that out now.

        logging.info(f"Collected synonyms for {len(synonyms_by_id)} UMLS IDs into the leftover UMLS synonyms file.")
        reportf.write(f"Collected synonyms for {len(synonyms_by_id)} UMLS IDs into the leftover UMLS synonyms file.\n")

        # Write out synonyms to synonym file.
        node_factory = NodeFactory(umls_labels_filename, biolink_version)
        count_synonym_objs = 0
        with jsonlines.open(umls_synonyms, 'w') as umls_synonymsf:
            for id in synonyms_by_id:
                synonyms_list = list(sorted(list(synonyms_by_id[id]), key=lambda syn:len(syn)))

                document = {
                    "curie": id,
                    "names": synonyms_list,
                    "clique_identifier_count": 1,
                    "taxa": [],
                    "types": [ t[8:] for t in node_factory.get_ancestors(umls_type_by_id[id])]
                }

                if id in preferred_name_by_id:
                    document["preferred_name"] = preferred_name_by_id[id]
                else:
                    document["preferred_name"] = None

                # We previously used the shortest length of a name as a proxy for how good a match it is, i.e. given
                # two concepts that both have the word "acetaminophen" in them, we assume that the shorter one is the
                # more interesting one for users. I'm not sure if there's a better way to do that -- for instance,
                # could we consider the information content values? -- but in the interests of getting something
                # working quickly, this code restores that previous method.

                # Since synonyms_list is sorted,
                if len(synonyms_list) == 0:
                    document["shortest_name_length"] = 0
                else:
                    document["shortest_name_length"] = len(synonyms_list[0])

                umls_synonymsf.write(document)
                count_synonym_objs += 1

        logging.info(f"Wrote out {count_synonym_objs} synonym objects into the leftover UMLS synonyms file.")
        reportf.write(f"Wrote out {count_synonym_objs} synonym objects into the leftover UMLS synonyms file.\n")

    logging.info("Complete")
