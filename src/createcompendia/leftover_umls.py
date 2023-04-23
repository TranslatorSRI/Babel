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


def write_leftover_umls(compendia, mrconso, mrsty, synonyms, umls_compendium, umls_synonyms, report, done, biolink_version):
    """
    Search for "leftover" UMLS concepts, i.e. those that are defined and valid in MRCONSO but are not
    mapped to a concept in Babel.

    As described in https://github.com/TranslatorSRI/NodeNormalization/issues/119#issuecomment-1154751451

    :param compendia: A list of compendia to collect.
    :param mrconso: MRCONSO.RRF file path
    :param mrsty: MRSTY.RRF file path
    :param synonyms: synonyms file for UMLS
    :param umls_compendium: The UMLS compendium file to write out.
    :param umls_synonyms: The synonyms file to generate for this compendium.
    :param report: The report file to write out.
    :param done: The done file to write out.
    :return: Nothing.
    """

    logging = Logger()
    logging.info(f"write_leftover_umls({compendia}, {mrconso}, {mrsty}, {synonyms}, {umls_compendium}, {umls_synonyms}, {report}, {done})")

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
        # This defaults to the version of the Biolink model that is included with this BMT.
        biolink_toolkit = Toolkit()

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

        # Load all the semantic types.
        umls_type_by_id = dict()
        preferred_name_by_id = dict()
        types_by_id = dict()
        types_by_tui = dict()
        with open(mrsty, 'r') as inf:
            for line in inf:
                x = line.strip().split('|')
                umls_id = f"{UMLS}:{x[0]}"
                tui = x[1]
                # stn = x[2]
                sty = x[3]

                if umls_id not in types_by_id:
                    types_by_id[umls_id] = dict()
                if tui not in types_by_id[umls_id]:
                    types_by_id[umls_id][tui] = set()
                types_by_id[umls_id][tui].add(sty)

                if tui not in types_by_tui:
                    types_by_tui[tui] = set()
                types_by_tui[tui].add(sty)

        logging.info(f"Completed loading {len(types_by_id.keys())} UMLS IDs from MRSTY.RRF.")
        reportf.write(f"Completed loading {len(types_by_id.keys())} UMLS IDs from MRSTY.RRF.\n")

        with open('babel_outputs/reports/umls-types.tsv', 'w') as outf:
            for tui in sorted(types_by_tui.keys()):
                for sty in sorted(list(types_by_tui[tui])):
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

                # Lookup type.
                def umls_type_to_biolink_type(umls_tui):
                    biolink_type = biolink_toolkit.get_element_by_mapping(f'STY:{umls_tui}', most_specific=True, formatted=True, mixin=True)
                    if biolink_type is None:
                        logging.debug(f"No Biolink type found for UMLS TUI {umls_tui}")
                    return biolink_type

                umls_type_results = types_by_id.get(umls_id, {'biolink:NamedThing': {'Named thing'}})
                biolink_types = set(list(map(umls_type_to_biolink_type, umls_type_results.keys())))

                # How to deal with multiple Biolink types? We currently only have the following multiple
                # types, so we can resolve these manually:
                biolink_types_as_set = set(map(lambda t: "(None)" if t is None else t, list(biolink_types)))
                biolink_types_as_str = '|'.join(sorted(list(biolink_types_as_set)))

                if None in biolink_types:
                    # One of the TUIs couldn't be converted; let's delete all of them so that we can report this.
                    biolink_types = list()

                # Some Biolink multiple types we handle manually.
                if biolink_types_as_set == {DEVICE, DRUG}:
                    biolink_types = [DRUG]
                elif biolink_types_as_set == {DRUG, SMALL_MOLECULE}:
                    biolink_types = [SMALL_MOLECULE]
                elif biolink_types_as_set == {AGENT, PHYSICAL_ENTITY}:
                    biolink_types = [AGENT]
                elif biolink_types_as_set == {PHYSICAL_ENTITY, PUBLICATION}:
                    biolink_types = [PUBLICATION]
                elif biolink_types_as_set == {ACTIVITY, PROCEDURE}:
                    biolink_types = [PROCEDURE]
                elif biolink_types_as_set == {DRUG, FOOD}:
                    biolink_types = [FOOD]

                if len(biolink_types) == 0:
                    logging.debug(f"No UMLS type found for {umls_id}: {umls_type_results} -> {biolink_types}, skipping")
                    reportf.write(f"NO_UMLS_TYPE [{umls_id}]: {umls_type_results} -> {biolink_types}\n")
                    count_no_umls_type += 1
                    continue
                if len(biolink_types) > 1:
                    logging.debug(f"Multiple UMLS types not yet supported for {umls_id}: {umls_type_results} -> {biolink_types}, skipping")
                    reportf.write(f"MULTIPLE_UMLS_TYPES [{umls_id}]\t{biolink_types_as_str}\t{umls_type_results} -> {biolink_types}\n")
                    count_multiple_umls_type += 1
                    continue
                biolink_type = list(biolink_types)[0]
                umls_type_by_id[umls_id] = biolink_type
                preferred_name_by_id[umls_id] = label

                # Write this UMLS term to UMLS.txt as a single-identifier term.
                cluster = {
                    'type': biolink_type,
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
        synonyms = dict()
        with open(synonyms, 'r') as synonymsf:
            for line in synonymsf:
                id, relation, synonym = line.rstrip().split('\t')
                if id in umls_ids_in_this_compendium:
                    # Add this synonym to the set of synonyms for this identifier.
                    if id not in synonyms:
                        synonyms[id] = set()
                    synonyms[id].add(synonym)

                    # We don't record the synonym relation (https://github.com/TranslatorSRI/Babel/pull/113#issuecomment-1516450124),
                    # so we don't need to write that out now.

        logging.info(f"Collected synonyms for {len(synonyms)} UMLS IDs into the leftover UMLS synonyms file.")
        reportf.write(f"Collected synonyms for {len(synonyms)} UMLS IDs into the leftover UMLS synonyms file.\n")

        # Write out synonyms to synonym file.
        node_factory = NodeFactory('babel_downloads/UMLS/labels', biolink_version)
        count_synonym_objs = 0
        with jsonlines.open(umls_synonyms, 'w') as umls_synonymsf:
            for id in synonyms:
                document = {
                    "curie": id,
                    "names": [synonyms[id]],
                    "types": [ t[8:] for t in node_factory.get_ancestors(umls_type_by_id[id])]
                }

                if id in preferred_name_by_id:
                    document["preferred_name"] = preferred_name_by_id[id]

                umls_synonymsf.write(document)
                count_synonym_objs += 1

        logging.info(f"Wrote out {count_synonym_objs} synonym objects into the leftover UMLS synonyms file.")
        reportf.write(f"Wrote out {count_synonym_objs} synonym objects into the leftover UMLS synonyms file.\n")


    # Write out `done` file.
    with open(done, 'w') as outf:
        outf.write(f"done\n{datetime.now()}\n")

    logging.info("Complete")