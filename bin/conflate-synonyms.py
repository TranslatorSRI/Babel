#!/usr/bin/env python3

"""
conflate-synonyms.py is a Python script for conflating synonym files using one or more
conflation files.
"""
import sys
import json
import logging
from collections import defaultdict

import click

# Set up default logging.
logging.basicConfig(level=logging.INFO)


@click.command()
@click.option('--conflation-file', multiple=True, type=click.File('r'))
@click.option('--output', type=click.File('w'), default=sys.stdout)
@click.argument("synonym_files", nargs=-1, type=click.File('r'))
def conflate_synonyms(synonym_files, conflation_file, output):
    """
    Generate a synonym file based on a single input synonym, the conflation described in the input conflation files,
    and any cross-references present in the input compendia files.

    :param synonym_files: The input synonym files.
    :param conflation_file: Any conflation files to apply.
    :param output: The file to write the synonyms to.
    :return:
    """

    # Some common code to manage the conflation index.
    # This is simply a large dictionary, where every key is an identifier and the value is the identifier to map it to.
    conflation_index = dict()
    conflations = defaultdict(list)

    def add_conflation(primary_id, secondary_id):
        if secondary_id in conflation_index and conflation_index[secondary_id] != primary_id:
            logging.warning(f"Secondary identifier {secondary_id} is mapped to both {conflation_index[secondary_id]} and {primary_id}, the latter will be used.")
        conflation_index[secondary_id] = primary_id

    # Step 1. Load all the conflations. We only need to work on these identifiers, so that simplifies our work.
    for conflationf in conflation_file:
        count_primary = 0
        count_secondary = 0
        for line in conflationf:
            conflation = json.loads(line)

            # The conflation line is a list of identifiers, e.g. `["ID1", "ID2", "ID3"]`
            # Note that we map the primary identifier to itself.
            for ident in conflation:
                add_conflation(conflation[0], ident)
                count_secondary += 1

            conflations[conflation[0]] = conflation
            count_primary += 1

        logging.info(f"Loaded {count_primary} primary identifiers mapped from {count_secondary} secondary identifiers "
                     f"from conflation file {conflationf.name}.")

    logging.info(f"Loaded all conflation files, found {len(conflation_index):,} identifiers in total.")

    # Step 2. Conflate the synonyms.
    synonyms_to_conflate = defaultdict(lambda: defaultdict(list))
    for synonymsf in synonym_files:
        for synonym_text in synonymsf:
            synonym = json.loads(synonym_text)

            # Do we need to conflate this synonym at all?
            curie = synonym['curie']
            bl_type = 'biolink:' + synonym.get('types', ['Entity'])[0]
            if curie not in conflation_index:
                # No known conflation. We can ignore.
                # json.dump(synonym, sort_keys=True, fp=output)
                logging.debug(f"Ignoring synonym {curie}, no known conflation.")
            else:
                # We need to conflate this. Add this to the synonyms_to_conflate list.
                conflated_id = conflation_index[curie]
                if curie in synonyms_to_conflate[conflated_id]:
                    logging.warning(f"Duplicate CURIE in conflation: {conflated_id} appears multiple times in {curie}")
                synonyms_to_conflate[conflated_id][curie].append(synonym)
                synonyms = synonyms_to_conflate[conflated_id].values()
                bl_types = set()
                for synonym_list in synonyms:
                    for synonym in synonym_list:
                        bl_types.add('biolink:' + synonym.get('types', ['Entity'])[0])
                logging.info(f"Conflating synonym {curie} ({bl_type}) to {conflated_id} ({bl_types}).")

    logging.info(f"Identified {len(synonyms_to_conflate)} conflated cliques that need to be synonymized.")
    logging.debug(f"Conflated cliques: {json.dumps(synonyms_to_conflate, sort_keys=True)}")

    # Step 3. Conflate any synonyms that need conflating.
    for curie in synonyms_to_conflate:
        synonyms_by_curie = synonyms_to_conflate[curie]

        # We conflate synonyms in this way:
        # 1. We traverse the list in the order of identifiers in the original conflation.
        # 2. We concatenate types onto the end of subsequent conflation types, i.e. we begin with
        #    all the types of the lead clique, and then add the types of subsequent cliques.
        conflation_order = conflations[curie]

        final_conflation = dict()

        names_included = set()
        types_included = set()
        for conflation_id in conflation_order:
            logging.info(f"Looking into {conflation_id}.")
            for synonym in synonyms_by_curie[conflation_id]:
                logging.info(f"conflation_order = {conflation_order}, synonyms_by_curie[{conflation_id}] = {synonyms_by_curie[conflation_id]}")
                if 'curie' not in final_conflation:
                    final_conflation['curie'] = synonym['curie']

                # Calculate the CURIE suffix, if applicable.
                curie_parts = curie.split(':', 1)
                if len(curie_parts) > 0:
                    # Try to cast the CURIE suffix to an integer. If we get a ValueError, don't worry about it.
                    try:
                        final_conflation['curie_suffix'] = int(curie_parts[1])
                    except ValueError:
                        pass

                if 'preferred_name' not in final_conflation and 'preferred_name' in synonym:
                    final_conflation['preferred_name'] = synonym['preferred_name']

                if 'names' not in final_conflation:
                    final_conflation['names'] = list()

                for name in synonym['names']:
                    # Don't repeat names that are already in the final conflation.
                    if name not in names_included:
                        final_conflation['names'].append(name)
                        names_included.add(name)

                if 'types' not in final_conflation:
                    final_conflation['types'] = list()

                for typ in synonym['types']:
                    # Don't repeat types that are already in the final conflation.
                    if typ not in types_included:
                        final_conflation['types'].append(typ)
                        types_included.add(typ)

                # Since we no longer use shortest_name_length, I'm just not going to bother writing it
                # into the final conflations.

        # Checks
        assert final_conflation['curie'] == curie

        # Write it out.
        logging.info(f"Conflated entries:\n{json.dumps('synonyms_by_curie', indent=2, sort_keys=True)}")
        logging.info(f"Into entry: {json.dumps(final_conflation)}")


if __name__ == '__main__':
    conflate_synonyms()
