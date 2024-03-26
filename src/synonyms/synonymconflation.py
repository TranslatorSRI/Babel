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


#click.command()
#click.option('--conflation-file', multiple=True, type=click.Path(exists=True))
#click.option('--output', type=click.Path(exists=False), default='-')
#click.argument("synonym_files", nargs=-1, type=click.Path(exists=True))
def conflate_synonyms(synonym_files, conflation_file, output):
    """
    Generate a synonym file based on a single input synonym, the conflation described in the input conflation files,
    and any cross-references present in the input compendia files.

    :param synonym_files: The input synonym files.
    :param conflation_file: Any conflation files to apply.
    :param output: The file to write the synonyms to.
    :return:
    """

    logging.info(f"conflate_synonyms({synonym_files}, {conflation_file}, {output})")

    # Some common code to manage the conflation index.
    # This is simply a large dictionary, where every key is an identifier and the value is the identifier to map it to.
    conflation_index = dict()
    conflations = defaultdict(list)

    # Step 1. Load all the conflations. We only need to work on these identifiers, so that simplifies our work.
    for conflation_filename in conflation_file:
        logging.info(f"Reading conflation file {conflation_filename}")
        with open(conflation_filename, "r") as conflationf:
            count_primary = 0
            count_secondary = 0
            for line in conflationf:
                conflation = json.loads(line)

                # The conflation line is a list of identifiers, e.g. `["ID1", "ID2", "ID3"]`
                # Note that we map the primary identifier to itself.
                for ident in conflation:
                    if ident in conflation_index and conflation_index[ident] != conflation[0]:
                        logging.error(f"Secondary ID {ident} is mapped to both {conflation_index[ident]} and " +
                                        f"{conflation[0]}, the latter will be used.")
                    conflation_index[ident] = conflation[0]
                    count_secondary += 1

                # Store the conflation for later use.
                if conflation[0] in conflations:
                    logging.error(f"Two conflations have the same primary ID: {conflation} and {conflations[conflation[0]]}")
                conflations[conflation[0]] = conflation
                count_primary += 1

            logging.info(f"Loaded {count_primary} primary identifiers mapped from {count_secondary} secondary identifiers "
                         f"from conflation file {conflationf.name}.")

    logging.info(f"Loaded all conflation files, found {len(conflation_index):,} identifiers in total.")

    logging.info(f"Writing output to {output}.")
    with open(output, 'w') as outputf:
        # Step 2. Conflate the synonyms.
        synonyms_to_conflate = defaultdict(lambda: defaultdict(list))
        for synonym_filename in synonym_files:
            logging.info(f"Reading synonym file {synonym_filename}")
            with open(synonym_filename, "r") as synonymsf:
                for synonym_text in synonymsf:
                    synonym = json.loads(synonym_text)

                    # Do we need to conflate this synonym at all?
                    curie = synonym['curie']
                    bl_type = 'biolink:' + synonym.get('types', ['Entity'])[0]
                    if curie not in conflation_index:
                        # No known conflation. We can just write it out.
                        print(json.dumps(synonym), file=outputf)
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
                        logging.debug(f"Conflating synonym {curie} ({bl_type}) to {conflated_id} ({bl_types}).")

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
            types_to_ignore = set('OntologyClass')
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
                        # Ignore types to be ignored.
                        if typ in types_to_ignore:
                            continue

                        # Don't repeat types that are already in the final conflation.
                        if typ not in types_included:
                            final_conflation['types'].append(typ)
                            types_included.add(typ)

                    # Handle shortest_name_length.
                    if 'shortest_name_length' in synonym:
                        # If we don't have a shortest_name_length in final_conflation OR if
                        # it is smaller than synonym['shortest_name_length'], use that instead.
                        if 'shortest_name_length' not in final_conflation or \
                                synonym['shortest_name_length'] < final_conflation['shortest_name_length']:
                            final_conflation['shortest_name_length'] = synonym['shortest_name_length']

                    # Handle clique_identifier_count.
                    if 'clique_identifier_count' in synonym:
                        if 'clique_identifier_count' not in final_conflation:
                            # If we don't have a clique_identifier_count in final_conflation, set it to zero.
                            final_conflation['clique_identifier_count'] = 0

                        # If we do have a clique_identifier_count in final_conflation, add the count from this synonym.
                        final_conflation['clique_identifier_count'] += synonym['clique_identifier_count']

                    # Handle taxa.
                    if 'taxa' in synonym:
                        if 'taxa' not in final_conflation:
                            final_conflation['taxa'] = set()
                        final_conflation.update(synonym['taxa'])

            # Convert the taxa into a list.
            final_conflation['taxa'] = sorted(final_conflation['taxa'])

            # Checks
            ## assert final_conflation['curie'] == curie
            if final_conflation['curie'] != curie:
                final_conflation['curie'] = curie
                logging.warning(f"Synonym entry {curie} was incorrectly assigned primary identifier {final_conflation['curie']} instead -- are you missing some input synonym files? Fixed.")

            # Write it out.
            logging.debug(f"Conflated entries:\n{json.dumps(synonyms_by_curie, indent=2, sort_keys=True)}")
            logging.debug(f"Into entry: {json.dumps(final_conflation)}")
            print(json.dumps(final_conflation), file=outputf)


if __name__ == '__main__':
    conflate_synonyms()
