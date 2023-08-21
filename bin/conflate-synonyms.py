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
@click.option('--compendium-file', multiple=True, type=click.File('r'))
@click.option('--output', type=click.File('w'), default=sys.stdout)
@click.argument("synonym_files", nargs=-1, type=click.File('r'))
def conflate_synonyms(synonym_files, conflation_file, compendium_file, output):
    """
    Generate a synonym file based on a single input synonym, the conflation described in the input conflation files,
    and any cross-references present in the input compendia files.

    :param synonym_files: The input synonym files.
    :param conflation_file: Any conflation files to apply.
    :param compendium_file: Any compendia files to incorporate cross-references from.
    :param output: The file to write the synonyms to.
    :return:
    """

    # Some common code to manage the conflation index.
    # This is simply a large dictionary, where every key is an identifier and the value is the identifier to map it to.
    conflation_index = dict()

    def add_conflation(primary_id, secondary_id):
        if secondary_id in conflation_index and conflation_index[secondary_id] != primary_id:
            logging.warning(f"Secondary identifier {secondary_id} is mapped to both {conflation_index[secondary_id]} and {primary_id}, the latter will be used.")
        conflation_index[secondary_id] = primary_id

    # Step 1. Load all the conflations. We only need to work on these identifiers, so that simplifiers our work.
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
            count_primary += 1

        logging.info(f"Loaded {count_primary} primary identifiers mapped from {count_secondary} secondary identifiers "
                     f"from conflation file {conflationf.name}.")

    logging.info(f"Loaded all conflation files, found {len(conflation_index):,} identifiers in total.")

    # Step 2. Identify the cliques to be conflated, i.e. cliques that contain an identifier we have in the
    # conflation index. Most cliques will end up being ignored.
    count_cliques = 0
    count_cliques_to_conflate = 0
    cliques_to_be_conflated = defaultdict(list)
    for compendiumf in compendium_file:
        count_file_cliques = 0
        count_file_cliques_to_conflate = 0
        for line in compendiumf:
            compendium = json.loads(line)
            identifiers = map(lambda identifier: identifier['i'], compendium.get('identifiers', []))

            for ident in identifiers:
                if ident in conflation_index:
                    # Add this to the list of the cliques to be conflated.
                    id_to_conflate_to = conflation_index[ident]
                    cliques_to_be_conflated[id_to_conflate_to].append(compendium)
                    count_cliques_to_conflate += 1
                    count_file_cliques_to_conflate += 1
                count_cliques += 1
                count_file_cliques += 1

        logging.info(f"Identified {count_file_cliques_to_conflate} cliques to conflate out of {count_file_cliques} ({(count_file_cliques_to_conflate/count_file_cliques*100):.2%}) in {compendiumf.name}.")

    logging.info(f"Identified {count_cliques_to_conflate} cliques to be conflated out of {count_cliques} cliques ({(count_cliques_to_conflate/count_cliques*100):.2%}) considered in compendia.")

    # Step 3. Conflate the synonyms.
    for synonymsf in synonym_files:
        for synonym in synonymsf:
            # Do we need to conflate this synonym at all?



if __name__ == '__main__':
    conflate_synonyms()
