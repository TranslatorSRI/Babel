#!/usr/bin/env python3

"""
conflate-synonyms.py is a Python script for conflating synonym files using one or more
conflation files.
"""
import sys
import json
import logging

import click

# Set up default logging.
logging.basicConfig(level=logging.INFO)


@click.command()
@click.option('--conflation-file', multiple=True, type=click.File('r'))
@click.option('--compendium-file', multiple=True, type=click.File('r'))
@click.option('--output', type=click.File('w'), default=sys.stdout)
@click.argument("synonym_file", type=click.File('r'))
def conflate_synonyms(synonym_file, conflation_file, compendium_file, output):
    """
    Generate a synonym file based on a single input synonym, the conflation described in the input conflation files,
    and any cross-references present in the input compendia files.

    :param synonym_file: The input synonym file.
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
            for id in conflation:
                add_conflation(conflation[0], id)
                count_secondary += 1
            count_primary += 1

        logging.info(f"Loaded {count_primary} primary identifiers mapped from {count_secondary} secondary identifiers "
                     f"from conflation file {conflationf.name}.")

    logging.info(f"Loaded all conflation files, found {len(conflation_index):,} identifiers in total.")

    # Step 2. For the conflations we've loaded, extend them to include all the identifiers in their clique.
    for compendiumf in compendium_file:
        count_cliques = 0
        count_ids_added = 0
        for line in compendiumf:
            compendium = json.loads(line)
            identifiers = map(lambda id: id['i'], compendium.get('identifiers', []))

            for id in identifiers:
                if id in conflation_index:
                    # This clique contains an identifier we conflate! We set them to the same primary ID.
                    primary_id = conflation_index[id]
                    for id_inner in identifiers:
                        if id_inner not in conflation_index or conflation_index[id_inner] != primary_id:
                            add_conflation(primary_id, id_inner)
                            count_ids_added += 1
                    count_cliques += 1
                    break

        logging.info(f"Added {count_cliques} cliques and {count_ids_added} IDs added from compendium file {compendiumf.name}.")

    logging.info(f"Loaded all compendium files, found {len(conflation_index):,} identifiers in total.")


if __name__ == '__main__':
    conflate_synonyms()
