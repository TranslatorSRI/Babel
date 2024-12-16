#!/usr/bin/env python3
#
# comparator.py - A script for comparing Babel files from different runs
#
import json
import os
import logging
from collections import defaultdict

import click

logging.basicConfig(level=logging.INFO)

class CompendiumFile:
    def __init__(self, path):
        self.path = path

        self.file_exists = os.path.exists(self.path)
        self.row_count = 0

        # TODO: replace with DuckDB or something else more memory efficient.
        self.curie_to_preferred_id = dict()
        self.curie_to_label = dict()
        self.curie_to_description = defaultdict(set)
        self.curie_to_taxa = defaultdict(set)
        self.preferred_id_to_type = defaultdict()
        self.preferred_id_to_preferred_name = defaultdict()
        self.preferred_id_to_ic = dict()


    def load(self):
        if not os.path.exists(self.path):
            logging.warning(f"Compendium file {self.path} does not exist.")
            return

        with open(self.path, "r") as f:
            for row in f:
                self.row_count += 1
                if self.row_count % 1000000 == 0:
                    logging.info(f"Now loading line {self.row_count:,} from {self.path}")

                clique = json.loads(row)

                preferred_curie = clique['identifiers'][0]['i']
                self.preferred_id_to_type[preferred_curie] = clique['type']
                self.preferred_id_to_preferred_name[preferred_curie] = clique['preferred_name']
                self.preferred_id_to_ic = clique['ic']

                for identifier in clique['identifiers']:
                    curie = identifier['i']
                    self.curie_to_preferred_id[curie] = preferred_curie
                    self.curie_to_label[curie] = identifier.get('l', '')
                    self.curie_to_description[curie].update(identifier.get('d', []))
                    self.curie_to_taxa[curie].update(identifier.get('t', []))

        logging.info(f"Loaded {self.row_count:,} lines from {self.path}.")


def compare_compendium_files(path1, path2):
    """ Compare two compendium files.

    @param path1: First path to compare.
    @param path2: Second path to compare.
    @return A comparison between the two compendium files as a dictionary.
    """

    compendium1 = CompendiumFile(path1)
    compendium2 = CompendiumFile(path2)

    # TODO: Figure out how to do this in parallel.
    compendium1.load()
    compendium2.load()

    # Craft results and return.
    return {
        'compendium1': {
            'path': path1,
            'file_exists': compendium1.file_exists,
            'row_count': compendium1.row_count,
            'curie_count': len(compendium1.curie_to_preferred_id),
            'clique_count': len(compendium1.preferred_id_to_type),
            'types': list(sorted(set(compendium1.preferred_id_to_type.values()))),
        },
        'compendium2': {
            'path': path2,
            'file_exists': compendium2.file_exists,
            'row_count': compendium2.row_count,
            'curie_count': len(compendium2.curie_to_preferred_id),
            'clique_count': len(compendium2.preferred_id_to_type),
            'types': list(set(sorted(compendium2.preferred_id_to_type.values()))),
        },
    }


@click.command()
@click.option('--input-type', type=click.Choice(['compendium', 'synonyms']), default='compendium')
@click.argument('input1', type=click.Path(exists=True, file_okay=True, dir_okay=True), required=True)
@click.argument('input2', type=click.Path(exists=True, file_okay=True, dir_okay=True), required=True)
def comparator(input_type, input1, input2):
    """
    Compares two compendium or synonym files.

    :param input_type: Specifies the type of the files to compare.
        Options are 'compendium' or 'synonyms' (not yet supported).
        Defaults to 'compendium'.
    :param input1: First path (file or directory) to compare.
    :param input2: Second file (file or directory) to compare.
    """

    # Some features haven't been implemented yet.
    if input_type != 'compendium':
        raise NotImplementedError(f"Input type '{input_type}' is not yet supported.")

    # Do the comparison.
    if os.path.isfile(input1) and os.path.isfile(input2):
        results = compare_compendium_files(input1, input2)
    elif os.path.isdir(input1) and os.path.isdir(input2):
        results = {
            'directory1': {'path': input1},
            'directory2': {'path': input2},
            'comparisons': [],
        }

        # Make a list of all the files in the directories input1 and input2.
        files1 = os.listdir(input1)
        files2 = os.listdir(input2)
        all_filenames = set(files1 + files2)
        for filename in sorted(all_filenames):
            if filename.startswith('.'):
                continue
            path1 = os.path.join(input1, filename)
            path2 = os.path.join(input2, filename)

            if os.path.isdir(path1):
                logging.warning(f"Skipping directory {path1} in comparison.")
                continue

            if os.path.isdir(path2):
                logging.warning(f"Skipping directory {path2} in comparison.")
                continue

            result = compare_compendium_files(path1, path2)
            results['comparisons'].append(result)
    else:
        raise RuntimeError(f"Cannot compare a file to a directory or vice versa: {input1} and {input2}.")
    
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    comparator()