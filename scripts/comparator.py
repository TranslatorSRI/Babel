#!/usr/bin/env python3
#
# comparator.py - A script for comparing Babel files from different runs
#
import concurrent
import json
import os
import logging
import threading
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

import click

logging.basicConfig(level=logging.INFO)

class CompendiumFile:
    """
    CompendiumFile represents a data handler for managing and processing a compendium
    file. It is used to load compendium data from a file, map identifiers to their
    preferred IDs, extract associated labels, descriptions, taxonomic information,
    and other metadata.

    The class provides methods to load data from a specified file path and maintains
    the mappings and metadata in memory for further processing.

    :ivar path: The path to the compendium file.
    :type path: str
    :ivar file_exists: A boolean indicating if the compendium file exists at the specified path.
    :type file_exists: bool
    :ivar row_count: The number of rows processed from the compendium file.
    :type row_count: int
    :ivar curie_to_preferred_id: A dictionary mapping CURIEs to their preferred identifiers.
    :type curie_to_preferred_id: dict
    :ivar curie_to_label: A dictionary mapping CURIEs to their associated labels.
    :type curie_to_label: dict
    :ivar curie_to_description: A defaultdict mapping CURIEs to sets of descriptions.
    :type curie_to_description: defaultdict
    :ivar curie_to_taxa: A defaultdict mapping CURIEs to sets of taxonomic identifiers.
    :type curie_to_taxa: defaultdict
    :ivar preferred_id_to_type: A defaultdict mapping preferred identifiers to their types.
    :type preferred_id_to_type: defaultdict
    :ivar preferred_id_to_preferred_name: A defaultdict mapping preferred identifiers to their preferred names.
    :type preferred_id_to_preferred_name: defaultdict
    :ivar preferred_id_to_ic: A dictionary mapping preferred identifiers to their information content scores.
    :type preferred_id_to_ic: dict
    """

    def __init__(self, path):
        """
        Initialize a CompendiumFile object with the specified path. We don't load the file until load() is called.

        :param path: File path to initialize and load metadata from.
        """
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
        """
        Loads compendium data from the specified file path into various mappings.

        This method reads data from a JSON lines file located at the path specified
        by the instance attribute `path`. Each line in the file should represent a
        clique object in JSON format. The method populates multiple mappings
        based on the contents of the file, including mappings between CURIEs and
        their preferred identifiers, labels, descriptions, taxa, types, and
        information content (IC).

        The method tracks and logs the progress of the file loading process. It will
        log a warning if the specified file path does not exist, and progress
        information is logged for every million lines processed. At the end, the
        method logs the total number of lines read.
        """
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

    def diffs_to(self, older_compendium_file: 'CompendiumFile'):
        """
        Generate diff counts between this compendium file and the older compendium file.

        :param older_compendium_file: A CompendiumFile object representing the older compendium file.
        :return: A dictionary.
        """

        # Step 1. Figure out which identifiers have changed cliques between these two compendia.
        identifiers_added = set()
        identifiers_not_changed = set()
        identifiers_changed = set()
        identifiers_deleted = set()
        for curie, preferred_curie in self.curie_to_preferred_id.items():
            if curie not in older_compendium_file.curie_to_preferred_id:
                identifiers_added.add((curie, self.curie_to_label[curie], None, '', preferred_curie, self.preferred_id_to_preferred_name[preferred_curie]))
            else:
                old_preferred_curie = older_compendium_file.curie_to_preferred_id.get(curie)
                if preferred_curie == old_preferred_curie:
                    identifiers_not_changed.add((curie, self.curie_to_label[curie], old_preferred_curie, older_compendium_file.preferred_id_to_preferred_name[old_preferred_curie], preferred_curie, self.preferred_id_to_preferred_name[preferred_curie]))
                else:
                    identifiers_changed.add((curie, self.curie_to_label[curie], old_preferred_curie, older_compendium_file.preferred_id_to_preferred_name[old_preferred_curie], preferred_curie, self.preferred_id_to_preferred_name[preferred_curie]))

        for old_curie, old_preferred_curie in older_compendium_file.curie_to_preferred_id.items():
            if old_curie not in self.curie_to_preferred_id:
                identifiers_deleted.add((old_curie, older_compendium_file.curie_to_label[old_curie], old_preferred_curie, older_compendium_file.preferred_id_to_preferred_name[old_preferred_curie], None, ''))

        # Step 2. Figure out the clique change.
        clique_count = len(self.preferred_id_to_type.keys())
        old_clique_count = len(older_compendium_file.preferred_id_to_type.keys())

        # Step 3. Report on all the identifiers.
        return {
            'net_identifier_change': len(identifiers_added) - len(identifiers_deleted),
            'net_clique_change': (clique_count - old_clique_count),
            'additions': sorted(map(lambda x: f"{x[0]} '{x[1]}' (to clique {x[4]} '{x[5]}')", identifiers_added)),
            'deletions': sorted(map(lambda x: f"{x[0]} '{x[1]}' (from clique {x[2]} '{x[3]}')", identifiers_deleted)),
            'changes': sorted(map(lambda x: f"{x[0]} '{x[1]}' moved from {x[2]} '{x[3]}' to {x[4]} '{x[5]}'", identifiers_changed)),
        }


def compare_compendium_files(path1, path2):
    """ Compare two compendium files.

    @param path1: First path to compare.
    @param path2: Second path to compare.
    @return A comparison between the two compendium files as a dictionary.
    """

    compendium1 = CompendiumFile(path1)
    compendium2 = CompendiumFile(path2)

    # Load the two files in parallel.
    thread_compendium1 = threading.Thread(target=compendium1.load)
    thread_compendium2 = threading.Thread(target=compendium2.load)
    thread_compendium1.start()
    thread_compendium2.start()
    thread_compendium1.join()
    thread_compendium2.join()

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
        'diffs': compendium2.diffs_to(compendium1),
    }


@click.command()
@click.option('--input-type', type=click.Choice(['compendium', 'synonyms']), default='compendium')
@click.argument('input1', type=click.Path(exists=True, file_okay=True, dir_okay=True), required=True)
@click.argument('input2', type=click.Path(exists=True, file_okay=True, dir_okay=True), required=True)
@click.option('--max-workers', '-j', type=int, default=None, help='Maximum number of workers to use for parallel processing.')
def comparator(input_type, input1, input2, max_workers):
    """
    Compares two compendium or synonym files.

    :param input_type: Specifies the type of the files to compare.
        Options are 'compendium' or 'synonyms' (not yet supported).
        Defaults to 'compendium'.
    :param input1: First path (file or directory) to compare.
    :param input2: Second file (file or directory) to compare.
    :param max_workers: Maximum number of workers to use for parallel processing.
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

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
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

                futures.append(executor.submit(compare_compendium_files, path1, path2))

            for future in concurrent.futures.as_completed(futures):
                try:
                    results['comparisons'].append(future.result())
                except Exception as exc:
                    logging.error(f"Error comparing files: {exc}")
                    raise exc

        print(json.dumps(results, indent=2))

    else:
        raise RuntimeError(f"Cannot compare a file to a directory or vice versa: {input1} and {input2}.")
    
    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    comparator()