#!/usr/bin/env python3
#
# comparator.py - A script for comparing Babel files from different runs
#
# You can run this script on a single compendium file:
#   python comparator.py dir1/compendia/Disease.txt dir2/compendia/Disease.txt
# Or on an entire directory:
#   python comparator.py dir1/compendia dir2/compendia
#
# It currently only writes out a JSON document to STDOUT, but in the future we might add a TSV output as well.
#

import concurrent
import json
import os
import logging
import threading
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor

import click

logging.basicConfig(level=logging.INFO)

class CompendiumFile:
    """
    Represents a compendium file at a particular path. The load() method will load the file into a series of in-memory
    dictionaries, and the diffs_to() method will generate a diff between this compendium file and another compendium file.
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
        self.preferred_id_to_clique = defaultdict(list)
        self.curie_to_preferred_id = dict()
        self.curie_to_label = dict()
        self.curie_to_description = defaultdict(set)
        self.curie_to_taxa = defaultdict(set)
        self.preferred_id_to_type = dict()
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

        time_started = time.time_ns()

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
                self.preferred_id_to_ic[preferred_curie] = clique['ic']
                self.preferred_id_to_clique[preferred_curie] = list(map(lambda x: x['i'], clique['identifiers']))

                for identifier in clique['identifiers']:
                    curie = identifier['i']
                    self.curie_to_preferred_id[curie] = preferred_curie
                    self.curie_to_label[curie] = identifier.get('l', '')
                    self.curie_to_description[curie].update(identifier.get('d', []))
                    self.curie_to_taxa[curie].update(identifier.get('t', []))

        time_ended = time.time_ns()
        logging.info(f"Loaded {self.row_count:,} lines from {self.path} in {(time_ended - time_started) / 1_000_000_000:.2f} seconds.")

    def add_labels(self, ids: list[str]):
        """
        Return a list of labels for the IDs in ids.

        :param ids: A list of identifiers.
        :return: A list of labels.
        """
        return list(map(lambda x: self.curie_to_label.get(x, ''), ids))

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

        # Step 2. Figure out the clique changes.
        clique_count = len(self.preferred_id_to_type.keys())
        old_clique_count = len(older_compendium_file.preferred_id_to_type.keys())

        cliques_additions = {}
        cliques_deletions = {}
        cliques_unchanged = {}
        clique_changes = {}
        for preferred_curie, typ in self.preferred_id_to_type.items():
            if preferred_curie not in older_compendium_file.preferred_id_to_type:
                # Addition.
                cliques_additions[preferred_curie] = {
                    'type': typ,
                    'preferred_curie': preferred_curie,
                    'preferred_name': self.preferred_id_to_preferred_name[preferred_curie],
                    'identifiers': self.preferred_id_to_clique[preferred_curie],
                }
            else:
                # The clique is present in both self and older_compendium_file, so we need to determine if it's
                # changed or not.
                clique_change = {
                    'type': typ,
                    'preferred_curie': preferred_curie,
                    'preferred_name': self.preferred_id_to_preferred_name[preferred_curie],
                    'identifiers': self.preferred_id_to_clique[preferred_curie],
                }

                # But did anything actually change?
                flag_actually_changed = False

                old_typ = older_compendium_file.preferred_id_to_type[preferred_curie]
                if old_typ != typ:
                    flag_actually_changed = True
                    clique_change['type_changed'] = {
                        'old': old_typ,
                        'new': typ,
                    }

                clique_label = self.preferred_id_to_preferred_name[preferred_curie]
                old_clique_label = older_compendium_file.preferred_id_to_preferred_name[preferred_curie]
                if clique_label != old_clique_label:
                    flag_actually_changed = True
                    clique_change['preferred_name_changed'] = {
                        'old': old_clique_label,
                        'new': clique_label,
                    }

                ids = self.preferred_id_to_clique[preferred_curie]
                old_ids = older_compendium_file.preferred_id_to_clique[preferred_curie]
                if ids != old_ids:
                    flag_actually_changed = True
                    clique_change['identifiers_changed'] = {
                        'old': old_ids,
                        'old_with_labels': list(map(lambda x: f"{x[0]} '{x[1]}'", zip(old_ids, older_compendium_file.add_labels(old_ids)))),
                        'new': ids,
                        'new_with_labels': list(map(lambda x: f"{x[0]} '{x[1]}'", zip(ids, self.add_labels(ids)))),
                        'added': sorted(set(ids) - set(old_ids)),
                        'deleted': sorted(set(old_ids) - set(ids)),
                    }

                # If something actually changed, add it to the clique changes list.
                if flag_actually_changed:
                    clique_changes[preferred_curie] = clique_change
                else:
                    cliques_unchanged[preferred_curie] = clique_change

        for old_preferred_curie, typ in older_compendium_file.preferred_id_to_type.items():
            if old_preferred_curie not in self.preferred_id_to_type:
                # Deletion.
                cliques_deletions[old_preferred_curie] = {
                    'type': typ,
                    'preferred_curie': old_preferred_curie,
                    'preferred_name': older_compendium_file.preferred_id_to_preferred_name[old_preferred_curie],
                    'identifiers': older_compendium_file.preferred_id_to_clique[old_preferred_curie],
                }

        # Step 3. Report on all the identifiers and cliques.
        return {
            'net_identifier_change': len(identifiers_added) - len(identifiers_deleted),
            'net_clique_change': (clique_count - old_clique_count),
            'identifiers': {
                'additions': sorted(map(lambda x: f"{x[0]} '{x[1]}' (to clique {x[4]} '{x[5]}')", identifiers_added)),
                'deletions': sorted(map(lambda x: f"{x[0]} '{x[1]}' (from clique {x[2]} '{x[3]}')", identifiers_deleted)),
                'changes': sorted(map(lambda x: f"{x[0]} '{x[1]}' moved from {x[2]} '{x[3]}' to {x[4]} '{x[5]}'", identifiers_changed)),
            },
            'cliques': {
                'additions': cliques_additions,
                'deletions': cliques_deletions,
                'changes': clique_changes,
            },
        }


def compare_compendium_files(path_old, path_new):
    """ Compare two compendium files.

    :param path_old: The older folder to compare
    :param path_new: The newer folder to compare.
    :return A comparison between the two compendium files as a dictionary.
    """

    time_started = time.time_ns()

    compendium_old = CompendiumFile(path_old)
    compendium_new = CompendiumFile(path_new)

    # Load the two files in parallel.
    thread_compendium1 = threading.Thread(target=compendium_old.load)
    thread_compendium2 = threading.Thread(target=compendium_new.load)
    thread_compendium1.start()
    thread_compendium2.start()
    thread_compendium1.join()
    thread_compendium2.join()

    # Craft results and return.
    result = {
        'compendium_old': {
            'path': path_old,
            'file_exists': compendium_old.file_exists,
            'row_count': compendium_old.row_count,
            'curie_count': len(compendium_old.curie_to_preferred_id),
            'clique_count': len(compendium_old.preferred_id_to_type),
            'types': list(sorted(set(compendium_old.preferred_id_to_type.values()))),
        },
        'compendium_new': {
            'path': path_new,
            'file_exists': compendium_new.file_exists,
            'row_count': compendium_new.row_count,
            'curie_count': len(compendium_new.curie_to_preferred_id),
            'clique_count': len(compendium_new.preferred_id_to_type),
            'types': list(set(sorted(compendium_new.preferred_id_to_type.values()))),
        },
        'diffs': compendium_new.diffs_to(compendium_old),
    }

    time_ended = time.time_ns()
    logging.info(f"Comparison of {path_old} to {path_new} took {(time_ended - time_started) / 1_000_000_000:.2f} seconds.")

    return result


class SynonymsFile:
    """
    Represents a synonyms file at a particular path. The load() method will load the file into a series of in-memory
    dictionaries, and the diffs_to() method will generate a diff between this synonyms file and another synonyms file.
    """

    def __init__(self, path):
        """
        Initialize a SynonymsFile object with the specified path. We don't load the file until load() is called.

        :param path: File path to initialize and load metadata from.
        """
        self.path = path

        self.file_exists = os.path.exists(self.path)
        self.row_count = 0

        # TODO: replace with DuckDB or something else more memory efficient.
        self.names = dict()
        self.type_by_preferred_curie = dict()
        self.preferred_name = dict()
        self.clique_identifier_count = dict()
        self.taxa = dict()

        self.preferred_ids_by_name = defaultdict(set)
        self.preferred_ids_by_preferred_name = defaultdict(set)


    def load(self):
        """
        Loads synonyms data from the specified file path into various mappings.

        This method reads data from a JSON lines file located at the path specified
        by the instance attribute `path`. Each line in the file should represent a
        synonyms object in JSON format.

        The method tracks and logs the progress of the file loading process. It will
        log a warning if the specified file path does not exist, and progress
        information is logged for every million lines processed. At the end, the
        method logs the total number of lines read.
        """

        time_started = time.time_ns()

        if not os.path.exists(self.path):
            logging.warning(f"Synonyms file {self.path} does not exist.")
            return

        with open(self.path, "r") as f:
            for row in f:
                self.row_count += 1
                if self.row_count % 1000000 == 0:
                    logging.info(f"Now loading line {self.row_count:,} from {self.path}")

                synonyms = json.loads(row)

                preferred_curie = synonyms['curie']
                if preferred_curie in self.type_by_preferred_curie:
                    raise RuntimeError(f"Duplicate preferred curie {preferred_curie} in {self.path}")

                self.type_by_preferred_curie[preferred_curie] = synonyms['types'][0]
                self.preferred_name[preferred_curie] = synonyms['preferred_name']
                self.preferred_ids_by_preferred_name[synonyms['preferred_name']].add(preferred_curie)

                self.clique_identifier_count[preferred_curie] = synonyms['clique_identifier_count']
                self.taxa[preferred_curie] = synonyms['taxa']

                self.names[preferred_curie] = synonyms['names']
                for name in synonyms['names']:
                    self.preferred_ids_by_name[name].add(preferred_curie)

        time_ended = time.time_ns()
        logging.info(f"Loaded {self.row_count:,} lines from {self.path} in {(time_ended - time_started) / 1_000_000_000:.2f} seconds.")

    def diffs_to(self, older_synonyms_file: 'SynonymsFile'):
        """
        Generate diff counts between this synonyms file and the older synonyms file.

        :param older_synonyms_file: A SynonymsFile object representing the older compendium file.
        :return: A dictionary.
        """

        # Step 1. Figure out which identifiers have changed cliques between these two compendia.
        identifiers_added = dict()
        identifiers_not_changed = dict()
        identifiers_changed = dict()
        identifiers_deleted = dict()

        names_changed = []
        types_changed = []

        preferred_curies = self.type_by_preferred_curie.keys()
        older_preferred_curies = older_synonyms_file.type_by_preferred_curie.keys()

        for preferred_curie in preferred_curies:
            if preferred_curie not in older_preferred_curies:
                identifiers_added[preferred_curie] = self.preferred_name[preferred_curie]
            else:
                names = set(self.names[preferred_curie])
                old_names = set(older_synonyms_file.names[preferred_curie])

                if names != old_names:
                    identifiers_changed[preferred_curie] = self.preferred_name[preferred_curie]

                    added_names = names - old_names
                    deleted_names = old_names - names
                    if added_names:
                        names_changed.append((preferred_curie, self.preferred_name[preferred_curie], list(sorted(added_names)), list(sorted(deleted_names))))

                else:
                    # This means that if the names haven't changed, but e.g. the type or clique_identifier_count has
                    # changed, we will not consider these identifiers as having changed. Which is okay, probably.
                    identifiers_not_changed[preferred_curie] = self.preferred_name[preferred_curie]

                # Let's check the type here.
                if self.type_by_preferred_curie[preferred_curie] != older_synonyms_file.type_by_preferred_curie[preferred_curie]:
                    added_types = self.type_by_preferred_curie[preferred_curie] - older_synonyms_file.type_by_preferred_curie[preferred_curie]
                    deleted_types = older_synonyms_file.type_by_preferred_curie[preferred_curie] - self.type_by_preferred_curie[preferred_curie]

                    types_changed.append((preferred_curie, self.preferred_name[preferred_curie], added_types, deleted_types))

        for old_preferred_curie in older_preferred_curies:
            if old_preferred_curie not in preferred_curies:
                identifiers_deleted[old_preferred_curie] = older_synonyms_file.preferred_name[old_preferred_curie]

        # Step 2. Figure out if the preferred names have changed.
        preferred_name_changes = []
        for preferred_name in self.preferred_ids_by_preferred_name.keys():
            preferred_name_ids = self.preferred_ids_by_preferred_name[preferred_name]

            if preferred_name not in older_synonyms_file.preferred_ids_by_preferred_name:
                preferred_name_changes.append((preferred_name, [], list(sorted(preferred_name_ids))))
            else:
                old_preferred_ids = older_synonyms_file.preferred_ids_by_preferred_name[preferred_name]
                if set(old_preferred_ids) == set(self.preferred_ids_by_preferred_name[preferred_name]):
                    # No change, do nothing.
                    pass
                else:
                    preferred_name_changes.append((preferred_name, list(sorted(old_preferred_ids)), list(sorted(preferred_name_ids))))

        for old_preferred_name in older_synonyms_file.preferred_ids_by_preferred_name:
            if old_preferred_name not in self.preferred_ids_by_preferred_name:
                preferred_name_changes.append((old_preferred_name, list(sorted(older_synonyms_file.preferred_ids_by_preferred_name[old_preferred_name])), []))

        # Step 3. Report on all the synonyms.
        return {
            'net_identifier_change': len(identifiers_added) - len(identifiers_deleted),
            'identifiers': {
                'additions': list(f"{i} '{identifiers_added[i]}'" for i in sorted(identifiers_added.keys())),
                'deletions': list(f"{i} '{identifiers_deleted[i]}'" for i in sorted(identifiers_deleted.keys())),
                'changes': list(
                    f"{i} '{identifiers_changed[i]}': {self.names[i]} -> {older_synonyms_file.names[i]} " +
                    f"(additions: {sorted(set(self.names[i]) - set(older_synonyms_file.names[i]))}, " +
                    f"deletions: {sorted(set(older_synonyms_file.names[i]) - set(self.names[i]))})"
                    for i in sorted(identifiers_changed.keys())),
            },
            'preferred_name_changes': preferred_name_changes,
            'type_changes': types_changed,
        }


def compare_synonym_files(path_old, path_new):
    """ Compare two synonym files.

    :param path_old: The older folder to compare
    :param path_new: The newer folder to compare.
    :return A comparison between the two synonym files as a dictionary.
    """

    time_started = time.time_ns()

    synonyms_old = SynonymsFile(path_old)
    synonyms_new = SynonymsFile(path_new)

    # Load the two files in parallel.
    thread_synonyms1 = threading.Thread(target=synonyms_old.load)
    thread_synonyms2 = threading.Thread(target=synonyms_new.load)
    thread_synonyms1.start()
    thread_synonyms2.start()
    thread_synonyms1.join()
    thread_synonyms2.join()

    # Craft results and return.
    result = {
        'synonyms_old': {
            'path': path_old,
            'file_exists': synonyms_old.file_exists,
            'row_count': synonyms_old.row_count,
            'curie_count': len(synonyms_old.type_by_preferred_curie.keys()),
            'type_count': len(set(synonyms_old.type_by_preferred_curie.values())),
        },
        'synonyms_new': {
            'path': path_new,
            'file_exists': synonyms_new.file_exists,
            'row_count': synonyms_new.row_count,
            'curie_count': len(synonyms_new.type_by_preferred_curie.keys()),
            'type_count': len(set(synonyms_new.type_by_preferred_curie.values())),
        },
        'diffs': synonyms_new.diffs_to(synonyms_old),
    }

    time_ended = time.time_ns()
    logging.info(f"Comparison of {path_old} to {path_new} took {(time_ended - time_started) / 1_000_000_000:.2f} seconds.")

    return result


@click.command()
@click.option('--input-type', type=click.Choice(['compendium', 'synonyms']), default='compendium')
@click.argument('input_old', type=click.Path(exists=True, file_okay=True, dir_okay=True), required=True)
@click.argument('input_new', type=click.Path(exists=True, file_okay=True, dir_okay=True), required=True)
@click.option('--max-workers', '-j', type=int, default=None, help='Maximum number of workers to use for parallel processing.')
def comparator(input_type, input_old, input_new, max_workers):
    """
    Compares either two files or two directories containing compendium or synonyms files.
    \f

    :param input_type: Specifies the type of the files to compare.
        Options are 'compendium' or 'synonyms' (not yet supported).
        Defaults to 'compendium'.
    :param input_old: Older path (file or directory) to compare.
    :param input_new: Newer path (file or directory) to compare.
    :param max_workers: Maximum number of workers to use for parallel processing.
    """

    # Some features haven't been implemented yet.
    match input_type:
        case 'compendium':
            comparison_function = compare_compendium_files
        case 'synonyms':
            comparison_function = compare_synonym_files
        case _:
            raise RuntimeError(f"Input type '{input_type}' is not defined.")

    # Do the comparison.
    if os.path.isfile(input_old) and os.path.isfile(input_new):
        results = comparison_function(input_old, input_new)
    elif os.path.isdir(input_old) and os.path.isdir(input_new):
        results = {
            'directory1': {'path': input_old},
            'directory2': {'path': input_new},
            'comparisons': [],
        }

        # Make a list of all the files in the directories input1 and input2.
        files1 = os.listdir(input_old)
        files2 = os.listdir(input_new)
        all_filenames = set(files1 + files2)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for filename in sorted(all_filenames):
                if filename.startswith('.'):
                    continue
                path1 = os.path.join(input_old, filename)
                path2 = os.path.join(input_new, filename)

                if os.path.isdir(path1):
                    logging.warning(f"Skipping directory {path1} in comparison.")
                    continue

                if os.path.isdir(path2):
                    logging.warning(f"Skipping directory {path2} in comparison.")
                    continue

                futures.append(executor.submit(comparison_function, path1, path2))

            for future in concurrent.futures.as_completed(futures):
                try:
                    results['comparisons'].append(future.result())
                except Exception as exc:
                    logging.error(f"Error comparing files: {exc}")
                    raise exc

    else:
        raise RuntimeError(f"Cannot compare a file to a directory or vice versa: {input_old} and {input_new}.")

    print(json.dumps(results, indent=2))


if __name__ == "__main__":
    comparator()
