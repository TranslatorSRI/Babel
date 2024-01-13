"""
compendia_per_file_reports.py - Generate reports for the individual files in the compendia directory.
"""
import json
import logging
import os
from collections import defaultdict
from datetime import datetime


def get_datetime_as_string():
    """
    Returns the current date and time as a string.

    :return: The current date and time as an ISO8601 string.
    """

    # Return the current date and time in ISO8601 time with time zone.
    return datetime.now().isoformat()


def assert_files_in_directory(dir, files, report_file):
    """
    Asserts that the list of files in a given directory are the list of files provided.

    :param dir: The directory to check files in.
    :param files: The files to compare the list against.
    :param report_file: Write a report to this file. We assume that this file is not intended
        to be read, but is created so that we can check this assertion has been checked.
    """

    logging.info(f"Expect files in directory {dir} to be equal to {files}")
    file_list = os.listdir(dir)
    assert set(file_list) == set(files)

    # If we passed, write the output to the check_file.
    with open(report_file, "w") as f:
        f.write(f"Confirmed that {dir} contains only the files {files} at {get_datetime_as_string()}\n")


def generate_content_report_for_compendium(compendium_path, report_path):
    """
    Generate a report of CURIE prefixes per file.

    :param compendium_path: The path of the compendium file to read.
    :param report_path: The path to write the CURIE prefixes per file report as a JSON file.
    """

    with open(report_path, "w") as report_file:
        with open(compendium_path, "r") as compendium_file:
            # This is a JSONL file, so we need to read each line as a JSON object.

            # Track CURIE breakdowns for this compendium.
            count_by_prefix = defaultdict(int)
            count_by_biolink_type = defaultdict(int)
            counters = defaultdict(int)

            # Since this is time-consuming, let's log a count as we go.
            count_lines = 0

            # Iterate through the compendium file.
            for line in compendium_file:
                count_lines += 1

                if count_lines % 1000000 == 0:
                    logging.info(f"Processed {count_lines} lines in {compendium_path}")

                # Parse each line as a JSON object.
                clique = json.loads(line)

                # Track the CURIEs we're looking for.
                identifiers = clique.get('identifiers', [])
                ids = list(map(lambda x: x['i'], identifiers))

                # Update counts by Biolink type.
                count_by_biolink_type['type'] += 1

                # Update counts by prefix.
                for curie in ids:
                    prefix = curie.split(':')[0]
                    count_by_prefix[prefix] += 1

                # Update counts by flags.
                counters['count_cliques'] += 1
                counters[f"count_cliques_with_{len(ids)}_ids"] += 1
                labels = list(filter(lambda x: x.trim() != '', map(lambda x: x.get('l', ''), identifiers)))
                counters[f"count_cliques_with_{len(labels)}_labels"] += 1
                unique_labels = list(set(labels))
                counters[f"count_cliques_with_{len(labels)}_unique_labels"] += 1
                descriptions = list(filter(lambda x: x.trim() != '', map(lambda x: x.get('d', ''), identifiers)))
                counters[f"count_cliques_with_{len(descriptions)}_descriptions"] += 1
                unique_descriptions = list(set(descriptions))
                counters[f"count_cliques_with_{len(unique_descriptions)}_unique_descriptions"] += 1

        json.dump({
            'compendium_path': compendium_path,
            'report_path': report_path,
            'count_lines': count_lines,
            'count_by_biolink_type': count_by_biolink_type,
            'count_by_prefix': count_by_prefix,
            'counters': counters,
        }, report_file)