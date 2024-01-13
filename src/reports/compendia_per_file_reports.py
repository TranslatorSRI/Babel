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


def generate_curie_prefixes_per_file_report(compendia_dir, report_path):
    """
    Generate a report of CURIE prefixes per file.

    :param compendia_dir: The path to the `compendia` directory.
    :param report_path: The path to write the CURIE prefixes per file report as a JSON file.
    """

    with open(report_path, 'w') as report_file:
        report_file.write("{\n")

        # Track the total number of CURIEs.
        # Note that we can't guarantee that these are unique CURIEs yet!
        total_curies = 0

        # Go through every file in the compendia directory.
        for filename in os.listdir(compendia_dir):
            file_path = os.path.join(compendia_dir, filename)
            if not os.path.isfile(file_path):
                logging.debug(f"Found {file_path} which is not a file, skipping.")

            with open(file_path, "r") as compendium_file:
                # This is a JSONL file, so we need to read each line as a JSON object.
                # Since this is time-consuming, let's log a count as we go.
                count_lines = 0

                # Track CURIE breakdowns.
                count_by_prefix = defaultdict(int)
                count_by_biolink_type = defaultdict(int)
                count_by_flags = defaultdict(int)

                # Iterate through the compendium file.
                for line in compendium_file:
                    count_lines += 1

                    if count_lines % 1000000 == 0:
                        logging.info(f"Processed {count_lines} lines in {file_path}")

                    # Parse each line as a JSON object.
                    clique = json.loads(line)

                    # Track the CURIEs we're looking for.
                    identifiers = clique.get('identifiers', [])
                    ids = list(map(lambda x: x['i'], identifiers))
                    total_curies += len(ids)

                    # Update counts by Biolink type.
                    count_by_biolink_type['type'] += 1

                    # Update counts by prefix.
                    for curie in ids:
                        prefix = curie.split(':')[0]
                        count_by_prefix[prefix] += 1

                    # Update counts by flags.
                    count_by_flags['count_cliques'] += 1
                    count_by_flags[f"count_cliques_with_{len(ids)}_ids"] += 1
                    labels = list(map(lambda x: x['l'], identifiers))
                    if labels:
                        count_by_flags['count_cliques_with_labels'] += 1
                    labels = list(map(lambda x: x['d'], identifiers))
                    if labels:
                        count_by_flags['count_cliques_with_descriptions'] += 1

            report_file.write(f'\t"{filename}": ')
            json.dump({
                'count_lines': count_lines,
                'count_by_biolink_type': count_by_biolink_type,
                'count_by_prefix': count_by_prefix,
                'count_by_flags': count_by_flags,
            }, report_file)
            report_file.write(f',\n')

        # Write out the totals.
        report_file.write(f'\t"totals": {"total_curies": {total_curies}}\n')


        report_file.write("}\n")