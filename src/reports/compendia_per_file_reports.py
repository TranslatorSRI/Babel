"""
compendia_per_file_reports.py - Generate reports for the individual files in the compendia directory.
"""

import logging
import os
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