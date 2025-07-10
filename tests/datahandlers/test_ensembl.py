# Tests for datahandlers/ensembl.py
import csv
import json
import logging
import os

from src.datahandlers.ensembl import pull_ensembl

logging.basicConfig(level=logging.INFO)

def read_biomart_file(biomart_file):
    """
    Reads a BioMart output file (TSV) and yields each row as a dictionary.

    :param biomart_file: An opened file object containing tab-delimited data, formatted
                         as output from BioMart.
    :type biomart_file: file-like object
    :return: A generator yielding dictionaries representing each row from the input file.
    :rtype: Iterator[Dict[str, str]]
    """
    reader = csv.DictReader(biomart_file, dialect="excel-tab")
    for row in reader:
        yield row

def normalize_list_of_dictionaries(dict_list):
    """
    Sort and normalize a list of dictionaries by converting them into sorted JSON strings.

    This function accepts a list of dictionaries, serializes each dictionary into its JSON
    representation, and ensures keys are sorted so that the overall lists can be compared.

    :param dict_list: A list containing dictionary objects to be normalized, with keys
        sorted and represented as JSON strings.
    :type dict_list: list[dict]
    :return: A list of JSON strings where each string represents a dictionary input
        with sorted keys.
    :rtype: list[str]
    """
    return sorted(json.dumps(dictionary, sort_keys=True) for dictionary in dict_list)

def test_pull_ensembl(tmp_path):
    # Make a temporary directory for testing.
    pull_ensembl_test_dir = tmp_path / "pull_ensembl_test"
    output_dir = pull_ensembl_test_dir / "download"
    os.makedirs(output_dir)

    # Pull a single ENSEMBL file to that. This should trigger https://github.com/TranslatorSRI/Babel/issues/193
    single_query_report = pull_ensembl(output_dir, output_dir / 'download_complete', ['choffmanni_gene_ensembl', 'hgfemale_gene_ensembl'])

    # uamericanus_gene_ensembl should be downloadable as a single file in the above example, but we're going to
    # deliberately download it in multiple chunks so it's clearer.
    download_as_splits = pull_ensembl_test_dir / "download_as_splits"
    os.makedirs(download_as_splits)
    split_query_report = pull_ensembl(download_as_splits, download_as_splits / 'download_complete', ['choffmanni_gene_ensembl'], max_attribute_count=4)

    # We need to check two things:
    # 1. Whether the single/split reports make sense.
    single_uamericanus = single_query_report['choffmanni_gene_ensembl']
    split_uamericanus = split_query_report['choffmanni_gene_ensembl']

    # No batches with the single query, two batches with the artificially lowered max_attribute_count limit.
    assert len(single_uamericanus['batches']) == 0
    assert len(split_uamericanus['batches']) == 2

    # Make sure we have the right counts in the reports returned by pull_ensembl().
    assert split_uamericanus['num_rows'] == single_uamericanus['num_rows']
    expected_attributes = set(single_uamericanus['attributes'])
    assert set(split_uamericanus['attributes']) == expected_attributes
    batched_attributes = {'ensembl_gene_id'}
    for batch in split_uamericanus['batches']:
        batched_attributes.update(batch['attributes'])
    assert batched_attributes == expected_attributes

    # 2. Whether the unsplit file is identical to the split file.
    unsplit_tsv = output_dir / 'choffmanni_gene_ensembl' / 'BioMart.tsv'
    split_tsv = download_as_splits / 'choffmanni_gene_ensembl' / 'BioMart.tsv'
    assert unsplit_tsv.exists()
    assert split_tsv.exists()
    with open(unsplit_tsv, 'r') as unsplit_file, open(split_tsv, 'r') as split_file:
        # So we can't compare these files directly, because rows with the same ensembl_gene_id shows up in an
        # undetermined order. So we need to load them, group them by ENSEMBL gene ID, and then compare those sets.
        unsplit_rows = list(read_biomart_file(unsplit_file))
        split_rows = list(read_biomart_file(split_file))
        assert len(unsplit_rows) == len(split_rows)
        assert unsplit_rows[0].keys() == split_rows[0].keys()

        # Confirm that the normalized lists of data dictionaries are the same.
        unsplit_rows_normalized = normalize_list_of_dictionaries(unsplit_rows)
        split_rows_normalized = normalize_list_of_dictionaries(split_rows)
        assert unsplit_rows_normalized == split_rows_normalized
