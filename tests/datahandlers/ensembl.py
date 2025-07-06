# Tests for datahandlers/ensembl.py

import logging
import os

from src.datahandlers.ensembl import pull_ensembl

logging.basicConfig(level=logging.INFO)

def test_pull_ensembl(tmp_path):
    # Make a temporary directory for testing.
    pull_ensembl_test_dir = tmp_path / "pull_ensembl_test"
    output_dir = pull_ensembl_test_dir / "download"
    os.makedirs(output_dir)

    # Pull a single ENSEMBL file to that. This should trigger https://github.com/TranslatorSRI/Babel/issues/193
    pull_ensembl(output_dir, output_dir / 'download_complete', ['uamericanus_gene_ensembl', 'hgfemale_gene_ensembl'])

    # uamericanus_gene_ensembl should be downloadable as a single file in the above example, but we're going to
    # deliberately download it in multiple chunks so it's clearer.
    download_as_splits = pull_ensembl_test_dir / "download_as_splits"
    os.makedirs(download_as_splits)
    pull_ensembl(download_as_splits, download_as_splits / 'download_complete', ['uamericanus_gene_ensembl'], max_attribute_count=4)

    # We need to check three things:
    # 1. Whether the unsplit file is identical to the split file.
    unsplit_tsv = output_dir / 'uamericanus_gene_ensembl' / 'BioMart.tsv'
    split_tsv = download_as_splits / 'uamericanus_gene_ensembl' / 'BioMart.tsv'
    assert unsplit_tsv.exists()
    assert split_tsv.exists()
    with open(unsplit_tsv, 'r') as unsplit_file, open(split_tsv, 'r') as split_file:
        assert unsplit_file.read() == split_file.read()

    # 2. Whether the split files were actually split... but how?
    # TODO
