# Tests for datahandlers/ensembl.py

import logging
from src.datahandlers.ensembl import pull_ensembl

logging.basicConfig(level=logging.INFO)

def test_pull_ensembl(tmp_path):
    # Make a temporary directory for testing.
    output_dir = tmp_path / "pull_ensembl_test"
    output_dir.mkdir()

    # Pull a single ENSEMBL file to that. This should trigger https://github.com/TranslatorSRI/Babel/issues/193
    pull_ensembl(output_dir, output_dir / 'download_complete', ['hgfemale_gene_ensembl'])

    # Full list: ["elucius_gene_ensembl",
    # "hgfemale_gene_ensembl",
    # "charengus_gene_ensembl",
    # "otshawytscha_gene_ensembl",
    # "aocellaris_gene_ensembl",
    # "rnorvegicus_gene_ensembl"]
