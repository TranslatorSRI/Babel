import os
from pathlib import Path

from src.babel_utils import pull_via_wget, WgetRecursionOptions


def download_pubmed(done_file, pubmed_base='https://ftp.ncbi.nlm.nih.gov/pubmed/'):
    """
    Download PubMed. We download both the PubMed annual baseline and the daily update files.

    :param done_file: A `done` file that should be created to indicate that we are done.
    :param pubmed_base: The PubMed base URL to download files from.
    """

    # Create directories if they don't exist.
    os.makedirs(os.path.dirname(done_file), exist_ok=True)

    # Step 1. Download all the files for the PubMed annual baseline.
    pull_via_wget(
        pubmed_base, 'baseline',
        decompress=False,
        subpath='PubMed',
        recurse=WgetRecursionOptions.RECURSE_DIRECTORY_ONLY)

    # We're all done!
    Path.touch(done_file)
