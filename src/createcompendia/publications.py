import gzip
import json
import os
from collections import defaultdict
from pathlib import Path
import xml.etree.ElementTree as ET

from src.babel_utils import pull_via_wget, WgetRecursionOptions


def download_pubmed(download_file,
                    pubmed_base='ftp://ftp.ncbi.nlm.nih.gov/pubmed/',
                    pmc_base='https://ftp.ncbi.nlm.nih.gov/pub/pmc/'):
    """
    Download PubMed. We download both the PubMed annual baseline and the daily update files.

    :param download_file: A `done` file that should be created to indicate that we are done.
    :param pubmed_base: The PubMed base URL to download files from.
    """

    # Create directories if they don't exist.
    os.makedirs(os.path.dirname(download_file), exist_ok=True)

    # Step 1. Download all the files for the PubMed annual baseline.
    pull_via_wget(
        pubmed_base, 'baseline',
        decompress=False,
        subpath='PubMed',
        recurse=WgetRecursionOptions.RECURSE_SUBFOLDERS)

    # Step 2. Download all the files for the PubMed update files.
    pull_via_wget(pubmed_base, 'updatefiles',
        decompress=False,
        subpath='PubMed',
        recurse=WgetRecursionOptions.RECURSE_SUBFOLDERS)

    # Step 3. Download the PMC/PMID mapping file from PMC.
    pull_via_wget(pmc_base, 'PMC-ids.csv.gz', decompress=True, subpath='PubMed')

    # We're all done!
    Path.touch(download_file)


def parse_pubmed_into_tsvs(baseline_dir, updatefiles_dir, titles_file, status_file, pmid_doi_concord_file):
    """
    Read through the PubMed files in the baseline_dir and updatefiles_dir, and writes out label and status information.

    :param baseline_dir: The PubMed baseline directory to parse.
    :param updatefiles_dir: The PubMed updatefiles directory to parse.
    :param titles_file: An output TSV file in the format `<PMID>\t<TITLE>`.
    :param status_file: A TSV file in the format `<PMID>\t<status>`, where status tells us if the publication was retracted etc.
    :param pmid_doi_concord_file: A concord file in the format `<PMID>\teq\t<DOI>` and other identifiers.
    """

    # We can write labels and concords as we go.
    with open(titles_file, 'w') as titlesf, open(pmid_doi_concord_file, 'w') as concordf:
        # However, we will need to track statuses in memory.
        pmid_status = defaultdict(str)

        # Read every file in the baseline directory.
        for baseline_filename in os.listdir(baseline_dir):
            if baseline_filename.endswith(".xml.gz"):
                file_path = os.path.join(baseline_dir, baseline_filename)
                with gzip.open(file_path, 'rt') as baselinef:
                    parser = ET.XMLPullParser(['end'])
                    for line in baselinef:
                        parser.feed(line)
                        for event, elem in parser.read_events():
                            if event == 'end' and elem.tag == 'PubmedArticle':
                                pmids = elem.findall("//PubmedData/ArticleIdList/ArticleId[@IdType='pubmed']")
                                dois = elem.findall("//PubmedData/ArticleIdList/ArticleId[@IdType='doi']")
                                pub_statuses = elem.findall("//PubmedData/PublicationStatus")
                                titles = elem.findall('.//ArticleTitle')

                                # Write concord.
                                for pmid in pmids:
                                    for pub_status in pub_statuses:
                                        pmid_status['PMID:' + pmid] = pub_status

                                    for title in titles:
                                        # Convert newlines into '\n'.
                                        title = title.replace('\n', '\\n')

                                        titlesf.write(f"PMID:{pmid}\t{title}\n")

                                    for doi in dois:
                                        concordf.write(f"PMID:{pmid}\teq\tdoi:{doi}\n")

    with open(status_file, 'w') as statusf:
        json.dump(pmid_status, statusf, indent=2, sort_keys=True)



