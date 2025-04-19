import gzip
import hashlib
import json
import logging
import os
import time
from collections import defaultdict
from mmap import ACCESS_READ, mmap
from pathlib import Path
import xml.etree.ElementTree as ET

from src.babel_utils import pull_via_wget, WgetRecursionOptions, glom, read_identifier_file, write_compendium
from src.categories import JOURNAL_ARTICLE, PUBLICATION
from src.prefixes import PMID, DOI, PMC


def download_pubmed(download_file,
                    pubmed_base='ftp://ftp.ncbi.nlm.nih.gov/pubmed/',
                    pmc_base='https://ftp.ncbi.nlm.nih.gov/pub/pmc/'):
    """
    Download PubMed. We download both the PubMed annual baseline and the daily update files,
    which are in the same format, but the baseline is set up at the start of the year and then
    updates are included in the daily update files.

    We would love to use HTTPS to do the downloads, but unfortunately the robots.txt
    (https://ftp.ncbi.nlm.nih.gov/robots.txt) prevents this from working recursively.

    :param download_file: A `done` file that should be created to indicate that we are done.
    :param pubmed_base: The PubMed base URL to download files from.
    :param pmc_base: The PubMed Central base URL to download files from.
    """

    # Create directories if they don't exist.
    os.makedirs(os.path.dirname(download_file), exist_ok=True)

    # Step 1. Download all the files for the PubMed annual baseline.
    pull_via_wget(
        pubmed_base, 'baseline',
        decompress=False,
        subpath='PubMed',
        recurse=WgetRecursionOptions.RECURSE_SUBFOLDERS,
        timestamping=True)

    # Step 2. Download all the files for the PubMed update files.
    pull_via_wget(
        pubmed_base, 'updatefiles',
        decompress=False,
        subpath='PubMed',
        recurse=WgetRecursionOptions.RECURSE_SUBFOLDERS,
        timestamping=True)

    # Step 3. Download the PMC/PMID mapping file from PMC.
    pull_via_wget(pmc_base, 'PMC-ids.csv.gz', decompress=True, subpath='PubMed')

    # We're all done!
    Path.touch(download_file)


def verify_pubmed_download_against_md5(pubmed_filename, md5_filename):
    """
    Verify a single PubMed download file against its MD5 checksum in the `.md5` file.

    :param pubmed_filename: The PubMed download file to verify.
    :param md5_filename: The MD5 checksum file to verify against.
    :return: True if the file is verified, but False if it is not verified.
    """

    # Is the PubMed file readable? Non-zero size?
    if not os.path.exists(pubmed_filename) or os.path.getsize(pubmed_filename) == 0:
        logging.warning(f"Could not verify {pubmed_filename}: no such file or zero size.")
        return False

    # Calculate the MD5 sum of the PubMed file.
    with open(pubmed_filename, 'rb') as pubmedf, mmap(pubmedf.fileno(), 0, access=ACCESS_READ) as file:
        md5hash = hashlib.md5(file.read()).hexdigest()

    # Read the expected MD5 checksum.
    if not os.path.exists(md5_filename):
        logging.warning(f"Could not verify {pubmed_filename}: no MD5 file found at {md5_filename}.")
        return False

    with open(md5_filename, 'r') as md5f:
        md5_line = md5f.readline().strip()
        expected_md5 = md5_line.split('= ')[1]
        if len(expected_md5) != 32:
            raise RuntimeError(f"Could not verify {pubmed_filename}: could not read MD5 hash from MD5 file {md5_filename}: '{md5_line}'")

    if md5hash == expected_md5:
        return True
    logging.warning(f"Could not verify {pubmed_filename}: calculated MD5 hash {md5hash} is not equal to expected MD5 hash {expected_md5} in {md5_filename}.")
    return False


def verify_pubmed_downloads(pubmed_directories,
                            done_filename,
                            pubmed_base='https://ftp.ncbi.nlm.nih.gov/pubmed/'):
    """
    download_pubmed() does a good job of downloading all the PubMed files, but every once in a while the download
    is corrupted (https://github.com/TranslatorSRI/Babel/issues/352). Luckily, PubMed also gives up `.md5` files
    for all the downloaded files, so we can use those to verify each file. If a file is incorrect, we can re-download
    it using the more reliable HTTPS URL.

    :param pubmed_directories: A list of PubMed directories to verify.
    :param done_filename: A 'done' file to write once we're done.
    :param pubmed_base: The PubMed base URL to download files from.
    :return:
    """

    for pubmed_directory in pubmed_directories:
        for filename in os.listdir(pubmed_directory):
            file_path = os.path.join(pubmed_directory, filename)
            subdir = os.path.basename(pubmed_directory)

            if file_path.endswith('.gz'):
                md5_filename = filename + '.md5'
                md5_file_path = file_path + '.md5'

                while not verify_pubmed_download_against_md5(file_path, file_path + '.md5'):
                    pubmed_url_base = pubmed_base + subdir + '/'
                    logging.warning(f"Re-downloading {file_path} from HTTPS URL {pubmed_url_base}{filename}.")
                    if os.path.exists(file_path):
                        os.remove(file_path)
                    if os.path.exists(md5_file_path):
                        os.remove(md5_file_path)
                    pull_via_wget(pubmed_url_base, filename, decompress=False, subpath='PubMed/' + subdir)
                    pull_via_wget(pubmed_url_base, md5_filename, decompress=False, subpath='PubMed/' + subdir)

    # We're all done!
    Path.touch(done_filename)


def parse_pubmed_into_tsvs(baseline_dir, updatefiles_dir, titles_file, status_file, pmid_id_file,
                           pmid_doi_concord_file):
    """
    Read through the PubMed files in the baseline_dir and updatefiles_dir, and writes out label and status information.

    :param baseline_dir: The PubMed baseline directory to parse.
    :param updatefiles_dir: The PubMed updatefiles directory to parse.
    :param titles_file: An output TSV file in the format `<PMID>\t<TITLE>`.
    :param status_file: A JSON file containing publication status information.
    :param pmid_doi_concord_file: A concord file in the format `<PMID>\teq\t<DOI>` and other identifiers.
    """

    # We can write labels and concords as we go.
    with open(titles_file, 'w') as titlesf, open(pmid_id_file, 'w') as pmidf, open(pmid_doi_concord_file,
                                                                                   'w') as concordf:
        # Track PubMed article statuses. In theory the final PubMed entry should have all the dates, which should
        # tell us the final status of a publication, but really we just want to know if the article has ever been
        # marked as retracted, so instead we track every status that has ever been attached to any article. We
        # don't have a way of tracking properties yet (https://github.com/TranslatorSRI/Babel/issues/155), so for now
        # we write this out in JSON to the status_file.
        pmid_status = defaultdict(set)

        # Read every file in the baseline and updatefiles directories (they have the same format).
        baseline_filenames = list(map(lambda fn: os.path.join(baseline_dir, fn), sorted(os.listdir(baseline_dir))))
        updatefiles_filenames = list(
            map(lambda fn: os.path.join(updatefiles_dir, fn), sorted(os.listdir(updatefiles_dir))))

        for pubmed_filename in (baseline_filenames + updatefiles_filenames):
            if not pubmed_filename.endswith(".xml.gz"):
                logging.warning(f"Skipping non-gzipped-XML file {pubmed_filename} in PubMed files.")
                continue

            with gzip.open(pubmed_filename, 'rt') as pubmedf:
                logging.info(f"Parsing PubMed Baseline {pubmed_filename}")

                start_time = time.time_ns()
                count_articles = 0
                count_pmids = 0
                count_dois = 0
                count_pmcs = 0
                count_titles = 0
                file_pubstatuses = set()

                # Read every XML entry from every PubMed file.
                parser = ET.XMLPullParser(['end'])
                for line in pubmedf:
                    parser.feed(line)
                    for event, elem in parser.read_events():
                        if event == 'end' and elem.tag == 'PubmedArticle':
                            count_articles += 1

                            # Look for the pieces of information we want.
                            pmids = elem.findall("./PubmedData/ArticleIdList/ArticleId[@IdType='pubmed']")
                            dois = elem.findall("./PubmedData/ArticleIdList/ArticleId[@IdType='doi']")
                            pmcs = elem.findall("./PubmedData/ArticleIdList/ArticleId[@IdType='pmc']")
                            titles = elem.findall('.//ArticleTitle')

                            # Retrieve the PubDates containing PubStatuses.
                            pubdates_with_pubstatus = elem.findall("./PubmedData/History/PubMedPubDate[@PubStatus]")
                            pubstatuses = set()
                            for pubdate in pubdates_with_pubstatus:
                                # We ignore the dates, and instead record all the PubStatuses that a PMID has ever had.
                                if pubdate.get('PubStatus'):
                                    pubstatuses.add(pubdate.get('PubStatus'))

                            # Write information for each PMID.
                            for pmid in pmids:
                                count_pmids += 1

                                # Write out PMID type.
                                pmidf.write(f"{PMID}:{pmid.text}\t{JOURNAL_ARTICLE}\n")

                                # Update PMID status.
                                pmid_status[f'{PMID}:' + pmid.text].update(pubstatuses)
                                file_pubstatuses.update(pubstatuses)

                                # Write out the titles.
                                for title in titles:
                                    count_titles += 1
                                    # Convert newlines into '\n'.
                                    title_text = title.text
                                    if not title_text:
                                        continue
                                    title_text = title_text.replace('\n', '\\n')

                                    titlesf.write(f"{PMID}:{pmid.text}\t{title_text}\n")

                                # Write out the DOIs to the concords file.
                                for doi in dois:
                                    count_dois += 1
                                    concordf.write(f"{PMID}:{pmid.text}\teq\t{DOI}:{doi.text}\n")

                                # Write out the PMCIDs to the concords file.
                                for pmc in pmcs:
                                    count_pmcs += 1
                                    concordf.write(f"{PMID}:{pmid.text}\teq\t{PMC}:{pmc.text}\n")

                time_taken_in_seconds = float(time.time_ns() - start_time) / 1_000_000_000
                logging.info(
                    f"Parsed {count_articles} articles from PubMed {pubmed_filename} in " +
                    f"{time_taken_in_seconds:.4f} seconds: {count_pmids} PMIDs, {count_dois} DOIs, " +
                    f"{count_pmcs} PMCs, " +
                    f"{count_titles} titles with the following PubStatuses: {sorted(file_pubstatuses)}.")

    # Write the statuses into a gzipped JSONL file.
    with gzip.open(status_file, 'wt') as statusf:
        # This will be more readable as a JSONL file, so let's write it out that way.
        for pmid, statuses in pmid_status.items():
            statusf.write(json.dumps({'id': pmid, 'statuses': sorted(statuses)}, sort_keys=True) + '\n')


def generate_compendium(concordances, identifiers, titles, publication_compendium, icrdf_filename):
    """
    Generate a Publication compendium using the ID and Concord files provided.

    :param concordances: A list of concordances to use.
    :param identifiers: A list of identifiers to use.
    :param publication_compendium: The publication concord file to produce.
    :param icrdf_filename: The ICRDF file.
    """

    dicts = {}
    types = {}
    uniques = [PMID]

    # Load PMID identifiers.
    for ifile in identifiers:
        print('loading', ifile)
        new_identifiers, new_types = read_identifier_file(ifile)
        glom(dicts, new_identifiers, unique_prefixes=uniques)
        types.update(new_types)

    # Load concordances.
    for infile in concordances:
        print(infile)
        print('loading', infile)
        pairs = []
        with open(infile, 'r') as inf:
            for line in inf:
                x = line.strip().split('\t')
                pairs.append({x[0], x[2]})
        glom(dicts, pairs, unique_prefixes=uniques)

    # Publications have titles, not labels. We load them here.
    labels = dict()
    for title_filename in titles:
        print('loading titles from', title_filename)
        with open(title_filename, 'r') as titlef:
            for line in titlef:
                id, title = line.strip().split('\t')
                if id in labels:
                    # Don't emit a warning unless the title has actually changed.
                    if labels[id] != title:
                        logging.warning(
                            f"Duplicate title for {id}: ignoring previous title '{labels[id]}', using new title '{title}'.")
                labels[id] = title

    # Write out the compendium.
    publication_sets = set([frozenset(x) for x in dicts.values()])
    baretype = PUBLICATION.split(':')[-1]
    write_compendium(publication_sets, os.path.basename(publication_compendium), PUBLICATION, labels,
                     icrdf_filename=icrdf_filename)
