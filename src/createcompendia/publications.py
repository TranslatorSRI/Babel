import gzip
import hashlib
import json
import os
import time
import xml.etree.ElementTree as ET
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from mmap import ACCESS_READ, mmap
from pathlib import Path

from src.babel_utils import WgetRecursionOptions, glom, pull_via_wget, read_identifier_file, write_compendium
from src.categories import JOURNAL_ARTICLE, PUBLICATION
from src.metadata.provenance import write_concord_metadata
from src.prefixes import DOI, PMC, PMID
from src.util import ensure_parent_dir, get_logger

logger = get_logger(__name__)

# The same PubMed corpus, reachable two ways. We need both: NCBI's robots.txt
# (https://ftp.ncbi.nlm.nih.gov/robots.txt) blocks a recursive HTTPS download, so the bulk fetch has
# to go over FTP — but FTP is the less reliable of the two, so the single-file re-downloads that
# repair a failed checksum use HTTPS.
PUBMED_FTP_BASE = "ftp://ftp.ncbi.nlm.nih.gov/pubmed/"
PUBMED_HTTPS_BASE = "https://ftp.ncbi.nlm.nih.gov/pubmed/"


def download_pubmed(download_file, pubmed_base=PUBMED_FTP_BASE, pmc_base="https://ftp.ncbi.nlm.nih.gov/pub/pmc/"):
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
    ensure_parent_dir(download_file)

    # Download baseline and updatefiles in parallel — each directory contains ~750 files,
    # so running them concurrently roughly halves wall time on the HPC.
    subdirs = ["baseline", "updatefiles"]

    def _download_subdir(subdir):
        pull_via_wget(
            pubmed_base,
            subdir,
            decompress=False,
            subpath="PubMed",
            recurse=WgetRecursionOptions.RECURSE_SUBFOLDERS,
            # A recursive download must be timestamped and must not resume: see pull_via_wget().
            timestamping=True,
            continue_incomplete=False,
        )

    with ThreadPoolExecutor(max_workers=len(subdirs)) as pool:
        futures = {pool.submit(_download_subdir, sd): sd for sd in subdirs}
        for future in as_completed(futures):
            subdir = futures[future]
            exc = future.exception()
            if exc:
                raise RuntimeError(f"Failed to download PubMed/{subdir}: {exc}") from exc
            logger.info(f"Finished downloading PubMed/{subdir}.")

    # Step 3. Download the PMC/PMID mapping file from PMC.
    # We don't actually use this file -- we currently only use the PMC IDs already included in the PubMed XML files.
    # pull_via_wget(pmc_base, 'PMC-ids.csv.gz', decompress=True, subpath='PubMed')

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
        logger.warning(f"Could not verify {pubmed_filename}: no such file or zero size.")
        return False

    # Calculate the MD5 sum of the PubMed file.
    with open(pubmed_filename, "rb") as pubmedf, mmap(pubmedf.fileno(), 0, access=ACCESS_READ) as file:
        md5hash = hashlib.md5(file.read()).hexdigest()

    # Read the expected MD5 checksum.
    if not os.path.exists(md5_filename):
        logger.warning(f"Could not verify {pubmed_filename}: no MD5 file found at {md5_filename}.")
        return False

    with open(md5_filename) as md5f:
        md5_line = md5f.readline().strip()
        expected_md5 = md5_line.split("= ")[1]
        if len(expected_md5) != 32:
            raise RuntimeError(
                f"Could not verify {pubmed_filename}: could not read MD5 hash from MD5 file {md5_filename}: '{md5_line}'"
            )

    if md5hash == expected_md5:
        return True
    logger.warning(
        f"Could not verify {pubmed_filename}: calculated MD5 hash {md5hash} is not equal to expected MD5 hash {expected_md5} in {md5_filename}."
    )
    return False


def verify_pubmed_downloads(pubmed_directories, done_filename, pubmed_base=PUBMED_HTTPS_BASE):
    """
    download_pubmed() does a good job of downloading all the PubMed files, but every once in a while the download
    is corrupted (https://github.com/NCATSTranslator/Babel/issues/352). Luckily, PubMed also gives up `.md5` files
    for all the downloaded files, so we can use those to verify each file. If a file is incorrect, we can re-download
    it using the more reliable HTTPS URL.

    :param pubmed_directories: A list of PubMed directories to verify.
    :param done_filename: A 'done' file to write once we're done.
    :param pubmed_base: The PubMed base URL to download files from.
    :return:
    """

    for pubmed_directory in pubmed_directories:
        # These directories aren't Snakemake outputs (see the comment on rule download_pubmed), so
        # Snakemake won't rebuild them if they go missing: say what to do instead of dying in
        # os.listdir() with a bare FileNotFoundError.
        if not os.path.isdir(pubmed_directory):
            raise RuntimeError(
                f"PubMed download directory {pubmed_directory} does not exist: delete the "
                f"'downloaded' marker file beside it to re-run download_pubmed."
            )

        for filename in os.listdir(pubmed_directory):
            file_path = os.path.join(pubmed_directory, filename)
            subdir = os.path.basename(pubmed_directory)

            if file_path.endswith(".gz"):
                md5_filename = filename + ".md5"
                md5_file_path = file_path + ".md5"

                while not verify_pubmed_download_against_md5(file_path, file_path + ".md5"):
                    pubmed_url_base = pubmed_base + subdir + "/"
                    logger.warning(f"Re-downloading {file_path} from HTTPS URL {pubmed_url_base}{filename}.")

                    # We should delete the files before redownloading them, but if we did that and the download
                    # failed, we would not retry the download again. So instead, we truncate both files -- this
                    # should cause MD5 verification to fail until they are in sync again.
                    if os.path.exists(file_path):
                        os.truncate(file_path, 0)
                    if os.path.exists(md5_file_path):
                        os.truncate(md5_file_path, 0)

                    # Redownload the file.
                    pull_via_wget(pubmed_url_base, filename, decompress=False, subpath="PubMed/" + subdir)
                    pull_via_wget(pubmed_url_base, md5_filename, decompress=False, subpath="PubMed/" + subdir)

    # We're all done!
    Path.touch(done_filename)


def parse_pubmed_into_tsvs(
    baseline_dir, updatefiles_dir, titles_file, status_file, pmid_id_file, pmid_doi_concord_file, metadata_yaml
):
    """
    Read through the PubMed files in the baseline_dir and updatefiles_dir, and writes out label and status information.

    :param baseline_dir: The PubMed baseline directory to parse.
    :param updatefiles_dir: The PubMed updatefiles directory to parse.
    :param titles_file: An output TSV file in the format `<PMID>\t<TITLE>`.
    :param status_file: A JSON file containing publication status information.
    :param pmid_doi_concord_file: A concord file in the format `<PMID>\teq\t<DOI>` and other identifiers.
    :param metadata_yaml: The metadata YAML file to write.
    """

    # We can write labels and concords as we go.
    with (
        open(titles_file, "w") as titlesf,
        open(pmid_id_file, "w") as pmidf,
        open(pmid_doi_concord_file, "w") as concordf,
    ):
        # Track PubMed article statuses. In theory the final PubMed entry should have all the dates, which should
        # tell us the final status of a publication, but really we just want to know if the article has ever been
        # marked as retracted, so instead we track every status that has ever been attached to any article. We
        # don't have a way of tracking properties yet (https://github.com/NCATSTranslator/Babel/issues/155), so for now
        # we write this out in JSON to the status_file.
        pmid_status = defaultdict(set)

        # Read every file in the baseline and updatefiles directories (they have the same format).
        baseline_filenames = list(map(lambda fn: os.path.join(baseline_dir, fn), sorted(os.listdir(baseline_dir))))
        updatefiles_filenames = list(
            map(lambda fn: os.path.join(updatefiles_dir, fn), sorted(os.listdir(updatefiles_dir)))
        )

        for pubmed_filename in baseline_filenames + updatefiles_filenames:
            if not pubmed_filename.endswith(".xml.gz"):
                logger.warning(f"Skipping non-gzipped-XML file {pubmed_filename} in PubMed files.")
                continue

            with gzip.open(pubmed_filename, "rt") as pubmedf:
                logger.info(f"Parsing PubMed Baseline {pubmed_filename}")

                start_time = time.time_ns()
                count_articles = 0
                count_pmids = 0
                count_dois = 0
                count_pmcs = 0
                count_titles = 0
                file_pubstatuses = set()

                # Read every XML entry from every PubMed file.
                #
                # iterparse pulls the stream in blocks itself. The previous version fed an
                # XMLPullParser one line at a time, which cost a parser call per line of a
                # ~30 MB file, and never released a parsed article -- every PubmedArticle
                # stayed reachable from the root for the whole file. The `start` event exists
                # only to capture that root so the loop can drop articles as it finishes them.
                context = ET.iterparse(pubmedf, events=("start", "end"))
                _, root = next(context)
                for event, elem in context:
                    if event == "end" and elem.tag == "PubmedArticle":
                        count_articles += 1

                        # Look for the pieces of information we want.
                        pmids = elem.findall("./PubmedData/ArticleIdList/ArticleId[@IdType='pubmed']")
                        dois = elem.findall("./PubmedData/ArticleIdList/ArticleId[@IdType='doi']")
                        pmcs = elem.findall("./PubmedData/ArticleIdList/ArticleId[@IdType='pmc']")
                        titles = elem.findall(".//ArticleTitle")

                        # Retrieve the PubDates containing PubStatuses.
                        pubdates_with_pubstatus = elem.findall("./PubmedData/History/PubMedPubDate[@PubStatus]")
                        pubstatuses = set()
                        for pubdate in pubdates_with_pubstatus:
                            # We ignore the dates, and instead record all the PubStatuses that a PMID has ever had.
                            if pubdate.get("PubStatus"):
                                pubstatuses.add(pubdate.get("PubStatus"))

                        # Write information for each PMID.
                        for pmid in pmids:
                            count_pmids += 1

                            # Write out PMID type.
                            pmidf.write(f"{PMID}:{pmid.text}\t{JOURNAL_ARTICLE}\n")

                            # Update PMID status.
                            pmid_status[f"{PMID}:" + pmid.text].update(pubstatuses)
                            file_pubstatuses.update(pubstatuses)

                            # Write out the titles.
                            for title in titles:
                                count_titles += 1
                                # Convert newlines into '\n'.
                                title_text = title.text
                                if not title_text:
                                    continue
                                title_text = title_text.replace("\n", "\\n")

                                titlesf.write(f"{PMID}:{pmid.text}\t{title_text}\n")

                            # Write out the DOIs to the concords file.
                            for doi in dois:
                                count_dois += 1
                                concordf.write(f"{PMID}:{pmid.text}\teq\t{DOI}:{doi.text}\n")

                            # Write out the PMCIDs to the concords file.
                            for pmc in pmcs:
                                count_pmcs += 1
                                concordf.write(f"{PMID}:{pmid.text}\teq\t{PMC}:{pmc.text}\n")

                        # Done with this article: drop it from the root so the parsed tree
                        # does not grow to hold the whole file. PubmedArticle is a direct
                        # child of PubmedArticleSet, so clearing the root releases every
                        # article finished so far.
                        root.clear()

                time_taken_in_seconds = float(time.time_ns() - start_time) / 1_000_000_000
                logger.info(
                    f"Parsed {count_articles} articles from PubMed {pubmed_filename} in "
                    + f"{time_taken_in_seconds:.4f} seconds: {count_pmids} PMIDs, {count_dois} DOIs, "
                    + f"{count_pmcs} PMCs, "
                    + f"{count_titles} titles with the following PubStatuses: {sorted(file_pubstatuses)}."
                )

    # Write the statuses into a gzipped JSONL file.
    with gzip.open(status_file, "wt") as statusf:
        # This will be more readable as a JSONL file, so let's write it out that way.
        for pmid, statuses in pmid_status.items():
            statusf.write(json.dumps({"id": pmid, "statuses": sorted(statuses)}, sort_keys=True) + "\n")

    write_concord_metadata(
        metadata_yaml,
        name="parse_pubmed_into_tsvs()",
        description="Parse PubMed files into TSVs and JSONL status files.",
        sources=[
            {"type": "download", "name": "PubMed Baseline", "url": f"{PUBMED_FTP_BASE}baseline/"},
            {"type": "download", "name": "PubMed Updates", "url": f"{PUBMED_FTP_BASE}updatefiles/"},
        ],
        counts={
            "pmid_count": len(pmid_status.keys()),
        },
        concord_filename=pmid_doi_concord_file,
    )


def generate_compendium(concordances, metadata_yamls, identifiers, titles, publication_compendium, icrdf_filename):
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
        print("loading", ifile)
        new_identifiers, new_types = read_identifier_file(ifile)
        glom(dicts, new_identifiers, unique_prefixes=uniques)
        types.update(new_types)

    # Load concordances.
    for infile in concordances:
        print(infile)
        print("loading", infile)
        pairs = []
        with open(infile) as inf:
            for line in inf:
                x = line.strip().split("\t")
                pairs.append({x[0], x[2]})
        glom(dicts, pairs, unique_prefixes=uniques)

    # Publications have titles, not labels. We load them here.
    labels = dict()
    for title_filename in titles:
        print("loading titles from", title_filename)
        with open(title_filename) as titlef:
            for line in titlef:
                id, title = line.strip().split("\t")
                if id in labels:
                    # Don't emit a warning unless the title has actually changed.
                    if labels[id] != title:
                        logger.warning(
                            f"Duplicate title for {id}: ignoring previous title '{labels[id]}', using new title '{title}'."
                        )
                labels[id] = title

    # Write out the compendium.
    publication_sets = set([frozenset(x) for x in dicts.values()])
    write_compendium(
        metadata_yamls,
        publication_sets,
        os.path.basename(publication_compendium),
        PUBLICATION,
        labels,
        icrdf_filename=icrdf_filename,
    )
