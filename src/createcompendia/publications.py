"""
Build the Publication compendium from a pubmed2db NDJSON export.

Babel used to download and parse the PubMed XML baseline and update files itself (~1,500 files, 20 hours
single-threaded in 2026jul22). That work now happens in pubmed2db (https://github.com/TranslatorSRI/pubmed2db),
which loads PubMed into DuckDB and exports one NDJSON record per PMID -- latest version only, deleted PMIDs
removed -- as gzipped shards named ``pubmed_metadata_*.ndjson.gz``, with a ``validation_report.json.gz`` beside
them. Babel downloads the export pinned by ``config.yaml: pubmed2db_url`` and reads just three fields from each
record: ``id`` (``PMID:...``), ``identifiers`` (the PMID plus its ``doi:`` and PubMed Central CURIEs) and
``article_title``. See docs/sources/PubMed/README.md for the contract Babel relies on.

Because every record already carries its complete identifier set, the compendium is written straight from
the shards without glom(): a record *is* a clique. The one thing that needs a corpus-wide view is a DOI or
PMCID that appears in more than one record -- see ``parse_pubmed2db_into_tsvs()`` for how that is resolved.
"""

import glob
import gzip
import json
import os
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

from src.babel_utils import TypedClique, WgetRecursionOptions, pull_via_wget, write_compendium
from src.categories import JOURNAL_ARTICLE, PUBLICATION
from src.metadata.provenance import write_concord_metadata
from src.util import ensure_parent_dir, get_logger

logger = get_logger(__name__)

# The shards and report that a pubmed2db export directory is expected to contain.
SHARD_GLOB = "pubmed_metadata_*.ndjson.gz"
VALIDATION_REPORT = "validation_report.json.gz"


def download_pubmed2db(url, download_dir, done_file):
    """
    Download every file in a pubmed2db export directory (the NDJSON shards and the validation report).

    ``download_dir`` is deliberately not a Snakemake ``directory()`` output, so shards carried over from a
    previous run survive and ``wget --timestamping`` skips any whose size and mtime are unchanged.

    :param url: The export directory URL (``config.yaml: pubmed2db_url``), ending in ``/``.
    :param download_dir: The local directory to download into.
    :param done_file: Marker file to touch once the download is complete.
    """
    pull_via_wget(
        url,
        "",
        decompress=False,
        outpath=download_dir,
        recurse=WgetRecursionOptions.RECURSE_DIRECTORY_ONLY,
        # A recursive download must be timestamped and must not resume: see pull_via_wget().
        timestamping=True,
        continue_incomplete=False,
    )
    shards = shard_paths(download_dir)
    report = os.path.join(download_dir, VALIDATION_REPORT)
    if not shards or not os.path.exists(report):
        raise RuntimeError(
            f"pubmed2db export at {url} downloaded into {download_dir} without {SHARD_GLOB} shards and a "
            f"{VALIDATION_REPORT}: found {len(shards)} shard(s), report exists={os.path.exists(report)}."
        )
    logger.info(f"Downloaded {len(shards)} pubmed2db shards from {url} into {download_dir}.")
    ensure_parent_dir(done_file)
    Path(done_file).touch()


def shard_paths(download_dir):
    """The NDJSON shards in a pubmed2db export directory, in a fixed (sorted) order."""
    return sorted(glob.glob(os.path.join(download_dir, SHARD_GLOB)))


def expected_record_count(download_dir):
    """
    The record count pubmed2db's validator saw when it checked this export, and a guard against a failed one.

    The validation report is pubmed2db's statement of what the export should contain; it is what lets us
    tell a truncated or partially-downloaded shard from a complete one, since no per-shard checksum is
    published. Raises if the report's overall status is ``fail``.
    """
    with gzip.open(os.path.join(download_dir, VALIDATION_REPORT), "rt") as f:
        report = json.load(f)
    if report.get("status") == "fail":
        raise RuntimeError(
            f"pubmed2db validation report in {download_dir} has status 'fail': {report.get('errors')}. "
            "Refusing to build a compendium from an export its own validator rejected."
        )
    for check in report["checks_run"]:
        if check["name"] == "records-present":
            # e.g. "40,923,261 record(s)"
            return int(check["observed"].split()[0].replace(",", ""))
    raise RuntimeError(f"pubmed2db validation report in {download_dir} has no 'records-present' check.")


def _parse_shard(shard_path):
    """
    Parse one shard into ``(pmid, title, other_identifiers)`` tuples.

    This is the only place that knows the export's record format. ``identifiers`` always starts with the
    record's own PMID, so ``identifiers[1:]`` is its DOI and PMCID CURIEs (sorted and de-duplicated by
    pubmed2db). Module-level so ProcessPoolExecutor can pickle it.
    """
    records = []
    with gzip.open(shard_path, "rt") as f:
        for line in f:
            record = json.loads(line)
            pmid = record["id"]
            identifiers = record["identifiers"]
            if identifiers[0] != pmid:
                raise ValueError(
                    f"{shard_path}: record {pmid} has identifiers {identifiers} not starting with its PMID."
                )
            records.append((pmid, record["article_title"], identifiers[1:]))
    logger.info(f"Parsed {len(records):,} records from {shard_path}.")
    return records


def iter_shard_records(download_dir, workers):
    """
    Yield ``(pmid, title, other_identifiers)`` for every record in the export, shard by shard in sorted order.

    Shards are parsed in parallel; at most ``workers`` shards' worth of tuples are held at once.
    """
    shards = shard_paths(download_dir)
    if not shards:
        raise RuntimeError(f"No {SHARD_GLOB} shards found in {download_dir}.")
    with ProcessPoolExecutor(max_workers=workers) as pool:
        for records in pool.map(_parse_shard, shards):
            yield from records


def parse_pubmed2db_into_tsvs(
    download_dir, titles_file, pmid_id_file, concord_file, shared_ids_file, metadata_yaml, url, workers
):
    """
    Write the ids, titles and concords TSVs from a pubmed2db export, and resolve shared identifiers.

    The three TSVs keep the formats of the old XML parser so build-vs-build diffs stay meaningful:

    - ``pmid_id_file``: ``PMID:x\\tbiolink:JournalArticle``
    - ``titles_file``: ``PMID:x\\t<title>`` with newlines escaped as ``\\n``; empty titles skipped
    - ``concord_file``: ``PMID:x\\teq\\t<doi: or PMCID: CURIE>``

    A DOI or PMCID can appear in more than one record (PubMed publishes them as-is). Two cliques must never
    share an identifier, so ``shared_ids_file`` records each such identifier with the *lowest* PMID that
    carries it, and ``generate_compendium()`` keeps it only there. The old ``glom(unique_prefixes=[PMID])``
    silently ignored a concord that would have joined two PMIDs, so the first-seen PMID kept the identifier;
    lowest-PMID is the same policy made independent of shard order. The file doubles as a data-quality report
    of ambiguous identifiers.

    Raises if the number of records parsed differs from the count in pubmed2db's validation report.

    :param url: The export URL, recorded in the metadata YAML as the source of this build's PubMed data.
    """
    for path in (titles_file, pmid_id_file, concord_file, shared_ids_file):
        ensure_parent_dir(path)

    expected = expected_record_count(download_dir)
    count_records = 0
    count_concords = 0
    seen = set()
    duplicated = set()

    with (
        open(titles_file, "w") as titlesf,
        open(pmid_id_file, "w") as pmidf,
        open(concord_file, "w") as concordf,
    ):
        for pmid, title, others in iter_shard_records(download_dir, workers):
            count_records += 1
            pmidf.write(f"{pmid}\t{JOURNAL_ARTICLE}\n")
            if title:
                title_text = title.replace("\n", "\\n")
                titlesf.write(f"{pmid}\t{title_text}\n")
            for other in others:
                count_concords += 1
                concordf.write(f"{pmid}\teq\t{other}\n")
                if other in seen:
                    duplicated.add(other)
                else:
                    seen.add(other)
    del seen

    # Resolve each duplicated identifier to its lowest PMID with a second pass over the (much smaller)
    # concord file, rather than holding a PMID for all ~46M identifiers during the first pass.
    # identifier -> (lowest PMID carrying it, number of records carrying it).
    shared = {}
    if duplicated:
        with open(concord_file) as concordf:
            for line in concordf:
                pmid, _, other = line.rstrip("\n").split("\t")
                if other in duplicated:
                    winner, n = shared.get(other, (None, 0))
                    shared[other] = (_lower_pmid(winner, pmid), n + 1)

    if count_records != expected:
        raise RuntimeError(
            f"Parsed {count_records:,} records from {download_dir} but its validation report says "
            f"{expected:,}: a shard is probably truncated or missing. Delete the bad shard and re-run download."
        )

    with open(shared_ids_file, "w") as sharedf:
        for other in sorted(shared):
            winner, n = shared[other]
            sharedf.write(f"{other}\t{winner}\t{n}\n")
    logger.info(
        f"Parsed {count_records:,} records and {count_concords:,} concords; {len(shared):,} identifiers are "
        f"shared by more than one PMID (see {shared_ids_file})."
    )

    write_concord_metadata(
        metadata_yaml,
        name="parse_pubmed2db_into_tsvs()",
        description="Parse a pubmed2db NDJSON export into id, title and concord TSVs.",
        url=url,
        sources=[{"type": "download", "name": "pubmed2db NDJSON export", "url": url}],
        counts={
            "pmid_count": count_records,
            "shard_count": len(shard_paths(download_dir)),
            "shared_identifier_count": len(shared),
        },
        concord_filename=concord_file,
    )


def _lower_pmid(a, b):
    """The numerically lower of two ``PMID:n`` CURIEs (``None`` loses)."""
    if a is None:
        return b
    return a if int(a.split(":")[1]) <= int(b.split(":")[1]) else b


class _PublicationCliques:
    """A sized iterable of TypedCliques streamed from the export, so write_compendium() can log progress."""

    def __init__(self, download_dir, winners, workers):
        self.download_dir = download_dir
        self.winners = winners
        self.workers = workers
        self.count = expected_record_count(download_dir)

    def __len__(self):
        return self.count

    def __iter__(self):
        for pmid, title, others in iter_shard_records(self.download_dir, self.workers):
            identifiers = [pmid] + [o for o in others if self.winners.get(o, pmid) == pmid]
            yield TypedClique(PUBLICATION, identifiers, {pmid: title} if title else None)


def generate_compendium(download_dir, shared_ids_file, metadata_yamls, publication_compendium, icrdf_filename, workers):
    """
    Write the Publication compendium straight from the pubmed2db shards: one record, one clique.

    :param shared_ids_file: Output of parse_pubmed2db_into_tsvs(); a shared identifier stays only in the
        PMID named there.
    """
    winners = {}
    with open(shared_ids_file) as sharedf:
        for line in sharedf:
            other, winner, _ = line.rstrip("\n").split("\t")
            winners[other] = winner
    logger.info(f"Loaded {len(winners):,} shared identifiers from {shared_ids_file}.")

    write_compendium(
        metadata_yamls,
        _PublicationCliques(download_dir, winners, workers),
        os.path.basename(publication_compendium),
        None,
        # The export spells PubMed Central ids PMC:PMC123 up to 2026aug5 and PMCID:PMC123 afterwards, and
        # Babel passes whichever it gets straight through. Biolink registers PMC on biolink:Publication
        # but PMCID only on biolink:JournalArticle, so without this create_node() would silently drop
        # every PMCID. Which prefix Translator should use is
        # https://github.com/NCATSTranslator/Babel/issues/1044 -- nothing else in Babel spells it out.
        extra_prefixes=["PMCID"],
        icrdf_filename=icrdf_filename,
    )
