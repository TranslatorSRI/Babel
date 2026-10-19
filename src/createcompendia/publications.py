"""
Build the Publication compendium from a pubmed2db NDJSON export.

Babel used to download and parse the PubMed XML baseline and update files itself (~1,500 files, 20 hours
single-threaded in 2026jul22). That work now happens in pubmed2db (https://github.com/TranslatorSRI/pubmed2db),
which loads PubMed into DuckDB and exports one NDJSON record per PMID -- latest version only, deleted PMIDs
removed -- as gzipped ``pubmed_metadata_*.ndjson.gz`` shards, with a validation report that lists every shard
and how many records it holds. Babel downloads the export pinned by ``config.yaml: pubmed2db_version`` and
reads just three fields from each record: ``id`` (``PMID:...``), ``identifiers`` (the PMID plus its ``doi:``
and PubMed Central CURIEs) and ``article_title``.

**Whenever you change this module, check https://stars.renci.org/var/babel_outputs/pubmed2db/ for a newer
export** and move the pubmed2db settings at the top of ``config.yaml`` to it. The export's layout has changed
between versions before (shard count and naming, where the validation report lives), so read the new
validation report against docs/sources/PubMed/README.md, which describes the contract this module relies on.

Because every record already carries its complete identifier set, the compendium is written straight from
the shards without glom(): a record *is* a clique. The one thing that needs a corpus-wide view is a DOI or
PMCID that appears in more than one record -- see ``parse_pubmed2db_into_tsvs()`` for how that is resolved.
"""

import gzip
import json
import os
import shutil
import tempfile
import zlib
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

from src.babel_utils import TypedClique, pull_via_wget, write_compendium
from src.categories import JOURNAL_ARTICLE, PUBLICATION
from src.metadata.provenance import write_concord_metadata
from src.util import curie_format_problem, ensure_parent_dir, get_logger

logger = get_logger(__name__)

# The local name of the export's validation report, wherever the export keeps it (config.yaml:
# pubmed2db_validation_report) and whether or not it is gzipped there.
VALIDATION_REPORT = "validation_report.json"


def download_pubmed2db(export_url, report_path, download_dir, done_file):
    """
    Download a pubmed2db export: its validation report, then exactly the shards that report lists.

    The report doubles as the export's manifest, so we never crawl the export directory -- since 2026sep26
    it also holds pubmed2db's own ~76 GB DuckDB database and a ``manifests/`` subdirectory, which a recursive
    wget would fetch.

    ``download_dir`` belongs to a single export version, and a published export is never modified, so a
    shard left half-downloaded by an interrupted run is safely resumed (``wget --continue``) and a complete
    one is left alone. That is also what makes carrying ``download_dir`` forward into a new run safe.

    :param export_url: The export directory URL, ending in ``/``.
    :param report_path: The validation report's path relative to ``export_url``
        (``config.yaml: pubmed2db_validation_report``); gzipped if it ends in ``.gz``.
    :param download_dir: The local directory to download into.
    :param done_file: Marker file to touch once every shard is downloaded.
    """
    report_file = os.path.join(download_dir, VALIDATION_REPORT)
    report_gzipped = report_path.endswith(".gz")
    # The report is small and always re-fetched; clear any old copy so gunzip doesn't refuse to overwrite it.
    for stale in (report_file, report_file + ".gz"):
        if os.path.exists(stale):
            os.remove(stale)
    pull_via_wget(
        export_url,
        report_path,
        decompress=report_gzipped,
        outpath=report_file + (".gz" if report_gzipped else ""),
        continue_incomplete=False,
    )

    shard_counts = expected_shard_counts(download_dir)
    for shard in shard_counts:
        pull_via_wget(
            export_url, shard, decompress=False, outpath=os.path.join(download_dir, shard), continue_incomplete=True
        )
    logger.info(
        f"Downloaded {len(shard_counts)} pubmed2db shards ({sum(shard_counts.values()):,} records) from "
        f"{export_url} into {download_dir}."
    )
    ensure_parent_dir(done_file)
    Path(done_file).touch()


def expected_shard_counts(download_dir):
    """
    The shards of the export downloaded into ``download_dir``, mapped to the record count pubmed2db's validator
    saw in each, in a fixed (sorted) order.

    The validation report is pubmed2db's statement of what the export contains: it is our manifest of which
    shards to download and read, and the per-shard counts are the only way to tell a truncated shard from a
    complete one, since no checksums are published. Reads ``checks.structure.records_by_shard`` and
    ``records_total`` rather than the human-readable ``checks_run[].observed`` strings. Raises if the report's
    overall status is ``fail`` or those fields are missing or inconsistent.
    """
    report_file = os.path.join(download_dir, VALIDATION_REPORT)
    with open(report_file) as f:
        report = json.load(f)
    if report.get("status") == "fail":
        raise RuntimeError(
            f"pubmed2db validation report {report_file} has status 'fail': {report.get('errors')}. "
            "Refusing to build a compendium from an export its own validator rejected."
        )
    try:
        structure = report["checks"]["structure"]
        records_by_shard = structure["records_by_shard"]
        records_total = structure["records_total"]
    except KeyError as e:
        raise RuntimeError(
            f"pubmed2db validation report {report_file} has no checks.structure.records_by_shard and "
            f"records_total (missing {e}): the report format has changed; see docs/sources/PubMed/README.md."
        ) from e
    if not records_by_shard:
        raise RuntimeError(f"pubmed2db validation report {report_file} lists no shards.")
    for shard in records_by_shard:
        # Shard names become both a download URL and a local path, so they must be bare filenames.
        if os.path.basename(shard) != shard or not shard.endswith(".ndjson.gz"):
            raise RuntimeError(f"pubmed2db validation report {report_file} lists an unexpected shard name {shard!r}.")
    if sum(records_by_shard.values()) != records_total:
        raise RuntimeError(
            f"pubmed2db validation report {report_file}: records_by_shard sums to "
            f"{sum(records_by_shard.values()):,} but records_total is {records_total:,}."
        )
    return dict(sorted(records_by_shard.items()))


def _read_shard(shard_path):
    """
    Yield ``(pmid, title, other_identifiers, malformed_identifiers)`` for every record in one shard.

    This is the only place that knows the export's record format. ``identifiers`` always starts with the
    record's own PMID, so ``identifiers[1:]`` is its DOI and PMCID CURIEs (sorted and de-duplicated by
    pubmed2db). ``title`` is ``article_title`` exactly as exported, and is ``""`` when the record has none.

    Any of those CURIEs that ``curie_format_problem()`` rejects -- which ``write_compendium()`` would fail the
    build on -- go in ``malformed_identifiers`` instead of ``other_identifiers``, so both passes skip the same
    ones. PubMed publishes DOIs as supplied, including some with embedded spaces or appended text
    (``doi:10.1126/science. 1134405``, PMID:17124311; ~140 in shard 0 of 2026sep26). Repairing them instead
    is https://github.com/NCATSTranslator/Babel/issues/1112.

    A damaged shard raises a RuntimeError naming it. pubmed2db publishes no checksums, but a shard doesn't need
    one to be checked: gzip verifies its CRC32 at the end of the file, so a corrupted download fails on that if
    it hasn't already produced unparseable JSON, and a truncated one fails on the record count in the
    validation report (see ``parse_pubmed2db_into_tsvs()``) if it ends on a line boundary.
    """
    count_records = 0
    try:
        with gzip.open(shard_path, "rt") as f:
            for line in f:
                record = json.loads(line)
                pmid = record["id"]
                identifiers = record["identifiers"]
                title = record["article_title"]
                if not identifiers or identifiers[0] != pmid:
                    raise ValueError(
                        f"{shard_path}: record {pmid} has identifiers {identifiers} not starting with its PMID."
                    )
                others = []
                malformed = []
                for other in identifiers[1:]:
                    (malformed if curie_format_problem(other) else others).append(other)
                count_records += 1
                yield pmid, title, others, malformed
    # OSError covers gzip.BadGzipFile (a failed CRC), EOFError a truncated gzip stream, and the rest a record
    # mangled into something that still decompresses.
    except (OSError, EOFError, zlib.error, UnicodeDecodeError, json.JSONDecodeError, KeyError, TypeError) as e:
        raise RuntimeError(
            f"{shard_path} is corrupt or truncated: reading it failed after {count_records:,} records "
            f"({type(e).__name__}: {e}). Delete it and re-run download_pubmed2db."
        ) from e


def escape_tsv_field(value):
    """
    Escape a value for one field of a TSV row, losslessly: backslash, tab, newline and carriage return become
    ``\\\\``, ``\\t``, ``\\n`` and ``\\r``, so the row always has the expected number of fields and the
    original can be recovered.
    """
    return value.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n").replace("\r", "\\r")


def _write_shard_tsvs(shard_path, part_prefix):
    """
    Write one shard's ids, titles and concords rows to ``<part_prefix>.{ids,titles,concords}``.

    Run in a worker process, so it returns only ``(records, concords, malformed identifiers skipped)`` -- the
    last a short list -- and holds one record at a time; ``parse_pubmed2db_into_tsvs()`` concatenates the parts
    in shard order. Module-level so ProcessPoolExecutor can pickle it.
    """
    count_records = 0
    count_concords = 0
    skipped = []
    with (
        open(part_prefix + ".ids", "w") as pmidf,
        open(part_prefix + ".titles", "w") as titlesf,
        open(part_prefix + ".concords", "w") as concordf,
    ):
        for pmid, title, others, malformed in _read_shard(shard_path):
            count_records += 1
            skipped.extend(malformed)
            pmidf.write(f"{pmid}\t{JOURNAL_ARTICLE}\n")
            if title:
                titlesf.write(f"{pmid}\t{escape_tsv_field(title)}\n")
            for other in others:
                count_concords += 1
                concordf.write(f"{pmid}\teq\t{other}\n")
    logger.info(
        f"Parsed {count_records:,} records and {count_concords:,} concords from {shard_path}, skipping "
        f"{len(skipped):,} malformed DOIs/PMCIDs."
    )
    return count_records, count_concords, skipped


def parse_pubmed2db_into_tsvs(
    download_dir, titles_file, pmid_id_file, concord_file, shared_ids_file, metadata_yaml, url, workers
):
    """
    Write the ids, titles and concords TSVs from a pubmed2db export, and resolve shared identifiers.

    - ``pmid_id_file``: ``PMID:x\\tbiolink:JournalArticle``
    - ``titles_file``: ``PMID:x\\t<title>``, escaped with ``escape_tsv_field()``; empty titles skipped. Nothing
      in the build reads it -- the compendium takes titles from the shards -- but it is the easiest place to
      diff titles between builds.
    - ``concord_file``: ``PMID:x\\teq\\t<doi: or PMCID: CURIE>``

    Shards are parsed in parallel, each worker writing its own part files, so no worker or the parent ever
    holds more than one record of a shard; the parts are then concatenated in shard order.

    A DOI or PMCID can appear in more than one record (PubMed publishes them as-is). Two cliques must never
    share an identifier, so ``shared_ids_file`` records each such identifier with the *lowest* PMID that
    carries it, and ``generate_compendium()`` keeps it only there. The old ``glom(unique_prefixes=[PMID])``
    silently ignored a concord that would have joined two PMIDs, so the first-seen PMID kept the identifier;
    lowest-PMID is the same policy made independent of shard order. The file doubles as a data-quality report
    of ambiguous identifiers.

    DOIs and PMCIDs that ``write_compendium()`` would reject are skipped (see ``_read_shard()``) and counted
    in the metadata YAML as ``skipped_malformed_article_ids``.

    Raises if any shard holds a different number of records than pubmed2db's validation report says.

    :param url: The export URL, recorded in the metadata YAML as the source of this build's PubMed data.
    :param workers: How many shards to parse at once.
    """
    for path in (titles_file, pmid_id_file, concord_file, shared_ids_file):
        ensure_parent_dir(path)

    shard_counts = expected_shard_counts(download_dir)
    shards = list(shard_counts)
    shard_files = [os.path.join(download_dir, shard) for shard in shards]

    with tempfile.TemporaryDirectory(dir=os.path.dirname(concord_file), prefix="pubmed2db-parts-") as parts_dir:
        part_prefixes = [os.path.join(parts_dir, f"{i:05d}") for i in range(len(shards))]
        with ProcessPoolExecutor(max_workers=workers) as pool:
            results = list(pool.map(_write_shard_tsvs, shard_files, part_prefixes))

        mismatches = [
            f"{shard}: parsed {records:,}, report says {shard_counts[shard]:,}"
            for shard, (records, _, _) in zip(shards, results)
            if records != shard_counts[shard]
        ]
        if mismatches:
            raise RuntimeError(
                f"Shards in {download_dir} disagree with their validation report, so they are probably truncated: "
                f"{'; '.join(mismatches)}. Delete those shards and re-run download_pubmed2db."
            )

        for final_file, suffix in ((pmid_id_file, ".ids"), (titles_file, ".titles"), (concord_file, ".concords")):
            with open(final_file, "wb") as outf:
                for part_prefix in part_prefixes:
                    with open(part_prefix + suffix, "rb") as inf:
                        shutil.copyfileobj(inf, outf)

    count_records = sum(records for records, _, _ in results)
    count_concords = sum(concords for _, concords, _ in results)
    skipped = [curie for _, _, shard_skipped in results for curie in shard_skipped]
    logger.info(f"Skipped {len(skipped):,} malformed DOIs/PMCIDs, e.g. {skipped[:10]}")

    # Find the identifiers carried by more than one record...
    seen = set()
    duplicated = set()
    with open(concord_file) as concordf:
        for line in concordf:
            other = line.rstrip("\n").split("\t")[2]
            if other in seen:
                duplicated.add(other)
            else:
                seen.add(other)
    del seen

    # ...and resolve each to its lowest PMID with a second pass, rather than holding a PMID for all ~46M
    # identifiers in the first. identifier -> (lowest PMID carrying it, number of records carrying it).
    shared = {}
    if duplicated:
        with open(concord_file) as concordf:
            for line in concordf:
                pmid, _, other = line.rstrip("\n").split("\t")
                if other in duplicated:
                    winner, n = shared.get(other, (None, 0))
                    shared[other] = (_lower_pmid(winner, pmid), n + 1)

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
            "shard_count": len(shards),
            "shared_identifier_count": len(shared),
            "skipped_malformed_article_ids": len(skipped),
        },
        concord_filename=concord_file,
    )


def _lower_pmid(a, b):
    """The numerically lower of two ``PMID:n`` CURIEs (``None`` loses)."""
    if a is None:
        return b
    return a if int(a.split(":")[1]) <= int(b.split(":")[1]) else b


class _PublicationCliques:
    """
    A sized iterable of TypedCliques streamed from the export, so write_compendium() can log progress.

    Shards are read one record at a time in this process. write_compendium() is the bottleneck here, so
    parsing ahead in worker processes would buy little time and cost a parsed shard's worth of memory per
    worker.
    """

    def __init__(self, download_dir, winners):
        self.download_dir = download_dir
        self.winners = winners
        self.shard_counts = expected_shard_counts(download_dir)

    def __len__(self):
        return sum(self.shard_counts.values())

    def __iter__(self):
        for shard in self.shard_counts:
            for pmid, title, others, _ in _read_shard(os.path.join(self.download_dir, shard)):
                identifiers = [pmid] + [o for o in others if self.winners.get(o, pmid) == pmid]
                yield TypedClique(PUBLICATION, identifiers, {pmid: title} if title else None)


def generate_compendium(download_dir, shared_ids_file, metadata_yamls, publication_compendium, icrdf_filename):
    """
    Write the Publication compendium straight from the pubmed2db shards: one record, one clique.

    Each PMID's label is its ``article_title`` exactly as exported, so a title with an embedded newline or
    surrounding whitespace keeps it (see docs/sources/PubMed/README.md, "Titles").

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
        _PublicationCliques(download_dir, winners),
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
