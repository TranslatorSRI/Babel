import csv
import json
import os
from collections import Counter, defaultdict

from src import util
from src.exporters.duckdb_exporters import log_duckdb_settings_on_error, setup_duckdb
from src.memory import log_memory_snapshot

logger = util.get_logger(__name__)


def check_for_identically_labeled_cliques(
    parquet_root, duckdb_filename, identically_labeled_cliques_tsv, duckdb_config=None
):
    """
    Generate a list of identically labeled cliques.

    :param parquet_root: The root directory for the Parquet files. We expect these to have subdirectories named
        e.g. `filename=AnatomicalEntity/Clique.parquet`, etc.
    :param duckdb_filename: A temporary DuckDB file to use.
    :param identically_labeled_cliques_csv: The output file listing identically labeled cliques.
    """

    db = setup_duckdb(duckdb_filename, duckdb_config)
    cliques = db.read_parquet(os.path.join(parquet_root, "**/Clique.parquet"), hive_partitioning=True)

    # Pass 1: identify names shared by more than one clique, grouping on a fixed-size *hash* of the
    # lowercased name rather than the name itself.
    #
    # Grouping directly on LOWER(preferred_name) OOM-killed the job on the full graph even on a
    # largemem node: there are ~200M cliques, and preferred_name is often a long chemical/disease
    # label, so the aggregate's hash table held ~200M distinct long strings. That string heap is
    # largely untracked by DuckDB's memory accounting, so RSS overshot the cgroup hard limit
    # (~500 GiB above the configured memory_limit) before any spill kicked in. The sibling
    # check_for_duplicate_clique_leaders runs the same ~200M-group pattern comfortably at 512G
    # because its key is a short CURIE. hash(...) gives us a uniform 8-byte key, so the aggregate
    # is bounded and spillable. A 64-bit hash collision (which would merge two distinct names into
    # one "duplicate" group) is astronomically unlikely at this scale and only perturbs a
    # diagnostic count, so we accept it rather than carrying the long strings through the aggregate.
    with log_duckdb_settings_on_error(
        db, "check_for_identically_labeled_cliques pass 1 (GROUP BY hash(LOWER(preferred_name)))"
    ):
        db.execute("""
            CREATE OR REPLACE TEMP TABLE dup_names AS
            SELECT hash(LOWER(preferred_name)) AS preferred_name_hash, COUNT(*) AS clique_leader_count
            FROM cliques
            WHERE preferred_name <> '' AND preferred_name <> '""'
            GROUP BY preferred_name_hash HAVING clique_leader_count > 1
        """)

    # Pass 2: write one row per (duplicate name, clique_leader) pair via a streaming hash join.
    # The build side (dup_names) is small -- only the hashes of names that actually repeat -- so this
    # streams the Clique scan straight to the output with O(1) memory, recomputing the same hash on
    # each clique to probe and recovering the real lowercased name for the output. Earlier attempts
    # that aggregated the leaders into an in-SQL ``LIST(... ORDER BY ...)`` (not spillable -- OOMed;
    # one name was shared by 92k cliques) or globally sorted the pairs (the sort overshot the cgroup
    # limit) both failed on the full graph. We therefore emit the pairs unsorted; rows sharing a name
    # are not guaranteed adjacent, but clique_leader_count is on every row, so a consumer can
    # group/sort cheaply.
    with log_duckdb_settings_on_error(db, "check_for_identically_labeled_cliques pass 2 (stream duplicate pairs)"):
        db.sql("""
            SELECT LOWER(c.preferred_name) AS preferred_name_lc, d.clique_leader_count, c.clique_leader
            FROM cliques c
            JOIN dup_names d ON hash(LOWER(c.preferred_name)) = d.preferred_name_hash
            WHERE c.preferred_name <> '' AND c.preferred_name <> '""'
        """).write_csv(identically_labeled_cliques_tsv, sep="\t", header=True)

    log_memory_snapshot(db, "check_for_identically_labeled_cliques complete")
    # Teardown is wrapped because a `bad allocation` has struck here (cliques.close()) *after* the
    # query finished -- an address-space, not RAM, limit. The wrapper logs a snapshot at the exact
    # failing allocation so the address-space line is captured.
    with log_duckdb_settings_on_error(db, "check_for_identically_labeled_cliques teardown"):
        cliques.close()
        db.close()


def check_for_duplicate_curies(parquet_root, duckdb_filename, duplicate_curies_tsv, duckdb_config=None):
    """
    Generate a list of duplicate CURIEs.

    :param parquet_root: The root directory for the Parquet files. We expect these to have subdirectories named
        e.g. `filename=AnatomicalEntity/Clique.parquet`, etc.
    :param duckdb_filename: A temporary DuckDB file to use.
    :param duplicate_curies_tsv: The output file listing duplicate CURIEs.
    """

    db = setup_duckdb(duckdb_filename, duckdb_config)
    edges = db.read_parquet(os.path.join(parquet_root, "**/Edge.parquet"), hive_partitioning=True)

    # Pass 1: identify CURIEs present in more than one clique using only a COUNT.
    # A pure COUNT(*) GROUP BY can spill to disk; the LIST() aggregates below cannot, so
    # collecting them up front over every CURIE (most of which are not duplicated) would OOM.
    with log_duckdb_settings_on_error(db, "check_for_duplicate_curies pass 1 (GROUP BY curie over all edges)"):
        db.execute("""
            CREATE OR REPLACE TEMP TABLE dup_curies AS
            SELECT curie, COUNT(*) AS clique_leader_count
            FROM edges
            WHERE conflation = 'None'
            GROUP BY curie HAVING clique_leader_count > 1
        """)

    # Pass 2: collect LIST() only for the confirmed duplicate CURIEs (a small join target).
    with log_duckdb_settings_on_error(db, "check_for_duplicate_curies pass 2 (LIST over duplicate CURIEs)"):
        db.sql("""
            SELECT e.curie,
                LIST(e.clique_leader ORDER BY e.clique_leader) AS clique_leaders,
                LIST(e.filename ORDER BY e.clique_leader) AS filenames,
                LIST(e.conflation ORDER BY e.clique_leader) AS conflations,
                d.clique_leader_count
            FROM edges e
            JOIN dup_curies d USING (curie)
            WHERE e.conflation = 'None'
            GROUP BY e.curie, d.clique_leader_count
            ORDER BY d.clique_leader_count DESC
        """).write_csv(duplicate_curies_tsv, sep="\t")

    log_memory_snapshot(db, "check_for_duplicate_curies complete")
    with log_duckdb_settings_on_error(db, "check_for_duplicate_curies teardown"):
        edges.close()
        db.close()


def check_for_duplicate_clique_leaders(parquet_root, duckdb_filename, duplicate_clique_leaders_tsv, duckdb_config=None):
    """
    Generate a list of duplicate clique leaders.

    :param parquet_root: The root directory for the Parquet files. We expect these to have subdirectories named
        e.g. `filename=AnatomicalEntity/Clique.parquet`, etc.
    :param duckdb_filename: A temporary DuckDB file to use.
    :param duplicate_clique_leaders_tsv: The output file listing duplicate CURIEs.
    """

    db = setup_duckdb(duckdb_filename, duckdb_config)
    cliques = db.read_parquet(os.path.join(parquet_root, "**/Clique.parquet"), hive_partitioning=True)

    # Pass 1: identify duplicate clique leaders using only a COUNT.
    # A pure COUNT(*) GROUP BY can spill to disk, but the LIST() aggregates below cannot, so
    # collecting them over every clique leader (almost all unique) would OOM on the full set.
    with log_duckdb_settings_on_error(db, "check_for_duplicate_clique_leaders pass 1 (GROUP BY clique_leader)"):
        db.execute("""
            CREATE OR REPLACE TEMP TABLE dup_leaders AS
            SELECT clique_leader, COUNT(*) AS clique_leader_count
            FROM cliques
            GROUP BY clique_leader HAVING clique_leader_count > 1
        """)

    # Pass 2: collect LIST() only for the confirmed duplicates (a small join target).
    # Because the LIST() materialization is now trivial we can also afford to keep
    # biolink_type and clique_identifier_count, which were previously dropped to save memory.
    with log_duckdb_settings_on_error(db, "check_for_duplicate_clique_leaders pass 2 (LIST over duplicate leaders)"):
        db.sql("""
            SELECT d.clique_leader,
                LIST(c.filename ORDER BY c.filename) AS filenames,
                LIST(c.biolink_type ORDER BY c.filename) AS biolink_types,
                LIST(c.clique_identifier_count ORDER BY c.filename) AS clique_identifier_counts,
                d.clique_leader_count
            FROM cliques c
            JOIN dup_leaders d USING (clique_leader)
            GROUP BY d.clique_leader, d.clique_leader_count
            ORDER BY d.clique_leader_count DESC
        """).write_csv(duplicate_clique_leaders_tsv, sep="\t")

    log_memory_snapshot(db, "check_for_duplicate_clique_leaders complete")
    with log_duckdb_settings_on_error(db, "check_for_duplicate_clique_leaders teardown"):
        cliques.close()
        db.close()


def _parse_filenames(text):
    """Parse a compendia list as check_for_duplicate_clique_leaders() writes it ("[Gene, Protein]") or as the
    allowlist spells it ("Gene,Protein") into a sorted tuple of compendium names."""
    return tuple(sorted(name.strip().strip("'\"") for name in text.strip().strip("[]").split(",") if name.strip()))


def read_known_duplicate_clique_leaders(allowlist_tsv):
    """Read input_data/known_duplicate_clique_leaders.tsv: a TSV with a header (clique_leader, filenames, issue,
    note), `#` comment lines allowed, keyed like the duplicate-leader report."""
    allowed = {}
    with open(allowlist_tsv) as inf:
        rows = (line for line in inf if line.strip() and not line.startswith("#"))
        for row in csv.DictReader(rows, delimiter="\t"):
            allowed[(row["clique_leader"], _parse_filenames(row["filenames"]))] = row
    return allowed


def assert_no_unexpected_duplicate_clique_leaders(duplicate_clique_leaders_tsv, allowlist_tsv, checked_file):
    """Fail the build when a CURIE leads a clique in two compendia and is not on the committed allowlist.

    A clique leader shared by two compendia gives NodeNorm two answers for one CURIE: Redis NodeNorm serves
    whichever compendium loaded last, and NodeNorm-ES, which keys its documents by leader, merged the two cliques
    into one document typed with two Biolink classes until it was changed to handle that in October 2026
    (biothings/NodeNormalizationAPI#41). That fix is downstream; the build still assigns the CURIE two types.
    check_for_duplicate_clique_leaders() has reported these since 2025 without anything reading the report,
    and 7,571 of them shipped in babel-1.18 (https://github.com/NCATSTranslator/Babel/issues/276). This
    check reads that report *after* it is written -- so the TSV is there to inspect when the build goes red --
    and compares each (leader, compendia) pair against `input_data/known_duplicate_clique_leaders.tsv`, which
    lists the duplicates a named issue accepts for now. A duplicate is accepted when an allowlist row for its
    leader names all of its compendia: one that has shrunk to fewer compendia (say a source fix took one of
    three away) is still accepted, and logged so the row can be narrowed. Every other duplicate leader raises,
    which stops `reports/duckdb/done` and therefore `rule all`, while every other build output completes.

    Member-level duplicates (duplicate_curies.tsv) stay report-only: NodeNorm handles them as two well-formed
    cliques, and the residue after the source fixes needs per-source work rather than a gate.

    :param duplicate_clique_leaders_tsv: The report written by check_for_duplicate_clique_leaders().
    :param allowlist_tsv: The committed allowlist (see read_known_duplicate_clique_leaders()).
    :param checked_file: Written (with a summary) only when every duplicate leader is allowlisted.
    :raises RuntimeError: listing the unexpected duplicate leaders by compendia pair.
    """
    allowed = read_known_duplicate_clique_leaders(allowlist_tsv)
    allowed_by_leader = defaultdict(list)
    for leader, filenames in allowed:
        allowed_by_leader[leader].append(set(filenames))
    found = {}
    with open(duplicate_clique_leaders_tsv) as inf:
        for row in csv.DictReader(inf, delimiter="\t"):
            found[(row["clique_leader"], _parse_filenames(row["filenames"]))] = row
    unexpected = {
        (leader, filenames): row
        for (leader, filenames), row in found.items()
        if not any(set(filenames) <= listed for listed in allowed_by_leader[leader])
    }
    narrowed = sorted(key for key in found if key not in unexpected and key not in allowed)
    stale = sorted(
        (leader, filenames)
        for leader, filenames in allowed
        if not any(found_leader == leader and set(found_files) <= set(filenames) for found_leader, found_files in found)
    )

    def _list(keys):
        return ", ".join(f"{leader} [{', '.join(filenames)}]" for leader, filenames in keys[:20])

    summary = [
        f"{len(found)} duplicate clique leaders in {duplicate_clique_leaders_tsv}; "
        f"{len(found) - len(unexpected)} allowlisted in {allowlist_tsv}, {len(unexpected)} unexpected, "
        f"{len(stale)} allowlist entries no longer duplicated (prune them): {_list(stale)}; "
        f"{len(narrowed)} now in fewer compendia than their allowlist entry (narrow them): {_list(narrowed)}"
    ]
    if unexpected:
        by_pair = Counter(filenames for _, filenames in unexpected)
        summary.append(
            "Unexpected duplicate clique leaders by compendia: "
            + "; ".join(f"[{', '.join(pair)}]: {n}" for pair, n in by_pair.most_common())
        )
        summary.append(
            "First unexpected rows: "
            + "; ".join(
                f"{leader} [{', '.join(filenames)}] types={row['biolink_types']}"
                for (leader, filenames), row in list(unexpected.items())[:20]
            )
        )
        summary.append(
            "Fix the source (docs/sources/UMLS/CuiFollowsMesh.md lists the known mechanisms) or, for a duplicate an issue accepts, add it to the allowlist with that issue."
        )
        raise RuntimeError("\n".join(summary))
    logger.info(summary[0])
    with open(checked_file, "w") as outf:
        outf.write(summary[0] + "\n")


def generate_prefix_report(parquet_root, duckdb_filename, prefix_report_json, name, duckdb_config=None):
    """
    Generate the combined prefix report describing every CURIE and clique-leader prefix in the build.

    See thoughts at https://github.com/NCATSTranslator/Babel/issues/359

    This revives the output of the original ``generate_prefix_report`` (added in PR #363, later split
    into ``generate_curie_report``/``generate_clique_leaders_report``), but implemented with only
    *spillable* aggregates. The original OOM-killed the full graph because it used two non-spillable
    DuckDB operators -- ``COUNT(DISTINCT ...)`` (a full distinct hash set per group) and
    ``STRING_AGG(... '||' ...)`` (one giant delimited string per prefix). Neither can spill to disk,
    so no ``memory_limit`` setting saves them. Here every count comes from a plain ``GROUP BY <prefix>``
    (spillable) and every distinct/clique count from ``approx_count_distinct`` (a fixed-size HLL sketch,
    ~2% error). The exact occurrence counts (``COUNT``) stay exact; only distinct/clique counts are
    approximate.

    The output schema is the one used by the committed baselines in
    ``releases/<release>/reports/duckdb/prefix_report.json`` (and by babel-validation's
    PrefixComparator), so a freshly built report drops in beside them with no migration::

        {
            "name": <release name>,
            "count_curies": <int, exact>,
            "count_cliques": <int, approx>,
            "by_clique": {"<clique_leader_prefix>": {
                "by_file": {"<filename>": {"<curie_prefix>": <int, exact>}},
                "count_curies": <int, exact>, "count_cliques": <int, approx>}},
            "by_curie_prefix": {"<curie_prefix>": {
                "curie_count": <int, exact>, "curie_distinct_count": <int, approx>,
                "clique_distinct_count": <int, approx>, "filenames": {"<filename>": <int, exact>}}},
            "by_filename": {"<filename>": {                            # additive extension to the baseline schema
                "curie_count": <int, exact>, "distinct_curie_count": <int, approx>}},
        }

    The ``by_filename`` section is not present in the committed baselines and is ignored by the
    comparison report; it exists only so report_tables can build the per-file cliques table.

    :param parquet_root: The root directory for the Parquet files. We expect these to have subdirectories named
        e.g. `filename=AnatomicalEntity/Edge.parquet`, etc.
    :param duckdb_filename: A temporary DuckDB file to use.
    :param prefix_report_json: The combined prefix report to write as JSON.
    :param name: The release name to store in the report's top-level ``name`` field.
    """

    db = setup_duckdb(duckdb_filename, duckdb_config)
    edges = db.read_parquet(os.path.join(parquet_root, "**/Edge.parquet"), hive_partitioning=True)

    # Section 0: by_filename -- per-compendium-file totals. This is an additive extension to the
    # baseline schema (the committed baselines don't carry it); the comparison report ignores it, but
    # report_tables.generate_cliques_table needs the per-file exact CURIE count and approximate
    # distinct CURIE count, which cannot be recovered from the per-prefix sketches elsewhere in this
    # report.
    logger.info("Generating prefix report: by_filename totals...")
    with log_duckdb_settings_on_error(db, "generate_prefix_report: by_filename totals (approx distinct counts)"):
        by_filename_rows = db.sql("""
            SELECT
                filename,
                COUNT(curie) AS curie_count,
                approx_count_distinct(curie) AS distinct_curie_count
            FROM edges
            WHERE conflation = 'None'
            GROUP BY filename
        """).fetchall()
    by_filename = {}
    for filename, curie_count, distinct_curie_count in by_filename_rows:
        by_filename[filename] = {
            "curie_count": curie_count,
            "distinct_curie_count": distinct_curie_count,
        }

    # Section 1: by_curie_prefix -- one row per CURIE prefix. curie_count is exact; the distinct/clique
    # counts use approx_count_distinct (see the module/function note on non-spillability).
    logger.info("Generating prefix report: by_curie_prefix totals...")
    with log_duckdb_settings_on_error(db, "generate_prefix_report: by_curie_prefix (approx distinct counts)"):
        by_curie_prefix_rows = db.sql("""
            SELECT
                curie_prefix,
                COUNT(curie) AS curie_count,
                approx_count_distinct(curie) AS curie_distinct_count,
                approx_count_distinct(clique_leader) AS clique_distinct_count
            FROM edges
            WHERE conflation = 'None'
            GROUP BY curie_prefix
        """).fetchall()
    by_curie_prefix = {}
    for curie_prefix, curie_count, curie_distinct_count, clique_distinct_count in by_curie_prefix_rows:
        by_curie_prefix[curie_prefix] = {
            "curie_count": curie_count,
            "curie_distinct_count": curie_distinct_count,
            "clique_distinct_count": clique_distinct_count,
            "filenames": defaultdict(int),
        }

    # Section 2: the (filename, clique_leader_prefix, curie_prefix) breakdown. This single grouped scan
    # feeds both by_clique[leader].by_file[filename][curie_prefix] (directly) and
    # by_curie_prefix[curie_prefix].filenames[filename] (folded over clique_leader_prefix below).
    logger.info("Generating prefix report: by_file breakdown...")
    with log_duckdb_settings_on_error(db, "generate_prefix_report: by_file breakdown"):
        by_file_rows = db.sql("""
            SELECT
                filename,
                clique_leader_prefix,
                curie_prefix,
                COUNT(curie) AS curie_count
            FROM edges
            WHERE conflation = 'None'
            GROUP BY filename, clique_leader_prefix, curie_prefix
        """).fetchall()
    by_clique = {}
    for filename, clique_leader_prefix, curie_prefix, curie_count in by_file_rows:
        leader_entry = by_clique.setdefault(clique_leader_prefix, {"by_file": {}})
        leader_entry["by_file"].setdefault(filename, {})[curie_prefix] = curie_count
        # Fold into by_curie_prefix.filenames. A curie_prefix is guaranteed to be in by_curie_prefix
        # because both queries scan the same conflation='None' edges.
        by_curie_prefix[curie_prefix]["filenames"][filename] += curie_count

    # Section 3: per-clique-leader-prefix totals. count_curies is exact; count_cliques uses
    # approx_count_distinct. A clique has exactly one leader (hence one leader prefix), so leader
    # prefixes partition the cliques -- summing these count_cliques never double-counts a clique.
    logger.info("Generating prefix report: by_clique totals...")
    with log_duckdb_settings_on_error(db, "generate_prefix_report: by_clique totals (approx distinct counts)"):
        by_clique_total_rows = db.sql("""
            SELECT
                clique_leader_prefix,
                COUNT(curie) AS count_curies,
                approx_count_distinct(clique_leader) AS count_cliques
            FROM edges
            WHERE conflation = 'None'
            GROUP BY clique_leader_prefix
        """).fetchall()

    count_curies = 0
    count_cliques = 0
    for clique_leader_prefix, leader_count_curies, leader_count_cliques in by_clique_total_rows:
        # Every clique_leader_prefix here also appeared in the by_file breakdown (same edges).
        leader_entry = by_clique.setdefault(clique_leader_prefix, {"by_file": {}})
        leader_entry["count_curies"] = leader_count_curies
        leader_entry["count_cliques"] = leader_count_cliques
        count_curies += leader_count_curies
        count_cliques += leader_count_cliques

    # Convert the defaultdict filenames maps to plain dicts for a clean JSON dump.
    for entry in by_curie_prefix.values():
        entry["filenames"] = dict(entry["filenames"])

    report = {
        "name": name,
        "count_curies": count_curies,
        "count_cliques": count_cliques,
        "by_clique": by_clique,
        "by_curie_prefix": by_curie_prefix,
        "by_filename": by_filename,
    }
    with open(prefix_report_json, "w") as fout:
        json.dump(report, fout, indent=2, sort_keys=True)

    log_memory_snapshot(db, "generate_prefix_report complete")
    with log_duckdb_settings_on_error(db, "generate_prefix_report teardown"):
        edges.close()
        db.close()


def get_label_distribution(duckdb_filename, output_filename):
    db = setup_duckdb(duckdb_filename)

    # Thanks, ChatGPT.
    db.sql("""
       WITH Lengths AS (
            SELECT
                curie,
                label,
                LENGTH(label) AS label_length
            FROM
                Cliques
        ), Examples AS (
            SELECT
                curie,
                label,
                label_length,
                ROW_NUMBER() OVER (PARTITION BY label_length ORDER BY label) AS rn
            FROM
                Lengths
        )
        SELECT
            label_length,
            COUNT(*) AS frequency,
            MAX(CASE WHEN rn = 1 THEN curie ELSE NULL END) AS example_curie,
            MAX(CASE WHEN rn = 1 THEN label ELSE NULL END) AS example_label
        FROM
            Examples
        GROUP BY
            label_length
        ORDER BY
            label_length ASC;
    """).write_csv(output_filename)
