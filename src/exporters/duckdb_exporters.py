# The DuckDB exporter can be used to export particular intermediate files into the
# in-process database engine DuckDB (https://duckdb.org) for future querying.
import os.path
import stat
import tempfile
from contextlib import contextmanager
from pathlib import Path

import duckdb

from src.memory import _bytes_to_gib, cgroup_memory_hard_limit_bytes, log_memory_snapshot
from src.util import ensure_parent_dir, get_config, get_logger

logger = get_logger(__name__)


# The DuckDB settings most relevant to diagnosing an out-of-memory or spill failure. Kept short
# so the on-error log line stays readable; the full set is dumped by setup_duckdb() at connect.
_DUCKDB_DIAGNOSTIC_SETTINGS = (
    "memory_limit",
    "max_memory",
    "threads",
    "temp_directory",
    "max_temp_directory_size",
    "preserve_insertion_order",
)


@contextmanager
def log_duckdb_settings_on_error(db, operation):
    """Log the effective DuckDB memory/spill settings if the wrapped call raises.

    DuckDB's OutOfMemory and IO errors don't say which limits were in force or which step hit
    them, which makes the bare Snakemake traceback hard to act on. Wrapping a query in this
    context manager emits one ERROR line naming the `operation` and the relevant settings before
    re-raising the original exception unchanged.
    """
    try:
        yield
    except duckdb.Error as exc:
        try:
            placeholders = ", ".join("?" for _ in _DUCKDB_DIAGNOSTIC_SETTINGS)
            rows = db.execute(
                f"SELECT name, value FROM duckdb_settings() WHERE name IN ({placeholders})",
                list(_DUCKDB_DIAGNOSTIC_SETTINGS),
            ).fetchall()
            settings = ", ".join(f"{name}={value}" for name, value in sorted(rows))
        except Exception:  # diagnostics must never mask the real error
            settings = "<could not read duckdb_settings()>"
        logger.error(
            "DuckDB operation failed during %s (%s: %s); effective settings: %s",
            operation,
            type(exc).__name__,
            exc,
            settings,
        )
        # Peak/tracked/untracked memory at the point of failure -- the single most useful thing
        # for distinguishing "needs more headroom" from "untracked allocation / query rewrite".
        log_memory_snapshot(db, f"at failure of {operation}")
        raise


# Some configuration items for controlling loads.
MIN_FILE_SIZE_FOR_SPLITTING_LOAD = 44_000_000_000
CHUNK_LINE_SIZE = 60_000_000


def setup_duckdb(duckdb_filename, duckdb_config=None):
    """
    Set up a DuckDB instance using the settings in the config.

    :return: The DuckDB instance to be used.
    """
    if not duckdb_config:
        duckdb_config = {}

    # We want to use (1) the global duckdb_config, then (2) the duckdb_config passed to this function.
    complete_duckdb_config = {**get_config().get("duckdb_config", {}), **duckdb_config}

    # These two keys are Babel conveniences, not DuckDB connect() settings, so pull them out
    # before handing the rest to duckdb.connect(). They let an individual rule override where
    # DuckDB spills and how large that spill may grow.
    temp_directory_override = complete_duckdb_config.pop("temp_directory", None)
    max_temp_directory_size = complete_duckdb_config.pop("max_temp_directory_size", None)

    db = duckdb.connect(duckdb_filename, config=complete_duckdb_config)

    # Apply some Babel-wide settings to DuckDB.
    config = get_config()

    # Decide where DuckDB spills larger-than-memory intermediates. Precedence:
    #   1. an explicit per-call `temp_directory` (a rule that wants a specific scratch area),
    #   2. the BABEL_DUCKDB_TEMP_DIR environment variable (set per job at submission time),
    #   3. the global `tmp_directory` from config.yaml.
    # Hatteras has no large node-local disk we can use: /tmp is a ~16 GB rootfs, /dev/shm is
    # RAM (charged against the job's cgroup), and /scratch and /projects are both NFS. So the
    # default stays on the parallel filesystem; see slurm/README.md ("Temporary scratch space").
    temp_directory = temp_directory_override or os.environ.get("BABEL_DUCKDB_TEMP_DIR") or config.get("tmp_directory")
    if temp_directory:
        # Give every job its own spill subdirectory. Multiple report jobs previously shared a
        # single NFS temp directory, which produced "stale file handle" / "could not read
        # enough bytes" IO errors when one job's temp files raced against another's; per-job
        # isolation removes that contention regardless of which filesystem is used.
        job_id = os.environ.get("SLURM_JOB_ID") or str(os.getpid())
        temp_directory = os.path.join(temp_directory, f"duckdb-{job_id}")
        os.makedirs(temp_directory, exist_ok=True)
        db.execute(f"SET temp_directory = '{temp_directory}'")
        db.execute(f"SET max_temp_directory_size = '{max_temp_directory_size or '500GB'}'")

    # We need to set local settings after the connection has been opened.
    db.execute("SET enable_progress_bar=true")

    # Display all the settings.
    settings = db.sql("SELECT * FROM duckdb_settings()")
    logger.info("DuckDB connected with the following settings:")
    for row in settings.fetchall():
        logger.info(f" - {row[0]}: {row[1]}")

    # Log the memory_limit vs the cgroup hard limit (the SLURM mem= allocation) at connect time.
    # When threads > 1, a background-thread OOM aborts the process with SIGABRT and no Python
    # exception, so log_duckdb_settings_on_error never runs -- this connect-time line is then the
    # only record of how much headroom DuckDB had under the cgroup. A small or negative headroom
    # here means an untracked allocation can overshoot the cgroup before DuckDB's soft limit
    # triggers a spill; lower memory_limit (and/or drop to a single thread) to widen it.
    try:
        memory_limit_row = db.execute("SELECT value FROM duckdb_settings() WHERE name = 'memory_limit'").fetchone()
        memory_limit = memory_limit_row[0] if memory_limit_row else "unknown"
    except Exception:
        memory_limit = "unknown"
    cgroup_limit = cgroup_memory_hard_limit_bytes()
    logger.info(
        "DuckDB memory headroom: memory_limit=%s, cgroup hard limit (SLURM mem)=%s",
        memory_limit,
        _bytes_to_gib(cgroup_limit),
    )

    return db


def _prepare_duckdb_output(duckdb_filename):
    """Refuse to clobber an existing DuckDB file and ensure its parent directory exists.

    Shared by every exporter that writes a fresh DuckDB scratch file before emitting Parquet.
    """
    if os.path.exists(duckdb_filename):
        raise RuntimeError(f"Will not overwrite existing file {duckdb_filename}")
    ensure_parent_dir(duckdb_filename)


def export_compendia_to_parquet(compendium_filename, clique_parquet_filename, edge_parquet_filename, duckdb_filename):
    """
    Export a compendium to a Parquet file via a DuckDB.

    :param compendium_filename: The compendium filename to read.
    :param clique_parquet_filename: The filename for the Clique.parquet file.
    :param edge_parquet_filename: The filename for the Edge.parquet file.
    :param duckdb_filename: The DuckDB filename to write.

    The Node.parquet file is written alongside clique_parquet_filename (same directory); the
    Clique and Edge Parquet paths are passed explicitly so the Snakemake rule and this function
    agree on them and Snakemake can track them as declared outputs.
    """

    _prepare_duckdb_output(duckdb_filename)

    # Node.parquet is written alongside the Clique.parquet file and is also declared as a rule
    # output; the Clique and Edge paths arrive as explicit arguments.
    parquet_dir = os.path.dirname(clique_parquet_filename)
    ensure_parent_dir(clique_parquet_filename)
    ensure_parent_dir(edge_parquet_filename)
    node_parquet_filename = os.path.join(parquet_dir, "Node.parquet")

    compendium_basename = os.path.basename(compendium_filename)
    with setup_duckdb(duckdb_filename) as db:
        # Step 1. Create a Nodes table with all the nodes from compendium_filename.
        db.sql(
            """CREATE TABLE Node (curie STRING, curie_prefix STRING, label STRING, label_lc STRING, description STRING[], taxa STRING[])"""
        )

        compendium_filesize = os.path.getsize(compendium_filename)
        if compendium_filesize < MIN_FILE_SIZE_FOR_SPLITTING_LOAD:
            # This seems to be around the threshold where 500G is inadequate on Hatteras. So let's try splitting it.
            logger.info(
                f"Loading {compendium_filename} into DuckDB (size {compendium_filesize}) in a single direct ingest."
            )
            with log_duckdb_settings_on_error(
                db, f"export_compendia_to_parquet: load Node table from {compendium_basename} (single ingest)"
            ):
                db.execute(
                    """INSERT INTO Node
                          WITH extracted AS (
                              SELECT json_extract_string(identifier_row.value, ['i', 'l', 'd', 't']) AS extracted_list
                              FROM read_json($1, format='newline_delimited') AS clique,
                                   json_each(clique.identifiers) AS identifier_row
                          )
                          SELECT
                              extracted_list[1] AS curie,
                              split_part(extracted_list[1], ':', 1) AS curie_prefix,
                              extracted_list[2] AS label,
                              LOWER(label) AS label_lc,
                              extracted_list[3] AS description,
                              extracted_list[4] AS taxa
                          FROM extracted""",
                    [compendium_filename],
                )
        else:
            logger.info(
                f"Loading {compendium_filename} into DuckDB (size {compendium_filesize}) in multiple chunks of {CHUNK_LINE_SIZE:,} lines:"
            )
            chunk_filenames = []
            lines_added = 0
            lines_added_file = 0
            output_file = None
            with open(compendium_filename, encoding="utf-8") as inf:
                for line in inf:
                    if output_file is None:
                        output_file = tempfile.NamedTemporaryFile(delete=False, mode="w", encoding="utf-8")
                        chunk_filenames.append(output_file.name)
                        logger.info(f" - Created chunk file {output_file.name}.")
                    output_file.write(line)
                    lines_added += 1
                    lines_added_file += 1
                    if lines_added % CHUNK_LINE_SIZE == 0:
                        logger.info(f" - Wrote {lines_added_file:,} lines into {output_file.name}.")
                        lines_added_file = 0
                        output_file.close()
                        output_file = None

            if output_file is not None:
                output_file.close()

            logger.info(f"Loaded {len(chunk_filenames)} containing {lines_added:,} lines into chunk files.")
            for chunk_filename in chunk_filenames:
                with log_duckdb_settings_on_error(
                    db,
                    f"export_compendia_to_parquet: load Node table from {compendium_basename} (chunk {chunk_filename})",
                ):
                    db.execute(
                        """INSERT INTO Node
                              WITH extracted AS (
                                  SELECT json_extract_string(identifier_row.value, ['i', 'l', 'd', 't']) AS extracted_list
                                  FROM read_json($1, format='newline_delimited') AS clique,
                                       json_each(clique.identifiers) AS identifier_row
                              )
                              SELECT
                                  extracted_list[1] AS curie,
                                  split_part(extracted_list[1], ':', 1) AS curie_prefix,
                                  extracted_list[2] AS label,
                                  LOWER(label) AS label_lc,
                                  extracted_list[3] AS description,
                                  extracted_list[4] AS taxa
                              FROM extracted""",
                        [chunk_filename],
                    )
                logger.info(f" - Loaded chunk file {chunk_filename} into DuckDB.")
                os.remove(chunk_filename)
                logger.info(f" - Deleted chunk file {chunk_filename}.")

            logger.info(f"Completed loading {compendium_filename} into DuckDB.")
            logger.info(f" - Line count: {lines_added:,}.")

            node_count = db.execute("SELECT COUNT(*) FROM Node").fetchone()[0]
            logger.info(f" - Identifier count: {node_count:,}.")

        with log_duckdb_settings_on_error(
            db, f"export_compendia_to_parquet: write Node Parquet for {compendium_basename}"
        ):
            db.table("Node").write_parquet(node_parquet_filename)

        # Step 2. Create a Cliques table with all the cliques from this file.
        db.sql("""CREATE TABLE Clique
                (clique_leader STRING, preferred_name STRING, clique_identifier_count INT, biolink_type STRING,
                information_content FLOAT)""")
        with log_duckdb_settings_on_error(
            db, f"export_compendia_to_parquet: build and write Clique table for {compendium_basename}"
        ):
            db.execute(
                """INSERT INTO Clique SELECT
                            json_extract_string(identifiers, '$[0].i') AS clique_leader,
                            preferred_name,
                            len(identifiers) AS clique_identifier_count,
                            type AS biolink_type,
                            ic AS information_content
                        FROM read_json(?, format='newline_delimited')""",
                [compendium_filename],
            )
            db.table("Clique").write_parquet(clique_parquet_filename)

        # Step 2. Create an Edge table with all the clique/CURIE relationships from this file.
        #
        # biolink_type is denormalized onto every edge here, where it is free: it is the same
        # top-level ``type`` field the Clique build above already reads, and each compendium record
        # carries it inline. Storing it per-edge lets the cross-compendium curie report group by
        # (curie_prefix, biolink_type) with a plain scan instead of joining the full Edge table
        # (~1.5B rows) against the Clique table (~200M rows) -- a large-vs-large join that OOM-killed
        # the report rule even on a largemem node.
        db.sql(
            """CREATE TABLE Edge (clique_leader STRING, curie STRING, conflation STRING,
                clique_leader_prefix STRING, curie_prefix STRING, biolink_type STRING)"""
        )
        with log_duckdb_settings_on_error(
            db, f"export_compendia_to_parquet: build and write Edge table for {compendium_basename}"
        ):
            db.execute(
                """INSERT INTO Edge
                    WITH unnested AS (
                        SELECT
                            json_extract_string(identifiers, '$[0].i') AS clique_leader,
                            UNNEST(json_extract_string(identifiers, '$[*].i')) AS curie,
                            'None' AS conflation,
                            type AS biolink_type
                        FROM read_json(?, format='newline_delimited')
                    )
                    SELECT
                        clique_leader,
                        curie,
                        conflation,
                        split_part(clique_leader, ':', 1) AS clique_leader_prefix,
                        split_part(curie, ':', 1) AS curie_prefix,
                        biolink_type
                    FROM unnested""",
                [compendium_filename],
            )
            db.table("Edge").write_parquet(edge_parquet_filename)


def export_conflation_to_parquet(conflation_filename, conflation_type, duckdb_filename, parquet_filename):
    """
    Export a conflation file to a Parquet file via DuckDB.

    The conflation file is NDJSON where each line is a JSON array of CURIEs; the first element
    is the conflation group leader.

    :param conflation_filename: The conflation file (NDJSON) to read.
    :param conflation_type: A string identifying the conflation type, e.g. 'GeneProtein'.
    :param duckdb_filename: A temporary DuckDB file to use during export.
    :param parquet_filename: The output Parquet file path.
    """
    _prepare_duckdb_output(duckdb_filename)
    ensure_parent_dir(parquet_filename)

    with setup_duckdb(duckdb_filename) as db:
        db.sql(
            "CREATE TABLE Conflation (conflation_type STRING, conflation_leader STRING, curie STRING, curie_prefix STRING)"
        )
        with log_duckdb_settings_on_error(
            db,
            f"export_conflation_to_parquet: build and write {conflation_type} Conflation from {os.path.basename(conflation_filename)}",
        ):
            db.execute(
                """INSERT INTO Conflation
                WITH raw AS (
                    SELECT column0 AS line_text
                    FROM read_csv(?, header=False, columns={'column0': 'VARCHAR'})
                    WHERE trim(column0) != ''
                ),
                unnested AS (
                    SELECT
                        ? AS conflation_type,
                        json_extract_string(line_text::JSON, '$[0]') AS conflation_leader,
                        UNNEST(json_extract_string(line_text::JSON, '$[*]')) AS curie
                    FROM raw
                )
                SELECT
                    conflation_type,
                    conflation_leader,
                    curie,
                    split_part(curie, ':', 1) AS curie_prefix
                FROM unnested""",
                [conflation_filename, conflation_type],
            )
            db.table("Conflation").write_parquet(parquet_filename)


def export_synonyms_to_parquet(synonyms_filename_gz, duckdb_filename, synonyms_parquet_filename, memory_limit_mb=None):
    """
    Export a synonyms file to a DuckDB directory.

    :param synonyms_filename_gz: The synonym file (gzipped JSONL) to export to Parquet.
    :param duckdb_filename: A DuckDB file to temporarily store data in.
    :param synonyms_parquet_filename: The Parquet file to store the synonyms in.
    :param memory_limit_mb: DuckDB memory limit in MB. When set, overrides DuckDB's default
        (75% of system RAM), which can exceed the SLURM allocation on shared HPC nodes.
    """

    _prepare_duckdb_output(duckdb_filename)

    duckdb_config = {}
    if memory_limit_mb is not None:
        duckdb_config["memory_limit"] = f"{memory_limit_mb}MB"

    with setup_duckdb(duckdb_filename, duckdb_config=duckdb_config) as db:
        synonyms_jsonl = db.read_json(synonyms_filename_gz, format="newline_delimited")

        # We can't execute the following query unless we have at least one row in the input data.
        result = db.execute("SELECT COUNT(*) AS row_count FROM synonyms_jsonl").fetchone()
        row_count = result[0]

        if row_count > 0:
            # Write directly to Parquet without an intermediate in-memory Synonym table.
            # INSERT INTO a table would materialise all unnested rows in RAM; .write_parquet()
            # streams row groups to disk so peak memory stays proportional to one row group.
            with log_duckdb_settings_on_error(
                db, f"export_synonyms_to_parquet: write Parquet from {os.path.basename(synonyms_filename_gz)}"
            ):
                db.sql("""
                    SELECT curie AS clique_leader, preferred_name,
                        LOWER(preferred_name) AS preferred_name_lc,
                        CONCAT('biolink:', json_extract_string(types, '$[0]')) AS biolink_type,
                        unnest(names) AS label, LOWER(label) AS label_lc
                    FROM synonyms_jsonl
                """).write_parquet(synonyms_parquet_filename)
        else:
            db.sql("""
                SELECT NULL::VARCHAR AS clique_leader, NULL::VARCHAR AS preferred_name,
                    NULL::VARCHAR AS preferred_name_lc, NULL::VARCHAR AS biolink_type,
                    NULL::VARCHAR AS label, NULL::VARCHAR AS label_lc
                WHERE FALSE
            """).write_parquet(synonyms_parquet_filename)

        synonyms_jsonl.close()


# Intermediate metadata sidecar files are named `metadata-<subject>.yaml` (describing a sibling
# file) or a bare `metadata.yaml` (describing the directory it lives in).
METADATA_FILENAME_PREFIX = "metadata-"
METADATA_FILENAME_SUFFIX = ".yaml"
METADATA_DIRECTORY_FILENAME = "metadata.yaml"


def _metadata_subject_filename(filename):
    """Return the name of the file a metadata sidecar describes, or None if `filename` is not a
    metadata file.

    `metadata-<subject>.yaml` describes the sibling file `<subject>`; a bare `metadata.yaml`
    describes the directory it lives in, so its own name is returned unchanged.
    """
    lower_filename = filename.lower()
    if lower_filename.startswith(METADATA_FILENAME_PREFIX) and lower_filename.endswith(METADATA_FILENAME_SUFFIX):
        return filename[len(METADATA_FILENAME_PREFIX) : -len(METADATA_FILENAME_SUFFIX)]
    if lower_filename == METADATA_DIRECTORY_FILENAME:
        return filename
    return None


def _insert_metadata(db, path, subject_filename):
    """Insert one metadata sidecar file into the Metadata table, recording the subject it
    describes and the full path to that subject.

    For a `metadata-<subject>.yaml` sidecar, the subject path is the sibling file it describes; for
    a bare `metadata.yaml`, which describes its directory, the subject path is that directory.
    """
    logger.info(f"Loading metadata from {path} describing subject {subject_filename}")
    if path.name.lower() == METADATA_DIRECTORY_FILENAME:
        subject_file_path = path.parent
    else:
        subject_file_path = path.parent / subject_filename
    db.execute(
        "INSERT INTO Metadata VALUES (?, ?, ?, ?)",
        [str(path), subject_filename, str(subject_file_path), path.read_text(encoding="utf-8")],
    )


def _iter_loadable_files(root, kind):
    """Yield `(path, subject_filename)` for each non-empty file of the given `kind`
    ('concords' or 'ids') under `root`.

    Directories and empty files are skipped. `subject_filename` is the metadata subject for a
    metadata sidecar file (see `_metadata_subject_filename`) and `None` for a data file the caller
    should bulk-load; the caller routes on it. The generator has no side effects so partial
    consumption can't silently drop metadata.
    """
    for path in root.glob(f"**/{kind}/**/*"):
        # One stat() serves both the directory and the empty-file checks.
        st = path.stat()
        if stat.S_ISDIR(st.st_mode):
            logger.info(f"Skipping directory {path}")
            continue
        if st.st_size == 0:
            logger.warning(f"Skipping empty {kind} file {path}")
            continue

        yield path, _metadata_subject_filename(path.name)


def export_intermediates_to_parquet(
    intermediate_directory,
    duckdb_filename,
    ids_parquet_filename,
    concords_parquet_filename,
    metadata_parquet_filename,
    properties_parquet_filename,
    memory_limit_mb=None,
):
    """
    Export all the intermediate files into Parquet files, which will be easier to download and manipulate
    than the multiple original files.

    The `filename` column in every output table is the absolute path of the source file, so
    `intermediate_directory` is resolved to an absolute path before globbing (config passes it in
    relative to the run directory).

    :param intermediate_directory: The intermediate directory containing the concords.
    :param duckdb_filename: A DuckDB file to temporarily store data in.
    :param ids_parquet_filename: The Parquet file to store the IDs.
    :param concords_parquet_filename: The Parquet file to store the concords.
    :param metadata_parquet_filename: The Parquet file to store the ID and concord metadata in.
    :param properties_parquet_filename: The Parquet file to store the properties in.
    :param memory_limit_mb: DuckDB memory limit in MB. When set, overrides DuckDB's default
        (75% of system RAM), which can exceed the SLURM allocation on shared HPC nodes.
    """

    _prepare_duckdb_output(duckdb_filename)
    # DuckDB's write_parquet won't create missing parent directories, so ensure each output's
    # directory exists -- this exporter is public and its Parquet paths need not share a directory.
    for parquet_filename in (
        ids_parquet_filename,
        concords_parquet_filename,
        metadata_parquet_filename,
        properties_parquet_filename,
    ):
        ensure_parent_dir(parquet_filename)

    duckdb_config = {}
    if memory_limit_mb is not None:
        duckdb_config["memory_limit"] = f"{memory_limit_mb}MB"

    with setup_duckdb(duckdb_filename, duckdb_config=duckdb_config) as db:
        # We don't include labels here: the Node-writing code currently emits only nulls for them,
        # so there is nothing useful to join against.
        db.sql("""CREATE TABLE Concord (filename STRING, subj STRING, pred STRING, obj STRING)""")
        db.sql("""CREATE TABLE Identifier (filename STRING, curie STRING, biolink_type STRING)""")
        db.sql(
            """CREATE TABLE Metadata (filename STRING, subject_filename STRING, subject_file_path STRING, metadata_json STRING)"""
        )
        db.sql(
            """CREATE TABLE Property (filename STRING, curie STRING, predicate STRING, value STRING, source STRING)"""
        )

        # Resolve to an absolute path so every `filename` value (and the metadata subject paths
        # derived from it) is absolute, as the DataFormats docs promise -- config passes the
        # intermediate directory in relative to the run directory.
        intermediate_path = Path(intermediate_directory).resolve()

        # Load concord files. A concord line that is not exactly three tab-separated columns is
        # skipped rather than aborting the whole export: this matches write_concord_metadata() in
        # src/metadata/provenance.py, which warns-and-skips such lines instead of failing, so the
        # export must not be stricter than the format the rest of the pipeline enforces.
        # ignore_errors drops the bad line; store_rejects routes it to DuckDB's reject_errors
        # table so we can log each one (with file, line, and reason) after the load.
        loaded_any_concord = False
        for concord_path, subject_filename in _iter_loadable_files(intermediate_path, "concords"):
            if subject_filename is not None:
                _insert_metadata(db, concord_path, subject_filename)
                continue
            logger.info(f"Loading concords from {concord_path}")
            loaded_any_concord = True
            db.execute(
                "INSERT INTO Concord SELECT $1 AS filename, subj, pred, obj "
                "FROM read_csv($1, delim='\\t', header=false, quote='', "
                "ignore_errors=true, store_rejects=true, "
                "columns={'subj': 'VARCHAR', 'pred': 'VARCHAR', 'obj': 'VARCHAR'})",
                [str(concord_path)],
            )

        # DuckDB only creates the reject_errors/reject_scans tables once a store_rejects read has
        # run, so guard the lookup on having loaded at least one concord file. Each rejected line
        # is a concord line that wasn't three columns; warn once per line and once with the total.
        if loaded_any_concord:
            rejected_concord_lines = db.execute(
                "SELECT s.file_path, e.line, e.csv_line, e.error_message "
                "FROM reject_errors e JOIN reject_scans s USING (scan_id, file_id) "
                "ORDER BY s.file_path, e.line"
            ).fetchall()
            for file_path, line_number, csv_line, error_message in rejected_concord_lines:
                logger.warning(
                    f"Skipping malformed concord line {file_path}:{line_number} ({error_message}): {csv_line!r}"
                )
            if rejected_concord_lines:
                logger.warning(
                    f"Skipped {len(rejected_concord_lines)} malformed concord line(s) during intermediate export."
                )

        # Load identifier files.
        for ids_path, subject_filename in _iter_loadable_files(intermediate_path, "ids"):
            if subject_filename is not None:
                _insert_metadata(db, ids_path, subject_filename)
                continue
            # ID files sometimes have a single column and sometimes have two, so we need to
            # determine which one this is. This is a cheap heuristic: it inspects only the first
            # two lines, not the whole file. A later line with a different column count is not
            # caught here and will instead surface as a read_csv error below.
            with open(ids_path, encoding="utf-8") as f:
                first_line = f.readline()
                second_line = f.readline()

            num_cols = len(first_line.rstrip("\n").split("\t"))
            if second_line and len(second_line.rstrip("\n").split("\t")) != num_cols:
                raise RuntimeError(
                    f"Inconsistent number of columns in {ids_path}: {num_cols} (first line: '{first_line}', second line: '{second_line}')."
                )
            if num_cols == 1:
                logger.info(f"Loading identifiers from {ids_path} without a Biolink type column")
                select_type, csv_columns = "NULL AS biolink_type", "{'curie': 'VARCHAR'}"
            elif num_cols == 2:
                logger.info(f"Loading identifiers from {ids_path} with a Biolink type column")
                select_type, csv_columns = "biolink_type", "{'curie': 'VARCHAR', 'biolink_type': 'VARCHAR'}"
            else:
                raise RuntimeError(
                    f"Unexpected number of columns in {ids_path}: {num_cols} (first line: '{first_line}')."
                )
            db.execute(
                f"INSERT INTO Identifier SELECT $1 AS filename, csv.curie, {select_type} "
                f"FROM read_csv($1, delim='\\t', header=false, quote='', columns={csv_columns}) AS csv",
                [str(ids_path)],
            )

        # Load property files. These are gzipped JSONL of src.properties.Property, so unlike the
        # concord and ids files they need no column guessing -- the four keys are fixed by
        # Property.valid_keys(), and read_json's explicit `columns` makes a renamed or missing key
        # fail loudly here rather than arriving as a column of NULLs.
        for property_path, subject_filename in _iter_loadable_files(intermediate_path, "properties"):
            if subject_filename is not None:
                _insert_metadata(db, property_path, subject_filename)
                continue
            logger.info(f"Loading properties from {property_path}")
            db.execute(
                "INSERT INTO Property SELECT $1 AS filename, curie, predicate, value, source "
                "FROM read_json($1, format='newline_delimited', "
                "columns={'curie': 'VARCHAR', 'predicate': 'VARCHAR', 'value': 'VARCHAR', 'source': 'VARCHAR'})",
                [str(property_path)],
            )

        db.table("Concord").write_parquet(concords_parquet_filename)
        db.table("Identifier").write_parquet(ids_parquet_filename)
        db.table("Metadata").write_parquet(metadata_parquet_filename)
        db.table("Property").write_parquet(properties_parquet_filename)
