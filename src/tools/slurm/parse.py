"""Shared parsers for Babel SLURM run artifacts.

Three kinds of artifact are produced by a Snakemake-on-SLURM run and copied into a
run-analysis directory such as ``data/babel-1.17/``:

- ``benchmarks/<rule>.tsv``     — Snakemake ``benchmark:`` output (actual usage).
- ``reports/slurm/slurm_efficiency_reports/`` — the SLURM executor's efficiency
  report (a *directory* containing ``efficiency_report_*.csv``).
- ``logs/rule_<name>/<jobid>.log`` — per-rule control-node logs. ``babel-slurm-resources``
  reads only the declared ``resources:`` block out of these; see :class:`RuleLog` for what
  else is in there and why it is documented rather than parsed. ``babel-slurm-errors`` reads
  more: :func:`declared_runtime_min` takes the ``runtime=`` limit and
  :func:`extract_error_content` quotes the whole log, so an archived run needs these files
  intact for the failure report, not just for the requested side.

Every reader tolerates partial runs and missing/``NA``/``-`` cells.
"""

from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

# --- numeric helpers ---------------------------------------------------------


def _to_float(value: str | None) -> float:
    """Parse a benchmark/report cell to float; missing markers become 0.0."""
    if value is None:
        return 0.0
    value = value.strip()
    if value in ("", "-", "NA", "nan"):
        return 0.0
    try:
        return float(value)
    except ValueError:
        return 0.0


# --- benchmark TSVs ----------------------------------------------------------


@dataclass
class Benchmark:
    """Worst-case actual resource usage for a rule across its benchmark rows.

    A benchmark TSV usually holds one row, but ``repeat()`` runs append more; we
    keep the per-column maximum so sizing decisions reflect the worst observed run.

    Memory figures are **mebibytes**, not megabytes: Snakemake labels the columns "MB" but
    computes them as ``psutil`` bytes ``/ 1024 / 1024``. SLURM's ``mem`` and the efficiency
    report's ``RequestedMem_MB`` are decimal MB, so anything comparing the two must convert --
    see ``MIB_TO_MB`` in ``resources.py``. ``mean_load`` is a percentage where 100% == one
    fully-used core.
    """

    rule: str
    seconds: float
    max_rss_mb: float
    max_vms_mb: float
    max_pss_mb: float
    cpu_time: float
    mean_load: float
    io_in: float
    io_out: float

    @property
    def cores_used(self) -> float:
        """Approximate mean cores used (``mean_load`` is %CPU, 100% == 1 core)."""
        return self.mean_load / 100.0


def read_benchmarks(benchmarks_dir: str | Path) -> dict[str, Benchmark]:
    """Read every ``*.tsv`` benchmark in ``benchmarks_dir`` keyed by rule name.

    The rule name is the file stem (``anatomy_compendia.tsv`` -> ``anatomy_compendia``).

    What this measures across retries and repeated runs: Snakemake rewrites the file on each
    execution and writes nothing for a job that failed, so a rule's row is its **last successful**
    execution -- a rule that died after 30s and then succeeded in 2h reads as 2h. The per-column
    worst case taken here therefore only matters for a ``repeat()`` rule; every file in the
    2026jul22 build held a single row. See "What a retry, or a second run, does to each number" in
    ``docs/tools/Resources.md``.
    """
    benchmarks_dir = Path(benchmarks_dir)
    result: dict[str, Benchmark] = {}
    for path in sorted(benchmarks_dir.glob("*.tsv")):
        with open(path, newline="") as handle:
            rows = list(csv.DictReader(handle, delimiter="\t"))
        if not rows:
            continue
        rule = path.stem
        result[rule] = Benchmark(
            rule=rule,
            seconds=max(_to_float(r.get("s")) for r in rows),
            max_rss_mb=max(_to_float(r.get("max_rss")) for r in rows),
            max_vms_mb=max(_to_float(r.get("max_vms")) for r in rows),
            max_pss_mb=max(_to_float(r.get("max_pss")) for r in rows),
            cpu_time=max(_to_float(r.get("cpu_time")) for r in rows),
            mean_load=max(_to_float(r.get("mean_load")) for r in rows),
            io_in=max(_to_float(r.get("io_in")) for r in rows),
            io_out=max(_to_float(r.get("io_out")) for r in rows),
        )
    return result


# --- SLURM efficiency report -------------------------------------------------


@dataclass
class EfficiencyRow:
    """Per-rule row from the SLURM executor's efficiency report.

    Only ``requested_mem_mb`` and ``ncpus`` are consumed, by ``resources.py``'s
    requested side. The rest are parsed and kept deliberately -- reading them is
    how a future run *confirms* what this cluster does and does not record --
    so do not delete one on the grounds that nothing imports it:

    - ``max_rss_mb`` and ``total_cpu_sec`` come back 0 on clusters without
      ``jobacct_gather``/cgroup accounting, which is why :class:`Benchmark` is
      the authority on usage. A run where they are non-zero means Hatteras
      started recording per-step accounting, and the tool could stop relying on
      the benchmarks for rules that have no ``benchmark:`` block.
    - ``elapsed_sec`` is sacct's ``Elapsed``: the job's **allocation** span,
      start to end. It is *not* what the report prints -- every wall time
      measured there is :attr:`Benchmark.seconds`, the benchmark TSV's ``s`` column,
      which times the rule body from inside the job. The difference is the job's
      setup and teardown (``Elapsed`` exceeded ``s`` for 57 of 57 rules on the
      2026jul22 run, median 5s), and ``--time`` polices ``Elapsed``. Note that
      neither includes time spent pending in the queue; the only number that
      does is ``babel-slurm-errors``', which subtracts the *submit* timestamp.
      See "Three clocks" in ``docs/tools/Resources.md``.
    """

    rule: str
    requested_mem_mb: float
    ncpus: int
    elapsed_sec: float
    total_cpu_sec: float
    max_rss_mb: float


def _efficiency_csvs(path: str | Path) -> list[Path]:
    """Resolve *every* efficiency CSV shard from a file, the ``*.csv`` directory, or a parent.

    The SLURM executor writes ``slurm_efficiency_reports`` as a *directory* and appends a
    fresh ``efficiency_report_<uuid>.csv`` on every Snakemake (re)start, so a single run leaves
    many shards, each covering only the jobs from that invocation. We must read **all** of them
    -- picking just the newest (as an earlier version did) drops almost every rule, since the
    final restart usually re-ran only a handful of jobs.
    """
    path = Path(path)
    if path.is_file():
        return [path]
    if not path.is_dir():
        raise FileNotFoundError(f"No efficiency report found at {path}")
    candidates = sorted(path.rglob("efficiency_report_*.csv")) or sorted(path.rglob("*.csv"))
    if not candidates:
        raise FileNotFoundError(f"No efficiency report CSV under {path}")
    return candidates


def read_efficiency_report(path: str | Path) -> dict[str, EfficiencyRow]:
    """Read the SLURM efficiency report keyed by rule name (``rule_`` prefix stripped).

    Merges every shard (see :func:`_efficiency_csvs`); when a rule appears in more than one shard
    (retries across restarts) we keep the per-column worst case, mirroring how
    :func:`read_benchmarks` keeps the worst observed run.

    Unlike the benchmarks, this includes **failed** attempts: rows are per job step, several per
    attempt, and no state column is consulted. Harmless for the two fields consumed -- a retry asks
    for the same memory and CPUs -- but it is why ``elapsed_sec`` would be the wrong source for a
    duration, since a job killed at its time limit would win the ``max``.
    """
    result: dict[str, EfficiencyRow] = {}
    for csv_path in _efficiency_csvs(path):
        with open(csv_path, newline="") as handle:
            for row in csv.DictReader(handle):
                rule = (row.get("RuleName") or "").strip()
                if rule.startswith("rule_"):
                    rule = rule[len("rule_") :]
                if not rule:
                    continue
                new = EfficiencyRow(
                    rule=rule,
                    requested_mem_mb=_to_float(row.get("RequestedMem_MB")),
                    ncpus=int(_to_float(row.get("NCPUS"))),
                    elapsed_sec=_to_float(row.get("Elapsed_sec")),
                    total_cpu_sec=_to_float(row.get("TotalCPU_sec")),
                    max_rss_mb=_to_float(row.get("MaxRSS_MB")),
                )
                prev = result.get(rule)
                if prev is None:
                    result[rule] = new
                else:
                    result[rule] = EfficiencyRow(
                        rule=rule,
                        requested_mem_mb=max(prev.requested_mem_mb, new.requested_mem_mb),
                        ncpus=max(prev.ncpus, new.ncpus),
                        elapsed_sec=max(prev.elapsed_sec, new.elapsed_sec),
                        total_cpu_sec=max(prev.total_cpu_sec, new.total_cpu_sec),
                        max_rss_mb=max(prev.max_rss_mb, new.max_rss_mb),
                    )
    return result


# --- per-rule control-node logs ---------------------------------------------

_MEM_RE = re.compile(r"\bmem_mb=(\d+)")
_RUNTIME_RE = re.compile(r"\bruntime=(\d+)")
_CPUS_RE = re.compile(r"\bcpus_per_task=(\d+)")

# A per-rule log also carries the job's wall-clock span and whether it failed, and this parser used
# to extract both into fields nothing read. Regexes over free-form log text rot silently when
# Snakemake changes its output, and a frozen test fixture cannot catch that -- it pins the format we
# copied it from, not the one the cluster emits next year. So the extraction is written down here
# instead of carried as code, for whoever needs it:
#
# - **Span.** Each phase is announced by a bracketed local timestamp on its own line, e.g.
#   `[Mon Jul 13 00:57:13 2026]` (`%b %d %H:%M:%S %Y`, day space-padded). Take the first and last
#   in the file: `re.compile(r"\[(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun) (\w+ +\d+ [\d:]+ \d{4})\]")`.
#   That span covers the job's own execution, so it is close to the benchmark's `s` rather than to
#   sacct's `Elapsed` -- see "Three clocks" in docs/tools/Resources.md before comparing it to
#   anything.
# - **Failure.** `Error in rule `, `RuleException`, or `Traceback (most recent call last):` appears
#   in a failed attempt's log. Note this has to be checked in *every* attempt's log, not just the
#   newest, since a rule that failed twice and then succeeded leaves all three.
#
# Prefer `parse_job_events()` (below) over either: the aggregate sbatch `.err` log names every
# attempt with submit and finish timestamps in one place, which is what `babel-slurm-errors` reports
# from. Reach for the per-rule logs only when that file is gone.
# `logs/rule_process_ec_ids/52504.log` in the babel-1.18 run of 2026-07-13 is a worked example of
# both markers.


@dataclass
class RuleLog:
    """The ``resources:`` a rule's job declared, read from its newest log.

    Only what ``resources.py`` consumes: the declared block is a fallback for the requested side
    (:class:`EfficiencyRow` first) and the first choice for the runtime limit. The comment above
    covers the span and failure state this deliberately does not extract.
    """

    rule: str
    job_id: str
    log_path: Path
    mem_mb: int | None
    runtime_min: int | None
    cpus: int | None


def read_rule_logs(logs_dir: str | Path) -> dict[str, RuleLog]:
    """Walk ``logs_dir/rule_<name>/<jobid>.log`` and read each rule's declared ``resources:``.

    When a rule has several job logs (retries) only the newest is read: a retry declares the same
    block as the attempt before it unless the snakefile changed mid-run.
    """
    logs_dir = Path(logs_dir)
    result: dict[str, RuleLog] = {}
    for rule_dir in sorted(logs_dir.glob("rule_*")):
        if not rule_dir.is_dir():
            continue
        rule = rule_dir.name[len("rule_") :]
        logs = sorted(rule_dir.glob("*.log"), key=lambda p: p.stat().st_mtime)
        if not logs:
            continue
        newest = logs[-1]
        text = newest.read_text(errors="replace")
        mem = _MEM_RE.search(text)
        runtime = _RUNTIME_RE.search(text)
        cpus = _CPUS_RE.search(text)
        result[rule] = RuleLog(
            rule=rule,
            job_id=newest.stem,
            log_path=newest,
            mem_mb=int(mem.group(1)) if mem else None,
            runtime_min=int(runtime.group(1)) if runtime else None,
            cpus=int(cpus.group(1)) if cpus else None,
        )
    return result


# --- aggregate sbatch error log (used by the ``errors`` subcommand) ----------

_SUBMIT_RE = re.compile(
    r"(?:INFO|ERROR) snakemake\.logging \[(\S+)\]: Job (\d+) has been submitted with SLURM jobid (\d+) \(log: (\S+)\)\."
)
_FINISH_RE = re.compile(r"(?:INFO|ERROR) snakemake\.logging \[(\S+)\]: Finished jobid: (\d+) \(Rule: (\w+)\)")
_ERROR_RE = re.compile(r"ERROR snakemake\.logging \[(\S+)\]: Error in rule (\w+), jobid: (\d+)")
_RULE_RE = re.compile(r"Error in rule (\w+):")
_LOG_RE = re.compile(r"log: (\S+\.log)")

# Substrings identifying the DuckDB memory-diagnostic log lines emitted by
# src/exporters/duckdb_exporters.py. These pinpoint cgroup vs memory_limit headroom and the
# tracked/untracked split at a `bad allocation` OOM. The connect-time headroom line sits near the
# top of the log and the threads>1 SIGABRT path leaves no traceback, so the default
# "last N lines" / traceback extraction misses them; we scan the whole log and surface them
# explicitly. (We deliberately do not match the verbose per-setting dump, e.g. " - memory_limit:".)
_MEMORY_DIAGNOSTIC_MARKERS = (
    "DuckDB memory headroom:",
    "Memory snapshot (",
    "DuckDB operation failed during",
)

# Characters that only appear in DuckDB's in-place progress bar. Snakemake captures every
# carriage-return redraw, so a single "line" can be hundreds of KB of repeated bar frames; we
# collapse any run of them to one marker so the full log stays readable.
_PROGRESS_BAR_RE = re.compile(r"[▕▏█▎▍▌▋▊▉▐]")


def find_err_file(version: str | None, logs_dir: Path) -> Path:
    """Locate the main Snakemake ``sbatch-<version>.err`` control-node log."""
    if version:
        path = logs_dir / f"sbatch-{version}.err"
        if not path.exists():
            raise FileNotFoundError(f"Error log not found: {path}")
        return path
    candidates = list(logs_dir.glob("sbatch-*.err"))
    if not candidates:
        raise FileNotFoundError(f"No sbatch-*.err files found in {logs_dir}")
    return max(candidates, key=lambda p: p.stat().st_mtime)


def parse_failures(err_file: Path) -> list[tuple[str, Path]]:
    """Return ``(rule_name, log_path)`` pairs from the main Snakemake error log."""
    results: list[tuple[str, Path]] = []
    current_rule: str | None = None
    for line in err_file.read_text(errors="replace").splitlines():
        if m := _RULE_RE.search(line):
            current_rule = m.group(1)
        if (m := _LOG_RE.search(line)) and current_rule:
            results.append((current_rule, Path(m.group(1))))
            current_rule = None
    return results


def _collect_memory_diagnostics(lines: list[str]) -> list[str]:
    """Return the DuckDB memory-diagnostic lines anywhere in the log, in order, de-duplicated."""
    return list(
        dict.fromkeys(line.rstrip() for line in lines if any(marker in line for marker in _MEMORY_DIAGNOSTIC_MARKERS))
    )


def _collapse_progress_noise(lines: list[str]) -> list[str]:
    """Replace each run of DuckDB progress-bar redraw lines with a single elision marker."""
    cleaned: list[str] = []
    in_progress = False
    for line in lines:
        if _PROGRESS_BAR_RE.search(line):
            if not in_progress:
                cleaned.append("[... DuckDB progress-bar output elided ...]")
                in_progress = True
            continue
        in_progress = False
        cleaned.append(line)
    return cleaned


def extract_error_content(log_path: Path, max_lines: int, logs_dir: Path | None = None) -> str:
    """Return the failed rule's log for the report.

    We show the *whole* log (so the real exception is never hidden by tail/traceback heuristics --
    Snakemake's RuleException/OutOfMemory blocks are neither a Python "Traceback" nor always within
    the last N lines), with two cleanups: DuckDB's progress-bar redraw spam is collapsed, and the
    memory-diagnostic lines are echoed in a labelled section at the end so they are easy to find.
    ``max_lines`` caps a pathologically long log to a head + tail so the report stays usable.

    The main error log records each rule's log by its *absolute* path on the cluster. When
    analyzing a run copied off the cluster, that path won't resolve, so if ``logs_dir`` is given we
    fall back to the same ``rule_<name>/<jobid>.log`` under it (the relative form
    :func:`print_job_summary` already uses).
    """
    if not log_path.exists() and logs_dir is not None:
        local = logs_dir / log_relative(str(log_path))
        if local.exists():
            log_path = local
    if not log_path.exists():
        return f"(log file not found: {log_path})"

    raw_lines = log_path.read_text(errors="replace").splitlines()
    lines = _collapse_progress_noise(raw_lines)

    # Show the full (de-spammed) log, but guard against a pathologically long one by keeping a
    # generous head and tail. The cap is large enough that ordinary rule logs are shown in full.
    if len(lines) > max_lines:
        head = lines[: max_lines // 4]
        tail = lines[-(max_lines - max_lines // 4) :]
        elided = len(lines) - len(head) - len(tail)
        content = "\n".join(head + [f"[... {elided} log lines elided ...]"] + tail)
    else:
        content = "\n".join(lines)

    # Echo the memory diagnostics in a clearly-labelled trailer so they are easy to find even in a
    # long log (and present even if the head/tail cap dropped them).
    diagnostics = _collect_memory_diagnostics(raw_lines)
    if diagnostics:
        content += "\n\n--- DuckDB memory diagnostics ---\n" + "\n".join(diagnostics)

    return content


# --- job-event timeline (used by the ``errors`` subcommand's run summary) ----


@dataclass
class JobEvent:
    """One SLURM job attempt parsed from the main Snakemake error log."""

    snakemake_jobid: int
    slurm_jobid: int
    rule_name: str
    wildcard: str  # "" for simple rules; "Cell.txt" etc. for parametrised rules
    log_relative: str  # e.g. rule_get_HMDB/672.log
    submitted_at: datetime
    finished_at: datetime | None = None
    failed: bool = False


_TZ_OFFSET_RE = re.compile(r"([+-])(\d{2})(\d{2})$")


def _parse_ts(ts_str: str) -> datetime:
    # Normalise any bare ±HHMM offset → ±HH:MM for Python < 3.11 fromisoformat compatibility.
    ts_str = _TZ_OFFSET_RE.sub(r"\1\2:\3", ts_str)
    return datetime.fromisoformat(ts_str)


def log_relative(remote_log_path: str) -> str:
    """Extract the logs-dir-relative path (``rule_FOO/.../N.log``) from a remote path."""
    parts = remote_log_path.split("/rule_", 1)
    return ("rule_" + parts[1]) if len(parts) == 2 else remote_log_path


def declared_runtime_min(log_relative_path: str, logs_dir: Path, default: int = 120) -> int:
    """Read the declared ``runtime=`` (minutes) from a job's per-rule log, or ``default``."""
    local_log = logs_dir / log_relative_path
    if not local_log.exists():
        return default
    for line in local_log.read_text(errors="replace").splitlines():
        if "resources:" in line:
            if m := _RUNTIME_RE.search(line):
                return int(m.group(1))
    return default


def parse_job_events(err_file: Path) -> list[JobEvent]:
    """Return the SLURM job attempts parsed from the main Snakemake error log."""
    # Snakemake reuses the same snakemake jobid across retries, so we track both the
    # currently-open attempt per snakemake jobid and all superseded attempts separately.
    current: dict[int, JobEvent] = {}
    all_jobs: list[JobEvent] = []
    for line in err_file.read_text(errors="replace").splitlines():
        if m := _SUBMIT_RE.search(line):
            ts, snakemake_id, slurm_id, log_path = m.group(1), int(m.group(2)), int(m.group(3)), m.group(4)
            rel = log_relative(log_path)
            parts = rel.split("/")
            rule = parts[0][len("rule_") :]
            wildcard = "/".join(parts[1:-1])
            if snakemake_id in current:
                all_jobs.append(current[snakemake_id])  # save prior attempt before retry overwrites it
            current[snakemake_id] = JobEvent(
                snakemake_jobid=snakemake_id,
                slurm_jobid=slurm_id,
                rule_name=rule,
                wildcard=wildcard,
                log_relative=rel,
                submitted_at=_parse_ts(ts),
            )
        elif m := _FINISH_RE.search(line):
            snakemake_id = int(m.group(2))
            if snakemake_id in current and current[snakemake_id].finished_at is None:
                current[snakemake_id].finished_at = _parse_ts(m.group(1))
        elif m := _ERROR_RE.search(line):
            snakemake_id = int(m.group(3))
            if snakemake_id in current:
                current[snakemake_id].failed = True
                # First one wins. Snakemake reports a failure twice: once when the job dies, and
                # again in the summary it prints when the workflow gives up, which can be many
                # hours later -- `process_ec_ids` died 39s into the 2026jul22 build and was
                # re-reported 23.4h later, at the end of the run. Overwriting turned that into a
                # 23.4h "failed after", i.e. the length of the run rather than of the job.
                if current[snakemake_id].finished_at is None:
                    current[snakemake_id].finished_at = _parse_ts(m.group(1))
    all_jobs.extend(current.values())
    return all_jobs


# --- declared resources in the snakefiles ------------------------------------

# `mem="512G"` / `mem=8000`, `runtime="7h"` / `runtime=240`, `cpus_per_task=4`. Only literals are
# matched: `mem=lambda wildcards: ...` (export_synonyms_to_duckdb) resolves per-wildcard at runtime
# and has no single declared value, so it is left as None rather than mis-parsed.
_DECL_RULE_RE = re.compile(r"^(?:rule|checkpoint)\s+(\w+)\s*:")
# The trailing comma is optional (snakefmt supplies one, a hand-edited last entry may not) and a
# trailing `# ...` comment is common -- `chemical_compendia` explains its runtime that way. Any unit
# suffix is matched here and checked against the key in _parse_resource(), so a swapped unit
# (`mem="7h"`) is recorded as unreadable rather than crashing the parse.
_DECL_RESOURCE_RE = re.compile(r'^\s+(mem|runtime|cpus_per_task)=("?)([0-9]+(?:\.[0-9]+)?[A-Za-z]?)\2\s*,?\s*(?:#.*)?$')
# The same keys with *any* value. A line matching this but not the regex above is a declaration this
# parser cannot read; it is recorded rather than dropped (see DeclaredResources). `mem\w*` so that
# `mem_mb=8000` -- the name Snakemake normalizes `mem` to internally, and a spelling a rule could
# reasonably use -- is recorded rather than matching neither pattern and vanishing.
_DECL_RESOURCE_KEY_RE = re.compile(r"^\s+(?:mem\w*|runtime|cpus_per_task)\s*=")
# A `resources:` block ends at the next top-level directive (`run:`, `shell:`, `input:`, ...).
_DECL_SECTION_RE = re.compile(r"^\s{4}\w+:")


@dataclass
class DeclaredResources:
    """The static ``resources:`` a rule declares in its snakefile.

    The efficiency report has no time-limit column and a run's ``logs/`` are often incomplete, so the
    snakefiles are the only broad source of the declared ``runtime``. Any field may be None: the rule
    declares nothing (and gets the profile default) or declares a callable.
    """

    rule: str
    mem_mb: int | None
    runtime_min: int | None
    cpus: int | None
    #: ``resources:`` lines naming one of the three keys whose value this parser could not read -- a
    #: callable, or a unit it does not know. A missed declaration would otherwise read as "declares
    #: nothing", i.e. as the cluster-wide default, which makes an over-provisioned rule look
    #: correctly sized; the failure is invisible in the report, so it is recorded here and asserted
    #: against the real snakefiles in ``tests/tools/slurm/test_parse.py``.
    unparsed: tuple[str, ...] = ()


# The unit suffixes each key accepts, lowercased. Sized resources are decimal, not binary:
# Snakemake reads `mem="512G"` as 512000 MB, not 524288.
_RESOURCE_UNITS: dict[str, dict[str, float]] = {
    "mem": {"": 1, "m": 1, "g": 1000, "t": 1_000_000},  # -> MB
    "runtime": {"": 1, "h": 60},  # -> minutes
    "cpus_per_task": {"": 1},  # -> cores; a unit here is a typo
}


def _parse_resource(key: str, value: str) -> int | None:
    """``("mem", "512G")`` -> 512000, ``("runtime", "7h")`` -> 420, a bare number as-is.

    Returns None when the value carries a unit that key does not take (``mem="7h"``). That is a
    declaration this parser cannot read, not a value to guess at, so it goes to
    :attr:`DeclaredResources.unparsed` -- which is loud in the report and asserted against the real
    snakefiles -- rather than raising a ``ValueError`` with no file or line to point at.
    """
    suffix = value[-1].lower() if value[-1].isalpha() else ""
    units = _RESOURCE_UNITS[key]
    if suffix not in units:
        return None
    return int(float(value[:-1] if suffix else value) * units[suffix])


def read_snakefile_resources(snakefile_dir: str | Path) -> dict[str, DeclaredResources]:
    """Read every rule's statically-declared ``resources:`` from the ``*.snakefile`` files in a
    directory.

    Only ``src/snakefiles/*.snakefile`` is read, not the root ``Snakefile``: the rules it defines
    (``all``, ``clean_*``, ``uncompress_synonym_file``) declare no ``resources:`` and never appear in
    a sizing pass. Add it here if that changes.

    Raises ``FileNotFoundError`` when the directory is missing or holds no ``*.snakefile``. Returning
    an empty mapping instead would be the :attr:`DeclaredResources.unparsed` failure one level up: a
    typo'd ``--snakefile-dir`` would yield a full, plausible report in which every rule inherits the
    cluster-wide default, so ``generate_pubmed_concords`` (``runtime="24h"``) reads as a 1000%
    overrun and every genuinely trimmable rule vanishes from the report.
    """
    snakefile_dir = Path(snakefile_dir)
    paths = sorted(snakefile_dir.glob("*.snakefile"))
    if not paths:
        raise FileNotFoundError(f"no *.snakefile files in {snakefile_dir}")

    result: dict[str, DeclaredResources] = {}
    for path in paths:
        rule: str | None = None
        in_resources = False
        values: dict[str, int] = {}
        unparsed: list[str] = []
        for line in path.read_text(errors="replace").splitlines():
            if match := _DECL_RULE_RE.match(line):
                if rule:
                    result[rule] = DeclaredResources(
                        rule, values.get("mem"), values.get("runtime"), values.get("cpus_per_task"), tuple(unparsed)
                    )
                rule, in_resources, values, unparsed = match.group(1), False, {}, []
            elif rule and _DECL_SECTION_RE.match(line):
                in_resources = line.strip().startswith("resources:")
            elif rule and in_resources and (match := _DECL_RESOURCE_RE.match(line)):
                parsed = _parse_resource(match.group(1), match.group(3))
                if parsed is None:
                    unparsed.append(line.strip())  # a literal, but in a unit that key doesn't take
                else:
                    values[match.group(1)] = parsed
            elif rule and in_resources and _DECL_RESOURCE_KEY_RE.match(line):
                unparsed.append(line.strip())
        if rule:
            result[rule] = DeclaredResources(
                rule, values.get("mem"), values.get("runtime"), values.get("cpus_per_task"), tuple(unparsed)
            )
    return result
