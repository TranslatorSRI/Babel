"""Unit tests for src.tools.slurm.parse."""

import os
from pathlib import Path

import pytest

from src.tools.slurm import parse
from src.util import get_repo_root

pytestmark = pytest.mark.unit


def _write_benchmark(path, rows):
    header = "s\th:m:s\tmax_rss\tmax_vms\tmax_uss\tmax_pss\tio_in\tio_out\tmean_load\tcpu_time"
    lines = [header] + ["\t".join(str(c) for c in r) for r in rows]
    path.write_text("\n".join(lines) + "\n")


# --- parse.read_benchmarks ---------------------------------------------------


def test_read_benchmarks_takes_worst_case_across_rows(tmp_path):
    bdir = tmp_path / "benchmarks"
    bdir.mkdir()
    # two attempts; reader should keep the per-column maximum
    _write_benchmark(
        bdir / "my_rule.tsv",
        [
            [10.0, "0:00:10", 100.0, 200.0, 90.0, 95.0, 1.0, 2.0, 95.0, 9.0],
            [20.0, "0:00:20", 300.0, 400.0, 280.0, 290.0, 3.0, 4.0, 190.0, 38.0],
        ],
    )
    benches = parse.read_benchmarks(bdir)
    assert set(benches) == {"my_rule"}
    b = benches["my_rule"]
    assert b.seconds == 20.0
    assert b.max_rss_mb == 300.0
    assert b.mean_load == 190.0
    assert b.cores_used == pytest.approx(1.9)


def test_read_benchmarks_tolerates_missing_cells(tmp_path):
    bdir = tmp_path / "benchmarks"
    bdir.mkdir()
    _write_benchmark(bdir / "r.tsv", [[5.0, "0:00:05", 50.0, 60.0, 40.0, 45.0, "-", "NA", 90.0, 4.0]])
    b = parse.read_benchmarks(bdir)["r"]
    assert b.io_in == 0.0 and b.io_out == 0.0


# --- parse.read_efficiency_report --------------------------------------------


def test_read_efficiency_report_from_directory_and_strips_rule_prefix(tmp_path):
    # SLURM writes the report as a *directory* of efficiency_report_*.csv files.
    rep = tmp_path / "reports" / "slurm" / "slurm_efficiency_reports"
    rep.mkdir(parents=True)
    (rep / "efficiency_report_abc.csv").write_text(
        ",JobID,JobName,RuleName,Elapsed,TotalCPU,NNodes,NCPUS,MaxRSS,ReqMem,"
        "Elapsed_sec,TotalCPU_sec,MaxRSS_MB,RequestedMem_MB,MainJobID,CPU Efficiency (%),Memory Usage (%)\n"
        "3,427.0,python,rule_taxon_compendia,00:05:00,00:00:00,1,4,,,300.0,NA,,64000.0,427,0.0,0.0\n"
    )
    eff = parse.read_efficiency_report(tmp_path / "reports" / "slurm")
    assert "taxon_compendia" in eff
    row = eff["taxon_compendia"]
    assert row.requested_mem_mb == 64000.0
    assert row.ncpus == 4
    # accounting gap: MaxRSS / TotalCPU come back empty -> 0
    assert row.max_rss_mb == 0.0
    assert row.total_cpu_sec == 0.0


def test_read_efficiency_report_merges_all_shards_worst_case(tmp_path):
    # A real run leaves one efficiency_report_<uuid>.csv per Snakemake (re)start; each shard only
    # covers that invocation's jobs. Reading just the newest (as an earlier version did) would drop
    # almost every rule -- so all shards must be merged.
    rep = tmp_path / "reports" / "slurm" / "slurm_efficiency_reports"
    rep.mkdir(parents=True)
    header = ",RuleName,NCPUS,Elapsed_sec,TotalCPU_sec,MaxRSS_MB,RequestedMem_MB\n"
    # Older, larger shard with two rules.
    (rep / "efficiency_report_aaa.csv").write_text(
        header + "0,rule_alpha,1,100.0,0.0,,16000\n0,rule_beta,2,200.0,0.0,,32000\n"
    )
    # Newest shard re-ran only alpha, with a larger reservation.
    newest = rep / "efficiency_report_zzz.csv"
    newest.write_text(header + "0,rule_alpha,4,150.0,0.0,,64000\n")
    os.utime(newest, (10**10, 10**10))  # make it unambiguously newest

    eff = parse.read_efficiency_report(tmp_path / "reports" / "slurm")
    # beta survives even though it is absent from the newest shard.
    assert set(eff) == {"alpha", "beta"}
    assert eff["beta"].requested_mem_mb == 32000
    # alpha keeps the worst-case (largest) reservation across shards.
    assert eff["alpha"].requested_mem_mb == 64000
    assert eff["alpha"].ncpus == 4


# --- parse.read_rule_logs ----------------------------------------------------


def test_read_rule_logs_parses_declared_resources(tmp_path):
    """Every line here is copied verbatim from a real log, not composed to suit the parser.

    Source: `logs/rule_process_ec_ids/52504.log` from the babel-1.18 run of 2026-07-13 (the job
    that failed on a missing `babel_downloads/EC/enzyme.rdf`), lines 31, 32, 37, 81-83, 95 and 96.
    A real `resources:` line carries keys an invented one omits -- `mem_mib` and `disk_mib` sit
    beside `mem_mb`, and `disk_mb` precedes it -- which is exactly what `_MEM_RE`'s `\\bmem_mb=`
    has to not confuse; the fixture this replaced had none of them.

    The failure and timestamp lines are kept in the fixture even though nothing reads them now:
    they are what a real log looks like around the `resources:` line, and the comment in `parse.py`
    that documents how to extract them points here.
    """
    logs = tmp_path / "logs"
    rdir = logs / "rule_process_ec_ids"
    rdir.mkdir(parents=True)
    (rdir / "52504.log").write_text(
        "[Mon Jul 13 00:57:13 2026]\n"
        "rule process_ec_ids:\n"
        "    resources: tmpdir=<TBD>, disk_mb=50000, disk=50 GB, disk_mib=47684, mem_mb=16000,"
        " mem=16 GB, mem_mib=15259, runtime=120, cpus_per_task=1\n"
        "RuleException:\n"
        'FileNotFoundError in file "/projects/babel/runs/gaurav/babel-1.18/src/snakefiles/process.snakefile", line 46:\n'
        "[Errno 2] No such file or directory: 'babel_downloads/EC/enzyme.rdf'\n"
        "[Mon Jul 13 00:57:17 2026]\n"
        "Error in rule process_ec_ids:\n"
    )
    out = parse.read_rule_logs(logs)
    assert "process_ec_ids" in out
    log = out["process_ec_ids"]
    # mem_mb, not mem_mib (15259) and not disk_mb (50000).
    assert (log.mem_mb, log.runtime_min, log.cpus) == (16000, 120, 1)
    assert log.job_id == "52504"


# --- parse.extract_error_content ---------------------------------------------


def test_extract_error_content_shows_full_log_and_real_exception(tmp_path):
    """The whole log is shown, so a RuleException far from the tail (not a Python Traceback,
    not in the last N lines) is never hidden -- this is the 1870.log failure shape."""
    log = (
        "INFO src.exporters.duckdb_exporters: DuckDB memory headroom: "
        "memory_limit=400G, cgroup hard limit (SLURM mem)=512.0 GiB\n"
        + "\n".join(f"filler {i}" for i in range(40))
        + "\nRuleException:\n"
        "OutOfMemoryException in file duckdb.snakefile, line 163:\n"
        "Out of Memory Error: Failed to allocate block of 8650496 bytes (bad allocation)\n"
        + "\n".join(f"snakemake boilerplate {i}" for i in range(80))
    )
    log_path = tmp_path / "1870.log"
    log_path.write_text(log)

    content = parse.extract_error_content(log_path, max_lines=1000)

    # The exception (76+ lines from the end, not a Python Traceback) is present in the full log...
    assert "Failed to allocate block of 8650496 bytes" in content
    assert "filler 0" in content  # ...and so is the top of the log.
    # The memory line appears both inline and in the labelled trailer.
    assert "--- DuckDB memory diagnostics ---" in content
    assert "cgroup hard limit (SLURM mem)=512.0 GiB" in content


def test_extract_error_content_collapses_progress_bar_spam(tmp_path):
    """DuckDB progress-bar redraw lines are collapsed to a single marker, not dumped verbatim."""
    progress = " 58% ▕██████████████████████                ▏ (~9 seconds remaining)"
    log = (
        "starting\n"
        + "\n".join(progress for _ in range(500))
        + "\nMemory snapshot (complete): process peak RSS=66.7 GiB; cgroup current=120.0 GiB\n"
        "done\n"
    )
    log_path = tmp_path / "p.log"
    log_path.write_text(log)

    content = parse.extract_error_content(log_path, max_lines=1000)

    assert "[... DuckDB progress-bar output elided ...]" in content
    assert content.count("seconds remaining") == 0
    assert "starting" in content and "done" in content


def test_extract_error_content_caps_pathologically_long_log(tmp_path):
    """A very long log is capped to a head + tail with an elision marker so the report stays usable."""
    log = "\n".join(f"line {i}" for i in range(5000))
    log_path = tmp_path / "long.log"
    log_path.write_text(log)

    content = parse.extract_error_content(log_path, max_lines=1000)

    assert "log lines elided" in content
    assert "line 0" in content  # head kept
    assert "line 4999" in content  # tail kept


def test_extract_error_content_falls_back_to_logs_dir_for_remote_path(tmp_path):
    """The main error log records each rule log by its absolute cluster path; when the run has been
    copied off the cluster, extraction falls back to the same rule_<name>/<jobid>.log under
    logs_dir instead of reporting the file as missing."""
    logs_dir = tmp_path / "logs"
    (logs_dir / "rule_get_HMDB").mkdir(parents=True)
    (logs_dir / "rule_get_HMDB" / "672.log").write_text("RuleException:\nHTTP Error 503\n")
    remote = Path("/projects/babel/runs/whoever/babel_outputs/logs/rule_get_HMDB/672.log")

    # Without logs_dir the absolute remote path can't resolve...
    assert "log file not found" in parse.extract_error_content(remote, max_lines=1000)
    # ...with it, the local copy is found and its content is shown.
    content = parse.extract_error_content(remote, max_lines=1000, logs_dir=logs_dir)
    assert "HTTP Error 503" in content


# --- parse._collect_memory_diagnostics ---------------------------------------


def test_collect_memory_diagnostics_dedupes_and_ignores_settings_dump():
    """Diagnostic markers are collected and de-duplicated; the verbose settings dump is ignored."""
    lines = [
        "INFO ...:  - memory_limit: 1.2 TiB",  # verbose dump, must NOT be collected
        "INFO ...: DuckDB memory headroom: memory_limit=700G, cgroup hard limit (SLURM mem)=1500.0 GiB",
        "INFO ...: DuckDB memory headroom: memory_limit=700G, cgroup hard limit (SLURM mem)=1500.0 GiB",  # dup
        "INFO ...: Memory snapshot (complete): process peak RSS=120.0 GiB; cgroup peak=unknown",
    ]

    found = parse._collect_memory_diagnostics(lines)

    assert len(found) == 2
    assert not any("- memory_limit:" in line for line in found)
    assert any("DuckDB memory headroom" in line for line in found)
    assert any("Memory snapshot (complete)" in line for line in found)


# --- parse.parse_job_events --------------------------------------------------


def test_parse_job_events_tracks_retries_and_outcomes(tmp_path):
    err = tmp_path / "sbatch-test.err"
    err.write_text(
        "INFO snakemake.logging [2026-06-04T05:00:00+0000]: "
        "Job 5 has been submitted with SLURM jobid 100 (log: /remote/babel_outputs/logs/rule_get_x/100.log).\n"
        "ERROR snakemake.logging [2026-06-04T05:10:00+0000]: Error in rule get_x, jobid: 5\n"
        "INFO snakemake.logging [2026-06-04T05:11:00+0000]: "
        "Job 5 has been submitted with SLURM jobid 101 (log: /remote/babel_outputs/logs/rule_get_x/101.log).\n"
        "INFO snakemake.logging [2026-06-04T05:20:00+0000]: Finished jobid: 5 (Rule: get_x)\n"
    )
    jobs = parse.parse_job_events(err)
    assert len(jobs) == 2  # the retry does not clobber the first attempt
    first, second = sorted(jobs, key=lambda j: j.slurm_jobid)
    assert first.slurm_jobid == 100 and first.failed is True and first.finished_at is not None
    assert second.slurm_jobid == 101 and second.failed is False and second.finished_at is not None
    assert {j.rule_name for j in jobs} == {"get_x"}


def test_parse_job_events_against_a_real_control_log():
    """Parse an excerpt of a real sbatch `.err`, kept verbatim at `tests/data/`.

    Source: `logs/sbatch-1.18-run-1.err` from the 2026jul22 build, lines 709-716, 929-946 and
    3234-3244, with the elided spans marked. A composed log cannot stand in for this one, because
    the real file says everything **twice** -- once bare and once prefixed
    `INFO/ERROR snakemake.logging [<ts>]:` -- and only the prefixed form carries the timestamp the
    parser needs. It also holds the two shapes this test exists for: the same failure reported at
    both the moment it happened and again in the end-of-run summary, and an error for a job whose
    submission is outside the excerpt.
    """
    err = get_repo_root() / "tests" / "data" / "sbatch-2026jul22-run-1-excerpt.err"

    jobs = parse.parse_job_events(err)

    # One attempt per *prefixed* submission line, of which the excerpt has six: the bare duplicates
    # must not double-count, and the `Error in rule export_synonyms_to_duckdb, jobid: 302` in the
    # summary must not invent an attempt for a job this excerpt never saw submitted.
    assert len(jobs) == 6
    assert 302 not in {j.snakemake_jobid for j in jobs}

    by_rule = {j.rule_name: j for j in jobs}
    # process_ec_ids was submitted 04:56:58 and died 04:57:37 -- 39 seconds. The identical error
    # line reappears at 04:21:18 the *next day* when the workflow gave up; taking that one reported
    # this job as having failed after 23.4h, which is the length of the run.
    failed = by_rule["process_ec_ids"]
    assert failed.failed is True
    assert (failed.finished_at - failed.submitted_at).total_seconds() == 39

    # A job that finished is unaffected: `Finished jobid:` is logged once.
    ok = by_rule["get_wikidata_cell_relationships"]
    assert ok.failed is False
    assert (ok.finished_at - ok.submitted_at).total_seconds() == 39

    # The four jobs with neither a finish nor an error line in the excerpt are still open.
    assert sum(1 for j in jobs if j.finished_at is None) == 4


# --- parse.parse_failures ----------------------------------------------------


def test_parse_failures_extracts_rule_and_log_path(tmp_path):
    """parse_failures() returns (rule_name, log_path) pairs from the sbatch .err log,
    grouping each 'Error in rule X:' header with the nearest 'log: ...' line that follows it."""
    err = tmp_path / "sbatch-test.err"
    err.write_text(
        "Error in rule get_HMDB:\n"
        "    log: /remote/babel_outputs/logs/rule_get_HMDB/672.log (check log file(s) for error details)\n"
        "Error in rule anatomy_compendia:\n"
        "    log: /remote/babel_outputs/logs/rule_anatomy_compendia/891.log (check log file(s) for error details)\n"
        # A bare log: line with no preceding rule header should be ignored.
        "    log: /remote/babel_outputs/logs/rule_orphan/1.log\n"
    )
    failures = parse.parse_failures(err)
    assert len(failures) == 2
    names = [rule for rule, _ in failures]
    assert names == ["get_HMDB", "anatomy_compendia"]
    paths = [str(path) for _, path in failures]
    assert any("rule_get_HMDB/672.log" in p for p in paths)
    assert any("rule_anatomy_compendia/891.log" in p for p in paths)


def test_parse_failures_returns_empty_for_clean_log(tmp_path):
    err = tmp_path / "sbatch-clean.err"
    err.write_text("INFO snakemake.logging: All done.\n")
    assert parse.parse_failures(err) == []


# --- parse._parse_ts ---------------------------------------------------------


def test_parse_ts_normalises_non_utc_offsets():
    """_parse_ts must handle non-UTC offsets like -0400 and +0530, not just +0000."""
    from src.tools.slurm.parse import _parse_ts

    dt_utc = _parse_ts("2026-06-04T05:00:00+0000")
    assert dt_utc.utcoffset().total_seconds() == 0

    dt_neg = _parse_ts("2026-06-04T05:00:00-0400")
    assert dt_neg.utcoffset().total_seconds() == -4 * 3600

    dt_pos = _parse_ts("2026-06-04T05:00:00+0530")
    assert dt_pos.utcoffset().total_seconds() == 5.5 * 3600


def test_read_snakefile_resources_parses_literals_and_skips_callables(tmp_path):
    """A `mem=lambda wildcards: ...` has no single declared value; it must read as None, not a bad parse."""
    (tmp_path / "a.snakefile").write_text(
        "rule plain:\n"
        "    input:\n"
        '        mem="not a resource",\n'
        "    resources:\n"
        '        mem="512G",\n'
        '        runtime="7h",\n'
        "        cpus_per_task=4,\n"
        "    run:\n"
        "        go()\n"
        "\n"
        "rule callable_mem:\n"
        "    resources:\n"
        '        mem=lambda wildcards: "512G" if wildcards.filename == "Protein" else "128G",\n'
        "        runtime=240,\n"
        "    run:\n"
        "        go()\n"
        "\n"
        "rule no_resources:\n"
        "    run:\n"
        "        go()\n"
    )
    parsed = parse.read_snakefile_resources(tmp_path)

    # "512G" is 512000 MB, not 524288: Snakemake's sized resources are decimal.
    assert parsed["plain"].mem_mb == 512000
    assert parsed["plain"].runtime_min == 420  # "7h"
    assert parsed["plain"].cpus == 4
    # A callable mem yields None while the rule's other literals still parse.
    assert parsed["callable_mem"].mem_mb is None
    assert parsed["callable_mem"].runtime_min == 240  # a bare number is already minutes
    # A rule with no resources: block is present but empty, not missing.
    assert parsed["no_resources"] == parse.DeclaredResources("no_resources", None, None, None)


def test_read_snakefile_resources_reads_checkpoints_comments_and_a_missing_trailing_comma(tmp_path):
    """snakefmt supplies a trailing comma, but a hand-edited last entry may not have one; a
    `checkpoint` declares resources exactly like a `rule`; and a trailing `# ...` comment is how
    several rules explain their limit. Missing any of these silently drops the rule's declared
    limits, which then read as the cluster default -- `chemical_compendia`'s commented
    `runtime="7h"` is the real case.
    """
    (tmp_path / "b.snakefile").write_text(
        "checkpoint split_things:\n"
        "    resources:\n"
        '        mem="64G",  # explained inline, as chemical_compendia does\n'
        '        runtime="4h"\n'  # no trailing comma
        "    run:\n"
        "        go()\n"
    )
    parsed = parse.read_snakefile_resources(tmp_path)
    assert parsed["split_things"].mem_mb == 64000
    assert parsed["split_things"].runtime_min == 240


def test_read_snakefile_resources_matches_the_real_snakefiles(tmp_path):
    """Guard against a regex tightening that silently stops matching real declarations.

    A resource the parser misses reads as the cluster-wide default, which makes an over-provisioned
    rule look correctly sized -- the failure is invisible in the report.
    """
    parsed = parse.read_snakefile_resources(get_repo_root() / "src" / "snakefiles")
    # chemical_compendia declares both, and its runtime carries a trailing `# ...` comment.
    assert parsed["chemical_compendia"].mem_mb == 512000
    assert parsed["chemical_compendia"].runtime_min == 420
    # Across all snakefiles, a healthy number of rules declare something; a broken regex zeroes this.
    assert sum(1 for d in parsed.values() if d.mem_mb or d.runtime_min) > 20

    # The count above survives a single dropped rule, so check the misses directly: every
    # mem/runtime/cpus_per_task line the parser could not read is recorded, and there is exactly one
    # in the repo -- export_synonyms_to_duckdb's per-wildcard `mem=lambda`. Anything else here is a
    # declaration silently reading as the cluster default, which makes an over-provisioned rule look
    # correctly sized.
    unparsed = {rule: d.unparsed for rule, d in parsed.items() if d.unparsed}
    assert list(unparsed) == ["export_synonyms_to_duckdb"], f"unreadable resource declarations: {unparsed}"
    assert unparsed["export_synonyms_to_duckdb"][0].startswith("mem=lambda wildcards:")


def test_read_snakefile_resources_records_a_declaration_it_cannot_read(tmp_path):
    """A unit the parser doesn't know must be recorded, not silently read as "declares nothing"."""
    (tmp_path / "c.snakefile").write_text(
        "rule terabyte:\n"
        "    resources:\n"
        '        mem="1.5T",\n'  # known unit: parsed
        "        runtime=config['runtime'],\n"  # unknown shape: recorded
        "    run:\n"
        "        go()\n"
    )
    parsed = parse.read_snakefile_resources(tmp_path)
    assert parsed["terabyte"].mem_mb == 1_500_000
    assert parsed["terabyte"].runtime_min is None
    assert parsed["terabyte"].unparsed == ("runtime=config['runtime'],",)


def test_read_snakefile_resources_records_a_unit_the_key_does_not_take(tmp_path):
    """`mem="7h"` is a typo, not a size. It must be recorded like any other unreadable declaration
    rather than raising `ValueError: could not convert string to float: '7h'` from somewhere with no
    file or line to point at."""
    (tmp_path / "d.snakefile").write_text(
        'rule swapped_units:\n    resources:\n        mem="7h",\n        runtime="512G",\n    run:\n        go()\n'
    )
    parsed = parse.read_snakefile_resources(tmp_path)
    assert parsed["swapped_units"].mem_mb is None
    assert parsed["swapped_units"].runtime_min is None
    assert parsed["swapped_units"].unparsed == ('mem="7h",', 'runtime="512G",')


def test_read_snakefile_resources_records_a_key_spelling_it_does_not_parse(tmp_path):
    """`mem_mb=` is what Snakemake normalizes `mem` to internally, so a rule could reasonably be
    written that way. It matches neither the value pattern nor `mem=`, so without `mem\\w*` in the
    key pattern it would vanish -- reading as "declares nothing", i.e. as the cluster default, which
    is the failure `unparsed` exists to make visible."""
    (tmp_path / "e.snakefile").write_text(
        "rule normalized:\n    resources:\n        mem_mb=8000,\n    run:\n        go()\n"
    )
    parsed = parse.read_snakefile_resources(tmp_path)
    assert parsed["normalized"].mem_mb is None
    assert parsed["normalized"].unparsed == ("mem_mb=8000,",)


def test_read_snakefile_resources_raises_when_it_finds_no_snakefiles(tmp_path):
    """A typo'd --snakefile-dir must fail loudly, not return an empty mapping.

    Empty means "no rule declares anything", so every rule inherits the cluster-wide default and the
    report reads as plausible: `generate_pubmed_concords` (`runtime="24h"`) becomes a 1000% overrun
    and every genuinely trimmable rule disappears. That is the `unparsed` failure one level up."""
    with pytest.raises(FileNotFoundError):
        parse.read_snakefile_resources(tmp_path / "does_not_exist")
    with pytest.raises(FileNotFoundError):
        parse.read_snakefile_resources(tmp_path)  # exists, but holds no *.snakefile
