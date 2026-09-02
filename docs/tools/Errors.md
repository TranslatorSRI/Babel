# `babel-slurm-errors` — aggregate failing-rule logs

When a Babel run on SLURM stalls or fails, the failure is usually spread across the main Snakemake
control-node log and one per-rule log per failing job. This subcommand gathers all of that into a
single report so you can paste it somewhere (a code review tool, an issue, a chat) and decide which
rules to re-run.

## Two workflows

**During an ongoing run — poll for status and act on failures early.**
Run `babel-slurm-errors` on a loop while the cluster job is still running. The stderr summary tells
you at a glance which rules have finished, which are still in flight, and which have already failed
(with elapsed vs. timeout). The stdout report gives you the full log for each failure, formatted for
pasting directly into a coding agent. You can hand a failure off to the agent to diagnose and fix
while the rest of the DAG continues running on the cluster — failures that are code bugs get a fix
in flight; failures that are transient (HTTP 503s, OOM) you can queue for a re-run at the end.

**After a run completes or stalls — triage before re-running.**
`slurm/run-babel-on-slurm.sh` generates this report automatically into
`babel_outputs/logs/error-report-<version>.md` when Snakemake exits non-zero, so the report is
already waiting when you log back in. Use it to decide which rules need a code fix vs. a simple
re-run, and to confirm nothing was left silently broken.

```bash
uv run babel-slurm-errors <version> [--logs-dir DIR] [--logs] [--markdown] [--traceback-only] [--lines N]
```

`<version>` is the tag in the `sbatch-<version>.err` control-node log (e.g. `1.17-try-2`); omit it
to auto-detect the newest `sbatch-*.err` in the logs directory. `--logs-dir` defaults to
`babel_outputs/logs`.

By default only the stderr job summary is printed — a failing rule is usually faster to diagnose
by reproducing it locally (`uv run pytest`, or re-running the rule directly) than by reading its
log, and a rule that failed and retried several times would otherwise print the same log body
once per attempt. Pass `--logs` to also print the deduplicated error report on stdout.

## What it reads and writes

It reads the main `sbatch-<version>.err` log, follows each `Error in rule X` to that rule's
per-rule `*.log`, and produces two outputs on two streams:

- **stdout — the error report (only with `--logs`).** One section per distinct failure. Rules
  whose logs are byte-for-byte identical are grouped together, so a recurring transient failure (an
  HTTP 503 from a data source hitting many rules) shows up once rather than dozens of times. With
  `--markdown` each section is a fenced code block (handy for pasting into a review tool or chat);
  otherwise sections are separated by `===` banners. `slurm/run-babel-on-slurm.sh` passes `--logs`
  and redirects this stream to `babel_outputs/logs/error-report-<version>.md`.
- **stderr — the job summary.** A roll-up of every job attempt parsed from the control-node log,
  split into three buckets: still-running rules (with elapsed time versus the declared timeout and
  the time remaining), completed rules, and truly-failed jobs (those with no active retry). A rule
  that failed and was retried is listed under its still-running attempt with the prior failure as an
  indented sub-line. Because this is on stderr, it stays visible in the terminal (or the sbatch
  `.err` log) even when stdout is redirected to a file.

## Design notes

A few behaviors exist because of the specific failure shapes this pipeline produces:

- **A failed job is timed from when it died, not from when the run gave up.** Snakemake logs
  `Error in rule X, jobid: N` twice: once as the job fails, and again in the summary it prints when
  the workflow finally aborts — which on a long build is many hours later. The parser keeps the
  **first**, because the second is a property of the run, not of the job. Across the 2026jul22 and
  babel-1.17 builds this affected 63 of 103 failed attempts, overstating them by a median of ~3.9
  hours: `get_ensembl` read as `85,777s` where the job actually died after `20s`. Those figures come
  from [`failed-job-durations/`](failed-job-durations/check_failed_durations.py), which runs both
  parses over the archived logs and diffs them — rerun that rather than re-deriving by hand. A
  duration here spans submit → finish, so unlike the durations
  [`babel-slurm-resources`](Resources.md) reports, it *does* include time spent queueing.
- **The whole rule log is shown, not a tail or a Python traceback.** Snakemake's `RuleException` and
  `OutOfMemoryException` blocks are neither Python `Traceback`s nor reliably near the end of the
  log, so a tail/traceback heuristic would routinely hide the real error. A pathologically long log
  is capped to a head + tail with an elision marker (`--lines N`, default 1000) so the report stays
  usable.
- **DuckDB progress-bar spam is collapsed.** DuckDB redraws an in-place progress bar with a carriage
  return, and Snakemake captures every redraw, so one logical "line" can be hundreds of KB of
  repeated bar frames. Runs of those are collapsed to a single marker.
- **DuckDB memory diagnostics are surfaced.** The connect-time memory-headroom line sits near the
  top of a log and the multi-threaded out-of-memory path can abort without a traceback, so any
  DuckDB memory-diagnostic lines are echoed in a labelled trailer at the end of the section where
  they are easy to find.

`--traceback-only` restricts the report to rules whose logs contain a Python traceback, which is
occasionally useful when you only care about code bugs rather than transient infrastructure errors.
