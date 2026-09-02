# `babel-slurm-resources` — right-size SLURM resources

Babel runs on the RENCI Hatteras cluster as a Snakemake-on-SLURM pipeline. Each rule reserves
memory, CPUs, and wall time. Over-reserving throttles parallelism (a 191 GB batch node fits only
~3 jobs that each ask for 64 GB); under-reserving causes OOM kills and timeouts. This subcommand
measures what rules actually used on a past run and turns that into recommended, right-sized
`resources:`.

```bash
uv run babel-slurm-resources <run-dir> [--csv PATH] [--safety F] [--floor-gb N] \
    [--new-default-mem-gb N] [--new-default-cpus N] \
    [--snakefile-dir DIR] [--default-runtime-min N]
```

`<run-dir>` is a directory containing `benchmarks/`, `logs/`, and (optionally) `reports/slurm/` —
either `babel_outputs/` itself or a copy archived for analysis, such as `data/babel-1.17/`.

A captured example of the full report is committed at
[`examples/babel-slurm-resources-babel-1.17.md`](examples/babel-slurm-resources-babel-1.17.md).
The 2026jul22 pass has no committed capture: it was run before the mebibyte/megabyte fix below, so
every figure in it needed a mental ×1.048576 to be read correctly, which is not something a captured
example should ask of anyone. The conclusions it produced live where they are useful instead — in
each rule's `resources:` comment and in the hotspot table in
[`slurm/README.md`](../../slurm/README.md), both in corrected decimal GB.

## The data a run produces

A Snakemake-on-SLURM run leaves three kinds of artifact under `babel_outputs/`:

- `benchmarks/<rule>.tsv` — Snakemake `benchmark:` output, written from *inside* each job. The
  columns include `s` (wall seconds), `max_rss` (peak RAM), `mean_load` (%CPU, where 100 = one
  fully-used core), and `cpu_time`. This is the **authoritative source for actual memory and CPU
  usage**. When a rule has several benchmark rows (from retries or `repeat()`), the reader keeps the
  per-column worst case. Snakemake labels the memory columns "MB" but computes them as bytes
  `/ 1024 / 1024`, so they are really **mebibytes** — see "Units" below.
- `reports/slurm/` — the SLURM executor's efficiency report. The executor appends a **fresh
  `efficiency_report_<uuid>.csv` shard on every Snakemake (re)start**, and each shard covers only
  that invocation's jobs, so a run that restarted several times leaves many shards and the final one
  usually holds just a handful of rules. The reader therefore merges *all* shards (worst case per
  rule); reading only the newest would drop the requested-side data for almost every rule. When
  archiving a run, copy the whole directory, not just the newest file.
- `logs/rule_<name>/<jobid>.log` — per-rule control-node logs. The only thing *this* tool reads out
  of them is the declared `resources:` line, as a fallback for the requested side and as the first
  choice for the runtime limit. A log also carries the job's own start/end timestamps and its
  failure state; neither is parsed here, and the comment above `RuleLog` in
  `src/tools/slurm/parse.py` says how to extract them if you ever need to. **Every** wall time the
  report *measures* comes from the benchmarks. Do not trim these logs from an archive on the
  strength of that, though: [`babel-slurm-errors`](Errors.md) reads the `runtime=` limit *and*
  quotes the whole log for its failure excerpts.

### Why the benchmark TSVs, not the efficiency report

The efficiency report is the natural place to look for memory and CPU usage, but on Hatteras its
`MaxRSS` and `TotalCPU` columns come back **empty** — the cluster's `jobacct_gather`/cgroup
accounting isn't capturing per-step usage, so every `CPU Efficiency (%)` and `Memory Usage (%)` is
`0`. The tool therefore consumes the efficiency report only for the *requested* side
(`RequestedMem_MB` and `NCPUS` — those two columns and no others; the rest are parsed but unread,
see `EfficiencyRow`) and relies on the `benchmark:` TSVs for actual usage. Because the
recommendations come from the benchmarks, the override list (below) is reliable even when the
requested side is sparse.

That includes wall time: **every duration the report measures is the benchmark TSV's `s` column**
(`Benchmark.seconds`, the per-column worst case across a rule's rows), never the efficiency
report's `Elapsed_sec` or the span between a log's timestamps. The durations it does *not* measure
are the runtime limits it prints beside them, which come from the log, the snakefile, or the
cluster default (see "Runtime fit" below).

### Three clocks, and which one a time limit polices

A run records a job's duration three times, over three different spans:

| Number | Source | Spans |
|--------|--------|-------|
| benchmark `s` | Snakemake, from inside the job | the rule's execution |
| `Elapsed_sec` | sacct, via the efficiency report | job start → end |
| `babel-slurm-errors`' duration | the aggregate sbatch `.err` log | submit → finish |

They are not interchangeable. [`babel-slurm-errors`](Errors.md) subtracts the Snakemake *submit*
timestamp, so its figure includes time the job spent **pending in the queue**; sacct's `Elapsed`
starts when the job is allocated and so excludes it. On the 2026jul22-era run under `data/`,
submit→finish exceeded `Elapsed_sec` by a median of 35s and a maximum of 306s (over 60s for 15 of
57 rules) — small only because that cluster was mostly free. Do not read a long duration in the
errors report as a slow rule without checking whether the job was waiting.

That holds for a *failed* attempt too — it is timed from the moment it died, not from when the run
gave up hours later. Getting that right needed a fix, because Snakemake reports each failure twice;
see "A failed job is timed from when it died" under
[`Errors.md`'s design notes](Errors.md#design-notes).

`--time` polices `Elapsed`, which was ≥ the benchmark's `s` for **57 of 57** rules on that run, by
a median of 5s: the gap is job startup and teardown around the benchmarked body. So sizing from
`s` slightly *understates* the span the limit applies to. At the default `--safety 1.5` that is
noise, but it is the reason not to trim a runtime to a hair above the benchmark.

`Elapsed_sec` is parsed into `EfficiencyRow` and simply not used, which is deliberate: it is a
named column read straight into a float, so it cannot quietly start meaning something else, and
having it parsed and documented is what a future "how long do jobs hold their allocation?" question
needs.

### What a retry, or a second run, does to each number

A Babel build usually takes several `sbatch` runs to finish, and rules fail and are retried inside
each one. The three clocks handle that differently, so a number is only comparable to another
number of the same kind:

- **Benchmarks: the last *successful* execution, and nothing else.** Snakemake rewrites
  `benchmarks/<rule>.tsv` on each execution (all 355 files from the 2026jul22 build hold exactly
  one row), and a failed job writes no benchmark at all — the two rules whose every attempt failed
  in that build left no file. So a rule that died after 30s and then succeeded in 2h reports 2h,
  with no averaging and no trace of the failure. `leftover_umls` on babel-1.17 is the worked
  example: attempts failed at 9885s, 17254s and 2148s, succeeded at ~2367s, and the benchmark
  reads 2292s. `read_benchmarks()` does keep the per-column worst case across rows, but that only
  fires for a `repeat()` rule.

  Two consequences for a multi-run build. The benchmark set is a **mixture**: each rule's numbers
  come from whichever run last succeeded at it, not from one coherent run. And a success is
  *sticky* — a rule that succeeded in run 1 and then failed in run 3 still reports run 1's
  numbers, which are real but older than the build you think you are sizing.

- **The efficiency report: the per-column maximum over every attempt, failures included.** Its rows
  are per job *step* (`53155.0`, `53155.1`, …), several per attempt and several attempts per rule,
  and every shard is merged with `max`. Nothing consults a state column, so a failed attempt cannot
  be excluded. That is harmless for the two fields actually consumed — a retry requests the same
  memory and CPUs — but it is another reason not to reach for `Elapsed_sec`, which would take the
  worst attempt including one killed at its time limit.

- **`babel-slurm-errors`: one entry per attempt**, marked failed or not, which is the only one of
  the three that can tell you a rule failed twice before it worked.

The per-rule logs' start/end timestamps and failure state are the opposite case and are **not**
parsed. Extracting those means matching free-form log text, which rots silently when Snakemake
changes its output — and a test fixture cannot catch that, since it pins the format it was copied
from rather than the one the cluster emits next year. The recipe for both lives in a comment above
`RuleLog` in `src/tools/slurm/parse.py`, so re-deriving them is a few lines rather than an
investigation. Prefer `parse_job_events()` anyway: the aggregate sbatch `.err` log names every
attempt with its submit and finish timestamps in one place.

### Units

> **TODO** ([#1014](https://github.com/NCATSTranslator/Babel/issues/1014)): this whole section
> exists to help readers navigate an inconsistency, not to document a design. Once memory is
> reported in one unit everywhere, cut it down to a single sentence naming that unit and delete the
> two-factor table below.

Everything the tool prints is **decimal GB/MB**, so a recommended `mem` is literally the string to
paste into a `resources:` block: `mem="8G"` reaches SLURM as 8000 MB, and the efficiency report's
`RequestedMem_MB` is decimal too.

The benchmark TSVs are the exception — their "MB" columns are mebibytes — so they are converted on
the way in (`MIB_TO_MB` in `src/tools/slurm/resources.py`). Comparing the two unconverted is a ~4.9%
error, and always in the unsafe direction: a rule looks further from its limit than it is. This is
the same trap as `duckdb_memory_limit_mb()` in `src/snakefiles/util.py`, where Snakemake's
`resources.mem` re-exposes `mem="512G"` as the decimal `"512 GB"` and `mem_mb` as `512000`.

Practical consequence when reading a rule's comment: a benchmark peak of "132G" needs `mem="142G"`
to be at 100% of its limit, not `mem="132G"`.

Converting a peak by hand — as the `resources:` comments quote it — needs the right one of two
factors, and they are easy to swap:

| From | To | Factor | Use it on |
|------|----|--------|-----------|
| MiB | MB | ×1.048576 | a benchmark's raw `max_rss` column |
| GiB | GB | ×1.073741824 | a peak already displayed in GiB, e.g. `132.1G` in a captured report |

Applying the MiB factor to a GiB figure leaves you ~2.4% low, which is small enough to look right:
`111.6 GiB` is `119.8 GB`, not the `117.0 GB` that factor gives.

## What it reports

For each rule with a benchmark, it joins actual usage against the requested resources and prints:

- a per-rule listing sorted by peak RSS — actual RSS, requested mem, percent of the request used,
  cores used, wall time, and the recommended `mem`/`cpus`;
- a **recommended `mem`** — the observed peak times a safety factor (`--safety`, default 1.5),
  rounded up to a bucket (8/16/24/32/48/64 GB…), floored at `--floor-gb` (default 8). A safety
  factor is used because an OOM is a hard kill that wastes the whole job and one benchmark captures
  only a single run's peak (source data grows between runs);
- a **"rules needing an explicit override before lowering the default"** list — rules whose
  recommendation exceeds the proposed new cluster default (`--new-default-mem-gb`, default 16, and
  `--new-default-cpus`, default 1). This is the safety gate: lowering the cluster-wide default
  without giving these rules an explicit `resources:` block would silently starve them.

  **This list is a superset of the rules you must act on**, and a `ran on default` column splits it:
  **yes** means the rule requested the run's default mem (no explicit `resources:` block) and needs
  a *new* one before the default drops — the actionable subset; **no** means it already carries a
  block (e.g. `protein_compendia` at 512G) and is safe; **?** means there was no requested-side data
  for it at all — neither an efficiency-report row nor a declared `mem` — so check that one by hand.
  The default
  the run used is auto-detected as the modal requested mem and printed in the header, so there's no
  need to know or pass it.

Each rule is classified `over` (requested ≥ 2× the recommendation), `at-risk` (actual > 80% of the
request), `ok`, or `no-request-data` (a benchmark with no matching requested-side row). Pass `--csv`
to also write the full per-rule table for further analysis.

**One row per rule, not per wildcard.** A wildcard rule writes a benchmark per wildcard value
(`export_compendia_to_duckdb_Food`) but declares its `resources:` once, so its instances are folded
into a single row whose usage columns are the worst case across them — marked `(×25)` in the report
and counted in the CSV's `instances` column. Scoring the instances separately measures every small
one against a limit sized for the largest (`export_compendia_to_duckdb_Food`: 20 seconds and 0.5 GB
against Protein's `runtime="4h", mem="512G"`), which added ~80 unactionable `over` rows across the
four wildcard rules, and let a 25-instance rule outvote 24 ordinary ones in the run-default
detection below.

### Runtime fit

A **Runtime fit** section does the same for wall time. The limit comes from the rule's log, then its
snakefile's declared `runtime`, then the cluster default (`--default-runtime-min`, 120 to match
`slurm/config.yaml`). The snakefile is what makes this usable: the efficiency report has no
time-limit column and a run usually retains logs for only a handful of rules, so without
`--snakefile-dir` (defaulting to this repo's `src/snakefiles`) a rule declaring `runtime="24h"`
would look like a catastrophic overrun against the 2-hour default.

Time matters in *both* directions, which is why the recommendation isn't simply generous. Too little
and the job is killed outright; too much and Snakemake's remaining-time estimate becomes useless and
a job that has become pathologically slow no longer stands out. Rules are `at-risk` above 80% of
their limit and `over` when the limit is at least twice what they need.

**`over` only applies to a rule that declares its own `runtime`.** Nearly every rule runs for
seconds against the cluster-wide default, so classifying those would bury the real findings under
hundreds of rows nobody can act on. Whether the *default itself* is the right size is one decision,
reported as a single line naming the slowest rule still on it. The value that line suggests leaves
that rule *below* the 80% at-risk line, not merely above its wall time — a bucket chosen to just
cover a 58-minute rule would put it at 97% of the new default, at-risk from the first run. That
value can be *above* the current default (the slowest rule on it is already at risk), so the line
says which direction to move: it is compared against `--default-runtime-min` before being phrased.

Two traps the 2026jul22 pass hit, both worth checking before trimming anything:

- **A rule whose input shrank is not over-provisioned.** UniProtKB dropped ~41% upstream in
  2026jul22, so `protein_compendia` ran 5.5h/246G there against 7.6h/337G on babel-1.17. Sizing from
  the smaller run alone would have set limits that OOM the next normal release.
- **Network-bound rules vary by an order of magnitude.** `get_ensembl` took 3 minutes on babel-1.17
  and 1.9h on 2026jul22. Their generous runtimes are deliberate; leave them.

Always compare against a second run's benchmarks before trimming — the numbers for a compute-bound
rule are usually stable to within a percent or two (`untyped_chemical_compendia` peaked at 132.0G
and 132.1G across the two runs), so a rule that *isn't* stable is telling you something.

### Two ways the declared side can be wrong

The actual-usage side comes from the run; the declared side comes from wherever the tool can find
it, and those two can describe different worlds.

- **`--snakefile-dir` reads the checkout you are standing in, not the one that produced the run.**
  It defaults to this repo's `src/snakefiles`, so re-running the tool over an old run *after* a
  sizing pass compares that run against limits it never had — every rule you just re-sized reports
  against the new number. Point `--snakefile-dir` at a checkout of the run's own tag
  (`git worktree add ../babel-2026jul22 2026jul22`) whenever the declarations have moved since,
  which is exactly the case when comparing two sizing passes. A path holding no `*.snakefile` is a
  hard error rather than an empty result: silently reading nothing would make every rule inherit the
  cluster default, so `generate_pubmed_concords` (`runtime="24h"`) would read as a 1000% overrun and
  every genuinely trimmable rule would vanish from the report.
- **A `mem=lambda wildcards: ...` rule is summarised by its largest branch.** The report is per
  rule (see "One row per rule" above), and a rule whose request *varies* per wildcard has no single
  declared value to report: `export_synonyms_to_duckdb` reads as 136.9G of actual usage against a
  512G request — the `Protein`/`GeneProteinConflated` branch — and says nothing about the 128G
  branch the other sixteen instances run under. It is the only such rule today; size its branches
  from the individual `benchmarks/export_synonyms_to_duckdb_*.tsv` files against the per-instance
  rows in the efficiency report, not from the classification.

## Workflow

1. Run the pipeline; let it write `benchmarks/`, `logs/`, and `reports/slurm/`.
2. If the run stalls, use [`babel-slurm-errors`](Errors.md) to find which rules failed (often
   transient HTTP errors from data sources) and re-run them.
3. After a complete run, run `babel-slurm-resources <run-dir>` and, for each rule in the override
   list marked `ran on default = yes`, add a `resources:` block. Bias the numbers
   **down**: pick the smallest bucket comfortably above the observed peak (~10-15% headroom is fine)
   rather than the padded `--safety 1.5` recommendation. An OOM is cheap — we track per-rule peak
   RSS, so it's easy to bump the limit and re-run — whereas padding every rule slowly ratchets the
   whole cluster's reservation upward, which is exactly the over-provisioning we lowered the default
   to escape. The known heavy rules and the current defaults are documented in
   [`slurm/README.md`](../../slurm/README.md).
4. Work the **Runtime fit** section the same way: give every `at-risk` rule an explicit `runtime`
   (or raise the one it has), and trim the `over` rules that declare their own. Check each against a
   second run's benchmarks first — see the two traps above.
5. Re-run the analyzer to confirm the override list is empty (modulo rules that already carry a
   block) and that every rule left `at-risk` on either axis is one you *decided* to leave. A
   deliberate exception is fine — `generate_pubmed_concords` sits at 83% of its 24h limit because
   that limit is known to work and the real fix is to make the rule cheaper — but it belongs in
   [`slurm/README.md`](../../slurm/README.md) with the reasoning, or the next sizing pass will
   "fix" it by reserving wall time nobody needed.
6. On later runs, re-check the *existing* override rules against the "req mem" vs "actual RSS"
   columns and trim any that have grown over-provisioned. There is no CI guard for this (it would
   require committing benchmark data); it's a periodic manual pass on the files a run leaves behind.
   A release is the natural cadence — see the `release-notes` skill.
