# Running Babel on SLURM

This directory contains the Snakemake profile for running Babel on SLURM clusters (tested on the
RENCI Hatteras cluster).

## Quick Start

Snakemake
[recommends](https://snakemake.github.io/snakemake-plugin-catalog/plugins/executor/slurm.html#should-i-run-snakemake-on-the-login-node-of-my-cluster)
running the primary Snakemake process on the login node of your Slurm cluster. I've found that
running it in a low-memory low-CPU node (by running `sbatch run-babel-on-slurm.sh` to run
[run-babel-on-slurm.sh](./run-babel-on-slurm.sh)) works fine, and ensures that you don't get
complaints from your cluster manager about long-running login node processes.

```bash
# Activate the environment
uv sync

# Run the full pipeline on SLURM (up to 50 parallel jobs)
uv run snakemake --profile slurm

# Run a single semantic type
uv run snakemake --profile slurm anatomy
uv run snakemake --profile slurm chemical
```

## Profile Overview (`slurm/config.yaml`)

| Setting | Value | Notes |
|---------|-------|-------|
| `executor` | `slurm` | Uses Snakemake's built-in SLURM executor |
| `jobs` | 50 | Max parallel SLURM jobs |
| `default-resources.mem` | 16G | Per-job default RAM (90% of rules peak below 8G) |
| `default-resources.cpus_per_task` | 1 | Per-job default CPUs (only DuckDB rules use more) |
| `default-resources.runtime` | 120 min | Per-job default wall time |
| `python.executable` | `/usr/bin/time -v python` | Captures memory/time to job stderr |
| `slurm-efficiency-report` | True | Writes per-job efficiency CSV |

Download-only rules (e.g. `get_EFO`, `get_mesh`, `get_rhea`, `get_pubchem`, `download_umls`)
override the defaults with `mem="8G", cpus_per_task=1` since they are I/O-bound. Large downloads
(UniProtKB idmapping/trembl, UMLS) also set `runtime="6h"`.

## Benchmark Data

Every non-trivial rule writes a benchmark TSV file to:

```text
babel_outputs/benchmarks/<rule_name>.tsv
```

For wildcard rules (e.g. one job per compendium), files are named:

```text
babel_outputs/benchmarks/export_compendia_to_duckdb_<filename>.tsv
babel_outputs/benchmarks/generate_kgx_<filename>.tsv
babel_outputs/benchmarks/generate_sapbert_training_data_<filename>.tsv
babel_outputs/benchmarks/generate_content_report_for_compendium_<compendium_basename>.tsv
babel_outputs/benchmarks/export_synonyms_to_duckdb_<filename>.tsv
babel_outputs/benchmarks/uncompress_synonym_file_<synonym_file>.tsv
```

### Benchmark TSV Fields

| Column | Description |
|--------|-------------|
| `s` | Wall-clock time in seconds |
| `h:m:s` | Wall-clock time formatted as hours:minutes:seconds |
| `max_rss` | Maximum resident set size (MB) — peak RAM usage |
| `max_vms` | Maximum virtual memory size (MB) |
| `max_uss` | Maximum unique set size (MB) |
| `max_pss` | Maximum proportional set size (MB) |
| `io_in` | MB read from disk |
| `io_out` | MB written to disk |
| `mean_load` | Average CPU load (100 = 1 full core) |
| `cpu_time` | Total CPU time in seconds |

`max_rss` is the most useful column for right-sizing SLURM memory allocations. Add ~20–30%
headroom over `max_rss` when setting `mem:` for a rule.

## SLURM Efficiency Report

In addition to per-rule benchmarks, the SLURM executor writes an efficiency report at:

```text
babel_outputs/reports/slurm/slurm_efficiency_reports
```

This is a *directory*, not a file: the executor appends a fresh `efficiency_report_<uuid>.csv`
shard on every Snakemake (re)start, each covering only that invocation's jobs. It captures
SLURM-level efficiency metrics (CPU efficiency, memory efficiency) per job, complementing the
Snakemake benchmark TSVs. The two sources measure slightly different things:

- **Benchmark TSVs** — measured by Snakemake inside the job; independent of SLURM accounting
- **Efficiency report** — reported by SLURM's `sacct`; includes job-scheduling overhead. On
  Hatteras its `MaxRSS`/`TotalCPU` columns come back empty (the `jobacct_gather`/cgroup accounting
  isn't capturing them), so the benchmark TSVs are authoritative for actual usage.

The `src.tools.slurm resources` subcommand reads and merges all shards in this directory; see
[`../docs/tools/Resources.md`](../docs/tools/Resources.md).

## Known Resource Hotspots

These rules have hard-coded `resources:` overrides and should not be reduced without new benchmarks:

| Rule | File | `mem` | `runtime` | Notes |
|------|------|-------|-----------|-------|
| `protein_compendia` | `protein.snakefile` | 512G | 12h | Largest protein join |
| `chemical_compendia` | `chemical.snakefile` | 512G | 7h | Full chemical graph; raised from 6h when Food.txt pushed it to 5.5h |
| `untyped_chemical_compendia` | `chemical.snakefile` | 256G | — | Pre-typing step; 132 GiB = 141.8 GB peak on both babel-1.17 and 2026jul22. Cut from 512G to the peak plus the standard 1.5x safety factor; revisit after the next full run |
| `gene_compendia` | `gene.snakefile` | 256G | 6h | Gene graph |
| `export_compendia_to_duckdb` | `duckdb.snakefile` | 512G | 4h | Per-compendium DuckDB export; `cpus_per_task=4` (DuckDB auto-threads) |
| `export_synonyms_to_duckdb` | `duckdb.snakefile` | 512G / 128G | 1h | Per-synonyms DuckDB export (512G for Protein/GeneProteinConflated); `cpus_per_task=4` |
| `check_for_identically_labeled_cliques` | `duckdb.snakefile` | 512G | — | Two-pass: GROUP BY hash(LOWER(preferred_name)) + streaming-join pair output; memory_limit **16G**, 1 thread — capped low to bound the buffer-pool mapping count under vm.max_map_count (see Known Issues) |
| `check_for_duplicate_curies` | `duckdb.snakefile` | 1500G | — | GROUP BY curie over all edges; memory_limit 1000G, 1 thread |
| `check_for_duplicate_clique_leaders` | `duckdb.snakefile` | 512G | — | Two-pass over the smaller Clique table; memory_limit 400G, 4 threads |
| `generate_prefix_report` | `duckdb.snakefile` | 1500G | — | approx_count_distinct() over all edges, biolink_type read from the denormalized Edge column (no join); memory_limit 1000G, 1 thread. Replaced the former `generate_curie_report` + `generate_clique_leader_report`, scanning the Edge set once instead of twice |
| `chembl_labels_and_smiles` | `datacollect.snakefile` | 128G | — | RDF parse |
| `chemical_unichem_concordia` | `chemical.snakefile` | 192G | — | UniChem merge (119.8 GB peak, was 94% of 128G) |
| `generate_pubmed_concords` | `publications.snakefile` | 128G | 24h | Full PubMed parse; 17.5h on babel-1.17, 20.0h on 2026jul22. Reported at-risk (83%) and left alone on purpose — see below |
| `generate_pubmed_compendia` | `publications.snakefile` | 192G | 4h | PubMed compendium build; 132.5 GB peak was at or past its own 128G request, and 88% of the 2h default |
| `geneprotein_conflated_synonyms` | `geneprotein.snakefile` | 512G | 6h | Conflated synonym merge |
| `drugchemical_conflation` | `drugchemical.snakefile` | 96G | — | Drug/chemical conflation (61.2 GB peak, was 96% of 64G) |
| `geneprotein_conflation` | `geneprotein.snakefile` | 64G | — | Gene/protein conflation (~48G peak) |
| `generate_sapbert_training_data` | `exports.snakefile` | 64G | 3h | Slowest wildcard instance 1.9h; estimate for the pair deduplication set, recheck against the benchmarks |
| `get_uniprotkb_labels` | `datacollect.snakefile` | 48G | — | UniProtKB label parse (~40G peak) |
| `hmdb_labels_and_synonyms` | `datacollect.snakefile` | 48G | — | HMDB XML parse (~30G peak) |
| `check_protein_completeness` | `protein.snakefile` | 24G | — | Loads full Protein compendium (~21G peak) |
| `get_chemical_unichem_relationships` | `chemical.snakefile` | 32G | — | UniChem structure parse (22.4 GB peak, was 93% of 24G) |
| `check_chemical_completeness` | `chemical.snakefile` | 24G | — | 14.7 GB peak, was 92% of the 16G default |
| `taxon_compendia` | `taxon.snakefile` | 24G | — | 15.1 GB peak, was 95% of the 16G default |
| `chemical` | `chemical.snakefile` | — | 4h | Gzips every chemical synonyms file; 1.9h on both runs, 93% of the 2h default |
| `generate_kgx` | `exports.snakefile` | — | 4h | Slowest wildcard instance 2.7h |
| `protein` | `protein.snakefile` | — | 4h | Gzips protein synonyms; `cpus_per_task=6` |
| `drugchemical_conflated_synonyms` | `drugchemical.snakefile` | — | 4h | 2.7h on 2026jul22 |

The block below the divider was added when the default dropped from 64G to 16G: these rules ran on
the old default with no explicit block and peak above 16G, so they need one now.

The `mem` column and the peaks quoted beside it are **decimal GB**, the unit `mem="NG"` actually
means to SLURM. A Snakemake benchmark reports mebibytes under an "MB" label, so a rule "peaking at
132G" in a benchmark needs 141.8 GB of `mem`. Convert a whole-GiB figure with **×1.073741824**, not
×1.048576: the latter is the MiB→MB factor and converts the benchmark's raw `max_rss` column, so
applying it to a figure already displayed in GiB leaves you ~2.4% low. The error is always in the
direction of looking safer than it is — as a *fraction of the limit* it is ~4.9%, which is what made
`untyped_chemical_compendia` read as 84% of a 160G limit when it was 89%.
`babel-slurm-resources` converts on the way in and reports decimal throughout.

Sizes were last reviewed against the **2026jul22** run (`babel-slurm-resources`, which since that
review also reports runtime fit). Two cautions from it:

- `protein_compendia` will be reported as over-provisioned until UniProtKB recovers: it shrank
  ~41% upstream in 2026jul22, so the rule ran 5.5h/246G there against 7.6h/337G on babel-1.17.
  Its 12h/512G is right for a full-size UniProt. Do not trim it from one small run.
- `get_ensembl` swung from 3 minutes (babel-1.17) to 1.9h (2026jul22). Network-bound rules vary
  by an order of magnitude between runs, so their runtimes are deliberately generous.

After the 2026jul22 sizing pass one rule is still within 80% of its runtime limit, deliberately:
`generate_pubmed_concords` sits at 83% of 24h. That limit is known to work, 4h of slack has been
enough across two runs, and a rewrite that removes the rule's cost is in progress — so the fix is
to make the rule cheaper, not to reserve a day and a half of wall time for it. Raise it only if a
run actually times out.

The slowest rule still on the *default* runtime is `untyped_chemical_compendia` at 58 minutes, so
the 120-minute default has roughly 2x headroom and was left alone.

## Temporary Scratch Space

The DuckDB rules (`export_*_to_duckdb` and the cross-compendium report rules in
`duckdb.snakefile`) spill larger-than-memory intermediates to a temp directory. By default that
is `tmp_directory` from `config.yaml` (`babel_downloads/tmp`, on the parallel filesystem).
`setup_duckdb()` gives each job its own `duckdb-$SLURM_JOB_ID` subdirectory there, so concurrent
jobs never share spill files — sharing one directory previously caused `stale file handle` and
`could not read enough bytes` IO errors when several report jobs ran at once.

Hatteras has no large node-local disk suitable for spilling:

- `/tmp` is the node-local rootfs SLURM advertises (`TmpFS=/tmp`), but it is only ~16 GB and
  there is no per-job isolation (`JobContainerType` is unset), so it fills almost immediately.
- `/dev/shm` is node-local but RAM-backed; its pages count against the job's cgroup memory limit
  (`ConstrainRAMSpace=yes`), so spilling there is effectively spilling to RAM and can trigger an
  OOM-kill.
- `/scratch` and `/projects` are both NFS, so they don't avoid the networked-filesystem class of
  error, though `/scratch` is a separate, less-contended server (50 TB free).

The cross-compendium report rules used to be sized to run entirely in RAM, because their
aggregations (a grouped `COUNT(DISTINCT)`, or a `LIST(... ORDER BY ...)`) could not spill and would
OOM rather than overflow to disk. Even on a full largemem node (`memory_limit` ≈ 1400G) several of
them still exceeded the physical ceiling of the largemem partition (~1.46 TiB) — and not always at
DuckDB's *tracked* limit: the killers were untracked allocations (string heaps, hash-join build
sides) that overshot the cgroup hard limit by hundreds of GiB before any spill kicked in, producing
a hard SIGKILL with no DuckDB error. Adding memory was exhausted, so they were rewritten so peak
memory is small by construction:

- `generate_curie_report` / `generate_clique_leader_report` use `approx_count_distinct()` (a
  fixed-size HyperLogLog sketch per group, ~2% error) instead of an exact `COUNT(DISTINCT)`. The
  exact totals (`COUNT`) stay exact; only the distinct counts are approximate, which is fine for a
  summary report.
- `generate_curie_report` also reads `biolink_type` straight off the Edge table (it is denormalized
  there at export time) instead of joining the full Edge table (~1.5B rows) against the Clique table
  (~200M rows). That large-vs-large join was the only one of its kind in the report suite and was
  the specific allocation that OOM-killed the rule; every other report either has no join or joins a
  huge table to a tiny one.
- `check_for_identically_labeled_cliques` finds duplicate names by grouping on a fixed-size
  `hash(LOWER(preferred_name))` rather than the name itself: there are ~200M cliques and
  `preferred_name` is often a long label, so grouping on the raw string built a ~200M-entry
  long-string heap that DuckDB does not fully track and that overshot the cgroup. It then emits the
  duplicate `(name, clique_leader)` pairs via a streaming hash join (small build side, no aggregate,
  no sort). The output is unsorted; each row carries the per-name count so a consumer can group/sort
  it cheaply.

All of these run **single-threaded** with `memory_limit` set well below `mem` so DuckDB has a large
headroom under the cgroup hard limit. Single-threaded matters for two reasons: a second thread
multiplies per-thread hash tables (raising the peak), and a background-thread OOM aborts the process
with `terminate called ... bad allocation` (SIGABRT, core dump) instead of a catchable
`duckdb.OutOfMemoryException`, so the Python error handler — and its memory snapshot — never runs.
`check_for_duplicate_curies` was the last rule still on `1400G` / 2 threads and OOMed exactly this
way on the larger 1.17 graph; it is now `1000G` / 1 thread like the rest.

`check_for_identically_labeled_cliques` is the cautionary tale that a `bad allocation` OOM is not
always about running out of RAM. After the two-pass rewrite it is cheap: the snapshot showed it
completing on the 1.17 graph with only ~67 GiB RSS, an
**85 GiB cgroup peak against a 500 GiB limit**, and ~0.1 GiB DuckDB-tracked memory. Yet it died with
`bad allocation` failing an ~8 MB allocation during teardown. The address-space snapshot named the
cause outright: **`mappings=65532` against the kernel's `vm.max_map_count=65530`** — an *mmap-count*
limit, not a RAM shortage (`Committed_AS` was 80 GiB against a 1359 GiB `CommitLimit`, and
`MemAvailable` was ~1.5 TiB). Pinning down *which* mappings took several tries, all of which the
snapshot adjudicated:

- `MALLOC_ARENA_MAX=2` did **not** help — the 65k mappings averaged ~1.3 MB, not glibc's 64 MB
  arenas.
- `enable_external_file_cache: false` did **not** change the count either — the file cache was not
  the source (kept off anyway, since it cannot increase the count and removes one variable).
- What the numbers actually say: `VmSize` (84.8 GiB) ≈ the query's peak memory, at ~1.3 MB per
  mapping, and DuckDB's allocator **retains those mappings after freeing the pages** (cgroup current
  falls to ~24 GiB while `VmSize`/mappings stay at the peak). So the mapping count tracks DuckDB's
  **peak buffer-pool memory**, which is bounded by `memory_limit`. Capping `memory_limit` at 16G
  holds only ~16 GiB of buffers at once (~12k mappings, well under 65530); the spillable two-pass
  query just spills the rest to disk.

The **definitive** fix is for the cluster to raise `vm.max_map_count` (it is the old default of
65530; 262144+ is typical for data-heavy mmap workloads — tracking issue #846). The `memory_limit`
cap keeps the rule running until then. `mem` stays at 512G (the cgroup was never the constraint).

The memory snapshot is built to tell these shapes apart. `setup_duckdb()` logs a connect-time
`DuckDB memory headroom: memory_limit=…, cgroup hard limit=…` line (the only memory record that
survives a SIGABRT), and `log_duckdb_settings_on_error()` plus the end of each report log two lines:
a `Memory snapshot (…)` line (process peak RSS, cgroup current/peak/hard-limit, DuckDB tracked, and
the untracked remainder) and an `Address-space snapshot (…)` line (`VmSize`/`VmPeak`, `mappings` vs
`max_map_count`, `RLIMIT_AS`, `overcommit_memory`, and `Committed_AS` vs `CommitLimit`). Read them
as:

- **cgroup current near the hard limit, DuckDB tracked small** → the killer is memory DuckDB does
  not account for (string heap, hash-join build side, or file page cache). Lower `memory_limit`,
  rewrite the query, or shrink the working set.
- **cgroup current well below the limit but a small allocation fails** → an address-space limit, not
  a RAM shortage. Check the address-space line: `mappings` near `max_map_count` (mmap exhaustion —
  the source for the report rules was DuckDB's **buffer-pool allocations**, not the external file
  cache; lower `memory_limit` to cap the mapping count, or have the cluster raise
  `vm.max_map_count`), `VmSize` near `RLIMIT_AS` (an address-space ulimit), or `Committed_AS` near
  `CommitLimit` with `overcommit_memory=2` (strict node-level overcommit). Adding `mem=` does not
  help any of these.
- **cgroup current at the limit and DuckDB tracked large** → the query genuinely needs the RAM.

The cgroup figures come from the job's SLURM cgroup, resolved via `/proc/self/cgroup` (the bare
`/sys/fs/cgroup` root would report the node total, not the `mem=` allocation); all fields read
`unknown` off Linux/cgroups. See `src/exporters/duckdb_exporters.py`.

To override the spill location for a run — for example to point spills at a larger or less-contended
filesystem — set `BABEL_DUCKDB_TEMP_DIR` in the job environment; an individual rule can also pass
`temp_directory` / `max_temp_directory_size` in its `duckdb_config`. Do not point the
500 GB-spilling `export_*` rules at `/tmp` or `/dev/shm`.

## Known Issues

### `bad allocation` with plenty of free RAM (`vm.max_map_count` / mmap-count exhaustion)

A DuckDB rule can fail with `Out of Memory Error: Failed to allocate block of N bytes (bad
allocation)` for a *small* `N` (a few MB) even though there is hundreds of GiB of free RAM. This is
an address-space limit, not a RAM shortage: Linux caps the number of memory-mapping areas a process
may have at `vm.max_map_count` (the old default is 65530), and a new mmap-backed allocation fails
with `ENOMEM` once that ceiling is reached.

How to confirm: look at the `Address-space snapshot (…)` line that `log_memory_snapshot()` writes
(`src/exporters/duckdb_exporters.py`); it is surfaced automatically by `babel-slurm-errors`. If
`mappings` is at or near `max_map_count` while `Committed_AS` is well under `CommitLimit` and
`MemAvailable` is large, it is this issue. (Compare against the RAM shapes documented under
"Temporary Scratch Space" above.)

What we have seen: `check_for_identically_labeled_cliques` hit `mappings=65532` vs
`max_map_count=65530`. The mappings are DuckDB's **buffer-pool allocations** — `VmSize` tracked the
query's peak memory at ~1.3 MB per mapping, and DuckDB's allocator retains the mappings after
freeing the pages (so `VmSize`/`mappings` stay at the peak even as `cgroup current` falls). Two
things that did **not** help, both ruled out by the snapshot: `MALLOC_ARENA_MAX=2` (the mappings
were not glibc's 64 MB arenas) and `enable_external_file_cache: false` (the count was unchanged —
the file cache was not the source; we leave it off anyway as it cannot increase the count).

Two mitigations:

1. **Cap `memory_limit` low** (in the rule's `duckdb_config`). The mapping count scales with peak
   buffer-pool memory, which `memory_limit` bounds, so a low cap (16G for this rule) keeps the
   spillable query's live mappings well under the ceiling. This is what keeps the rule running
   today.
2. **Raise `vm.max_map_count`** — the definitive fix. The old default of 65530 is low for
   data-heavy mmap workloads; 262144+ is typical (`sysctl -w vm.max_map_count=262144`). This is a
   cluster-level change and the right long-term answer. Tracking issue:
   <https://github.com/NCATSTranslator/Babel/issues/846>.

## Localrules (Run on Head Node, No SLURM Slot)

The following rules are declared `localrules` and run on the head/login node without consuming a
SLURM allocation. They are trivial done-marker rules or cleanup rules:

| Rule | File |
|------|------|
| `all` | `Snakefile` |
| `all_outputs` | `Snakefile` |
| `clean_compendia` | `Snakefile` |
| `clean_downloads` | `Snakefile` |
| `export_all_to_kgx` | `exports.snakefile` |
| `export_all_to_sapbert_training` | `exports.snakefile` |
| `export_all_compendia_to_duckdb` | `duckdb.snakefile` |
| `export_all_synonyms_to_duckdb` | `duckdb.snakefile` |
| `export_all_to_duckdb` | `duckdb.snakefile` |
| `all_duckdb_reports` | `duckdb.snakefile` |
| `all_reports` | `reports.snakefile` |
| `geneprotein` | `geneprotein.snakefile` |
| `drugchemical` | `drugchemical.snakefile` |
| `publications` | `publications.snakefile` |
| `get_mesh_synonyms` | `datacollect.snakefile` |

## Out-of-Scope Improvements (Future Work)

The following improvements are tracked here for visibility but not yet implemented:

- **`uv run snakemake` vs `conda activate babel`**: The SLURM job scripts (`slurm/job`,
  `slurm/run-babel-on-slurm.sh`) still reference the old conda environment and hardcoded paths.
  Migrate to `uv run` for consistency with the development workflow.

- **`run_babel_one_node.job` cleanup**: This job script references a hardcoded conda env
  path and should be updated to use `uv run`.

- **Cross-compendium DuckDB report sizing**: `check_for_identically_labeled_cliques`,
  `generate_curie_report`, and `generate_clique_leader_report` were rewritten to keep peak memory
  small (`approx_count_distinct()` sketches; a hashed-key + streaming-join duplicate dump; a
  denormalized `biolink_type` column that removes the report's Edge-to-Clique join) and run
  single-threaded, but they still default to a full largemem node (1500G, `memory_limit` 1000G) out
  of caution. Once a full run confirms their real peak RSS, they can likely drop to a much smaller
  `mem`, freeing the largemem partition. `check_for_duplicate_*` use a two-pass spillable
  `COUNT(*)`-then-`LIST()`-over-confirmed-duplicates query; `check_for_duplicate_clique_leaders`
  already runs at 512G. Note: because `generate_curie_report` now reads `biolink_type` from the Edge
  parquet,
  `export_compendia_to_duckdb` must have been re-run after that column was added — the report cannot
  run against older Edge parquets that predate it.

- **Per-rule resource tuning**: After collecting benchmark data from a full run, add explicit
  `resources:` to every rule based on observed `max_rss + 30% headroom`. This will greatly reduce
  wasted SLURM allocations across the ~100+ rules currently defaulting to 64G/4-CPU. The
  `src.tools.slurm resources` subcommand automates the measurement and recommends per-rule sizes
  from a run's benchmark TSVs (see [`../docs/tools/Resources.md`](../docs/tools/Resources.md)).
  Applying the recommendations — the `slurm/config.yaml` default and the per-rule overrides — is
  tracked separately.

- **`--local-cores N` flag**: Use this to limit the number of CPUs consumed by local rules
  when running on a shared login node.

- **Log accumulation**: Consider setting `slurm-delete-logfiles-older-than` to a nonzero value
  once log volume has been studied across multiple runs.
