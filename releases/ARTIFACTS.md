# Archived build reports

Each release directory here — [`2026jul22/`](2026jul22/), [`2025sep1/`](2025sep1/) and so on — holds
the release note as `README.md` plus a small subset of that build's own output: the summary tables,
the per-compendium content reports, and the provenance metadata. A finished build is hundreds of
gigabytes on a cluster that is eventually cleaned; this is the part worth keeping, about 420 KB per
release.

Directories are named by **build**, which is what every artifact and `config.yaml`'s
`previous_release` pin already use. For recent releases that is also the release id; for the older
Translator-named ones it is not (`TranslatorFuguJuly2024` was built as `2024jul13`).

## The layout mirrors the build directory

Every file sits at the same relative path it has in `babel_outputs/`. That is deliberate: a release
directory *is* a build directory, just a very small one, so anything that takes a build directory
takes a release directory unchanged.

```bash
uv run python releases/scripts/draft_release_notes.py 2026jul22 --build-dir releases/2026jul22
```

That is what keeps an old note re-draftable from the repository alone. Do not reorganise these
directories by kind; the flattening would buy nothing and cost that property.

## What each file is

### `reports/tables/prefix_table.csv`

One row per CURIE prefix: exact occurrence count, approximate distinct count (HyperLogLog, ~2%), and
which compendia it appears in. Written by `generate_prefix_table` →
`src/reports/report_tables.py:generate_prefix_table()`. Answers "how much of this build is
UniProtKB, and where does it live".

### `reports/tables/cliques_table.csv`

One row per compendium, grouped by pipeline: description, Biolink types, CURIE and clique counts,
the prefixes that lead its cliques, and the prefixes that appear in them. Written by
`generate_cliques_table` → `src/reports/report_tables.py:generate_cliques_table()`. The most
readable single view of what a build contains.

### `reports/tables/mapping_sources_table.csv`

One row per mapping source: which compendia it feeds and how many mappings it contributed, read out
of the `metadata/` YAMLs. Written by `generate_mapping_sources_table` →
`src/reports/report_tables.py:generate_mapping_sources_table()`. Use it to see which source is
responsible for a given join.

### `reports/tables/prefix_comparison.md`, `_overall.csv`, `_by_clique_prefix.csv`

This build against the release pinned as `previous_release` when it ran, at three granularities:
a Markdown summary naming the baseline and flagging notable changes, per-compendium totals, and one
row per (compendium, clique-leader prefix, CURIE prefix). Written by `generate_prefix_comparison` →
`src/reports/prefix_comparison.py:generate_prefix_comparison()`. The `_overall.csv` is what the
release note's compendium-size table is built from.

Note the baseline is whatever was pinned at build time, which is not necessarily the previous
*deployed* release — the Markdown file states which release it actually compared against.

### `metadata/<Type>.txt.yaml`

Per-compendium provenance: `counts` (cliques, equivalent identifiers, synonyms) and a nested
`combined_from` tree recording every concord and download that fed the compendium, each with its own
description and `prefix_counts` such as `xref(CHEBI, DrugCentral): 4302`. Written during compendium
building via `src/metadata/provenance.py`.

These are aggregate counts per prefix *pair*: they confirm that a join pathway exists between two
prefixes, not that any specific CURIE is joinable.

### `reports/content/compendia/<Type>.json`

Per-compendium content report: `count_lines`, `count_by_biolink_type`, `count_by_prefix`, and
clique-shape counters (identifiers, labels, descriptions, and their distinct counts). Written by
`generate_content_report_for_compendium_*` →
`src/reports/compendia_per_file_reports.py:generate_content_report_for_compendium()`. Comparing
`count_by_prefix` between two releases is what turns "Protein is down 38%" into "UniProtKB is down
103.8M and nothing else moved".

### `reports/content/compendia_report.json`

The same numbers summed over the whole build — the file the release notes call the CURIE summary.
Written by `generate_compendia_summary_report` →
`src/reports/compendia_per_file_reports.py:summarize_content_report_for_compendia()`.

### `reports/duckdb/prefix_report.json`

The combined prefix report every table above is derived from: exact occurrence counts and
approximate clique counts, by clique-leader prefix and by CURIE prefix. Written by
`generate_prefix_report` → `src/reports/duckdb_reports.py:generate_prefix_report()`.

This file is also **the next release's comparison baseline**, which `config.yaml`'s
`previous_release` selects by naming its release directory. Its `name` field must match that
directory — it is stamped from `release_name` at build time, so a run that started before the pin
moved carries the previous build's name, and that value labels the baseline in the next comparison.
`archive_build.py` refuses to archive a report whose `name` disagrees.

## These files are build output, not repository content

Everything under `releases/<build>/reports/` and `releases/<build>/metadata/` is copied
byte-for-byte out of a build. Nothing in those two subdirectories is authored, and nothing in them
should be edited: an edit makes the archive disagree with the build it claims to be a subset of,
which is the one property the layout rests on.

The Markdown at the directory root is the exception — `README.md` is the release note, and
`2025sep1/v1.11.md` is the note for a release that never got a build of its own. Those are ordinary
repository content, written by hand and edited like any other document.

That means **a repo-wide check that walks files has to exclude the two build-output subdirectories,
not the release directory** — a check scoped to the whole of `releases/<build>/` would stop looking
at the notes, which are exactly the files it should be checking. Two checks already learned the
first half the hard way:

- `rumdl` (`[tool.rumdl] exclude` in `pyproject.toml`) — `prefix_comparison.md` is generated
  Markdown with long lines, and reflowing it would rewrite build output to satisfy a style rule.
- `tests/test_docs_links.py` — two provenance YAMLs record an upstream URL pinned to `master`. That
  is what the build recorded, not a link this repository offers.

Both are already scoped that way — rumdl excludes `releases/*/reports/` and
`test_docs_links.py:_is_archived_build_report()` tests `parts[2] in {"reports", "metadata"}` — so
neither needed changing when the notes moved in. If you add a third such check, scope it the same
way. `releases/scripts/`, the notes and this file are ordinary repository content and stay in scope.

## What is not archived

Everything else in a build directory, on purpose:

- `reports/duckdb/*.tsv{,.gz}` — the duplicate-CURIE and identically-labelled-clique dumps, ~200 MB,
  regenerable from the compendia. (`duplicate_clique_leaders.tsv` is also a build control: every row
  must be in `input_data/known_duplicate_clique_leaders.tsv` or the build fails, so a released build
  has no unexpected duplicate leaders by construction; the tiny `duplicate_clique_leaders.checked`
  summary beside it says so.)
- `benchmarks/` and `reports/slurm/` — per-run resource data. Useful while sizing a run with
  [`babel-slurm-resources`](../docs/tools/Resources.md), not afterwards.
- `logs/` — the control-node logs.
- `reports/umls/` and the loose `reports/<Type>.txt` / `*_completeness.txt` — build diagnostics
  rather than summaries. `<Type>.txt` is a cluster-size histogram; `*_completeness.txt` should read
  `Missing identifiers: 0`.

If you need any of these while writing a note, you need the real build directory.
[`docs/Downloads.md`](../docs/Downloads.md) describes the full set.

## Adding a release

```bash
uv run python releases/scripts/archive_build.py <build> --build-dir <a copy of the build> --dry-run
uv run python releases/scripts/archive_build.py <build> --build-dir <a copy of the build>
```

The script copies the manifest above, refuses any file over 5 MB (naming it, and exiting non-zero so
a widened glob is noticed), and checks the prefix report's `name`. It is idempotent. The rest of the
release bookkeeping is in [`README.md`](README.md) and
[`docs/RunningBabel.md`](../docs/RunningBabel.md#archiving-a-builds-reports).
