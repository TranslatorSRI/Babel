# `source-impact-report` — what does adding this source do?

Answers "what does adding *source X* do to Babel's cliques?" by re-glomming the intermediate
ids/concords twice — once with the source's files excluded, once with them included — and
diffing the two clique states.

```bash
uv run source-impact-report --source EMAPA
```

## Naming a source's concords

The tool finds a source's contributions at `intermediate/<pipeline>/ids/<source>` and
`intermediate/<pipeline>/concords/<source>`. A source whose concord is named after it needs nothing
more; one that writes a concord under another name has to say so, repeatably:

```bash
uv run source-impact-report --source GARD --concord GARD --concord GARD_label
```

Without it, `GARD_label`'s rows are counted as another source's data: section 3 reports **zero**
cross-references added and the join-pathway table marks the rows `from_other_source`. Naming them
is deliberately a CLI choice rather than a naming rule, because no rule separates the two cases —
`GARD_label` is GARD's own label-match concord, while `MONDO_GARD` is MONDO's data *about* GARD and
belongs to MONDO, and both contain "GARD". The report header records the concords it counted
(`- Source concords: GARD_label`), so a regeneration that forgets the flag is visible in the diff
rather than silent.

The *before* state does not need the flag: `compute_cliques_for_impact_report()` excludes a concord
when any `_`-separated part of its basename names an excluded source, so `GARD_label` and
`MONDO_GARD` are both correctly held out of the baseline for `--source GARD`.

## Output files

Writes `docs/sources/<SOURCE>/impact-report.md` plus an `impact-report/` subdirectory holding six
detail files: two reductions that are **committed**, and the four full tables they reduce, which are
gitignored. The unqualified filename is always the full table; the qualified one is the reduction.

| file | committed? | what it is |
|---|---|---|
| `new-cliques-top-100.csv` | yes | ranked slice of the pure-new cliques |
| `new-xrefs-summary.csv` | yes | xrefs aggregated into join pathways, with example rows |
| `new-cliques.csv` | no | every pure-new clique |
| `new-xrefs.csv` | no | every cross-reference row touching a source CURIE |
| `modified-cliques.csv` / `.json` | no | every existing clique the source expands or merges |

Each reduction takes the shape its data calls for. The cliques file is a **slice**, ranked so that
identifiers the Biolink prefix filter would drop come first, then the largest cliques, then CURIE
order; `NEW_CLIQUES_TOP_N` is the knob. The xrefs file is an **aggregate**, because a source's 4,336
xrefs can be a single join pathway (EMAPA's are) and the pathway is what a reviewer needs — so it
enumerates every pathway with its total plus `XREF_EXAMPLES_PER_GROUP` examples each. Both constants
live in `src/reports/source_impact_details.py` / `src/model/source.py`.

Re-run the tool to regenerate the full tables for SME review; see
[`docs/AddingNewSources.md`](../AddingNewSources.md) for the rationale and the upload convention.

[`docs/AddingNewSources.md`](../AddingNewSources.md) is the workflow guide — when to run this,
how to read it, and how to assemble the intermediate inputs from a `stars.renci.org` snapshot
when a full local build is impractical. This page documents the tool itself.

The Snakemake rule `report_source_impact` shells out to this console script, so the two never
drift:

```bash
uv run snakemake babel_outputs/reports/source_impact/EMAPA.md
```

## Modes

- `--mode synthetic` (default) re-gloms in memory with and without the source. Needs the
  intermediate ids/concords and a registered pipeline hook. `glom()` runs twice, so memory cost
  doubles: anatomy is tractable on a laptop, chemical or gene needs an HPC node.
- `--mode remote` compares against the finished compendia of a previous published build, given
  `--remote-url` (e.g. `https://stars.renci.org/var/babel/2025dec11/`). The fallback when
  synthetic mode is too expensive or no hook is registered.
- `--mode both` runs each and reports them side by side.

Run `uv run source-impact-report --help` for the full flag list, including the roots
(`--intermediate-root`, `--compendia-root`, `--downloads-root`), the sample limits, and
`--no-biolink-lookup` for offline use.

## `PIPELINE_CONFIG`, the registry

`src/tools/source_impact_report/cli.py` holds `PIPELINE_CONFIG`, mapping each
[`babel_pipeline`](../../AGENTS.md) to the hooks the report needs. Without an entry the report
still runs: it warns, skips the synthetic clique diff for that pipeline, and falls back to remote
mode if `--remote-url` was supplied.

An entry needs **more than just `compute_fn`**:

| Key | What it is |
|-----|------------|
| `compute_fn` | The pipeline's `compute_cliques_for_impact_report`, returning `(clique_dict, types_dict)` and accepting `excluded_sources`. |
| `compendium_files` | The compendium filenames this pipeline writes. |
| `compendium_prefixes` | The prefixes whose `labels` files are loaded to enrich clique samples. |
| `clique_classifier` | A `classify_*_clique` callable returning a clique's Biolink type. |
| `biolink_types` | The types whose `id_prefixes` order decides the preferred CURIE. |

Omit `clique_classifier` / `biolink_types` and every clique renders with a blank `biolink_type`,
while `preferred_curie()` silently falls back to the lexicographically-smallest CURIE — a `DOID`
or `Fyler` leader instead of `MONDO` or `HP`. Extract the classifier from the pipeline's
`create_typed_sets()` so the report types and orders identifiers exactly like the build does.

To register a new pipeline, split its `build_compendia()` into a "compute cliques in memory"
helper and a "write compendia" wrapper (see `src/createcompendia/anatomy.py` for the template),
then add the entry. The report loads preferred labels for each prefix listed under
`<pipeline>_prefixes` in `config.yaml`, so adding a new prefix there is what makes the new
source's labels visible — no separate change to the CLI is needed.

## What it cannot see

The "before" state is always "the same inputs with this source excluded", so the report only ever
shows cliques that were **added, expanded, or merged**.

- **Split, shrunk, and dropped cliques are not reported.** `diff_cliques` only walks
  after-cliques containing a source CURIE, so a before-clique that lost members or was split
  apart — by `unique_prefixes` rejecting a merge, or by a post-glom split like
  `split_mutually_exclusive_cliques` — produces no row. Both clique states are computed, so this
  is a missing pass rather than missing data:
  [#895](https://github.com/NCATSTranslator/Babel/issues/895). Use
  [`babel-clique-diff`](CliqueDiff.md) meanwhile — and always, for a change that restructures
  existing cliques or that isn't "add a source" at all.
- **Typing happens after the diff.** The synthetic diff is over untyped cliques.
- **Conflation is invisible.** DrugChemical and GeneProtein conflation runs after compendia are
  written, so a source contributing bridging xrefs looks quieter than its true downstream effect.

## Layout

The CLI is a thin frontend; its logic lives in `src/` where the pipeline can reach it:

- `src/tools/source_impact_report/cli.py` — argparse and `PIPELINE_CONFIG`.
- [`src/model/source.py`](../../src/model/source.py) — discovers where a source contributes.
- [`src/model/glom_diff.py`](../../src/model/glom_diff.py) — diffs the two glom states. (Not
  `compendium_diff.py`, which backs [`babel-clique-diff`](CliqueDiff.md).)
- [`src/reports/source_impact.py`](../../src/reports/source_impact.py) and
  `source_impact_details.py` — render the markdown, JSON, and detail files.
