# AGENTS.md

This file provides guidance to coding agents — Claude Code, Codex, Cursor, and others — when
working with code in this repository. It is the canonical, agent-agnostic instruction file; the
root `CLAUDE.md` is a one-line `@AGENTS.md` import so Claude Code picks it up automatically.

## Directory-scoped instructions

Some directories carry their own `CLAUDE.md` with conventions specific to that area. Claude Code
loads the nearest one automatically when you edit files there; **other agents should read the
`CLAUDE.md` nearest the files they are editing.** They live in:

- `tests/` — how to write a test once you know where it goes.
- `src/datahandlers/` — code-level conventions for the data-source handlers.
- `src/tools/` (plus `src/tools/clique_diff/`, `src/tools/slurm/`,
  `src/tools/source_impact_report/`) — the "thin CLI frontend" convention and per-tool notes.
- `docs/sources/` — cross-cutting xref / source-data conventions.
- `docs/sources/DRUGBANK/` — DrugBank ingest gotchas (CC-0 vocabulary only, the UNII typing bridge).

(Agents that merge nested `AGENTS.md` up the tree won't see these `CLAUDE.md` files — only this
root file. If per-directory cross-agent context ever matters, pair each with an `AGENTS.md` holding
the content and a one-line `@AGENTS.md` `CLAUDE.md` shim; not worth it today.)

## Project Overview

GitHub repository: <https://github.com/NCATSTranslator/Babel>

Babel is the Biomedical Data Translator's identifier normalization system. It creates "cliques" —
equivalence sets of identifiers across biomedical vocabularies (e.g., recognizing that MESH:D014867
and DRUGBANK:DB09145 both refer to water). Output is consumed by Node Normalization and Name
Resolver services.

## Scratch space: use `data/`

`data/` is a gitignored scratch directory — put ad hoc files there (downloads, build comparisons,
extracted intermediates), and prefer it over `/tmp` for anything worth keeping across a session.
Never write scratch files into the repository root, `input_data/`, or `docs/`.

## Key Commands

### Setup

```bash
uv sync
```

### Running the Pipeline

```bash
uv run snakemake --cores N                # Full pipeline (~500GB RAM)
uv run snakemake --cores 1 anatomy        # Single semantic type target
uv run snakemake --cores 1 chemical
```

### Testing

```bash
uv run pytest                           # All tests
uv run pytest --cov=src                 # With coverage report
uv run pytest tests/test_node_factory.py  # Single test file
uv run pytest -m unit -q               # Unit tests only (CI default)
uv run pytest --network                # Include network tests
uv run pytest --all                    # Run every test
uv run pytest -n auto                  # Parallel (all CPUs)
```

Tests use four marks (`unit`/`network`/`slow`/`pipeline`); network and pipeline tests are skipped
by default. See `tests/README.md` for the full taxonomy and where to add a new test.

Memory-hungry tests also carry a parametrized `min_memory_gb(n)` guard (registered in
`pyproject.toml`, enforced in `tests/conftest.py`) that auto-skips them on machines with less than
`n` GiB of RAM — an in-memory RDF store can need 8-10x a dump's on-disk size. Size a new rule's
`mem=` or a test's `min_memory_gb` empirically with `src/tools/memory/estimate_rdf_load_memory.py`
(see `docs/tools/Memory.md`).

- `docs/Testing.md` — testing strategy: cadence per environment (per-PR, nightly, weekly,
  pre-release), GitHub Actions vs HPC self-hosted runner trade-offs, and other strategies.

### Linting (all four checked in CI on PRs)

```bash
uv run ruff check                        # Python lint
uv run ruff check --fix                  # Python auto-fix
uv run ruff format --check               # Python format check
uv run ruff format                       # Python auto-format
uv run snakefmt --check --compact-diff . # Snakemake format check
uv run snakefmt .                        # Snakemake auto-fix
uv run rumdl check .                     # Markdown lint
uv run rumdl fmt .                       # Markdown auto-fix
```

### Configuration

- Line length is 120 for both Python (ruff) and Snakemake (snakefmt). Markdown (`rumdl`,
  rule `MD013`) wraps at 100 instead, though tables are exempt.
- Main config: `config.yaml` (directory paths, version strings, prefix lists per semantic type).
- `UMLS_API_KEY` environment variable required for UMLS/RxNorm downloads.
- `compendium_directories` in `config.yaml` maps Python compendium names to the Snakemake
  intermediate directory names when they differ (e.g., `diseasephenotype → disease`,
  `processactivitypathway → process`). Update this when adding a new semantic type whose
  directory name doesn't match its Python module name.

## Architecture

[`docs/Architecture.md`](docs/Architecture.md) is the full tour: the two-phase pipeline (data
collection → compendium building), the `src/` layout, the core data structures (concord files,
compendium JSONL), and the factory / `glom()` / `TSVSQLiteLoader` / Biolink patterns. Read it
before making structural changes. The notes below are the ones worth having in front of you every
session.

### Biolink Model Usage

The Biolink Model version is set in `config.yaml` — read it via `get_config()["biolink_version"]`
rather than hard-coding a version — and feeds both `NodeFactory` and `get_biolink_model_toolkit()`.
That helper takes the version as a **required** argument, and the version may be a git SHA rather
than an `x.y.z` tag. To check whether a prefix is registered for a Biolink class — the check that
decides whether `NodeFactory.create_node()` keeps or silently drops a CURIE — use
`get_biolink_model_toolkit(version).get_element(<class>).id_prefixes`.

Always use the mapped `biolink:`-prefixed class URI (`biolink:ChemicalEntity`), never the raw
element name (`chemical entity`); `get_element()` (no `.get()` method — see `get_prefixes()`'s
comment in `src/node.py`) and `get_ancestors()` return the mapped form. `src/prefixes.py` is the
canonical prefix-constant registry; its `id_prefixes` order in the Biolink Model drives which CURIE
`NodeFactory` picks as preferred. To resolve a CURIE to a URL use
`src/util.py:get_biolink_prefix_map()` (`converter.expand("EMAPA:0")`), not a hand-rolled map.

**Node output schema** — see `NodeFactory.create_node()`'s docstring in `src/node.py`:

```python
{"identifiers": [{"identifier": CURIE, "label": str}, ...], "type": "biolink:Foo", "id": {"identifier": CURIE, "label": str}}
```

### Subsystem gotchas

- **Clique-building skeleton** — `glom_from_files()` in `src/model/cliques.py` (see its docstring
  for the three hooks); route a pipeline's `build_compendia` and
  `compute_cliques_for_impact_report` through the same wrapper so the impact report provably matches
  the build.
- **Concord row order is load-bearing** — `glom()`'s `unique_prefixes` keeps whichever CURIE of a
  restricted prefix it sees *first*, and a loser with no ids-file row is dropped outright.
  `build_sets()` sorts its output so this is reproducible; never reintroduce unordered iteration
  there. Before restricting a prefix, count what it makes compete — see step 3 of
  `docs/AddingNewSources.md`.
- **`SynonymFilter`** (`src/synonyms/filter.py`) checks every label/synonym against
  `input_data/obsolete_synonyms.yaml` before it enters a compendium — see its docstring for the
  `action` field and the `should_suppress()` contract.
- **Logging** — always use `get_logger(__name__)` from `src.util`, never `logging.getLogger`
  directly (see its docstring for why and the deferred-import exception).
- **Leftover UMLS** — `src/createcompendia/leftover_umls.py` (rule `leftover_umls`) runs last and
  writes every unclaimed valid MRCONSO concept as a single-identifier clique into
  `compendia/umls.txt` so its label survives downstream. Manual Biolink-type override tables and
  their drift test: [`docs/sources/CLAUDE.md`](docs/sources/CLAUDE.md) and
  `docs/sources/UMLS/Leftover.md`.
- **DuckDB export** — `src/snakefiles/duckdb.snakefile` builds a queryable DuckDB database
  (`Node`/`Clique`/`Edge`/`Conflation`) alongside the JSONL compendia (schema in
  `docs/DataFormats.md`); its `Edge` table answers "which clique contains CURIE X" in one query
  (`SELECT DISTINCT clique_leader FROM Edge WHERE curie IN (...)`), far cheaper than re-running glom
  or scanning JSONL.
- **Chemical compendium output types** — `config.yaml: chemical_outputs` is the single fan-out list
  for chemical compendium types; adding a subtype needs an entry there *and* a matching hardcoded
  `check_*` report rule in `chemical.snakefile` or `rule chemical`'s DAG breaks. Full note in
  [`docs/Architecture.md`](docs/Architecture.md#chemical-compendium-output-types).
- **Snakemake `resources.mem` in a `run:` block** — read `resources.mem_mb`, never `resources.mem`.
  Snakemake normalizes every sized resource to `mem_mb` internally and re-exposes `mem` as a
  *humanfriendly string*, so a rule's `mem="512G"` reaches Python as `"512 GB"` — and `mem_mb` is
  decimal, so it is `512000`, not `524288`. See `duckdb_memory_limit_mb()` in
  `src/snakefiles/util.py`, whose `.endswith("G")` parse of `resources.mem` broke on exactly this.
- **Benchmark memory is mebibytes; `mem` is decimal MB** — the same rule's numbers appear in both
  units. A `benchmark:` TSV's `max_rss` is labelled "MB" but computed as bytes `/ 1024 / 1024`, so a
  rule "peaking at 132G" needs `mem="142G"` to be at 100% of its limit. Two factors, don't mix them:
  ×1.048576 converts the raw `max_rss` column (MiB→MB), ×1.073741824 converts a figure already
  displayed in GiB. As a fraction of a rule's limit the error is ~4.9%; on a whole-GiB peak it is
  ~7.4%. Comparing them unconverted always errs toward *looking safe*. See the Units section of
  [`docs/tools/Resources.md`](docs/tools/Resources.md); `babel-slurm-resources` converts on the way
  in and reports decimal throughout, so its recommendation is the string to paste into a rule.
- **Per-compendium metadata YAMLs** — `babel_outputs/metadata/<Type>.yaml` records provenance with
  per-source `prefix_counts` like `xref(CHEBI, DrugCentral): 4302`. Aggregate (prefix-pair) only —
  confirms a join pathway exists, not whether *specific* CURIEs are joinable.

### Per-source & developer-tool docs

Source-specific notes live under `docs/sources/<PREFIX>/` — see
[`docs/sources/README.md`](docs/sources/README.md) for the index and
[`docs/sources/CLAUDE.md`](docs/sources/CLAUDE.md) for cross-cutting xref/data-quality conventions
(read it before adding or filtering a source's cross-references). Developer tools each live in
`src/tools/<tool>/` with their own `CLAUDE.md`, documented in `docs/tools/`; see
`src/tools/CLAUDE.md` for the "thin CLI frontend" convention.

## Running Babel

Run a particular rule with `uv run snakemake -c all --rerun-incomplete [rulename]`. Download steps
are easiest run through Snakemake directly; for a rule that needs intermediate files, it's often
faster to pull them from a cluster run's output, e.g.
<https://stars.renci.org/var/babel/2025dec11/>, than to rebuild every upstream source — check the
rule's resource requirements to decide.

A killed run can leave `LockException: Directory cannot be locked` on the next invocation; clear it
with `uv run snakemake --unlock`.

Most semantic-type targets are much cheaper than the full pipeline (anatomy builds end-to-end on a
laptop in ~25 minutes; the README's 500 GB figure is for the heaviest targets only). See
`docs/RunningBabel.md` for a per-target sizing breakdown and common build issues.

## Releasing a build

A Babel release is a build *plus* the NodeNorm and NameRes versions deployed against it, and the
combined note lives in `releases/<build>/README.md`. `releases/releases.yaml` records which versions
shipped with which build — that mapping is not derivable from anywhere else, and each entry is the
next release's comparison baseline. [`releases/README.md`](releases/README.md) is the process, and
`releases/scripts/draft_release_notes.py` drafts the mechanical parts.

That directory also holds ~420 KB of the build's summary reports, mirroring the build directory's
own paths so a release directory *is* a (tiny) build directory — `--build-dir releases/2026jul22`
works unchanged. [`releases/ARTIFACTS.md`](releases/ARTIFACTS.md) says what each file is, and why a
repo-wide lint or scan must exclude `reports/` and `metadata/` — but not the note beside them.

A release is also the natural cadence for re-checking SLURM sizing against the run's benchmarks
(`docs/tools/Resources.md`), and for the archive/pin steps in `docs/RunningBabel.md`.

## Adding a new data source

**Read [`docs/AddingNewSources.md`](docs/AddingNewSources.md) end to end before writing any code.**
It is a checklist, not background reading, and the steps that get skipped are the ones that decide
whether the addition is any good: whether the source's identifiers *join* existing cliques or pile
up beside them ("Prefer joining an existing clique"), which prefix restrictions the new source makes
compete (step 3), and what the source-impact report and clique diff have to show before it ships.
Every one of those is cheaper to get right first than to discover from a finished build.

The guide covers: how to wire a source (prefix, data handler, compendium
hook, Snakemake rules, `config.yaml`, docs, tests), then generate and read its source-impact report
— including assembling the intermediate inputs from a `stars.renci.org` snapshot when a full local
build (~500 GB RAM) is impractical. Two things the report exists to catch: an ids file missing its
Biolink type (see `docs/Development.md`), and a prefix not yet registered in the Biolink Model for
its class (`write_compendium()` silently drops such CURIEs — check, don't assume.
`extra_prefixes=[...]` is the escape hatch, and it is what keeps members alive when **retyping** a
clique to a class that doesn't register their prefix — see `docs/AddingNewSources.md`).
**Generate and commit the report** (`uv run source-impact-report --source <SOURCE>`) and, for
changes that *restructure* existing cliques, follow up with `babel-clique-diff` — see
`src/tools/source_impact_report/CLAUDE.md` and `src/tools/clique_diff/CLAUDE.md` for the tool
internals.

Use `retries: 3` (not `retries: 10`) on network-backed Snakemake rules (UberGraph, FTP, HTTP) — see
`docs/AddingNewSources.md` step 4 for why. `src/tools/slurm/CLAUDE.md` covers analyzing a
(possibly partial) run on the cluster.

## Conventions

Point-of-use conventions live where they apply: process-level guidance for adding/enhancing an
ingest is in `docs/Development.md` ("Enhancing a data source ingest"); datahandler code rules
(attribute files, IRI parsing, pyoxigraph, file-path args) are in
[`src/datahandlers/CLAUDE.md`](src/datahandlers/CLAUDE.md); test conventions are in
[`tests/CLAUDE.md`](tests/CLAUDE.md). The rules below apply repo-wide.

- **Configuration over constants** — `config.yaml` records the *big decisions that shape a build*:
  which ontologies a pipeline includes, which version of a download is used, which prefixes are
  unique within a clique, thresholds and flags that a maintainer would want to review or change
  between runs. Those belong in `config.yaml`, where they are visible to anyone reading the build's
  shape and where related settings sit next to each other.

  A value that is *closely tied to the content of one source* — above all, how that source is
  cleaned before Babel uses it — can stay as a documented module-level constant in that source's
  Python file. Lifting it into `config.yaml` would separate it from the parsing code it explains
  and would imply it is a knob to be tuned, when in practice it only changes if the upstream source
  changes. The xref ignore-lists and allowlists (`ANATOMY_OBO_IGNORE_LIST` in
  `src/createcompendia/anatomy.py`, `MP_XREF_ALLOWED_PREFIXES` in
  `src/createcompendia/diseasephenotype.py`) are the canonical examples: they encode "these xref
  targets in *this* ontology are junk or out of scope", not "this is how we want the build
  configured." Keep them beside the code that applies them.

  Pure implementation details with no user-facing meaning stay in Python without further thought.
  Why that split is safe (git tags, `metadata.yaml`): `docs/Development.md`.

- **Document every configuration value** — every entry in `config.yaml` and every module-level
  constant that remains in Python must have an inline comment explaining *what it controls* and
  *why the chosen value was picked*. One-word names are not self-documenting.

- **Keep related settings together** — configuration entries that constrain or depend on each other
  must sit adjacent in `config.yaml`, separated from unrelated entries. For example, the anatomy
  block groups `anatomy_prefixes`, `anatomy_ids`, `anatomy_concords`, `anatomy_outputs`, and
  `anatomy_unique_prefixes` together so that adding a new source requires reviewing all of them at
  once. Never scatter correlated settings across the file.

- **`babel_pipeline` vs `biolink_type`** — easy to confuse; keep distinct in code and variable
  names. See the Terminology section of `docs/AddingNewSources.md` for the full distinction
  (plus the unrelated third term, `umls_semantic_type`/`sty`).

- **Biolink class references** — always use the named constants from `src/categories.py`
  (e.g. `CHEMICAL_ENTITY`, `DRUG`) rather than hardcoding `"biolink:..."` strings; add a missing
  constant there first.

- **Shared helpers before hand-rolled ones** — before writing a small utility (a path, string,
  logging, or config helper), check [`src/util.py`](src/util.py) for one that already exists:
  `get_logger()`, `get_config()`, `ensure_parent_dir()`, `get_biolink_prefix_map()`, the `Text`
  CURIE helpers. `src/babel_utils.py` holds the pipeline-level ones (`glom()`,
  `write_compendium()`, `pull_via_wget()`, `make_local_name()`). When you *do* hand-roll one,
  grep for the pattern first: if the same few lines already appear in several modules, promote it
  to `src/util.py` and route the existing call sites through it rather than adding an
  n+1th copy. (`ensure_parent_dir()` came from ten hand-rolled copies of
  `os.makedirs(os.path.dirname(f), exist_ok=True)`, nine of which shared the same latent bug.)

- **Error handling** — raise exceptions (`RuntimeError`, `ValueError`) rather than
  `print(...) + exit(1)`, which bypasses Python's exception machinery and breaks unit testing.

- **A log warning is not a control.** When you ship a deliberate simplification whose risk you can
  name, decide whether its detection needs to *block* something. A `logger.warning` in a rule that
  runs for hours and emits thousands of lines will not be read; if the condition means the output is
  wrong, raise, or make the fix unnecessary. PR #918 is the cautionary case: it shipped a coarse
  clique-level retype, wrote a comment saying the retype "must never fire in a real build; if it
  does, the forced type should become a vote instead", added a warning to detect exactly that, filed
  the issue (#935) *and* drafted the fix (#936) — then shipped the override anyway. The warning
  fired seven times in `babel-1.18` and nobody saw it until the bad output was noticed downstream.
  If you have already written the safer version, prefer shipping it over shipping a detector for the
  version you know is wrong.

- **Docstrings** — give modules, classes, and non-trivial functions a docstring covering what they
  do and any non-obvious behavior. Name functions for what they do — `fetch_*` (not `get_*`) when
  the call hits the network.

## Debugging

Prefer invoking this repo's existing download code over a fresh API lookup when investigating a
source database, unless you suspect that code is wrong — then compare the two to see how they
differ.

Before changing how a source's free-text field is parsed, characterize the *whole* downloaded file
— stream it and tabulate how characters and delimiters are actually used — rather than generalizing
from a few sampled rows. A shape that reads as a quoting artifact in a sample is often legitimate at
scale: NCBIGene's trailing `''` looks like a stray quote but is genuine double-prime nomenclature
(RNA polymerase `beta''`), so a fix dropping every `''`-terminated value discarded ~4,000 real
synonyms. Commit the analysis script and its output so the finding stays reproducible; see
`docs/Development.md` ("Characterize a messy field before you parse it") and the worked example in
`docs/sources/NCBIGene/quoting/`.

Do not reason about source data from a derived artifact — a hand-written test fixture, a scratch
script, an earlier summary. Go to the downloaded file. Both errors in the #744 investigation came
from this: a fixture invented a row shape that `gene_info.gz` never contains, and a one-off shell
scan produced a count that was wrong in a way nobody could see until the check was committed. A
fixture standing in for a real record must be copied **verbatim** from the source file, with the
record's ID in a comment so the next person can re-derive it; and any claim about the data that
justifies a parsing decision belongs in a committed script that regenerates it, not in a PR
description.

The same rule applies one level up, to **clique structure**: concords are a derived artifact
relative to cliques, so they cannot answer "what else is in this clique?". `glom()` merges
*transitively* — two identifiers with no concord edge between them still share a clique via a third.
PR #918 justified a coarse clique-level retype with "every concord partner of these 685 cliques is
typed `ChemicalEntity`, so there is nothing to clobber", which was true of the concords and false of
the build: `DRUGBANK:DB09341` "Dextrose, unspecified form" reaches the D-glucose clique through
`RXCUI`/`UMLS`, and the retype shipped glucose, tocopherol and five others as `biolink:Food`
(#935/#948). Answer clique-membership questions from a finished build —
`partials/untyped_compendium` and the compendia themselves, the DuckDB `Edge` table, or Node
Normalization — never from the concords that fed it.

To measure a change to a compendium-building function, replaying it over a finished build's
`intermediate/` is seconds where a rebuild is hours — see `docs/sources/CLAUDE.md` ("Replaying a
pipeline function beats rebuilding to measure a change") for how, and for what it cannot show.

When a bug fix is easy to cover with a test, suggest adding one as part of the fix.

To see what type or clique Babel currently assigns a CURIE in the **last released** build without a
local build, query the development Node Normalization service, e.g.
`https://nodenormalization-sri.renci.org/get_normalized_nodes?curie=DRUGBANK:DB00965&conflate=true&drug_chemical_conflate=true`
— it returns the clique's `type` and `equivalent_identifiers`, so you can check current typing or
whether two CURIEs already share a clique (the released-build analogue of the DuckDB `Edge` table).

Two different compendia must never share an identifier, and no valid identifier should be dropped
without good reason — when changing how one compendium filters identifiers, check the effect on
every other compendium too.

## Documentation

When making a significant change, check whether it affects any documentation (`docs/*.md`, `*.md`)
and update it, or suggest a new doc file if one is needed.

Prefer section headings over horizontal pipes for dividing up documentation.

When a documentation file mentions a specific ontology term by CURIE, link it to its OBO
PURL and include the preferred label in double-quotes:

```markdown
[`EMAPA:0`](http://purl.obolibrary.org/obo/EMAPA_0) "anatomical structure"
```

Resolve CURIEs with `get_biolink_prefix_map()` (see Biolink Model Usage above). Preferred labels
come from `babel_downloads/<PREFIX>/labels` (tab-separated `CURIE\tlabel`).

### GitHub pull requests and issues

**Do not hard-wrap the body of a pull request or issue.** The 100-column rule is for Markdown files
in the repository; GitHub renders a description as flowing paragraphs in a variable-width column, so
wrapped text there is either re-flowed anyway or shows up as ragged half-lines. Write one long line
per paragraph and per list item, and let the browser wrap it. Write the body to a file and pass it
with `gh pr edit --body-file` / `gh issue create --body-file` rather than inlining it, so quoting
and newlines survive.

A description is for what shipped and why, not how the branch got there. Keep it to the final
methods chosen and the decisions a reviewer has to check; a PR is read again when the *next* release
is reviewed, so anything meant to last belongs in the code, the docs, or the report it describes —
never only in the description. Commit messages are effectively invisible after merge, so never
compress a rationale into "see commit abc1234": if the reasoning matters, and especially if it
records an alternative that was considered and rejected, put it in a comment or docstring beside the
code that would have to change to undo it, and let the description point there.
