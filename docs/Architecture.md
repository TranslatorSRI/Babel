# Babel Architecture

This document describes how Babel's source code is organized, how data flows through the pipeline,
and the key patterns and data structures that appear throughout the codebase. It is intended for
contributors who want to understand the system before making changes.

For instructions on how to run and configure the pipeline, see
[RunningBabel.md](./RunningBabel.md). For the development workflow and known challenges, see
[Development.md](./Development.md).

## Pipeline overview

Babel's pipeline has two phases, orchestrated by [Snakemake](https://snakemake.github.io/):

1. **Data collection** — individual data handlers download source data from FTP servers and the
   web, then parse and normalize it into two kinds of files per source:
   - `labels` files: CURIE → preferred name mappings
   - `synonyms` files: CURIE → predicate → synonym mappings

   These files are written into `babel_downloads/[PREFIX]/`.

2. **Compendium building** — for each semantic type (e.g. chemicals, genes, anatomy), a compendium
   creator module reads the relevant label and synonym files, extracts the identifiers for that type
   into `babel_outputs/intermediate/[SEMANTIC_TYPE]/ids/`, produces pairwise cross-reference files
   called **concords**, merges the concords into equivalence cliques using a union-find algorithm,
   and writes enriched JSONL compendia to `babel_outputs/compendia/[BIOLINK TYPE].txt`.

The top-level `Snakefile` coordinates the whole pipeline by including 18 specialized snakefiles
from `src/snakefiles/` — one per semantic type, plus files for data collection, reports, exports,
and DuckDB integration.

## Configuration

The main configuration file is [`config.yaml`](../config.yaml) at the repository root. It contains:

- Directory paths for inputs and outputs
- Version strings for the current build
- Per-semantic-type lists of valid CURIE prefixes and their priority ordering
- Chemical-specific settings such as `preferred_name_boost_prefixes` and `demote_labels_longer_than`

The `UMLS_API_KEY` environment variable is required for downloading UMLS and RxNorm data. You
can obtain a UMLS API key by setting up a [UMLS Terminology Services](https://uts.nlm.nih.gov/uts/)
account and looking up your API key in [your profile](https://uts.nlm.nih.gov/uts/profile).

## Source code layout

All Python and Snakemake source code lives under `src/`:

| Directory / file       | Purpose                                                                                                                                                                           |
|------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `src/datahandlers/`    | ~35 modules, one per external data source. Each module downloads, parses, and normalizes data from a specific source (ChEBI, UniProt, NCBI Gene, DrugBank, MeSH, etc.).           |
| `src/createcompendia/` | ~14 modules, one per semantic type (chemicals, genes, proteins, anatomy, disease/phenotype, etc.). These consume data handler outputs, build concords, and write final compendia. |
| `src/snakefiles/`      | Snakemake rule files that wire data handlers to compendium creators and define the full dependency graph.                                                                         |
| `src/node.py`          | Core factory classes: `NodeFactory`, `SynonymFactory`, `DescriptionFactory`, `TaxonFactory`, `InformationContentFactory`, `TSVSQLiteLoader`.                                      |
| `src/babel_utils.py`   | Core pipeline utilities: download/FTP helpers, `glom()` (clique merging), `write_compendium()` (compendium builder), and state management helpers.                                |
| `src/util.py`          | Logging setup, config loading, [Biolink Model Toolkit](https://github.com/biolink/biolink-model-toolkit) access.                                                                  |
| `src/exporters/`       | Output format handlers for KGX, SapBERT training data, Apache Parquet, and JSONL. The SapBERT export runs in Rust via `src/accel.py` (see `rust/README.md`).                      |
| `src/reports/`         | Report generation code.                                                                                                                                                           |
| `src/synonyms/`        | Synonym file generation.                                                                                                                                                          |
| `src/metadata/`        | Provenance and metadata handling.                                                                                                                                                 |

## Key data structures

### Concord files

Concord files are the central intermediate data structure in Babel. Each concord file is a
tab-separated file of triples:

```text
CURIE1 <TAB> Relation <TAB> CURIE2
```

Each triple expresses that `CURIE1` and `CURIE2` are related by `Relation` (typically
`skos:exactMatch` or an equivalent). The compendium building phase reads all concord files for a
semantic type and feeds them into the `glom()` function in `src/babel_utils.py` to merge them into
equivalence cliques.

### Compendium JSONL

Each line of a compendium file is a JSON object representing one clique. A clique includes:

- `identifiers` — list of all equivalent CURIEs, in preferred-prefix order. Each entry carries its
  own label (`l`), descriptions (`d`, collected from UberGraph and sorted shortest first) and taxa
  (`t`)
- `ic` — information content score (from UberGraph)
- `taxa` — associated taxa (for genes, proteins, etc.); the union of the per-identifier `t` values
- `preferred_name` — the preferred human-readable label for the clique
- `type` — Biolink semantic type

The first identifier in `identifiers` is the preferred identifier for the clique. See
[DataFormats.md](./DataFormats.md) for the full format specification.

## Key patterns

### Factory pattern for large datasets

`NodeFactory`, `SynonymFactory`, `DescriptionFactory`, `TaxonFactory`, and
`InformationContentFactory` (all in `src/node.py`) use a factory pattern for lazy loading.
Rather than loading entire datasets into memory up front, they load data on demand and cache
results. This is important because many source files are gigabytes in size.

### TSVSQLiteLoader

`TSVSQLiteLoader` (in [`src/node.py`](../src/node.py)) loads tab-separated files into in-memory
SQLite databases that spill to disk when memory pressure is high. This avoids the need to hold
entire large TSV files in RAM, which would be infeasible given Babel's data volumes.

### Clique merging via `glom()`

`glom()` in [`src/babel_utils.py`](../src/babel_utils.py) merges concord triples into equivalence
cliques. It maintains a dictionary (`conc_set`) where every CURIE key points to its equivalence
set. For each new `(CURIE1, relation, CURIE2)` triple, it unions all existing sets that contain
either CURIE, then adds both CURIEs to the resulting set. At the end, each value in `conc_set` is
one clique. `write_compendium()` in the same file drives the overall compendium-building process,
calling `glom()` and then sorting, enriching, and writing the output.

### Biolink Model integration

All semantic types, valid CURIE prefixes, and naming conventions follow the
[Biolink Model](https://biolink.github.io/biolink-model/). The
[Biolink Model Toolkit](https://github.com/biolink/biolink-model-toolkit) is accessed via
[`src/util.py`](../src/util.py) and is used throughout the codebase to validate types, look up
preferred prefix orderings, and check whether a given prefix is valid for a type.

### Conflation modules

GeneProtein and DrugChemical conflations each have dedicated conflation modules
([`src/createcompendia/geneprotein.py`](../src/createcompendia/geneprotein.py) and
[`src/createcompendia/drugchemical.py`](../src/createcompendia/drugchemical.py)) that merge their
respective cliques after the initial compendium build. See [Conflation.md](./Conflation.md) for
details on what conflation means and how it works.

### Chemical compendium output types

The chemical pipeline emits one compendium file per Biolink type, enumerated in one place:
`config.yaml: chemical_outputs`. That single list fans out to DrugChemical conflation (its input is
`expand(..., config["chemical_outputs"])`), the KGX/Parquet/JSONL/DuckDB exports (via
`get_all_compendia`), and the synonym outputs — so **adding a new chemical subtype only needs an
entry there** (plus, in `create_typed_sets`, whatever routes cliques to it). Conflation reads these
files but never re-types, so a retype done in the compendium survives downstream.

The one manual extra: the per-type report rules in
[`src/snakefiles/chemical.snakefile`](../src/snakefiles/chemical.snakefile) are hardcoded
(`check_drug`, `check_food`, …), and `rule chemical` expands `chemical_outputs` over `reports/`, so
a new output without a matching `check_*` rule breaks the DAG (no producer for
`reports/<Type>.txt`). See the DrugBank food-and-extract retype
([`docs/sources/DRUGBANK/food-and-extracts/README.md`](sources/DRUGBANK/food-and-extracts/README.md))
for a worked example that added `Food.txt`.

### Which Biolink type a chemical clique gets

`create_typed_sets` in [`src/createcompendia/chemicals.py`](../src/createcompendia/chemicals.py)
types each glommed clique by a vote over its members' per-identifier types (from
`intermediate/chemicals/partials/types`), with two wrinkles: a clique whose PubChem members all
agree short-circuits the vote, and sources can contribute *extra candidates* — today only the
DrugBank food/extract evidence — that join the vote rather than overriding it.

Ties are broken by `config.yaml: chemical_type_order`, most preferred first. Two things about that
ranking are deliberate and easy to get backwards: `biolink:Drug` is **last**, below
`biolink:ChemicalEntity`, because it comes almost entirely from RxNorm formulations and is only
useful where we failed to merge one with its active ingredient; and `biolink:Food` sits below every
structure-bearing type, so food evidence can improve on a vague `ChemicalEntity` but can never
demote a defined molecule. That second rule is a bug fix — see
[the food-and-extracts README](sources/DRUGBANK/food-and-extracts/README.md) for what happened when
the evidence was an override instead (issue #935).

## Output directories

When the pipeline runs, it creates and populates these directories:

| Directory                     | Contents                                                                                            |
|-------------------------------|-----------------------------------------------------------------------------------------------------|
| `babel_downloads/`            | Cached source data, organized by prefix (e.g. `babel_downloads/CHEBI/`). Can be reused across runs. |
| `babel_outputs/intermediate/` | Intermediate build artifacts: ids files, concord files, per-type label and synonym aggregates.      |
| `babel_outputs/`              | Final outputs: compendia (JSONL), synonym files, reports, and exports (Parquet, KGX).               |
