# Test Suite

## Overview

Tests are organized along two independent axes:

- **Mark** — controls *when* a test is run (see [Marks](#marks) below for the full table).
- **Directory** — reflects *what* is being tested.

| Directory | What lives there |
|-----------|-----------------|
| `tests/` (root) | Core utility tests: `glom`, `LabeledID`, `NodeFactory`, `ThrottledRequester`, FTP utilities, UberGraph, and gene-protein conflation |
| `tests/datahandlers/` | One test file per module in `src/datahandlers/` |
| `tests/datahandlers/pyoxigraph/` | Smoke tests for the pyoxigraph API surface itself (bulk_load formats, SPARQL row access). Future subdirectories will follow the same pattern for UberGraph, ENSEMBL, etc. |
| `tests/pipeline/` | Full pipeline integration tests that call `write_*_ids()` functions and check the resulting intermediate files; source data is auto-downloaded or skipped per vocabulary (see [Pipeline Tests](pipeline/README.md)) |
| `tests/pipeline/checks/` | Per-compendium regression assertions tied to specific GitHub issues, designed for test-driven development |

**CI** runs only `unit` tests (`uv run pytest -m unit -q`). Keep unit tests fast, offline, and
dependency-free so they remain cheap to run on every PR.

For *how* and *where* the different test tiers should be run (GitHub Actions vs HPC self-hosted
runners, cadence, what to automate vs leave manual, and other testing strategies worth
considering), see [`docs/Testing.md`](../docs/Testing.md).

**Pipeline tests** cache their output to the same stable paths that Snakemake uses
(`babel_outputs/intermediate/…`), so a prior full pipeline run is automatically reused. Pass
`--regenerate` to force `write_X_ids()` to re-run even if its output already exists in
`babel_outputs/intermediate/`. See
[Pipeline > Caching](pipeline/README.md#caching-of-intermediate-files) for details.

### Where to add a new test

- **Pure function or small data transform** → `unit` test in `tests/` root (or `tests/datahandlers/`
  if it exercises a specific data handler module).
- **Specific CURIE that should (or should not) appear in a compendium** → append a `ChemCheck` or
  `ConcordCheck` tuple to `tests/pipeline/checks/test_chemicals.py` (or create a parallel file for
  another compendium). No Snakemake needed for ID-presence checks.
- **Cross-vocabulary identifier exclusivity for a new vocabulary** → add fixtures to
  `tests/pipeline/conftest.py` and one entry to `VOCABULARY_REGISTRY` there. See
  [New pipeline tests](pipeline/README.md#new-pipeline-tests) in the pipeline README.
- **Pipeline behavior specific to one vocabulary** → add `tests/pipeline/test_X_pipeline.py`
  marked `pipeline`.
- **A compendium-building function (`src/createcompendia/X.py`)** →
  `tests/createcompendia/test_X.py`, one file per pipeline. Live-data extraction for one
  vocabulary belongs in `tests/pipeline/test_X_pipeline.py` instead — anatomy is split that way,
  with everything offline plus the endpoint checks in `tests/createcompendia/test_anatomy.py`
  and the UberGraph-backed EMAPA extraction in `tests/pipeline/test_emapa_pipeline.py`.
- **A developer tool under `src/tools/`** → `unit` test in `tests/tools/<tool>/`, mirroring the
  tool's own package name (e.g. `tests/tools/slurm/test_parse.py`). Test only the CLI layer here:
  a tool's reusable logic lives in `src/` and is tested beside it (e.g. `src/model/glom_diff.py`
  → `tests/model/test_glom_diff.py`). See [Developer tools](../docs/tools/README.md).

**Touching a data source that has no pipeline test? Consider writing one.** It is more work up
front, but it pays off three ways and is worth biting the bullet for:

- It forces you to write smaller **`network` tests** (e.g. that the upstream listing still returns
  files, that the header columns Babel reads are still where it expects) that can run regularly and
  catch an upstream format change *before* it silently corrupts output — see
  `tests/pipeline/test_complexportal.py` (header-column assertion) and the `network` test in
  `tests/datahandlers/test_complexportal.py`.
- It validates the handler's outputs end-to-end against real data, using the shared
  `assert_*_file_valid` helpers.
- It becomes the place to hang later assertions about that source's compendium output.

See [Future Plans](#future-plans) at the bottom of this file for a more detailed roadmap of
planned test locations and conventions.

## Running Tests

```bash
uv run pytest                           # All tests
uv run pytest --cov=src                 # With coverage report
uv run pytest tests/test_glom.py        # Single test file
```

Coverage is opt-in: pass `--cov=src` (or `--cov=src --cov-report=html`) to generate
a report. Coverage configuration is in `pyproject.toml` under `[tool.coverage.*]`.

## Marks

Tests are tagged with marks to control which subset runs in a given context:

| Mark       | What it covers                                                           | Network?  | Typical duration | Timeout |
|------------|--------------------------------------------------------------------------|-----------|------------------|---------|
| `unit`     | Pure functions, in-memory logic, small fixtures in `tests/data/`         | No        | Seconds          | 30 s    |
| `network`  | Requires live internet (FTP, SPARQL, BioMart, external APIs)             | Yes       | Seconds–minutes  | 600 s   |
| `slow`     | Genuinely long-running — a scan of a full 70M-row pipeline output, a multi-GB RDF load | Sometimes | Many minutes–hours | 3600 s |
| `pipeline` | Calls `write_*_ids()` directly; source data is auto-downloaded or the test skips — no manual setup required for most vocabularies | Sometimes | Minutes          | 900 s   |

A test carrying more than one mark gets the **longest** of their timeouts, so `slow` is how a
pipeline test that really does need an hour asks for one — the plain `pipeline` budget assumes the
download is already cached and the test itself finishes well inside 15 minutes. Add `slow` when it
won't. You can adjust the per-mark timeouts in [conftest.py](conftest.py), and disable the timeout
entirely for a run with `pytest --timeout=0` (pytest-timeout treats 0 as "no limit"); a single test
can opt out with `@pytest.mark.timeout(0)`, which conftest leaves alone.

There is also a parametrized guard marker, `min_memory_gb(n)`, that combines with the marks
above rather than replacing them. A test tagged `@pytest.mark.min_memory_gb(n)` is auto-skipped
when the machine has less than `n` GiB of RAM (detected via POSIX `sysconf`). It guards
memory-hungry tests against OOM/swap-thrash on small machines — for example the `test_chembl`
pipeline tests bulk-load a ~17 GB TTL into an in-memory store and are tagged
`min_memory_gb(128)`, matching the `chembl_labels_and_smiles` Snakemake rule's `mem="128G"`.

### Default behavior

- `pytest` alone: runs `unit` and `slow` tests; skips `network` and `pipeline`
- `pytest --network`: also runs `network` tests
- `pytest --pipeline`: also runs `pipeline` tests (source data is auto-downloaded per vocabulary, or
  the test skips if unavailable)
- `pytest --pipeline --regenerate`: forces `write_X_ids()` to re-run even if its output already
  exists in `babel_outputs/intermediate/` (useful after changing compendium filtering logic; see
  **Caching** below)
- `pytest --all`: runs everything (equivalent to `--network --pipeline`)
- `pytest --all --regenerate`: authoritative full run — reruns all `write_X_ids()` functions
  from source rather than reusing cached intermediate files; use this when you want to
  validate the pipeline end-to-end rather than just check tests against a prior run

### Convenience commands

```bash
uv run pytest -m unit                             # unit tests only (CI default)
uv run pytest -m "unit or network" --network      # unit + live-service checks
uv run pytest -m "unit or slow"                   # unit + slow offline tests
uv run pytest --all --no-cov                      # run every test (no coverage)
uv run pytest -m pipeline --pipeline -x --no-cov # one pipeline test at a time
uv run pytest -m "not pipeline"                   # everything except full pipeline runs
uv run pytest -n auto --no-cov                    # parallel (all CPUs), skip coverage
uv run pytest -n 4 -m unit                        # 4 workers, unit tests only
uv run pytest -n auto --pipeline --no-cov         # safe: pipeline tests run in 1 worker
```

**Parallel execution and pipeline tests** — pytest-xdist gives each worker its own Python
session, so session-scoped fixtures like `Mesh` (which loads `mesh.nt` into an in-memory
store) are not shared: N workers would each load `mesh.nt` simultaneously and exhaust
available RAM. To prevent this, the root `conftest.py` automatically assigns
`xdist_group("pipeline")` to every pipeline-marked test when `-n` is active, ensuring all
pipeline tests run in a single worker while unit/slow/network tests still parallelize freely.

## Test Files

### Core

- **`test_glom.py`** (`unit`) — Tests the `glom` utility, which merges pairwise identifier
  sets into equivalence cliques (union-find). Covers basic merging, iterative
  multi-call usage, tuple vs. set inputs, and the expected `ValueError` when sets
  contain more than two members.

- **`test_LabeledID.py`** (`unit`) — Tests that `LabeledID` objects (a CURIE paired with a
  human-readable label) compare correctly against bare strings and behave properly
  in sets.

- **`test_geneproteiny.py`** (`unit`) — Integration test for gene-protein conflation. Runs
  `build_compendium` with gene and protein compendia plus a concordance file from
  `data/` and verifies output is produced.

- **`test_node_factory.py`** (`network`) — Tests `NodeFactory`, the central class for building
  normalized nodes. Covers ancestor retrieval, prefix ordering, identifier
  normalization (selecting the best CURIE from an equivalence set), label
  application, UMLS filtering, PubChem disambiguation, and deduplication of
  `LabeledID` objects. Marked `network` because the `node_factory` fixture calls
  `bmt.Toolkit`, which fetches `biolink-model.yaml` and `predicate_mapping.yaml`
  from GitHub on first use.

### Data Handlers

All datahandler unit tests share helpers from `tests/datahandlers/conftest.py`:

- `nn(iri)`, `lit(val, language=None)`, `quad(s, p, o)` — concise pyoxigraph node constructors.
- `RDF_NS`, `RDFS_NS`, `SKOS_NS` — common namespace strings, so tests don't repeat them.
- `make_graph_from_store(cls, store, **attrs)` — constructs a handler object (e.g. `ECgraph`,
  `EFOgraph`) with a pre-built in-memory store, bypassing the file-loading `__init__`. Use this
  in every new datahandler test rather than repeating the `cls.__new__(cls); obj.m = store`
  pattern.

For handlers that produce label/synonym files, add a module-scoped `*_output` fixture using
`tmp_path_factory` that calls the extraction method once and stores the file contents as a dict.
Individual tests then receive the pre-computed output rather than re-running the extraction.

The **root** `tests/conftest.py` exports format-validation helpers shared by both datahandler and
pipeline tests: `assert_labels_file_valid`, `assert_synonyms_file_valid`, `assert_ids_file_valid`,
`assert_concordance_file_valid`, `assert_taxa_file_valid`, and `assert_descriptions_file_valid`.
Each asserts the file is non-empty and column-shaped correctly and returns the parsed rows. Use
these (and `read_tsv`) instead of hand-rolling TSV checks; when a handler grows a new output kind
(as ComplexPortal did for taxa and descriptions), add the matching helper here rather than keeping
a private one in the test file.

- **`datahandlers/test_mesh.py`** (`unit`) — Unit tests for `src/datahandlers/mesh.py`.
  Covers `write_ids()` parameter validation, SCR filtering logic (mock-based), and
  `Mesh.get_scr_terms_mapped_to_trees()` using an inline pyoxigraph store.

- **`datahandlers/test_complexportal.py`** (`unit`, one `network`) — Unit tests for
  `src/datahandlers/complexportal.py`. Covers HTML directory-listing parsing (only `.tsv`
  hrefs returned, sorted), download + manifest writing (mocked), and the cross-species
  deduplication rules for every output (labels by identifier with first-seen winning, synonyms
  and descriptions by pair, taxa keeping all `(id, taxon)` pairs, IDs including accessions with
  an empty label). Test rows are built from the real 19-column header via `COMPLEXTAB_COLUMNS`/
  `COMPLEXTAB_HEADER` imported from the source module, so a column-layout change in source is
  reflected in the fixtures automatically. The single `network` test hits the live EBI listing
  and asserts it returns at least one sorted `.tsv` file.

- **`datahandlers/test_ensembl.py`** (`network`, `xfail`) — Integration test for the Ensembl
  BioMart data handler. Pulls real data from BioMart, verifies that batched downloads
  (splitting attribute lists across multiple queries) produce the same results as
  single-query downloads, and checks TSV output correctness. Uses `tmp_path`.

- **`datahandlers/pyoxigraph/test_pyoxigraph_api.py`** (`unit`) — Smoke tests for the
  pyoxigraph API used by all RDF-based handlers: `Store.bulk_load()` for RDF/XML, Turtle,
  and N-Triples formats; the `base_iri` workaround required by EC/EFO/CLO (files that contain
  `<owl:Ontology rdf:about=""/>` raise a builtin `SyntaxError` without it); and SPARQL result
  row access by variable name.

### Pipeline

See [`tests/pipeline/README.md`](pipeline/README.md) for caching behavior, fixture setup,
and how to add new checks or vocabularies.

- **`pipeline/test_vocabulary_partitioning.py`** (`pipeline`) — Parametrized mutual-exclusivity
  checks for all registered vocabularies: every `write_X_ids()` must produce non-empty output
  and no identifier may appear in more than one compendium. Currently covers MESH, UMLS, OMIM,
  NCIT, and GO.

- **`pipeline/test_mesh.py`** (`pipeline`) — MeSH-specific targeted assertions
  ([issue #675](https://github.com/NCATSTranslator/Babel/issues/675)): chemicals must exclude
  D05 protein subtrees (D05.500, D05.875), D08 protein subtrees (D08.811, D08.622, D08.244),
  and D12.776 — but must include D08.211 Coenzymes.

- **`pipeline/test_umls.py`** (`pipeline`) — UMLS-specific targeted assertions:
  chemicals must not contain UMLS IDs claimed by the protein compendium.

- **`pipeline/test_ec.py`**, **`pipeline/test_rhea.py`**, **`pipeline/test_chembl.py`**,
  **`pipeline/test_clo.py`**, **`pipeline/test_efo.py`** (`pipeline`) — Output format and
  content checks for the EC, Rhea, ChEMBL, CLO, and EFO data handlers.

- **`pipeline/test_complexportal.py`** (`pipeline`) — Downloads all ComplexPortal TSV files and
  validates the `labels`, `synonyms`, `taxa`, and `descriptions` outputs with the shared
  `assert_*_file_valid` helpers. Two guards worth copying for other sources: a **header-column
  assertion** that pins the index of every column Babel reads, so an upstream format change is
  caught here rather than silently corrupting the taxon/description extraction; and a
  **cross-file consistency** test that re-reads every source TSV and confirms each `(CURIE,
  taxon)` pair survives into the taxa output while labels and synonyms stay deduplicated (using
  `collections.Counter`, not `O(n²)` `list.count()`).

- **`pipeline/checks/`** (`pipeline`) — Per-compendium regression assertions tied to GitHub
  issues (ID-presence and direct cross-reference checks), designed for TDD. See
  [`tests/pipeline/README.md`](pipeline/README.md#pipeline-checks) for the full guide.

### Utilities

- **`test_ThrottledRequester.py`** (`unit`) — Tests the `ThrottledRequester` HTTP client,
  verifying that rate-limiting delays are correctly applied between requests.
  Uses a local HTTP server, so no network access is required.

- **`test_ftp.py`** (`network`) — Tests FTP download utilities (`pull_via_ftp`) against the
  NCBI FTP server. Covers pulling plain text and gzipped files to memory or disk
  with optional decompression. Requires `--network` to run.

- **`test_docs_links.py`** (`unit`) — Checks the links in every tracked Markdown file and in the
  source that reports URLs to users (snakefiles, `CITATION.cff`, notebooks): relative links and
  heading anchors must resolve on disk, and no link may point at a `master` branch or at the
  pre-rename `TranslatorSRI` org. Entirely offline — nothing fetches a URL, because a test that
  fails when GitHub is slow is a test people learn to ignore. Ported from NameResolution's test of
  the same name; keep the three repos' copies in sync.

- **`test_uber.py`** (`network`) — Tests the `UberGraph` class for querying ontology
  subclasses and cross-references via SPARQL. Covers direct and indirect subclass retrieval,
  filtering by cross-reference presence, and exact-match label queries. Tests may xfail at
  runtime if the UberGraph server is reachable but returns an HTTP error on the probe request.

### createcompendia/

One file per compendium-building pipeline in `src/createcompendia/`. These test the pipeline
functions themselves — ids-file type maps, concord generation, clique typing, compendium
assembly — as opposed to `tests/datahandlers/`, which tests the per-source download and parse
step that feeds them.

- **`createcompendia/test_anatomy.py`** (`unit`, some `network`) — The anatomy pipeline:
  EMAPA's part_of walk and per-CURIE typing, the Wikidata cell concord and its one-to-one
  filter, the MESH/UMLS/NCIT ids-file type maps, bad-xref filtering, the GO/CL/UBERON/EMAPA
  typing precedence and the majority vote behind it, and an end-to-end `build_compendia()` run.
  The `network` tests query the live FRINK and UberGraph endpoints and fetch the Biolink Model.

- **`createcompendia/test_chemicals.py`** (`unit`) — `write_unichem_concords()`'s handling of
  UniChem compound IDs that already embed their source prefix (which once produced
  `CHEBI:CHEBI:12345`), and `make_chebi_relations()`'s reading of the ChEBI SDF, whose tags
  ChEBI renames between releases.

- **`createcompendia/test_diseasephenotype.py`** (`unit`) — The disease/phenotype pipeline: the
  UMLS semantic-type tree map, MONDO close-match concord parsing, source exclusion for the
  impact report, and the disease-vs-phenotype clique typing rules.

- **`createcompendia/test_drugchemical.py`** (`unit`) — `_validate_and_apply_manual_concords()`,
  which validates manual concord pairs against the chemical compendia and normalises their
  CURIEs.

- **`createcompendia/test_leftover_umls.py`** (`network`) — The manual UMLS semantic-type
  overrides used when building the leftover UMLS compendium. Marked `network` because it builds
  a Biolink Model Toolkit.

- **`createcompendia/test_publications.py`** (`unit`) — `verify_pubmed_downloads()`, the backstop
  that makes it safe to carry PubMed files forward from a previous run by MD5-checking each one.

### babel_utils/

- **`babel_utils/test_write_compendia.py`** (`unit`) — Unit tests for `choose_preferred_name()`,
  the label-selection helper extracted from `write_compendium()`. Covers per-type length demotion
  (demotion applies to chemicals and their subtypes via ancestor traversal; diseases, phenotypes,
  and other non-chemical types are never demoted), interaction with `preferred_name_boost_prefixes`,
  and the fall-through when all labels exceed the limit. Regression tests use real CURIEs from
  [#597](https://github.com/NCATSTranslator/Babel/issues/597),
  [#711](https://github.com/NCATSTranslator/Babel/issues/711),
  [#714](https://github.com/NCATSTranslator/Babel/issues/714), and
  [#723](https://github.com/NCATSTranslator/Babel/issues/723).

## Test Data

The `tests/data` directory contains fixture files used by several tests:

- `gptest_Gene.txt` — Sample gene compendium for gene-protein conflation tests
- `gptest_Protein.txt` — Sample protein compendium
- `gp_UniProtNCBI.txt` — Sample UniProt-NCBI concordance

## Future Plans

### Test infrastructure improvements

- **Bundle the Biolink Model locally** — The `node_factory` fixture calls `bmt.Toolkit`, which
  fetches `biolink-model.yaml` and `predicate_mapping.yaml` from GitHub on first use. Shipping a
  pinned copy of those files with the repo (or using VCR cassettes) would let all 13 tests in
  `test_node_factory.py` run offline and be re-marked `unit`.
- **`babel_config` conftest fixture** — A session fixture that patches `get_config()` to redirect
  `download_directory` and `output_directory` to `tmp_path`. Tests that exercise `create_node()`
  or other path-dependent code could use this instead of manually setting `common_labels = {}`.
- **VCR cassettes** — Record real HTTP/FTP responses for BioMart and UberGraph once, commit the
  cassette files, and replay them in CI. This would let `datahandlers/test_ensembl.py`,
  `test_chemicals.py`, and `test_uber.py` run offline and be promoted from `network + xfail` to
  `unit`.

### New `unit` test files (parsing transforms)

Create a `tests/fixtures/` directory with small golden files (10–20 rows each) per data handler,
then add offline parsing tests:

- **`tests/parsing/test_drugbank.py`** — `extract_drugbank_labels_and_synonyms` with a fixture CSV
- **`tests/parsing/test_ncbigene.py`** — `get_ncbigene_field` and `pull_ncbigene_labels` with a
  fixture `.gz` file
- **`tests/parsing/test_unii.py`** — `write_unii_ids` with a small fixture TSV
- **`tests/parsing/test_mesh.py`** — Mesh SPARQL methods with a fixture `.nt` RDF file

### New `unit` test files (babel_utils and TSVSQLiteLoader)

- **`tests/test_babel_utils.py`** — Cover `sort_identifiers_with_boosted_prefixes()`,
  `get_numerical_curie_suffix()`, `clean_sets()`, `get_prefixes()`, and
  `filter_out_non_unique_ids()`
- **`tests/test_tsvsqliteloader.py`** — Load a small fixture TSV into `TSVSQLiteLoader`, run
  `get_curies()` queries, verify case-insensitive lookups and missing-key handling

### New `unit` test files (compendium transforms)

- **`tests/compendium/test_chemicals_ids.py`** — `write_umls_ids`, `write_unii_ids`,
  `get_type_from_smiles`
- **`tests/compendium/test_gene_ids.py`** — `build_gene_ensembl_relationships` with a fixture
  BioMart TSV
- **`tests/compendium/test_make_cliques.py`** — `get_conflation_ids` parsing logic

See [`tests/pipeline/README.md`](pipeline/README.md#future-plans) for planned pipeline and ETL
test additions.

### Deduplication / cleanup

- Move `test_geneproteiny.py` assertions to also check individual clique contents, not just that
  the output file is non-empty.
