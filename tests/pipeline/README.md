# Pipeline Tests

Pipeline tests call `write_*_ids()` and related functions directly, write their output to the
same stable paths that Snakemake uses, and assert properties of that output. They are marked
`pipeline` and skipped by default — pass `--pipeline` (or `--all`) to run them.

Most pipeline tests require `babel_downloads/` to be pre-populated (either by a prior Snakemake
run or by the test fixture downloading the data automatically).

## Caching of intermediate files

Processing fixtures write intermediate ID files to the exact paths Snakemake uses:

```text
babel_outputs/intermediate/{semantic_type}/ids/{vocab}
```

For example, `anatomy.write_umls_ids()` writes to
`babel_outputs/intermediate/anatomy/ids/UMLS`. By default, if that file already
exists it is reused — `write_umls_ids()` is not called again. This means:

- **Second and later runs are fast** — only the test assertions execute.

- **A prior full Snakemake pipeline run can be reused directly** — the test fixtures
  will pick up any files Snakemake already produced.

- **To force re-processing**, pass `--regenerate`:

  ```bash
  uv run pytest tests/pipeline/ --pipeline --regenerate --no-cov -v
  ```

- **To selectively regenerate one vocabulary**, delete its files manually then run
  without `--regenerate`:

  ```bash
  rm babel_outputs/intermediate/*/ids/UMLS
  uv run pytest tests/pipeline/ --pipeline --no-cov -v -k UMLS
  ```

## Test files

- **`test_vocabulary_partitioning.py`** (`pipeline`) — Generic mutual-exclusivity
  tests parametrized over all registered vocabularies. For each vocabulary, verifies that
  (1) every compendium's `write_X_ids()` produces non-empty output and (2) no identifier
  appears in more than one compendium. Currently covers five vocabularies: MESH (5
  compendia), UMLS (7 compendia), OMIM (2 compendia), NCIT (2 compendia via UberGraph),
  GO (2 compendia via UberGraph). Adding a new vocabulary requires only adding its fixtures
  to `conftest.py` and one entry in `VOCABULARY_REGISTRY` — this file never changes.

- **`test_mesh.py`** (`pipeline`) — MeSH-specific targeted test
  ([issue #675](https://github.com/NCATSTranslator/Babel/issues/675)). Downloads
  `babel_downloads/MESH/mesh.nt` automatically if absent. One test: chemicals must exclude
  D05 protein subtrees (D05.500/D05.875), D08 protein subtrees (D08.811/D08.622/D08.244),
  and D12.776 — but must include D08.211 Coenzymes (NAD, Coenzyme A) and D05.374/D05.750/D05.937
  (Micelles, Polymers, Smart Materials), which are all classified as CHEMICAL_ENTITY.

- **`test_umls.py`** (`pipeline`) — UMLS-specific targeted test. Requires
  `UMLS_API_KEY` for the initial download (or cached files). One test: chemicals must not
  contain any UMLS IDs that the protein compendium claimed (semantic type tree
  A1.4.1.2.1.7, Amino Acid/Peptide/Protein).

- **`test_ec.py`**, **`test_rhea.py`**, **`test_chembl.py`**, **`test_clo.py`**,
  **`test_efo.py`** (`pipeline`) — Output format and content checks for the EC, Rhea,
  ChEMBL, CLO, and EFO data handlers.

- **`test_ncbigene.py`** (`pipeline` + `slow`) — Output format checks for the NCBIGene
  labels/synonyms/taxa/descriptions files, plus a real-data regression check for issue #744:
  no emitted synonym may *start* with `''` (a leading `''` is always a pipe-split fragment of a
  `''…''`-quoted alias). A *trailing* `''` is deliberately allowed — it is legitimate
  "double-prime" gene nomenclature (real symbols such as `U2B''` and `ycf1''` end in `''`). The
  test also asserts the two exact fragment strings from the issue's example row are absent. Marked
  `slow` because it downloads the full `gene_info.gz` (>1 GB compressed) and processes every row.

- **`test_compendia_duplicates.py`** (`pipeline`) — Reads a *finished* build's duplicate reports
  (`reports/umls/duplicate-curies.csv`, `reports/duckdb/duplicate_curies.tsv`) and counts the UMLS
  CUIs and MeSH descriptors shared between Protein and a chemical compendium — the overlap the
  ids-file tests above cannot see, because cross-references re-join what the ids files kept apart
  ([issue #308](https://github.com/NCATSTranslator/Babel/issues/308),
  `docs/sources/UMLS/ProteinChemicalDuplicates.md`). Xfails while the overlap exists and fails if
  it grows past the 2026jul22 baseline. Takes `--build-dir` (default `babel_outputs/`), so it runs
  against a build's reports copied from stars.renci.org.

- **`checks/`** (`pipeline`) — Per-compendium regression assertions tied to specific GitHub
  issues, designed for test-driven development. See [Pipeline Checks](#pipeline-checks) below.

## Pipeline Checks

`tests/pipeline/checks/` contains per-compendium regression assertion files driven by specific
GitHub issues. They are intended for test-driven development (TDD): add a failing check, run the
pipeline, iterate on source code until the check passes.

Two shared NamedTuple types in `tests/pipeline/checks/__init__.py` drive all checks:
`IdentifierCheck` for ID-presence checks and `ConcordCheck` for direct cross-reference
checks. Import them in any per-compendium check file.

Two kinds of assertions are supported in each file:

**ID-presence checks** (`EXPECTED_IN_CHEMICALS` / `NOT_IN_CHEMICALS`)

Verify that a specific CURIE appears in (or is absent from) the intermediate ID file for a
compendium. These only require `write_*_ids()` to have run — no Snakemake needed. They
depend on the existing vocab fixtures (`mesh_pipeline_outputs`, `umls_pipeline_outputs`, …).

The `expected_type` field documents the Biolink type; it is also asserted against the second
column of the intermediate file if the vocabulary writes type hints (e.g. UMLS does; MESH
does not).

**Direct cross-reference checks** (`EXPECTED_XREF` / `EXPECTED_NO_XREF`)

Verify that two CURIEs are (or are not) a direct xref pair in any concord file for the compendium.
These depend on `chemicals_concords_dir` (or the equivalent fixture for another compendium), which
runs `snakemake --rerun-incomplete --until get_chemical_wikipedia_relationships` if needed.

**Scope limitation**: only *direct* xref pairs are checked. Indirect equivalences through
multi-hop chains are not detected. This is intentional — it is fast enough for TDD and
locates the concord file that is the root cause of a bad link.

**Adding a new check** — append one tuple to the appropriate list in
`tests/pipeline/checks/test_chemicals.py` (or add a new `test_X.py` file for another
compendium):

```python
from tests.pipeline.checks import ConcordCheck, IdentifierCheck

# ID-presence check (MESH, no Snakemake required):
IdentifierCheck(
    "mesh_pipeline_outputs",
    "chemicals",           # key in the fixture output dict
    "MESH:C000001",
    "biolink:ChemicalEntity",
    "https://github.com/NCATSTranslator/Babel/issues/NNN",
),

# Direct-xref check (requires concords to be generated):
ConcordCheck(
    "chemicals_concords_dir",
    "MESH:C000001",
    "CHEBI:12345",
    False,   # False = must NOT be a direct xref
    "https://github.com/NCATSTranslator/Babel/issues/NNN",
),
```

**Adding a new vocabulary** — change the `fixture` field to the appropriate session fixture
name (e.g. `"umls_pipeline_outputs"` for UMLS ID checks) and set `compendium` to the
matching key in that fixture's output dict (e.g. `"protein"`, `"diseasephenotype"`). For a
new compendium's concord checks, add a `my_compendium_concords_dir` fixture to `conftest.py`
following the `chemicals_concords_dir` pattern, using the appropriate Snakemake sentinel rule.

```bash
# Run all checks:
uv run pytest tests/pipeline/checks/ --pipeline --no-cov -v

# Run only ID-presence checks (fast, no Snakemake):
uv run pytest tests/pipeline/checks/ -k "in_chemicals or not_in_chemicals" --pipeline --no-cov -v

# Run only direct-xref checks:
uv run pytest tests/pipeline/checks/ -k "xref" --pipeline --no-cov -v
```

## Future Plans

### New `network + slow` ETL tests

Add a `tests/etl/` sub-package with a shared `conftest.py` that provides an `etl_output_dir`
fixture (`tmp_path_factory`). Each file downloads and parses one data handler end-to-end and
asserts the output file is non-empty:

- `test_etl_ncbigene.py`, `test_etl_chebi.py`, `test_etl_mesh.py`, `test_etl_ensembl.py`, …

### New `pipeline` tests

The vocabulary-partitioning framework in `conftest.py` makes adding new multi-compendium
vocabularies straightforward. Each vocabulary needs:

1. A **download/connectivity fixture** in `conftest.py` — either a file download using
   `_download_or_skip`, a credential-checked download (like UMLS), or a network health
   check (like `ubergraph_connection` for NCIT/GO).

2. A **processing fixture** in `conftest.py` that calls all `write_X_ids()` functions for
   that vocabulary and returns a `{compendium_name: output_path}` dict.

3. **One line** in `VOCABULARY_REGISTRY`: `"MYVOCAB": "my_vocab_pipeline_outputs"`.

No new test file is needed for the standard non-empty and mutual-exclusivity checks —
`test_vocabulary_partitioning.py` picks them up automatically. Add a
`test_X_pipeline.py` only for vocabulary-specific targeted assertions.

Vocabularies not yet covered (candidates):

- **Rhea, ChEMBL** — fixtures exist in `conftest.py` but these handlers produce
  labels/concords/smiles rather than ID files. Adding them to `VOCABULARY_REGISTRY`
  requires writing `write_rhea_ids()` / `write_chembl_ids()` functions first.
  See [issue #749](https://github.com/NCATSTranslator/Babel/issues/749).
- **ENSEMBL** — appears in protein (`write_ensembl_protein_ids`) and gene
  (`write_ensembl_gene_ids`). Deferred because the download uses BioMart
  (`pull_ensembl(ensembl_dir, complete_file, ...)`) which is more complex to invoke
  outside Snakemake.
- **HGNC / other single-source** — vocabularies that appear in only one
  compendium don't need mutual-exclusivity tests, but could still get non-empty checks
  in a per-compendium ETL test (see above). NCBI Gene now has one: `test_ncbigene.py`.
