# Plan: centralize MeSH tree-number partitioning

> **Temporary working doc.** This describes an in-progress refactor (issue
> [#735](https://github.com/NCATSTranslator/Babel/issues/735)). Delete this file once the work is
> merged — the durable description belongs in `Ingestion.md`.

## Context

Babel ingests MeSH descriptors into five compendia, and each one independently declares its slice
of the MeSH tree in a local `write_mesh_ids()`:

- `src/createcompendia/anatomy.py:70` — A01–A20 → `ANATOMICAL_ENTITY` (A11 → `CELL`,
  A11.284 → `CELLULAR_COMPONENT`)
- `src/createcompendia/taxon.py:14` — B01–B05 → `ORGANISM_TAXON` (+ `SCR_Organism`)
- `src/createcompendia/diseasephenotype.py:57` — C01, C04–C22, C24–C26 → `DISEASE`;
  C23 → `PHENOTYPIC_FEATURE` (C02/C03 deliberately omitted)
- `src/createcompendia/chemicals.py:153` — D01–D26 → `CHEMICAL_ENTITY` with overrides
  (D12.125/D12.644 → `POLYPEPTIDE`, D20 → `COMPLEX_MOLECULAR_MIXTURE`), **plus six `EXCLUDE`
  markers** for the protein subtrees, plus `SCR_Chemical` gated by `scr_exclude_trees`
- `src/createcompendia/protein.py:46` — the **same six** D subtrees (D05.500, D05.875, D08.244,
  D08.622, D08.811, D12.776) → `PROTEIN`, plus `SCR_Chemical` gated by `scr_include_trees`

The problem: the chemical↔protein D-tree split is maintained as **two hand-synced copies** of the
protein-subtree list. If they drift, a descriptor lands in both compendia or in neither. Both files
carry a `TODO` to unify them; the risk is documented in `Ingestion.md` ("Chemical and protein
partition the same D-trees independently"). MeSH also changes over time, so we want tests that fail
loudly if a branch we depend on is renamed or emptied.

**Outcome:** a single central registry declaring the whole MeSH→Babel partition. The chemical
`EXCLUDE` entries (and its `scr_exclude_trees`) are *derived* from what protein owns, so the two can
never drift. Disjointness is validated by a unit test; a pipeline test confirms every depended-on
branch still exists and is non-empty in the real MeSH dump.

Design decisions (confirmed with the user):

- **Full registry with derived excludes** — not just the chemical/protein pair, but all five
  compendia declared centrally; chemical's excludes derived from protein's ownership.
- **Convert `mesh.py` into a package** — `src/datahandlers/mesh/__init__.py` (current contents,
  unchanged) plus `src/datahandlers/mesh/tree_numbers.py` (the registry).
- **Both unit and pipeline stability tests.**

## Import audit (already done — confirms the package conversion is transparent)

Every site that touches `src.datahandlers.mesh` uses only names that remain at module top level in
`__init__.py`, so the package move is a no-op for all of them:

- `src/snakefiles/datacollect.snakefile:2,219,230` — `import src.datahandlers.mesh as mesh`;
  `mesh.pull_mesh()`, `mesh.pull_mesh_labels()`
- `src/createcompendia/{anatomy,taxon,diseasephenotype,chemicals,protein}.py` —
  `import src.datahandlers.mesh as mesh`; `mesh.write_ids(...)`, `mesh.pull_mesh_registry()`
- `tests/pipeline/conftest.py:35` — `from src.datahandlers.mesh import Mesh, pull_mesh`
- `tests/datahandlers/test_mesh.py:14` — `from src.datahandlers.mesh import Mesh, write_ids`;
  also `@patch("src.datahandlers.mesh.Mesh")` (resolves into `__init__.py`, still valid)

## Step 1 — Convert `src/datahandlers/mesh.py` to a package

Move the file *unchanged* into `__init__.py`. Git can't `mv` a file into a directory of the same
name in one step, so go via a temp dir:

```bash
mkdir -p src/datahandlers/mesh_pkg_tmp
git mv src/datahandlers/mesh.py src/datahandlers/mesh_pkg_tmp/__init__.py
git mv src/datahandlers/mesh_pkg_tmp src/datahandlers/mesh
```

`Mesh`, `write_ids`, `pull_mesh`, `pull_mesh_labels`, `pull_mesh_registry`, `get_mesh_id_from_iri`,
`MESH_IRI_PREFIX` all stay defined in `__init__.py`. `write_ids` **stays here** (it does the Mesh
querying + file I/O); only the *declarations* move to `tree_numbers.py`.

## Step 2 — Add `src/datahandlers/mesh/tree_numbers.py` (the registry)

Single source of truth, organized into a clearly-commented section per compendium. Migrate the rich
rationale comments verbatim from `chemicals.py` / `protein.py` / the others so the "why" lives next
to the data (this is what preserves "understand a usage without going elsewhere"). Reuse the named
category constants from `src/categories.py`; never hardcode `"biolink:..."`. Define
`EXCLUDE = "EXCLUDE"` (the same literal `write_ids` already keys on in
`src/datahandlers/mesh/__init__.py`'s `write_ids`).

Compendium-name constants: `ANATOMY`, `TAXON`, `DISEASE`, `CHEMICAL`, `PROTEIN` (plain strings used
as registry keys).

Data model:

- A per-tree assignment carrying: `tree` (e.g. `"D05.500"`), owning `compendium`, `biolink_type`,
  human-readable `rationale`, and `expect_terms: bool` (whether MeSH is assumed to hold ≥1 term
  there — see Step 5). A frozen dataclass is fine.
- A per-compendium config carrying: its owned-tree assignments (incl. within-compendium overrides,
  which are just more-specific trees with a different type — e.g. A11.284 under A11, D12.125 under
  D12), the `order` priority list, `extra_vocab` (`{SCR_class: biolink_type}`), and an SCR gating
  mode ∈ {`none`, `include_owned`, `exclude_ceded`}.
- Programmatically-generated ranges (D01–D26, A01–A20, B01–B05) are produced by a small helper that
  marks the filler entries `expect_terms=False` (defensive coverage — D07/D11/D14–D19/D21–D22/D24
  are currently empty in MeSH per the `chemicals.py` comments), while explicitly-named trees default
  to `expect_terms=True`.

`compendium_spec(name)` derives the `write_ids` kwargs:

1. `meshmap` = the compendium's owned trees/types, **plus** an `EXCLUDE` entry for every tree owned
   by a *different* compendium that is equal to or a descendant of a tree this compendium broadly
   owns (descendant test: `T == P or T.startswith(P + ".")`). This reproduces chemical's six
   `EXCLUDE` entries automatically from protein's ownership.
2. `order` = the declared list, with `EXCLUDE` prepended automatically if the derived meshmap
   contains any `EXCLUDE`.
3. `scr_include_trees` / `scr_exclude_trees` derived from the gating mode:
   - `include_owned` → `scr_include_trees = sorted(owned trees)` (protein)
   - `exclude_ceded` → `scr_exclude_trees = sorted(derived EXCLUDE trees)` (chemical)
   - `none` → neither (taxon's `SCR_Organism`, which no other compendium consumes; anatomy/disease
     consume no SCRs)

Return these as a small spec object/namedtuple with fields `meshmap`, `order`, `extra_vocab`,
`scr_include_trees`, `scr_exclude_trees`.

Also expose `validate_partition()` (called by the unit test; optionally assert at import): no tree
owned by two compendia, and every cross-compendium descendant overlap is covered by a derived
`EXCLUDE` (so no descriptor can be claimed by two compendia). Raise `ValueError` on violation.

**Must reproduce today's exact values** (verify against the current code, see Step 4):

- chemical: meshmap = `{D01..D26: CHEMICAL_ENTITY}` + `{D05.500, D05.875, D08.811, D08.622, D08.244,
  D12.776: EXCLUDE}` + `{D12.125, D12.644: POLYPEPTIDE}` + `{D20: COMPLEX_MOLECULAR_MIXTURE}`;
  order = `["EXCLUDE", POLYPEPTIDE, COMPLEX_MOLECULAR_MIXTURE, CHEMICAL_ENTITY]`;
  extra_vocab = `{"SCR_Chemical": CHEMICAL_ENTITY}`;
  scr_exclude_trees = the six protein subtrees; scr_include_trees = None
- protein: meshmap = the six subtrees → `PROTEIN`; order = `[PROTEIN]`;
  extra_vocab = `{"SCR_Chemical": PROTEIN}`; scr_include_trees = the six subtrees;
  scr_exclude_trees = None
- anatomy: meshmap = `{A01..A20: ANATOMICAL_ENTITY}` + `{A11: CELL, A11.284: CELLULAR_COMPONENT}`;
  order = `[CELLULAR_COMPONENT, CELL, ANATOMICAL_ENTITY]`; extra_vocab = `{}`; no SCR gating
- taxon: meshmap = `{B01..B05: ORGANISM_TAXON}`; order = `[ORGANISM_TAXON]`;
  extra_vocab = `{"SCR_Organism": ORGANISM_TAXON}`; no SCR gating
- disease: meshmap = `{C01,C04..C22,C24..C26: DISEASE}` + `{C23: PHENOTYPIC_FEATURE}`;
  order = `[DISEASE, PHENOTYPIC_FEATURE]`; extra_vocab = `{}`; no SCR gating (preserve the C02/C03
  omission comment)

## Step 3 — Thin out the five `write_mesh_ids()` functions

Each becomes a uniform, behavior-preserving call with a short docstring pointing to the named
section in `tree_numbers.py`. Example (`chemicals.py`):

```python
def write_mesh_ids(outfile):
    """Write MeSH chemical identifiers. The D-tree partition (shared with the protein
    compendium) is declared centrally in src/datahandlers/mesh/tree_numbers.py — see the
    CHEMICAL section there for which subtrees are included/excluded and why."""
    spec = tree_numbers.compendium_spec(tree_numbers.CHEMICAL)
    mesh.write_ids(spec.meshmap, outfile, order=spec.order, extra_vocab=spec.extra_vocab,
                   scr_exclude_trees=spec.scr_exclude_trees, scr_include_trees=spec.scr_include_trees)
```

Add `from src.datahandlers.mesh import tree_numbers` to each compendium. Delete the now-obsolete
`TODO` blocks in `chemicals.py` (~line 201) and `protein.py` (~line 64). Drop category imports that
become unused and run `uv run ruff check --fix` (watch F401/F841). `mesh.write_ids` itself is
**unchanged** (signature and logic), keeping the blast radius small and the existing handler unit
tests valid.

## Step 4 — Unit tests (offline, `@pytest.mark.unit`)

Add `tests/datahandlers/test_mesh_tree_numbers.py` (or extend `tests/datahandlers/test_mesh.py`):

- **Disjointness**: `validate_partition()` passes for the real registry; and a constructed registry
  with the same tree owned by two compendia, or a descendant overlap missing its `EXCLUDE`, raises
  `ValueError`.
- **Behavior-preserving golden check**: `compendium_spec(CHEMICAL)` and `compendium_spec(PROTEIN)`
  produce exactly the meshmap / order / extra_vocab / scr_*_trees listed in Step 2 (assert against
  the literal expected dicts/lists). Same lighter assertions for anatomy/taxon/disease.
- **Derived-exclude check**: chemical's derived `EXCLUDE` set == protein's owned trees, and
  chemical's `scr_exclude_trees` == that set.

The existing `write_ids` mock/filter and `get_scr_terms_mapped_to_trees` tests stay untouched and
must still pass.

## Step 5 — Pipeline stability test (`@pytest.mark.pipeline`, real `mesh.nt`)

Add to `tests/pipeline/test_mesh.py`, reusing the `mesh_nt` fixture in `tests/pipeline/conftest.py`.
Load `Mesh()` once and, for every registry tree with `expect_terms=True`, assert
`get_terms_in_tree(tree)` returns ≥1 term. This fails loudly if MeSH renames/removes a depended-on
branch (e.g. D12.776). Filler ranges (`expect_terms=False`) are deliberately not asserted — the
assumption "this branch must be populated" is encoded precisely where it holds and nowhere it
doesn't.

## Step 6 — Docs

- `docs/sources/MESH/Ingestion.md`: replace the "Chemical and protein partition the same D-trees
  independently" section with a description of the central registry and the derived excludes; update
  the code-locations list to name `tree_numbers.py`; note #735 addressed.
- `CLAUDE.md`: update the `src/datahandlers/mesh.py` path references (e.g. the
  `get_mesh_id_from_iri()` IRI-parsing example) to the package path, and add a one-line pointer that
  MeSH tree partitioning is centralized in `tree_numbers.py`.
- **Delete this file** (`docs/sources/MESH/tree-number-centralization-plan.md`).
- Run `uv run rumdl check .`.

## Verification

```bash
uv run ruff check
uv run snakefmt --check --compact-diff .          # only if any snakefile touched (none expected)
uv run rumdl check .
uv run pytest tests/datahandlers/test_mesh.py -m unit            # handler tests
uv run pytest tests/datahandlers/test_mesh_tree_numbers.py -m unit  # registry tests
uv run pytest -m unit -q                                          # full offline suite (catches import breakage)
uv run pytest tests/pipeline/test_mesh.py --pipeline              # real mesh.nt: branch existence + chem/protein split
```

The full offline run is the key regression gate for the package conversion (any broken `mesh` import
surfaces at collection). The pipeline run validates the MeSH-stability assumptions against the live
dump.
