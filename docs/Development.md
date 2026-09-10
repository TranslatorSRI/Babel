# Developing Babel

This document describes the current development workflow for Babel and ideas for improving it.

## Coding Conventions

**Biolink class references** — always import and use the named constants from `src/categories.py`
(e.g. `CHEMICAL_ENTITY`, `DRUG`, `ANATOMICAL_ENTITY`) instead of hardcoding `"biolink:..."` strings.
If a needed constant is missing, add it to `src/categories.py` first. This keeps all Biolink class
names in one place so a rename only requires a single-file update.

**Why some constants stay in Python** — `AGENTS.md` states the rule ("configuration over
constants", with source-cleaning rules staying beside the parsing code). The reason that split is
safe is that the provenance is captured elsewhere, twice over. The whole package is git-tagged when
a pipeline run takes place, so the exact cleaning rules used for any given build can always be
recovered from the tag. And important or variable information can be recorded in a `metadata.yaml`:
per-source metadata YAMLs are folded into a pipeline's final `metadata.yaml`, preserving that
information alongside the results. If a constant matters enough that someone reading the output
would want to know its value, write it into the source's concord metadata rather than moving it
into `config.yaml`.

## Enhancing a data source ingest

When you add a new data source or extend an existing one (the
[ComplexPortal PR #831](https://github.com/NCATSTranslator/Babel/pull/831) is a worked example of
all of this), these lessons are worth applying. The Babel-specific conventions referenced here are
spelled out for Claude Code in [`../src/datahandlers/CLAUDE.md`](../src/datahandlers/CLAUDE.md) (and
[`../tests/CLAUDE.md`](../tests/CLAUDE.md) for the test rules); this section is the human-readable
overview.

### Emit the attribute files your source supports

The data-collection phase writes per-source attribute files into `babel_downloads/[PREFIX]/`, and
the factories in `src/node.py` pick them up by prefix. There are four, each an independent,
optional TSV:

- `labels` — `CURIE → name` (read by `NodeFactory`).
- `synonyms` — `CURIE → predicate → synonym` (`SynonymFactory`).
- `taxa` — `CURIE → NCBITaxon:NNNN` (`TaxonFactory`).
- `descriptions` — `CURIE → text` (`DescriptionFactory`).

Emit whichever your source supports — providing `taxa` and `descriptions` is simply how a source
enriches its cliques, and it costs little once you are already parsing the rows. Match each
output's **deduplication key to its downstream consumer**: labels key on the identifier alone
(first-seen wins), but taxa and descriptions keep every distinct `(identifier, value)` pair because
their factories accumulate per identifier.

### Write an explicit Biolink type in the IDs file

The `ids/[TYPE]/[PREFIX]` file is `CURIE → biolink:Type`. Write the type explicitly even when every
CURIE from the source has the same prefix and type: the column looks redundant in that case, but it
is **essential** the moment a source spans multiple types, and it makes intent obvious. Generate
the IDs from the source rows as each CURIE is first seen, rather than deriving the file from
`labels` via `awk` — deriving from labels silently drops any identifier whose label is empty.

### Document the code with docstrings

Give modules, classes, and non-trivial functions a docstring covering what they do and any
non-obvious behavior (dedup keys, side effects, why a network call happens). Name functions for
what they do — `fetch_*` rather than `get_*` when the call hits the network. Docstrings are cheap,
survive refactors, and are the first thing read when someone revisits the ingest.

### Bite the bullet and add a pipeline test

If the source has no pipeline test, consider writing one even though it is more work up front. It
pays off three ways:

- It forces you to write smaller, regularly-runnable **`network` tests** — that the upstream
  listing still returns files, that the header columns you read by index are still where you expect
  — which catch an upstream format change _before_ it silently corrupts output.
- It validates the handler end-to-end against real data using the shared `assert_*_file_valid`
  helpers in `tests/conftest.py`.
- It becomes the natural home for later assertions about that source's compendium output.

See [`tests/README.md`](../tests/README.md) and
`tests/{datahandlers,pipeline}/test_complexportal.py`.

### A few more habits that paid off

- **Split download from extract/validate.** Give a data-collection step two Snakemake rules: a
  `download_*` rule that only fetches the raw file(s), and a separate rule that validates format or
  extracts content. If upstream changes its format (e.g. a column rename), only the validation rule
  fails — Snakemake keeps the downloaded file, so a code fix doesn't force an expensive re-download.
  Format validation belongs in the extraction/filter rule, never in the download rule.
- **Validate upstream format and fail loudly.** Pin the column layout as a constant in the source
  module (e.g. `complexportal.COMPLEXTAB_COLUMNS`/`COMPLEXTAB_HEADER`) — that constant is the
  canonical format documentation living next to the code — import it into tests to build fixture
  rows, and assert the live header still matches at each index Babel reads, so a silent upstream
  re-ordering becomes a loud test failure instead of corrupted output. Raise `ValueError` /
  `RuntimeError` on anything else unexpected (a missing column, a zero-length file listing, an
  already-prefixed taxon) rather than producing wrong output quietly. See
  `src/datahandlers/complexportal.py`.
- **Use a manifest as the download sentinel** for multi-file downloads: write the list of
  downloaded files last and make _that_ the Snakemake output, so its presence proves the download
  phase finished and the extraction rule knows what to parse. See
  `complexportal.pull_complexportal()`.
- **Accept explicit file-path arguments** (`infile`/`outfile`/…) instead of calling
  `make_local_name` inside the handler, so unit tests can point at `tmp_path` without patching the
  config and Snakemake can declare inputs/outputs precisely.
- **Record provenance** with `write_metadata()` so the per-source `metadata.yaml` captures where
  each output came from.
- **Characterize a messy field before you parse it.** When a free-text field has irregular quoting
  or delimiters, stream the whole downloaded file and tabulate how characters and delimiters are
  actually used across every row before deciding how to split it. A pattern that reads as an
  artifact in a handful of sampled rows is often legitimate at scale — NCBIGene's trailing `''` is
  genuine double-prime nomenclature (`RNA polymerase subunit beta''`, `V-type proton ATPase subunit
  c''`), not a stray quote, and a semicolon-joined designation is usually an isoform enumeration of
  one name rather than two distinct synonyms. Keep the analysis script and its output next to the
  per-source doc so the conclusion is reproducible and reviewable; see
  [`docs/sources/NCBIGene/quoting/`](sources/NCBIGene/quoting/).

## Current Development Process

Developing a change to Babel is significantly more complicated than developing most software,
because the pipeline operates on very large data files that take hours to download, gigabytes of
disk space to store, and hundreds of gigabytes of RAM to process. This creates a feedback loop
that is slow by necessity.

### Typical workflow

1. **Build prerequisites.** Before writing any code, you need the input files for the Snakemake
   step you plan to modify. There are two ways to get these:
   - Run the upstream Snakemake rules yourself (which may themselves require large downloads).
   - Copy intermediate files from the last successful full run, e.g. from the SLURM cluster.

2. **Write code.** Implement the change locally, iterating against whatever prerequisite files are
   available.

3. **Run the relevant target.** For example, if you changed how anatomy compendia are built:

   ```bash
   uv run snakemake --cores 1 anatomy
   ```

   This produces compendia and synonym files for the anatomy semantic type, but does _not_ trigger
   the pipeline-wide reports, which require _all_ compendia, synonym, and conflation files.

4. **Review your own output.** Because reports are not available, you must inspect the output files
   manually — checking JSONL structure, spot-checking a few cliques, and reviewing completeness
   reports. There is no automated feedback at this stage.

5. **Merge.** The change goes in without confirmation that it behaves correctly in the context of
   the full pipeline.

6. **Wait for SLURM.** The next time someone runs the full pipeline on the SLURM cluster, you
   find out whether your change worked. If it did not, the turnaround for a fix is another full
   run.

### Updating the Biolink Model fixture

The `tests/compendium/` test package patches `bmt.Toolkit` to read pinned local
copies of `biolink-model.yaml`, `attributes.yaml`, `predicate_mapping.yaml`,
and `biolink_model_prefix_map.json` from `tests/fixtures/biolink-model/`. This
keeps those tests offline, but it means the fixture must stay in sync with the
`biolink_version` declared in `config.yaml`.

When you bump `biolink_version`, refresh the fixture in the same change:

```bash
uv run python tests/fixtures/biolink-model/refresh.py
```

Commit the updated fixture files together with the `config.yaml` change. The
network test `tests/test_biolink_model_freshness.py` (run with `--network`)
fails loudly when the fixture and config diverge.

### Why this is hard

- **Download cost.** Many data sources (UMLS, UniChem, PubChem, UniProt TrEMBL) are multi-gigabyte
  downloads that take hours. You cannot easily re-download them per experiment.
- **Memory cost.** The chemical and protein compendia steps require 512 GB of RAM, which is not
  available on a laptop or a typical workstation.
- **Cross-type report dependencies.** The `all_reports` target in `src/snakefiles/reports.snakefile`
  requires every compendium, synonym, and conflation file to exist before it will run. Building one
  semantic type's compendium in isolation does not satisfy these dependencies.
- **No unit-testable seams.** The core compendium-building logic (`createcompendia/`) reads and
  writes large files. There is no easy way to run it against a small, synthetic dataset without
  manually constructing the full file layout.
- **Opaque intermediate state.** Snakemake tracks what has been built via file existence and
  timestamps. There is no summary of what prerequisites are present and what is missing.

## Ideas for Improvement

Developer-tooling and workflow improvements — scripts and pipeline changes that help you build,
debug, and validate Babel without changing its outputs — are tracked as GitHub issues under the
[`developer tooling`](https://github.com/NCATSTranslator/Babel/issues?q=is%3Aissue+is%3Aopen+label%3A%22developer+tooling%22)
label, rather than as a static list here. Broader efforts they build on are tracked too: logging
([#453](https://github.com/NCATSTranslator/Babel/issues/453)), fixture/unit-test coverage
([#763](https://github.com/NCATSTranslator/Babel/issues/763)), shared format readers/writers
([#736](https://github.com/NCATSTranslator/Babel/issues/736),
[#759](https://github.com/NCATSTranslator/Babel/issues/759)), and release regression tooling
([#764](https://github.com/NCATSTranslator/Babel/issues/764)).

For the companion question of _how_ and _where_ to run the test suite (cadence, runner choice, what
to automate), see [`docs/Testing.md`](Testing.md). Before writing a new tool, read
[`tools/README.md`](tools/README.md): a tool is a thin CLI in `src/tools/<tool>/cli.py`, and the
logic it drives belongs in `src/` beside the code it models, so the next tool can reuse it.
