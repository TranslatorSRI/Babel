# Rust in Babel

Several developers have expressed an interest in adding Rust code to Babel. We know that some
expensive rules are CPU-bound single-threaded Python. From the `babel-1.18` Snakemake
`benchmark:` TSVs, the five costliest rules all run at ~100% of one core:

| Rule                             | Wall            | CPU      | `mean_load` | `max_rss` |
|----------------------------------|-----------------|----------|-------------|-----------|
| `generate_pubmed_concords`       | 71,947 s (20 h) | 71,753 s | 99.7%       | 31 GB     |
| `protein_compendia`              | 19,876 s        | 19,844 s | 99.8%       | 246 GB    |
| `chemical_compendia`             | 19,643 s        | 19,569 s | 99.5%       | 335 GB    |
| `geneprotein_conflated_synonyms` | 15,719 s        | 15,702 s | 99.8%       | 98 GB     |
| `gene_compendia`                 | 15,400 s        | 15,178 s | 98.5%       | 179 GB    |

Rust might provide better performance and more efficient memory use for these jobs. However,
most Babel developers are most familiar with Python, so Rust must be carefully incorporated
only where the Rust code is limited, works well with the overall Snakemake pipelines, and provides
either:

1. A significant performance improvement that cannot be easily replicated in Python, e.g. by
   improving the algorithm used by the Python code, or
2. Using Rust code allows well-maintained Rust libraries to replace older, unmaintained or
   poorly performing Python libraries.

This directory allows the [maturin](https://www.maturin.rs/) backend that uv uses for Babel to build
it as a mixed Rust/Python package: a `cdylib` crate under this directory compiled into `src/_accel`,
alongside the ordinary Python in `../src`. Code within this directory can be organized in any way
that makes sense going forward -- if we end up with a lot of Rust code, we can also consider mixing
the two codebases more closely.

## Rules for adding a Rust function

**Measure first, from the benchmark TSVs.** Not from reading code for quadratic-looking shapes.
Three candidate targets picked that way all turned out to be wrong: `SynonymFilter.should_suppress`
looks like an O(labels × entries) scan but `../input_data/obsolete_synonyms.yaml` holds three
entries; concord parsing looks like the obvious string-manipulation target but runs 159,283 lines in
0.148 s; and each genuinely expensive rule examined had a pure-Python defect (an eagerly evaluated
f-string feeding a suppressed `logger.debug`, a missing `elem.clear()`) worth more than any port.
Check `mean_load` before assuming a slow rule is CPU-bound — `get_ensembl` is 6,665 s wall but only
257 s CPU, so Rust would do nothing for it.

**The FFI boundary is coarse.** A `#[pyfunction]` takes a file path and returns the whole parsed
result. It never takes one row. Crossing pyo3 once per CURIE costs more than the Python it
replaces, so a per-row entry point would be *slower* while looking like an optimization. This is
enforced structurally: Rust functions open their own files, so there is no entry point that could
accept a row.

**A/B it before it replaces the Python it's based on, then delete that Python.** Write the Rust
against the existing Python implementation, and set `BABEL_DISABLE_RUST=1` to force the Python path
so the two can be timed and diffed against each other in one checkout. Once the Rust side is
confirmed correct and faster, delete the Python implementation it replaced — don't keep both in the
tree indefinitely. `BABEL_DISABLE_RUST=1` is an environment variable rather than a `config.yaml`
entry deliberately: which of two byte-identical implementations runs is an implementation detail
with no user-facing meaning, and `config.yaml` is threaded into Snakemake `params` and output paths,
where changing it risks perturbing the DAG of a running build. Precedent: `BABEL_DUCKDB_TEMP_DIR`.

**Bump `ABI_VERSION` in the same commit** as any change to an accelerated function's signature or
semantics — in both `src/lib.rs` and `_REQUIRED_ABI_VERSION` in `../src/accel.py`. See
"Staleness" below.

## Using it from Python

Import through [`../src/accel.py`](../src/accel.py), never `src._accel` directly:

```python
from src.accel import accel

def convert_synonyms_to_sapbert(synonym_filename_gz, sapbert_filename_gzipped):
    if accel is not None:
        return accel.convert_synonyms_to_sapbert(synonym_filename_gz, sapbert_filename_gzipped)
    return _convert_synonyms_to_sapbert_python(synonym_filename_gz, sapbert_filename_gzipped)
```

`accel` is the compiled module when it is present, importable and current, and `None` otherwise.
That is the whole of `src/exporters/sapbert.py`'s dispatch, and the Snakemake rule that calls it
(`generate_sapbert_training_data` in `src/snakefiles/exports.snakefile`) did not change at all: the
rule calls the same Python function it always did, and the function decides.

## What is accelerated today

| Function | Rule | Python | Rust | Cluster time at stake (babel-1.18) |
|---|---|---|---|---|
| `convert_synonyms_to_sapbert` (`src/sapbert.rs`) | `generate_sapbert_training_data_*` | 5.6 s | 2.2 s | 25 rules, 19,600 s (5.4 h) summed, ~99% CPU, ~200 MB |

Timings are for `babel_outputs/synonyms/Disease.txt.gz` (354,071 entries, 1.3 M names) on a
laptop. The Python is gzip-bound: at zlib level 9 (its default) 59% of its time is compression, and
at level 6 it takes 4.8 s, so a one-line `compresslevel=6` would have bought a fifth of what the
port did. The Rust uses `flate2` on the `zlib-rs` backend at level 6; the output was the same size
to within 1%. Two behaviours differ deliberately, both marked `ponytail:` in `src/sapbert.rs`: the
pair sampling is seeded per CURIE, so a rerun is byte-identical (the Python's `random.sample` is
unseeded and its `set(names)` order varies with `PYTHONHASHSEED`), and the gzip level. The
differential test is `tests/exporters/test_sapbert.py`; run it with `--pipeline` over a local
build's `babel_outputs/synonyms/*.gz` to reproduce the A/B.

## In-process vs a file boundary

Rust can also enter Babel as a standalone program at a Snakemake `shell:` rule, with a file in
between. [`../docs/rust-decision/`](../docs/rust-decision/) measured the one axis where in-process
could win, the cost of getting data across, by crediting pyo3 with a serialisation cost of *zero*
and comparing against the unavoidable floor of building the Python objects the consumer needs.
Extrapolated to `chemical_compendia`'s 256 M CURIEs, the most in-process could save is ~4 minutes,
1.3% of the rule -- and switching that rule's existing `repr(set)`/`ast.literal_eval` boundary
(`chemicals.py`) to JSONL would save ~9.5 minutes with no Rust at all. So the boundary cost never
justifies in-process on its own. What does: a function that takes paths and writes paths, so nothing
is materialised in Python at all (the SapBERT export above), or Rust keeping ownership of the data
behind a `#[pyclass]` so Python never builds it. Anything that ends with Python holding a big dict
pays the floor either way, which is why porting the `*Factory` loaders in `src/node.py` would not
help (issue #1004).

**A missing extension falls back to Python rather than raising.** Every snakefile does a top-level
`import src.foo` at DAG-parse time, so an `ImportError` would take down all 243 `run:` rules for a
contributor without a Rust toolchain, for a reviewer, and for a fork's CI — not just the rules that
would have used Rust. AGENTS.md's "a log warning is not a control" is about *wrong output*; a slower
path producing identical bytes is not that. The active implementation is logged once at INFO, which
lands in the SLURM job log that `babel-slurm-errors` reads.

## Staleness

The extension is installed **editable**: the compiled artifact lands in the checkout at
`src/_accel.*.so` (gitignored), and that is the copy `PYTHONPATH=.` finds. A `git pull` that brings
new Rust source does **not** rebuild it, so without a guard a stale binary would be used silently
— potentially for a 12-hour rule.

So `../src/accel.py` compares the extension's `ABI_VERSION` against its own `_REQUIRED_ABI_VERSION`
and raises if they disagree. The check runs at import, i.e. at DAG-parse time, so a stale build
fails in the first second of a run. To fix one:

```bash
uv sync --reinstall-package babel-pipeline
```

What the guard does not catch is editing the Rust body without bumping the constant — there is no
permanent Python copy left to diff against once a port has graduated, so review is the only check
on that. Bump the ABI_VERSION on EVERY change.

## Building

A Rust toolchain is a build prerequisite for **every** environment now, because `[tool.uv] package
= true` means every `uv run` and `uv sync` builds the project, and building the project invokes
cargo.

- **Locally:** `uv sync`. If `cargo` is not on `$PATH`, uv bootstraps a toolchain itself (via
  `puccinialin`) rather than failing — convenient, but it silently downloads ~600 MB into a
  platform cache directory that `UV_CACHE_DIR` does **not** cover. Installing rustup yourself
  (`brew install rustup && rustup default stable`, or <https://rustup.rs>) avoids that.
- **CI:** `dtolnay/rust-toolchain@stable` plus `Swatinem/rust-cache@v2` in `test.yml`. The
  formatting workflow deliberately runs snakefmt with `uv run --no-project` so that a lint job does
  not build the project at all.
- **Docker:** the image installs rustup rather than `apt install cargo`. Debian bookworm ships
  cargo 1.63, which is *exactly* pyo3 0.23's minimum, so the next pyo3 bump would break the image
  with an error that reads as unrelated. rustup also honours `../rust-toolchain.toml`, which apt's
  cargo ignores.
- **`cargo test`:** the crate's unit tests link a test binary against libpython, which pyo3's
  `extension-module` feature forbids. So that feature is opt-in in `Cargo.toml` and enabled only by
  maturin (`features` in `../pyproject.toml`); `cargo test`, `cargo clippy` and `cargo fmt` run
  without it. CI runs all three in `check-formatting.yml`.
- **Hatteras:** `uv sync` runs once on the login node and compute nodes reuse the `.venv` from
  `/projects`, so compilation happens in one place. Make sure a toolchain is available there before
  a build — `uv sync --frozen` fails outright without one, before Snakemake starts. See
  [`../slurm/README.md`](../slurm/README.md).

`../rust-toolchain.toml` pins the channel (`stable`) rather than an exact version: nothing here
publishes a wheel, so the value of pinning is that everyone's cargo comes from the same place
rather than from whatever a distro packages. The hard floor is `rust-version` in `Cargo.toml`.

## Layout

| Path                     | What                                                                                                                                                    |
|--------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------|
| `Cargo.toml`             | The crate. `version` is `0.0.0` on purpose — maturin takes the distribution version from the root `../pyproject.toml`, and nothing publishes this crate |
| `src/lib.rs`             | The `#[pymodule]`: module doc, `ABI_VERSION`, registrations. Functions go in their own modules, split from the first one                                |
| `../src/accel.py`        | The only thing that imports the compiled module                                                                                                         |
| `src/_accel.pyi`         | Hand-written stub. There is no mypy here, so it enforces nothing; it exists so a reader who does not read Rust can see what the module offers           |
| `../rust-toolchain.toml` | Channel pin                                                                                                                                             |
| `src/sapbert.rs`         | The SapBERT export accelerator, with its unit tests                                                                                                     |
| `../docs/rust-decision/` | The boundary-cost measurement behind "In-process vs a file boundary" above                                                                              |

## History

[PR #588](https://github.com/NCATSTranslator/Babel/pull/588) took a different approach — 19
standalone binaries under `babel_io/src/bin/` invoked from Snakemake `shell:` blocks — and is worth
reading as a cautionary tale. It targeted datacollect label/synonym rules totalling roughly 1.5 h of
CPU, under 2% of the pipeline, two of them download-bound where Rust does nothing; the one binary
aimed at an expensive rule (`build_compendia.rs`) is a 72-line stub. 3,252 lines across 44 files,
unmerged. Hence the "measure first" and "one path, deep" rules above.

The in-process pyo3 model that replaced it fits the codebase better for the case above: 243 of
Babel's Snakemake rules are `run:` blocks (26 are `shell:`), so an importable extension needs no
rule rewriting, whereas standalone binaries would mean converting rules to `shell:` one at a time.

[PR #975](https://github.com/NCATSTranslator/Babel/pull/975) built this plumbing and was closed
unmerged after its first accelerator became a Rust `glom()` -- the most important and least tested
function in the pipeline, exactly what a first port should not be. That `glom()` (686 lines,
union-find over interned CURIEs) still exists on its branch and was run once on the cluster; whether
it holds up is an open question worth answering with `babel-clique-diff`.
