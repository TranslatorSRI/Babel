# Does an in-process Rust boundary buy anything? — measured

> **This is an AI-written experiment** (PR #988, July 2026), kept because its script reproduces the
> numbers. The conclusions it feeds are summarised in [`rust/README.md`](../../rust/README.md)
> under "In-process vs a file boundary"; that section, not this page, is the current guidance.

## What was measured, and why not pyo3 directly

Rust can enter Babel two ways: **in-process** (a pyo3 extension,
[PR #975](https://github.com/NCATSTranslator/Babel/pull/975)) or **out-of-process** (a separate
program invoked from a Snakemake `shell:` rule, handing data over through a file). Out-of-process is
cheaper on nearly every axis — no toolchain for people who write no Rust, its own `benchmark:` TSV,
its own SLURM reservation, `diff` as the differential test, revertible by editing one rule. The one
axis where in-process could win is the cost of getting data across.

Benchmarking a pyo3 arm directly would not settle anything: it has no single honest shape (return a
list of tuples? an iterator? a `#[pyclass]`?), so any number invites "you picked the wrong return
type". So [`boundary_cost.py`](./boundary_cost.py) measures a **floor** that no pyo3 implementation
can beat:

> **F** = the cost of constructing the Python objects the consumer needs, from raw bytes.

Anything that ends with Python holding the data pays at least F. Therefore
**max possible saving from in-process = cost(file arm) − F**, and pyo3 is credited here with a
serialization cost of exactly *zero*. If that gap is small next to the rule it sits inside, no Rust
implementation can rescue it.

## Result

Payload: the anatomy clique state read straight out of `babel_outputs/compendia/*.txt` (178,870
cliques, 306,924 members — a finished compendium line *is* a clique), replicated to multiply
cardinality while preserving clique-size and string-length distributions. Every arm ends holding
`set[frozenset[str]]`, which is what all 14 of `glom()`'s consumers build. All arms were asserted to
produce an identical clique set before any timing was believed.

```text
=== scale 10x — 1,788,700 cliques, 3,069,240 members ===
arm         produce_s  consume_s   total_s   file_MB  peak_GB
floor            0.00       1.93      1.93       0.0     1.03
repr_set         1.30      10.50     11.80      53.4     1.01
jsonl            2.07       2.93      5.00      53.4     1.04
parquet          0.74       3.55      4.29      29.9     1.79
```

`repr_set` is not a straw man: `f"{set(s)}\n"` written at `chemicals.py:1110-1113` and read back
with `ast.literal_eval` at `:1165-1168` is the **actual** format of the boundary between
`untyped_chemical_compendia` and `chemical_compendia` today — a Python `repr` used as a wire format
for the largest clique state in Babel.

### Extrapolated to production scale

`reports/tables/cliques_table.csv` from `babel-1.18` puts the chemical pipeline at **256,427,006
CURIEs**, i.e. 83.5× the 10× payload. Against `chemical_compendia`'s measured 19,643 s:

| Boundary | Extrapolated | Share of the rule |
|---|---|---|
| floor F (unavoidable) | 161 s | 0.8% |
| JSONL round trip | 418 s | 2.1% |
| Parquet round trip | 358 s | 1.8% |
| `repr(set)` round trip (**status quo**) | 985 s | 5.0% |

**Two conclusions:**

1. **The maximum possible saving from going in-process is ~256 s — about 4 minutes, or 1.3% of a
   5.5-hour rule.** That is the ceiling, granted to pyo3 for free. No pyo3 implementation can do
   better, so the boundary cost cannot justify in-process on its own.
2. **Switching the existing boundary from `repr(set)` to JSONL would save ~568 s — about 9.5
   minutes, 2.2× more than in-process pyo3 could ever save** — and needs no Rust, no toolchain and
   no FFI. The serialization *format* matters more than the mechanism.

Per the interpretation pre-registered in the harness docstring before the first run: the file arms
did **not** all land within 20% of each other, so this is partly a finding about format choice; and
the gap to F, while a large *fraction* of each arm, is a rounding error against the rule it lives
inside. The decision therefore falls to the non-performance criteria in
[`rust/README.md`](../../rust/README.md).

Note also that **F grows faster than the file arms** (1×→10×: floor 16×, JSONL 11.6×, Parquet 6.0×).
Python object construction is the term that scales worst, so at larger payloads the relative
advantage of in-process shrinks further rather than growing.

## Reproduce

```bash
PYTHONPATH=. uv run python docs/rust-decision/boundary_cost.py --scale 1 --scale 10
```

Needs a local build's `babel_outputs/compendia/*.txt` — the anatomy target alone is enough
(~25 minutes on a laptop, per `docs/RunningBabel.md`). Each arm runs in a fresh subprocess so peak
RSS is per arm.

## What this does not establish

- **100× was not run.** It needs ~37M member CURIEs (~10-15 GB resident, ~1.5 GB of files) and the
  machine this ran on has 16 GB RAM and 20 GB free. `--scale 100` should be run on a larger machine
  or an HPC node before the extrapolation above is trusted. It would falsify this write-up if the
  arms stopped scaling near-linearly — for instance if Python-object construction hit an allocator
  cliff the file arms did not.
- **The extrapolation is a per-item cost model, not a measurement at scale.** It should be checked
  against a `py-spy` profile of a real `chemical_compendia` run: if the fraction of samples inside
  `ast.literal_eval` disagrees with the 5.0% predicted above, believe the profile and discard this.
- **I/O is served from page cache here.** A 53 MB file written and immediately reread on a laptop
  SSD measures `memcpy`, not a parallel filesystem. The real I/O term is already recorded in the
  benchmark TSVs (`untyped_chemical_compendia`: `io_out` 13,716 MB; `chemical_compendia`: `io_in`
  36,762 MB) and is not modelled here.
- **The one thing pyo3 can do that a file cannot** is never materialise into Python at all — Rust
  keeps ownership and Python iterates lazily via a `#[pyclass]`. That escapes the floor entirely.
  Its out-of-process equivalent is "port the consumer too", so it is a question about how far a
  rewrite goes rather than about which mechanism is faster. This harness deliberately says nothing
  about it, and it is the one argument for in-process that survives these numbers.
