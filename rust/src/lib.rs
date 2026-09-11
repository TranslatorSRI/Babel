//! Native accelerators for babel-pipeline, importable as `from src import _accel`.
//!
//! A function lands here only once benchmarking justifies it (see README.md), and is A/B tested
//! against its Python original with `BABEL_DISABLE_RUST=1` before replacing it -- see `src/accel.py`.
//! Once a port is confirmed correct and faster, the Python it replaced is deleted rather than kept
//! as a permanent parallel copy. Until that decision is made, `src/accel.py` falls back to the
//! Python implementation when this extension is missing, because every snakefile imports `src.*` at
//! DAG-parse time and a hard import failure would take down every rule for anyone without a
//! Rust toolchain.
//!
//! **The FFI boundary is coarse, and that is a rule, not a preference.** A `#[pyfunction]` here
//! takes a file path and returns the whole parsed result; it never takes one row. Crossing pyo3
//! once per CURIE costs more than the Python it would replace, so a per-row entry point would be
//! slower while looking like an optimisation. Functions open their own files so there is no entry
//! point that *could* accept a row.
//!
//! Accelerated so far: [`sapbert::convert_synonyms_to_sapbert`], the SapBERT training-data export
//! (rule `generate_sapbert_training_data`). Each function lives in its own module.

use pyo3::prelude::*;

mod sapbert;

/// Bumped by hand whenever a function's signature or semantics change, in the same commit.
///
/// `src/accel.py` compares this against its own `_REQUIRED_ABI_VERSION` at import time and raises
/// if they disagree. That matters because the extension is installed editable: the compiled
/// artifact sits in the checkout at `src/_accel.*.so` and a `git pull` does not rebuild it, so
/// without this check a stale binary would be used silently for a 12-hour rule.
///
// ponytail: a hand-bumped integer. Move to a build-time hash of this crate's sources if anyone
// forgets to bump it twice.
const ABI_VERSION: u32 = 1;

#[pymodule]
fn _accel(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("ABI_VERSION", ABI_VERSION)?;
    m.add_function(wrap_pyfunction!(sapbert::convert_synonyms_to_sapbert, m)?)?;
    Ok(())
}
