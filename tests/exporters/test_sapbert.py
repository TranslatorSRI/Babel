"""Differential tests for the SapBERT export: the Rust implementation against the Python reference.

`src/exporters/sapbert.py` dispatches to `rust/src/sapbert.rs` when the extension is built. The
Python is the specification, so every test here runs both over the same input and compares.

The comparison cannot be byte-for-byte: the Python samples pairs with an unseeded `random.sample`
over a hash-ordered `set(names)`, so even two Python runs differ. What *is* fixed is compared
exactly -- the three leading fields, the row count per CURIE, and, for records with at most
`MAX_SYNONYM_PAIRS` pairs (the large majority), the exact set of pairs. Sampled records are checked
structurally: every pair is a valid pair and exactly `MAX_SYNONYM_PAIRS` were written.
"""

import gzip
import itertools
import json
import os
import time

import pytest

import src.accel
from src.exporters.sapbert import (
    MAX_SYNONYM_PAIRS,
    _convert_synonyms_to_sapbert_python,
    convert_synonyms_to_sapbert,
)

HERE = os.path.dirname(__file__)

# Five records copied verbatim from babel_outputs/synonyms/Cell.txt.gz (Babel 1.17 outputs, Jul
# 2026), chosen for the code path each exercises:
#   CL:0000438      one name                      -> one row, preferred.lower() || name
#   UMLS:C1267974   three names                   -> all three pairs, no sampling
#   CL:0002252      two names equal after lower() -> set() collapses to one, so NO rows (Python quirk)
#   UMLS:C5401663   a non-ASCII name (ORBCEL C(TM))
#   CL:0000775      many names (neutrophil)        -> more than MAX_SYNONYM_PAIRS pairs, sampled
VERBATIM_FIXTURE = os.path.join(HERE, "..", "data", "sapbert_sample_synonyms.jsonl")

# Shapes the real outputs do not currently contain (checked against every local synonyms file).
# Synthetic, and labelled as such: they exist to exercise branches, not to stand in for a record.
SYNTHETIC_RECORDS = [
    # No names at all: the row repeats preferred.lower() twice.
    {"curie": "SYN:no-names", "names": [], "types": ["Disease"], "preferred_name": "Only A Label"},
    # Empty types: the Biolink type falls back to NamedThing.
    {"curie": "SYN:no-types", "names": ["x", "y"], "types": [], "preferred_name": "Typeless"},
    # Runs of pipes collapse to one pipe; a single pipe is untouched.
    {"curie": "SYN:pipes", "names": ["a||b", "c|||d", "e|f"], "types": ["Gene"], "preferred_name": "Pipes"},
    # No preferred_name: skipped and counted, no rows.
    {"curie": "SYN:no-preferred", "names": ["a", "b"], "types": ["Gene"]},
    # Whitespace-only preferred_name: also skipped.
    {"curie": "SYN:blank-preferred", "names": ["a", "b"], "types": ["Gene"], "preferred_name": "   "},
    # Exactly MAX_SYNONYM_PAIRS pairs (11 names = 55 pairs > 50, 10 names = 45 pairs <= 50).
    {"curie": "SYN:ten", "names": [f"n{i}" for i in range(10)], "types": ["Gene"], "preferred_name": "Ten"},
    {"curie": "SYN:eleven", "names": [f"n{i}" for i in range(11)], "types": ["Gene"], "preferred_name": "Eleven"},
]


def write_gz(path, lines):
    with gzip.open(path, "wt", encoding="utf-8") as f:
        f.writelines(lines)
    return str(path)


def read_rows(gz_path):
    """Parse a SapBERT file into {curie: (type, preferred, {frozenset pairs...}, row count)}."""
    by_curie = {}
    with gzip.open(gz_path, "rt", encoding="utf-8") as f:
        for line in f:
            fields = line.rstrip("\n").split("||")
            assert len(fields) == 5, f"not a 5-field SapBERT row: {line!r}"
            biolink_type, curie, preferred, a, b = fields
            entry = by_curie.setdefault(curie, {"type": biolink_type, "preferred": preferred, "pairs": [], "rows": 0})
            assert entry["type"] == biolink_type and entry["preferred"] == preferred
            entry["pairs"].append((a, b))
            entry["rows"] += 1
    return by_curie


def expected_pairs(record):
    """The set of pairs the Python *could* emit for a record, from its own rules."""
    names = [n.lower() for n in record["names"]]
    names = list({__import__("re").sub(r"\|\|+", "|", n) for n in names})
    preferred_lower = record["preferred_name"].strip().lower()
    if len(record["names"]) == 0:
        return {frozenset([preferred_lower])}, 1
    if len(record["names"]) == 1:
        return {frozenset([preferred_lower, names[0]])}, 1
    pairs = {frozenset(p) for p in itertools.combinations(names, 2)}
    return pairs, min(len(pairs), MAX_SYNONYM_PAIRS)


def assert_equivalent(records, python_rows, rust_rows, python_counts, rust_counts):
    assert python_counts == rust_counts
    assert set(python_rows) == set(rust_rows)
    for record in records:
        curie = record["curie"]
        if not record.get("preferred_name", "").strip():
            assert curie not in python_rows and curie not in rust_rows
            continue
        py, rs = python_rows.get(curie), rust_rows.get(curie)
        allowed, n_rows = expected_pairs(record)
        if n_rows == 0 or (py is None and rs is None):
            # The two-names-collapse-to-one quirk: neither writes anything.
            assert py is None and rs is None, f"{curie}: expected no rows"
            continue
        assert py["type"] == rs["type"] and py["preferred"] == rs["preferred"], curie
        assert py["rows"] == rs["rows"] == n_rows, curie
        py_pairs = {frozenset(p) for p in py["pairs"]}
        rs_pairs = {frozenset(p) for p in rs["pairs"]}
        if n_rows < MAX_SYNONYM_PAIRS or len(allowed) <= MAX_SYNONYM_PAIRS:
            assert py_pairs == rs_pairs == allowed, curie
        else:
            assert py_pairs <= allowed and rs_pairs <= allowed, curie
            assert len(py_pairs) == len(rs_pairs) == MAX_SYNONYM_PAIRS, curie


def run_both(tmp_path, input_gz, records):
    if src.accel.accel is None:
        pytest.skip("Rust extension not built (run `uv sync`); nothing to diff against")
    python_out = write_gz(tmp_path / "python.gz", [])
    rust_out = str(tmp_path / "rust.gz")
    python_counts = _convert_synonyms_to_sapbert_python(input_gz, python_out)
    rust_counts = src.accel.accel.convert_synonyms_to_sapbert(input_gz, rust_out)
    assert_equivalent(records, read_rows(python_out), read_rows(rust_out), python_counts, rust_counts)
    return rust_out


# THE RUST MATCHES THE PYTHON


@pytest.mark.unit
def test_rust_matches_python_on_real_records(tmp_path):
    """The five verbatim Cell.txt.gz records should convert identically (modulo sampling order)."""
    with open(VERBATIM_FIXTURE, encoding="utf-8") as f:
        lines = f.readlines()
    records = [json.loads(line) for line in lines]
    assert len(records) == 5
    run_both(tmp_path, write_gz(tmp_path / "in.gz", lines), records)


@pytest.mark.unit
def test_rust_matches_python_on_edge_cases(tmp_path):
    """Synthetic records for the branches the real files don't exercise should also match."""
    lines = [json.dumps(r) + "\n" for r in SYNTHETIC_RECORDS]
    run_both(tmp_path, write_gz(tmp_path / "in.gz", lines), SYNTHETIC_RECORDS)


@pytest.mark.unit
def test_counts_include_skipped_entries(tmp_path):
    """Both implementations should count skipped entries and rows the same way."""
    lines = [json.dumps(r) + "\n" for r in SYNTHETIC_RECORDS]
    input_gz = write_gz(tmp_path / "in.gz", lines)
    entries, rows, skipped = _convert_synonyms_to_sapbert_python(input_gz, str(tmp_path / "py.gz"))
    assert (entries, skipped) == (len(SYNTHETIC_RECORDS), 2)
    assert rows == 1 + 1 + 3 + 45 + 50  # no-names, no-types, pipes, ten, eleven
    if src.accel.accel is not None:
        assert src.accel.accel.convert_synonyms_to_sapbert(input_gz, str(tmp_path / "rs.gz")) == (
            entries,
            rows,
            skipped,
        )


@pytest.mark.unit
def test_two_names_that_collapse_after_lowercasing_write_no_rows(tmp_path):
    """Pins current behaviour: CL:0002252 has two names equal after lower(), so `set()` leaves one
    name, `combinations` yields nothing, and the record silently produces no training rows.

    That is arguably a bug (the record has a usable preferred name), but both implementations do
    it and the Rust mirrors the Python deliberately. If it is ever fixed, invert this assertion in
    both implementations at once.
    """
    with open(VERBATIM_FIXTURE, encoding="utf-8") as f:
        line = next(line for line in f if '"curie": "CL:0002252"' in line)
    out = write_gz(tmp_path / "py.gz", [])
    assert _convert_synonyms_to_sapbert_python(write_gz(tmp_path / "in.gz", [line]), out) == (1, 0, 0)
    assert read_rows(out) == {}


@pytest.mark.unit
def test_rust_output_is_reproducible(tmp_path):
    """Unlike the Python, the Rust seeds its sampling per CURIE, so two runs are byte-identical."""
    if src.accel.accel is None:
        pytest.skip("Rust extension not built")
    with open(VERBATIM_FIXTURE, encoding="utf-8") as f:
        input_gz = write_gz(tmp_path / "in.gz", f.readlines())
    outs = []
    for i in range(2):
        out = str(tmp_path / f"rust{i}.gz")
        src.accel.accel.convert_synonyms_to_sapbert(input_gz, out)
        with gzip.open(out, "rb") as f:
            outs.append(f.read())
    assert outs[0] == outs[1]


@pytest.mark.unit
def test_dispatch_uses_rust_when_built(tmp_path, caplog):
    """The public function should run the Rust path when the extension is present and return counts."""
    if src.accel.accel is None:
        pytest.skip("Rust extension not built")
    with open(VERBATIM_FIXTURE, encoding="utf-8") as f:
        input_gz = write_gz(tmp_path / "in.gz", f.readlines())
    counts = convert_synonyms_to_sapbert(input_gz, str(tmp_path / "out" / "sapbert.gz"))
    assert counts[0] == 5 and counts[2] == 0
    assert os.path.exists(tmp_path / "out" / "sapbert.gz")


# A/B OVER A LOCAL BUILD


@pytest.mark.pipeline
def test_rust_matches_python_on_local_synonyms_outputs(tmp_path, capsys):
    """Diff and time both implementations over every gzipped synonyms file of a local build.

    Requires babel_outputs/synonyms/*.gz (e.g. from `snakemake anatomy`). Prints the wall times so
    a run doubles as the A/B measurement rust/README.md asks for before a port replaces its Python.
    """
    import glob

    files = sorted(glob.glob("babel_outputs/synonyms/*.gz"))
    if not files:
        pytest.skip("no babel_outputs/synonyms/*.gz in this checkout")
    if src.accel.accel is None:
        pytest.skip("Rust extension not built")
    for path in files:
        with gzip.open(path, "rt", encoding="utf-8") as f:
            records = [json.loads(line) for line in f]
        py_out, rs_out = str(tmp_path / "py.gz"), str(tmp_path / "rs.gz")
        t = time.time()
        py_counts = _convert_synonyms_to_sapbert_python(path, py_out)
        t_py = time.time() - t
        t = time.time()
        rs_counts = src.accel.accel.convert_synonyms_to_sapbert(path, rs_out)
        t_rs = time.time() - t
        assert_equivalent(records, read_rows(py_out), read_rows(rs_out), py_counts, rs_counts)
        with capsys.disabled():
            print(f"\n{path}: {len(records)} entries; python {t_py:.1f}s, rust {t_rs:.1f}s ({t_py / t_rs:.1f}x)")
