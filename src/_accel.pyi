"""Type stub for the compiled Rust extension built from rust/src/lib.rs.

Hand-written and hand-maintained: there is no mypy in this repo, so nothing enforces that this
matches the Rust. It exists so a reader who does not read Rust can see what the module offers.
Import through src/accel.py rather than importing this module directly.
"""

# Bumped in lockstep with ABI_VERSION in rust/src/lib.rs; see src/accel.py.
ABI_VERSION: int

def convert_synonyms_to_sapbert(synonyms_gz_path: str, sapbert_gz_path: str) -> tuple[int, int, int]:
    """Convert a gzipped synonyms JSONL file into a gzipped SapBERT training file.

    Returns (entries_read, training_rows_written, entries_skipped_for_no_preferred_name). The
    Python reference is src/exporters/sapbert.py; see rust/src/sapbert.rs for the two deliberate
    differences (seeded pair sampling, gzip level).
    """
