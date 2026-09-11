//! SapBERT training-data export: the Rust implementation of
//! `src/exporters/sapbert.py::convert_synonyms_to_sapbert`.
//!
//! Reads a gzipped synonyms JSONL file (one `{"curie", "preferred_name", "names", "types", ...}`
//! object per line) and writes the pipe-delimited training file that
//! <https://github.com/RENCI-NER/sapbert> consumes:
//!
//! ```text
//! biolink:Gene||NCBIGene:10554||AGPAT1||1-acylglycerol-3-phosphate o-acyltransferase 1||lysophosphatidic acid acyltransferase, alpha
//! ```
//!
//! i.e. `biolink-type||preferred ID||preferred label||synonym 1||synonym 2`, with at most
//! [`MAX_SYNONYM_PAIRS`] rows per preferred ID.
//!
//! The Python it mirrors is the specification, and `tests/exporters/test_sapbert.py` diffs the two.
//! Two behaviours are deliberately different, both marked `ponytail:` below: the pair sampling is
//! seeded per CURIE so a rerun produces the same bytes (Python's `random.sample` is unseeded and
//! `set(names)` order varies with `PYTHONHASHSEED`), and gzip output uses flate2's default level
//! rather than zlib level 9, so the `.gz` bytes differ and the file is slightly larger.

use std::collections::HashSet;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};

use flate2::read::MultiGzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use rand::rngs::StdRng;
use rand::seq::IteratorRandom;
use rand::SeedableRng;
use serde::Deserialize;

/// Include up to this many synonym pairs for each preferred ID. Mirrors `MAX_SYNONYM_PAIRS` in
/// `src/exporters/sapbert.py`; SapBERT training saturates well before this.
const MAX_SYNONYM_PAIRS: usize = 50;

/// Lowercase every name before pairing. Mirrors `LOWERCASE_ALL_NAMES` in the Python; the model
/// is trained case-insensitively.
const LOWERCASE_ALL_NAMES: bool = true;

/// The fields of a synonyms record this export reads. Everything else on the line is ignored.
/// `names` and `types` are required, as they are in the Python (`entry["names"]`), so a malformed
/// file fails rather than silently producing an empty export.
#[derive(Deserialize)]
struct Entry {
    curie: String,
    #[serde(default)]
    preferred_name: Option<String>,
    names: Vec<String>,
    types: Vec<String>,
}

/// Convert a gzipped synonyms JSONL file into a gzipped SapBERT training file.
///
/// Returns `(entries_read, training_rows_written, entries_skipped)`, where skipped entries are
/// those with no `preferred_name`. Releases the GIL for the whole conversion.
#[pyfunction]
pub fn convert_synonyms_to_sapbert(
    py: Python<'_>,
    synonyms_gz_path: &str,
    sapbert_gz_path: &str,
) -> PyResult<(u64, u64, u64)> {
    py.allow_threads(|| convert(synonyms_gz_path, sapbert_gz_path))
}

fn convert(synonyms_gz_path: &str, sapbert_gz_path: &str) -> PyResult<(u64, u64, u64)> {
    // MultiGzDecoder, not GzDecoder: Python's gzip module reads concatenated gzip members, and
    // pigz / `cat a.gz b.gz` produce them.
    let input = BufReader::new(MultiGzDecoder::new(BufReader::new(File::open(
        synonyms_gz_path,
    )?)));
    // ponytail: flate2's default level (6), not Python's zlib default (9). ~60% of the Python
    // rule's time was zlib at level 9; the files come out a few percent larger. Raise to
    // Compression::best() if the consumer cares about size more than build time.
    let mut output = BufWriter::new(GzEncoder::new(
        BufWriter::new(File::create(sapbert_gz_path)?),
        Compression::default(),
    ));

    let mut count_entries: u64 = 0;
    let mut count_rows: u64 = 0;
    let mut count_skipped: u64 = 0;

    for (index, line) in input.lines().enumerate() {
        let line = line?;
        count_entries += 1;
        let entry: Entry = serde_json::from_str(&line).map_err(|e| {
            PyValueError::new_err(format!(
                "{synonyms_gz_path} line {}: not a synonyms record: {e}",
                index + 1
            ))
        })?;
        if preferred_name(&entry).is_none() {
            count_skipped += 1;
            continue;
        }
        count_rows += write_rows(&entry, &mut output)? as u64;
    }

    output.flush()?;
    output
        .into_inner()
        .map_err(std::io::Error::other)?
        .finish()?
        .flush()?;
    Ok((count_entries, count_rows, count_skipped))
}

/// The stripped preferred name, or None when the record has none -- the Python skips such
/// records with a warning.
fn preferred_name(entry: &Entry) -> Option<&str> {
    let name = entry.preferred_name.as_deref()?.trim();
    if name.is_empty() {
        None
    } else {
        Some(name)
    }
}

/// Write every training row for one record and return how many were written.
fn write_rows(entry: &Entry, out: &mut impl Write) -> PyResult<usize> {
    let Some(preferred) = preferred_name(entry) else {
        return Ok(0);
    };
    let names: Vec<String> = entry.names.iter().map(|name| clean_name(name)).collect();
    let biolink_type = entry.types.first().map_or("NamedThing", String::as_str);
    let prefix = format!("biolink:{biolink_type}||{}||{preferred}", entry.curie);
    let preferred_lower = preferred.to_lowercase();

    // The branching follows the Python exactly, including its quirk that a record whose two or
    // more names collapse to a single distinct name yields no rows at all.
    match names.len() {
        0 => {
            writeln!(out, "{prefix}||{preferred_lower}||{preferred_lower}")?;
            Ok(1)
        }
        1 => {
            writeln!(out, "{prefix}||{preferred_lower}||{}", names[0])?;
            Ok(1)
        }
        _ => {
            let distinct = dedup(&names);
            let mut written = 0;
            for (a, b) in choose_pairs(&distinct, &entry.curie) {
                writeln!(out, "{prefix}||{a}||{b}")?;
                written += 1;
            }
            Ok(written)
        }
    }
}

/// Lowercase (when [`LOWERCASE_ALL_NAMES`]) and collapse runs of two or more `|` to a single `|`,
/// since `||` is the row delimiter. A lone `|` is left alone, as in the Python's `re.sub(r"\|\|+", "|")`.
fn clean_name(name: &str) -> String {
    let lowered;
    let name = if LOWERCASE_ALL_NAMES {
        lowered = name.to_lowercase();
        lowered.as_str()
    } else {
        name
    };
    let mut cleaned = String::with_capacity(name.len());
    let mut pipes = 0;
    for c in name.chars() {
        if c == '|' {
            pipes += 1;
            continue;
        }
        if pipes > 0 {
            cleaned.push('|');
            pipes = 0;
        }
        cleaned.push(c);
    }
    if pipes > 0 {
        cleaned.push('|');
    }
    cleaned
}

/// Python's `set(names)`, keeping first-occurrence order so the pair enumeration is stable.
fn dedup(names: &[String]) -> Vec<&str> {
    let mut seen = HashSet::with_capacity(names.len());
    names
        .iter()
        .map(String::as_str)
        .filter(|name| seen.insert(*name))
        .collect()
}

/// All unordered pairs of `names`, or [`MAX_SYNONYM_PAIRS`] of them chosen at random when there
/// are more.
///
/// ponytail: the RNG is seeded from the CURIE so that a rerun reproduces the file byte for byte.
/// The Python uses an unseeded `random.sample`, so its output differs on every run; nothing
/// downstream depends on which pairs are picked, only on how many.
fn choose_pairs<'a>(names: &[&'a str], curie: &str) -> Vec<(&'a str, &'a str)> {
    let n = names.len();
    let pairs = (0..n).flat_map(|i| ((i + 1)..n).map(move |j| (i, j)));
    let total = n * (n.saturating_sub(1)) / 2;
    let chosen: Vec<(usize, usize)> = if total > MAX_SYNONYM_PAIRS {
        let mut rng = StdRng::seed_from_u64(fnv1a(curie));
        pairs.choose_multiple(&mut rng, MAX_SYNONYM_PAIRS)
    } else {
        pairs.collect()
    };
    chosen
        .into_iter()
        .map(|(i, j)| (names[i], names[j]))
        .collect()
}

/// FNV-1a: a small, stable hash. `std`'s `DefaultHasher` is explicitly not stable across Rust
/// releases, and the whole point of seeding is that two builds pick the same pairs.
fn fnv1a(text: &str) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in text.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clean_name_lowercases_and_collapses_pipe_runs() {
        assert_eq!(
            clean_name("Alpha||Beta|||Gamma|Delta"),
            "alpha|beta|gamma|delta"
        );
        assert_eq!(
            clean_name("||leading and trailing||"),
            "|leading and trailing|"
        );
        assert_eq!(clean_name("Ménière"), "ménière");
    }

    #[test]
    fn pair_count_follows_the_python() {
        let names = |n: usize| -> Vec<String> { (0..n).map(|i| format!("name{i}")).collect() };
        let count = |n: usize| {
            let owned = names(n);
            let distinct = dedup(&owned);
            choose_pairs(&distinct, "X:1").len()
        };
        assert_eq!(count(2), 1);
        assert_eq!(count(10), 45); // C(10,2) <= MAX_SYNONYM_PAIRS: every pair
        assert_eq!(count(11), MAX_SYNONYM_PAIRS); // C(11,2) = 55: sampled down
        assert_eq!(count(217), MAX_SYNONYM_PAIRS);
    }

    #[test]
    fn sampling_is_deterministic_per_curie() {
        let owned: Vec<String> = (0..20).map(|i| i.to_string()).collect();
        let distinct = dedup(&owned);
        assert_eq!(
            choose_pairs(&distinct, "A:1"),
            choose_pairs(&distinct, "A:1")
        );
        assert_ne!(
            choose_pairs(&distinct, "A:1"),
            choose_pairs(&distinct, "A:2")
        );
    }

    #[test]
    fn dedup_keeps_first_occurrence_order() {
        let owned: Vec<String> = ["b", "a", "b", "c", "a"]
            .iter()
            .map(|s| s.to_string())
            .collect();
        assert_eq!(dedup(&owned), vec!["b", "a", "c"]);
    }
}
