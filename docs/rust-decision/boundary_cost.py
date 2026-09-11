#!/usr/bin/env python3
"""Measure what an in-process Rust boundary could possibly save over a file boundary.

THE QUESTION. Babel's expensive rules are CPU-bound single-threaded Python. If some are
reimplemented in Rust, the Rust can either live in-process (a pyo3 extension, PR #975) or in a
separate process that hands data over through a file (a Snakemake `shell:` stage). The second is
cheaper on almost every axis -- no toolchain for people who don't write Rust, its own `benchmark:`
TSV, its own SLURM memory reservation, `diff` as the differential test, revertible by editing one
rule. The one axis where in-process could win is the cost of getting the data across.

WHY THIS DOES NOT BENCHMARK pyo3. A pyo3 arm has no single honest shape -- return a list of tuples,
a list of lists, an iterator, a `#[pyclass]` handle? -- so any number is answerable with "you picked
the wrong return type". Instead this measures a FLOOR that no pyo3 implementation can beat:

    F = the cost of constructing the Python objects the consumer needs, from raw bytes.

Any mechanism that ends with Python holding the data pays at least F. So:

    maximum possible saving from going in-process  =  cost(file arm) - F

If that gap is small next to the rule it sits inside, no Rust implementation can rescue it and the
in-process case is closed -- without writing any Rust, and without handicapping pyo3, which is here
credited with a serialization cost of exactly zero.

WHAT IT CANNOT DECIDE. The one thing pyo3 can do that a file cannot is never materialise into
Python at all: Rust keeps ownership and Python iterates lazily through a `#[pyclass]`. That is a
real capability, but its out-of-process equivalent is "port the consumer too", so it is a question
about how far a rewrite goes, not about which mechanism is faster. This harness deliberately does
not speak to it.

PRE-REGISTERED INTERPRETATION -- written before the first run, so the result cannot be read
backwards into whichever conclusion is convenient:

  * If the file arms land within ~20% of each other AND close to F, the finding is "the mechanism
    is irrelevant given this consumer" -- NOT "pyo3 is worthless". The decision then falls to the
    non-performance criteria in docs/Rust.md.
  * If a file arm is several times F, in-process has a real prize and the pyo3 case is open.
  * If the arms disagree with each other by a lot, the finding is about serialization format
    choice, which is worth acting on independently of Rust.

Usage:
    PYTHONPATH=. uv run python docs/rust-decision/boundary_cost.py --scale 1 --scale 10

Requires a local build's compendia under babel_outputs/compendia/*.txt. Peak RSS is per arm,
measured in a fresh subprocess, so arms cannot inflate each other.
"""

import argparse
import ast
import hashlib
import json
import os
import resource
import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]
COMPENDIA = REPO / "babel_outputs/compendia"
SCRATCH = REPO / "data/scratch/boundary_cost"

# Arms. Every one ends with Python holding `set[frozenset[str]]`, which is what all 14 of glom's
# consumers build (`set(frozenset(x) for x in dicts.values())`).
ARMS = ("floor", "repr_set", "jsonl", "parquet")


def fingerprint(cliques):
    """A stable, order-independent digest of a clique set, comparable across processes."""
    digest = hashlib.sha256()
    for clique in sorted("\t".join(sorted(c)) for c in cliques):
        digest.update(clique.encode())
        digest.update(b"\n")
    return digest.hexdigest()[:16]


def peak_rss_gb():
    """Peak RSS of this process. ru_maxrss is bytes on macOS, kilobytes on Linux."""
    maxrss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return maxrss / 1024**3 if sys.platform == "darwin" else maxrss / 1024**2


def load_cliques(scale):
    """Read cliques out of a finished build's compendia, replicated `scale` times.

    Each compendium line is one clique, and its `identifiers[].i` values are its members -- so the
    finished compendia already hold exactly the clique state glom produces, with no pipeline run
    needed. Replication suffixes every CURIE with a replica index, which multiplies cardinality
    while preserving the string-length and clique-size distributions that drive the costs here.

    Returned as lists of `bytes`, not `str`, deliberately: see the note in `arm_floor`.
    """
    base = []
    for path in sorted(COMPENDIA.glob("*.txt")):
        with open(path) as handle:
            for line in handle:
                base.append([identifier["i"] for identifier in json.loads(line)["identifiers"]])
    # Every scale suffixes, including 1x: otherwise 1x holds shorter, more interning-prone strings
    # than 10x and the two points are not comparable, which would make the scaling look superlinear
    # for reasons that have nothing to do with the boundary.
    return [[f"{curie}~{replica}".encode() for curie in clique] for replica in range(scale) for clique in base]


# ARMS


def arm_floor(cliques, _path):
    """The floor: no serialization at all, just build the Python objects from bytes.

    `.decode()` on each member is what makes this honest. Timing `[frozenset(c) for c in cliques]`
    over data that already holds `str` objects would only bump refcounts -- it would measure
    almost nothing, make every file arm look catastrophic by comparison, and "prove" that pyo3
    wins by a mile. Fresh string construction is the term both arms genuinely share, so it has to
    be inside the floor.
    """
    return 0.0, lambda: {frozenset(member.decode() for member in clique) for clique in cliques}


def arm_repr_set(cliques, path):
    """The status quo boundary: `f"{set(s)}\\n"` read back with `ast.literal_eval`.

    Not a straw man -- this is what `chemicals.py:1110-1113` writes and `:1165-1168` reads back
    between `untyped_chemical_compendia` and `chemical_compendia` today, i.e. a Python `repr` used
    as a wire format for the largest clique state in Babel.
    """

    def produce():
        with open(path, "w") as out:
            for clique in cliques:
                out.write(f"{ {member.decode() for member in clique} }\n")

    def consume():
        with open(path) as handle:
            return {frozenset(ast.literal_eval(line)) for line in handle}

    return produce, consume


def arm_jsonl(cliques, path):
    """JSONL: one JSON array of members per line. The format a new boundary should use."""

    def produce():
        with open(path, "w") as out:
            for clique in cliques:
                out.write(json.dumps([member.decode() for member in clique]) + "\n")

    def consume():
        with open(path) as handle:
            return {frozenset(json.loads(line)) for line in handle}

    return produce, consume


def arm_parquet(cliques, path):
    """Parquet via DuckDB: already a dependency, and already how Babel writes its exports.

    Stored long -- one row per (clique_index, member) -- because that is the natural columnar
    shape and avoids depending on DuckDB's LIST round-tripping.
    """
    import duckdb

    def produce():
        tsv = path.with_suffix(".staging.tsv")
        with open(tsv, "w") as out:
            for index, clique in enumerate(cliques):
                for member in clique:
                    out.write(f"{index}\t{member.decode()}\n")
        con = duckdb.connect()
        # Paths are interpolated, not bound: DuckDB does not accept a parameter as a COPY target.
        # Both are harness-controlled paths under data/scratch, not user input.
        con.execute(
            f"COPY (SELECT * FROM read_csv('{tsv}', delim='\t', header=false, "
            "columns={'clique': 'INTEGER', 'curie': 'VARCHAR'})) "
            f"TO '{path}' (FORMAT PARQUET)"
        )
        con.close()
        tsv.unlink()

    def consume():
        con = duckdb.connect()
        rows = con.execute("SELECT clique, curie FROM read_parquet(?) ORDER BY clique", [str(path)]).fetchall()
        con.close()
        grouped = {}
        for clique_index, curie in rows:
            grouped.setdefault(clique_index, []).append(curie)
        return {frozenset(members) for members in grouped.values()}

    return produce, consume


ARM_FUNCTIONS = {"floor": arm_floor, "repr_set": arm_repr_set, "jsonl": arm_jsonl, "parquet": arm_parquet}


def run_arm(arm, scale):
    """Run one arm in this process and print a JSON result line."""
    SCRATCH.mkdir(parents=True, exist_ok=True)
    path = SCRATCH / f"{arm}_{scale}x.data"

    cliques = load_cliques(scale)
    members = sum(len(clique) for clique in cliques)

    produce, consume = ARM_FUNCTIONS[arm](cliques, path)

    if callable(produce):
        start = time.perf_counter()
        produce()
        # fsync so the write cost is not deferred past the timer.
        with open(path, "rb") as handle:
            os.fsync(handle.fileno())
        produce_seconds = time.perf_counter() - start
        file_mb = path.stat().st_size / 1024**2
    else:
        produce_seconds, file_mb = 0.0, 0.0

    start = time.perf_counter()
    result = consume()
    consume_seconds = time.perf_counter() - start

    print(
        json.dumps(
            {
                "arm": arm,
                "scale": scale,
                "cliques": len(cliques),
                "members": members,
                "produce_s": produce_seconds,
                "consume_s": consume_seconds,
                "file_mb": file_mb,
                "peak_rss_gb": peak_rss_gb(),
                # Order-independent fingerprint so the parent can assert every arm agreed without
                # shipping the whole clique set between processes. hashlib, not hash(): Python
                # salts str hashing per process, so hash() would differ for identical content and
                # every comparison would spuriously fail.
                "fingerprint": fingerprint(result),
            }
        )
    )
    if path.exists():
        path.unlink()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--scale", type=int, action="append", help="replication factor; repeatable")
    parser.add_argument("--arm", choices=ARMS, help="internal: run a single arm and print JSON")
    parser.add_argument("--only", choices=ARMS, action="append", help="restrict to these arms")
    args = parser.parse_args()

    if args.arm:
        run_arm(args.arm, args.scale[0])
        return

    scales = args.scale or [1]
    arms = args.only or list(ARMS)

    for scale in scales:
        results = {}
        for arm in arms:
            completed = subprocess.run(
                [sys.executable, __file__, "--arm", arm, "--scale", str(scale)],
                capture_output=True,
                text=True,
                cwd=REPO,
            )
            if completed.returncode != 0:
                print(f"  {arm}: FAILED\n{completed.stderr[-2000:]}", file=sys.stderr)
                continue
            results[arm] = json.loads(completed.stdout.strip().splitlines()[-1])

        if not results:
            continue
        first = next(iter(results.values()))
        print(f"\n=== scale {scale}x — {first['cliques']:,} cliques, {first['members']:,} members ===")
        print(f"{'arm':<10} {'produce_s':>10} {'consume_s':>10} {'total_s':>9} {'file_MB':>9} {'peak_GB':>8}")
        for arm in arms:
            r = results.get(arm)
            if not r:
                continue
            total = r["produce_s"] + r["consume_s"]
            print(
                f"{arm:<10} {r['produce_s']:>10.2f} {r['consume_s']:>10.2f} {total:>9.2f} "
                f"{r['file_mb']:>9.1f} {r['peak_rss_gb']:>8.2f}"
            )

        fingerprints = {arm: r["fingerprint"] for arm, r in results.items()}
        if len(set(fingerprints.values())) != 1:
            print(f"  MISMATCH — arms disagree on the clique set: {fingerprints}", file=sys.stderr)
            raise SystemExit("arms produced different results; timings are meaningless")
        print("  all arms produced an identical clique set")

        if "floor" in results:
            floor = results["floor"]["consume_s"]
            print(f"\n  floor F (Python object construction, unavoidable): {floor:.2f} s")
            for arm in arms:
                if arm == "floor" or arm not in results:
                    continue
                total = results[arm]["produce_s"] + results[arm]["consume_s"]
                print(
                    f"  max possible saving from in-process vs {arm:<9}: {total - floor:6.2f} s "
                    f"({(total - floor) / total * 100:4.1f}% of that arm)"
                )


if __name__ == "__main__":
    main()
