#!/usr/bin/env python3
"""Measure parse_pubmed_into_tsvs against a git revision of itself, over real PubMed files.

`generate_pubmed_concords` is the most expensive rule in Babel (71,947 s wall, 99.7% CPU-bound,
31 GB RSS in the babel-1.18 benchmark TSVs), so a change to its parser is worth measuring rather
than assuming. This runs the working tree's implementation and a git revision's implementation over
the same input files in separate processes, reports wall time and peak RSS for each, and diffs
every output file — the parser must stay byte-identical.

    # Fetch a couple of real baseline files (~20 MB each) somewhere outside the repo tree:
    mkdir -p data/scratch/pubmed && cd data/scratch/pubmed
    curl -O https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/pubmed26n0001.xml.gz
    curl -O https://ftp.ncbi.nlm.nih.gov/pubmed/baseline/pubmed26n0002.xml.gz

    # Then, from the repo root:
    PYTHONPATH=. uv run python docs/sources/PubMed/bench_parse.py data/scratch/pubmed origin/main

See README.md in this directory for the numbers this produced.
"""

import argparse
import gzip
import importlib.util
import json
import resource
import subprocess
import sys
import time
from pathlib import Path

REPO = Path(__file__).resolve().parents[3]
OUTPUT_NAMES = ("titles.tsv", "PMID", "PMID_DOI", "statuses.jsonl.gz")


def load_revision(revision):
    """Import a git revision's publications.py under a private module name."""
    source = subprocess.run(
        ["git", "-C", str(REPO), "show", f"{revision}:src/createcompendia/publications.py"],
        capture_output=True,
        text=True,
        check=True,
    ).stdout
    path = Path(sys.argv[0]).parent / "_revision_publications.py"
    path.write_text(source)
    try:
        spec = importlib.util.spec_from_file_location("_revision_publications", path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        path.unlink(missing_ok=True)


def run_one(pubmed_dir, outdir, revision):
    """Parse `pubmed_dir` into `outdir`, returning (seconds, peak_rss_gb)."""
    outdir.mkdir(parents=True, exist_ok=True)
    empty_updatefiles = outdir / "empty_updatefiles"
    empty_updatefiles.mkdir(exist_ok=True)

    if revision:
        module = load_revision(revision)
    else:
        import src.createcompendia.publications as module

    start = time.perf_counter()
    module.parse_pubmed_into_tsvs(
        str(pubmed_dir),
        str(empty_updatefiles),
        str(outdir / "titles.tsv"),
        str(outdir / "statuses.jsonl.gz"),
        str(outdir / "PMID"),
        str(outdir / "PMID_DOI"),
        str(outdir / "metadata.yaml"),
    )
    elapsed = time.perf_counter() - start

    # ru_maxrss is bytes on macOS and kilobytes on Linux.
    maxrss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    peak_gb = maxrss / 1024**3 if sys.platform == "darwin" else maxrss / 1024**2
    return elapsed, peak_gb


def read_output(path):
    """Read an output file, decompressing it if it is gzipped, so comparisons ignore gzip framing."""
    if path.suffix == ".gz":
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            return handle.read()
    return path.read_text()


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("pubmed_dir", type=Path, help="directory of .xml.gz PubMed files to parse")
    parser.add_argument("baseline_revision", help="git revision to compare the working tree against")
    parser.add_argument("--outdir", type=Path, default=Path("data/scratch/bench_pubmed"))
    parser.add_argument(
        "--variant",
        choices=("baseline", "working"),
        help="internal: parse with one implementation and report. Omit to run both as subprocesses.",
    )
    args = parser.parse_args()

    if args.variant:
        revision = args.baseline_revision if args.variant == "baseline" else None
        seconds, peak_gb = run_one(args.pubmed_dir, args.outdir / args.variant, revision)
        print(json.dumps({"seconds": seconds, "peak_rss_gb": peak_gb}))
        return

    # Each variant gets its own process so peak RSS is that variant's alone.
    results = {}
    for variant in ("baseline", "working"):
        completed = subprocess.run(
            [
                sys.executable,
                sys.argv[0],
                str(args.pubmed_dir),
                args.baseline_revision,
                "--outdir",
                str(args.outdir),
                "--variant",
                variant,
            ],
            capture_output=True,
            text=True,
            check=True,
            cwd=REPO,
        )
        results[variant] = json.loads(completed.stdout.strip().splitlines()[-1])

    baseline, working = results["baseline"], results["working"]
    print(f"{args.baseline_revision:>24}: {baseline['seconds']:6.1f} s   {baseline['peak_rss_gb']:5.2f} GB peak RSS")
    print(f"{'working tree':>24}: {working['seconds']:6.1f} s   {working['peak_rss_gb']:5.2f} GB peak RSS")
    print(
        f"{'speedup':>24}: {baseline['seconds'] / working['seconds']:6.2f}x   "
        f"{baseline['peak_rss_gb'] / working['peak_rss_gb']:5.2f}x less memory"
    )

    mismatches = [
        name
        for name in OUTPUT_NAMES
        if read_output(args.outdir / "baseline" / name) != read_output(args.outdir / "working" / name)
    ]
    if mismatches:
        raise SystemExit(f"OUTPUT DIFFERS in {', '.join(mismatches)} — the change is not behaviour-preserving.")
    print(f"{'output':>24}: identical in {', '.join(OUTPUT_NAMES)}")


if __name__ == "__main__":
    main()
