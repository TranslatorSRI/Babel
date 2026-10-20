"""Fetch the inputs the MeSH/UMLS ownership replays need from a published Babel build.

``replay_mesh_ownership.py`` (beside this script) and
``docs/sources/MESH/cross-listed/scripts/list_cross_listed_descriptors.py`` read a build's ``ids/MESH`` and
``ids/UMLS`` files and its duplicate reports. A published build has no ``intermediate/`` tree, but its
``duckdb/Identifier.parquet`` holds every ids file row with the file's path, so this exports the seven
pipelines' MESH and UMLS ids files from it (one filtered read over https, under a minute) and downloads
``reports/duckdb/duplicate_clique_leaders.tsv`` and ``duplicate_curies.tsv``. It writes:

    <output-dir>/ids/<pipeline>/MESH, <output-dir>/ids/<pipeline>/UMLS   (CURIE<tab>biolink type)
    <output-dir>/duplicate_clique_leaders.tsv, <output-dir>/duplicate_curies.tsv

The replays also need UMLS and MeSH downloaded locally (``babel_downloads/UMLS/MRCONSO.RRF`` and ``MRSTY.RRF``,
``babel_downloads/MESH/mesh.nt``): ``uv run snakemake --cores 1 download_umls`` (needs ``UMLS_API_KEY``) and
``uv run snakemake --cores 1 <download_directory>/MESH/mesh.nt``. Example:

    uv run python docs/sources/UMLS/cui-follows-mesh/scripts/fetch_build_inputs.py \\
        --build-url https://stars.renci.org/var/babel/<build> --output-dir data/duplicate-curies
"""

import argparse
import os
import sys
import urllib.request

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "..")))

from src.util import get_config  # noqa: E402

# The pipelines whose ids/UMLS the replays read: the MeSH-owning ones plus the two that only drop.
DROP_ONLY_PIPELINES = ["gene", "process"]


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--build-url", required=True, help="A published build's root, e.g. .../var/babel/<build>")
    ap.add_argument("--output-dir", required=True)
    args = ap.parse_args()
    base = args.build_url.rstrip("/")

    import duckdb

    pipelines = get_config()["umls_mesh_owning_pipelines"] + DROP_ONLY_PIPELINES
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    pattern = "/intermediate/(" + "|".join(pipelines) + ")/ids/(MESH|UMLS)$"
    rows = con.execute(
        f"""SELECT regexp_extract(filename, '{pattern}', 1) AS pipeline,
                   regexp_extract(filename, '{pattern}', 2) AS vocab, curie, biolink_type
            FROM read_parquet('{base}/duckdb/Identifier.parquet')
            WHERE regexp_matches(filename, '{pattern}')
            ORDER BY pipeline, vocab, curie"""
    ).fetchall()
    written = {}
    handles = {}
    try:
        for pipeline, vocab, curie, biolink_type in rows:
            key = (pipeline, vocab)
            if key not in handles:
                path = os.path.join(args.output_dir, "ids", pipeline, vocab)
                os.makedirs(os.path.dirname(path), exist_ok=True)
                handles[key] = open(path, "w")
                written[key] = 0
            handles[key].write(f"{curie}\t{biolink_type or ''}\n")
            written[key] += 1
    finally:
        for handle in handles.values():
            handle.close()
    missing = [f"{p}/{v}" for p in pipelines for v in ("MESH", "UMLS") if (p, v) not in written]
    # Gene and process write no ids/MESH; anything else missing means the build or the pattern changed.
    unexpected = [m for m in missing if m not in {f"{p}/MESH" for p in DROP_ONLY_PIPELINES}]
    if unexpected:
        raise RuntimeError(f"{base}/duckdb/Identifier.parquet has no rows for {unexpected}")
    for (pipeline, vocab), n in sorted(written.items()):
        print(f"ids/{pipeline}/{vocab}: {n:,} rows")  # noqa: T201

    for report in ("duplicate_clique_leaders.tsv", "duplicate_curies.tsv"):
        urllib.request.urlretrieve(f"{base}/reports/duckdb/{report}", os.path.join(args.output_dir, report))
        print(f"{report}: downloaded")  # noqa: T201


if __name__ == "__main__":
    main()
