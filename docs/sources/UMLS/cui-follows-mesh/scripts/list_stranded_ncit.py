"""List the NCIt IDs that "a UMLS CUI follows its MeSH descriptor" leaves in no compendium.

Only anatomy and disease write an ``ids/NCIT``: an NCIt drug or organism concept reaches Babel only as a partner
in a concord. Protein's ``NCIT_UMLS`` concord and anatomy's and disease's ``UMLS`` concords are built from the
pipeline's own ``ids/UMLS``, so when apply_mesh_ownership() drops a CUI from that file because another pipeline
owns its MeSH descriptor, the concord loses the CUI's NCIt partners too. The owner adds the CUI back with its
own concords, but chemicals and taxon concord UMLS to no NCIt. An NCIt ID whose every concord edge goes this
way, and which no ids file lists, ends up in no compendium.

The script writes the production rule's ownership file (``umls.write_mesh_ownership``) from a build's ids files
and a local UMLS download, reads every concord edge that names an NCIt ID from the build's Concord.parquet, and
lists the NCIt IDs whose every edge is in a concord built from a pipeline's own ``ids/UMLS``
(``replay_mesh_ownership.CONCORDS_FROM_IDS_UMLS``) and names a CUI another pipeline owns. Labels are NCIt's
preferred terms from MRCONSO (``SAB=NCI``, ``TTY=PT``). Like the replay, it reads concords as the build wrote
them, so it is a prediction; confirm it against the next build's ``Edge`` tables.

Inputs are the replay's (see ``replay_mesh_ownership.py`` and ``fetch_build_inputs.py``), plus the build's
Identifier.parquet. Example (about a minute, mostly the MRCONSO pass):

    uv run python docs/sources/UMLS/cui-follows-mesh/scripts/list_stranded_ncit.py \\
        --ids-dir data/duplicate-curies/ids \\
        --concord-parquet <build-url>/duckdb/Concord.parquet \\
        --identifier-parquet <build-url>/duckdb/Identifier.parquet \\
        --build 2026jul22 --output-dir docs/sources/UMLS/cui-follows-mesh
"""

import argparse
import csv
import os
import sys
import tempfile
import textwrap
from collections import Counter, defaultdict

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "..")))

from src.datahandlers import umls  # noqa: E402
from src.util import get_config  # noqa: E402

# Concords built from a pipeline's own ids/UMLS, which apply_mesh_ownership() rewrites; the same set as in
# replay_mesh_ownership.py. Of these, only NCIT_UMLS has NCIt partners.
CONCORDS_FROM_IDS_UMLS = {"UMLS", "NCIT_UMLS"}

# How many NCIt IDs the Markdown report shows; the CSV has all of them.
SAMPLE_SIZE = 10


def write_ownership(ids_dir, umls_dir, outfile):
    """Write the umls_mesh_ownership rule's file from the build's ids files, as the rule does."""
    config = get_config()
    umls.write_mesh_ownership(
        os.path.join(umls_dir, "MRCONSO.RRF"),
        {p: os.path.join(ids_dir, p, "MESH") for p in config["umls_mesh_owning_pipelines"]},
        outfile,
        blocklist_files_by_pipeline={"disease": os.path.join(config["input_directory"], "badumls")},
        non_mesh_umls_ids_files=[os.path.join(ids_dir, p, "UMLS") for p in ("gene", "process")],
    )


def read_ncit_edges(concord_parquet):
    """(pipeline, concord, ncit, partner) for every concord edge with an NCIt side."""
    import duckdb

    con = duckdb.connect()
    if concord_parquet.startswith("http"):
        con.execute("INSTALL httpfs; LOAD httpfs;")
    return con.execute(
        f"""
        SELECT regexp_extract(filename, 'intermediate/([^/]+)/concords/(.+)$', 1),
               regexp_extract(filename, 'intermediate/([^/]+)/concords/(.+)$', 2),
               CASE WHEN subj LIKE 'NCIT:%' THEN subj ELSE obj END,
               CASE WHEN subj LIKE 'NCIT:%' THEN obj ELSE subj END
        FROM read_parquet('{concord_parquet}') WHERE subj LIKE 'NCIT:%' OR obj LIKE 'NCIT:%'
        """
    ).fetchall()


def read_ncit_in_ids_files(identifier_parquet):
    import duckdb

    con = duckdb.connect()
    if identifier_parquet.startswith("http"):
        con.execute("INSTALL httpfs; LOAD httpfs;")
    rows = con.execute(f"SELECT DISTINCT curie FROM read_parquet('{identifier_parquet}') WHERE curie LIKE 'NCIT:%'")
    return {curie for (curie,) in rows.fetchall()}


def read_ncit_labels(mrconso, codes):
    """NCIt code -> preferred term, from MRCONSO's SAB=NCI TTY=PT atoms."""
    labels = {}
    with open(mrconso) as inf:
        for line in inf:
            x = line.split("|")
            if x[11] == "NCI" and x[12] == "PT" and f"NCIT:{x[13]}" in codes:
                labels[f"NCIT:{x[13]}"] = x[14]
    return labels


def find_stranded(edges, owners, in_ids_files):
    """NCIt ID -> sorted [(pipeline, cui, owner)] for every NCIt ID the rule leaves with no concord edge.

    An edge in a concord built from a pipeline's ids/UMLS is lost when that pipeline drops the CUI, unless the
    owner's own concords built from ids/UMLS also carry NCIt partners (anatomy's and disease's UMLS, protein's
    NCIT_UMLS): those are built from MRCONSO for every CUI the owner adds, so they re-create the edge there.
    """
    reconcording = {p for p, concord, _, _ in edges if concord in CONCORDS_FROM_IDS_UMLS}
    lost = defaultdict(list)
    kept = set()
    for pipeline, concord, ncit, partner in edges:
        owner = owners.get(partner, (None,))[0]
        if concord in CONCORDS_FROM_IDS_UMLS and owner not in (None, pipeline) and owner not in reconcording:
            lost[ncit].append((pipeline, partner, owner))
        else:
            kept.add(ncit)
    return {ncit: sorted(set(v)) for ncit, v in lost.items() if ncit not in kept and ncit not in in_ids_files}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--umls-dir", default=os.path.join(get_config()["download_directory"], "UMLS"))
    ap.add_argument("--ids-dir", required=True, help="Directory with <pipeline>/MESH and <pipeline>/UMLS ids files")
    ap.add_argument("--concord-parquet", required=True, help="Path or URL of the build's Concord.parquet")
    ap.add_argument("--identifier-parquet", required=True, help="Path or URL of the build's Identifier.parquet")
    ap.add_argument("--build", default="2026jul22", help="Names the inputs in the report and the output files")
    ap.add_argument("--output-dir", required=True)
    args = ap.parse_args()

    with tempfile.TemporaryDirectory() as tmpdir:
        ownership = os.path.join(tmpdir, "mesh_ownership.tsv")
        write_ownership(args.ids_dir, args.umls_dir, ownership)
        owners = umls.read_mesh_ownership(ownership)

    stranded = find_stranded(
        read_ncit_edges(args.concord_parquet), owners, read_ncit_in_ids_files(args.identifier_parquet)
    )
    labels = read_ncit_labels(os.path.join(args.umls_dir, "MRCONSO.RRF"), set(stranded))
    ncits = sorted(stranded, key=lambda c: int(c.split(":C")[1]))

    os.makedirs(args.output_dir, exist_ok=True)
    stem = os.path.join(args.output_dir, f"stranded-ncit-{args.build}")
    with open(f"{stem}.csv", "w", newline="") as outf:
        writer = csv.writer(outf)
        writer.writerow(["ncit", "label", "pipeline", "cuis", "cui_owners"])
        for ncit in ncits:
            rows = stranded[ncit]
            writer.writerow(
                [
                    ncit,
                    labels.get(ncit, ""),
                    ",".join(sorted({p for p, _, _ in rows})),
                    ",".join(c for _, c, _ in rows),
                    ",".join(o for _, _, o in rows),
                ]
            )

    by_route = Counter((p, o) for ncit in ncits for p, o in sorted({(p, o) for p, _, o in stranded[ncit]}))
    # Spread the sample across the NCIt code range rather than taking the lowest codes.
    step = max(1, len(ncits) // SAMPLE_SIZE)
    sample = ncits[::step][:SAMPLE_SIZE]

    def fill(text):
        return textwrap.fill(text, width=100, break_long_words=False, break_on_hyphens=False)

    lines = [
        f"# NCIt IDs that MeSH ownership leaves in no compendium ({args.build})",
        "",
        fill(
            "Generated by `scripts/list_stranded_ncit.py` (see its docstring) from the "
            f"{args.build} ids files and Concord.parquet and the UMLS download in `{args.umls_dir}`. "
            f"Every one is in [`stranded-ncit-{args.build}.csv`](stranded-ncit-{args.build}.csv)."
        ),
        "",
        fill(
            f"{len(ncits)} NCIt IDs have no concord edge left once each pipeline drops the CUIs another "
            "pipeline owns, and no ids file lists them."
        ),
        "",
        "| concord's pipeline | owner of its CUIs | NCIt IDs |",
        "|---|---|---:|",
    ]
    lines += [f"| {p} | {o} | {n:,} |" for (p, o), n in sorted(by_route.items(), key=lambda kv: -kv[1])]
    lines += ["", f"A sample of {len(sample)}, spread across the NCIt code range:", ""]
    lines += ["| NCIt ID | NCIt preferred term | CUIs (owner) |", "|---|---|---|"]
    for ncit in sample:
        cuis = ", ".join(f"`{c}` ({o})" for _, c, o in stranded[ncit])
        lines.append(f"| `{ncit}` | {labels.get(ncit, '')} | {cuis} |")
    with open(f"{stem}.md", "w") as outf:
        outf.write("\n".join(lines) + "\n")
    print(f"Wrote {stem}.md and {stem}.csv: {len(ncits)} NCIt IDs")


if __name__ == "__main__":
    main()
