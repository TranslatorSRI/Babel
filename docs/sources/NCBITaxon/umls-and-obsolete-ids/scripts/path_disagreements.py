"""List UMLS CUIs whose direct NCBITaxon link disagrees with the one reached through MeSH.

A UMLS taxon CUI can reach NCBITaxon two ways: directly (an NCBI atom on the CUI, in the UMLS concord), or
through its MeSH heading (UMLS -> MESH in the UMLS concord, then MESH -> NCBITaxon from the MeSH registry
in the NCBI_MESH concord). When the two disagree, only one can win, because NCBITaxon, MESH and UMLS are
all unique prefixes in glom(). This script writes:

- path-disagreements.md: the count, plus a sample.
- path-disagreements.csv: every disagreeing CUI.

Usage (from the repository root, after building the taxon concords):

    PYTHONPATH=. uv run python docs/sources/NCBITaxon/umls-and-obsolete-ids/scripts/path_disagreements.py \\
        babel_outputs/intermediate/taxon/concords babel_downloads docs/sources/NCBITaxon/umls-and-obsolete-ids
"""

import csv
import random
import sys
import textwrap
from collections import defaultdict
from pathlib import Path

from src.prefixes import MESH, NCBITAXON, UMLS

SAMPLE_SIZE = 10


def read_concord(path):
    links = defaultdict(set)
    with open(path) as inf:
        for line in inf:
            subject, _, obj = line.rstrip("\n").split("\t")
            links[subject].add(obj)
    return links


def main(concords_dir, downloads_dir, outdir):
    outdir = Path(outdir)
    mesh_to_ncbi = read_concord(Path(concords_dir) / "NCBI_MESH")
    umls_links = read_concord(Path(concords_dir) / "UMLS")

    rows = []
    with_both = 0
    for cui, targets in sorted(umls_links.items()):
        direct = {t for t in targets if t.startswith(f"{NCBITAXON}:")}
        meshes = sorted(t for t in targets if t.startswith(f"{MESH}:"))
        via_mesh = {n for m in meshes for n in mesh_to_ncbi.get(m, ())}
        if not direct or not via_mesh:
            continue
        with_both += 1
        if not direct & via_mesh:
            rows.append({"cui": cui, "direct": sorted(direct), "mesh": meshes, "via_mesh": sorted(via_mesh)})

    needed = {c for r in rows for c in [r["cui"], *r["direct"], *r["mesh"], *r["via_mesh"]]}
    labels = {}
    for prefix in [NCBITAXON, MESH, UMLS]:
        with open(Path(downloads_dir) / prefix / "labels") as inf:
            for line in inf:
                curie, label = line.rstrip("\n").split("\t")[:2]
                if curie in needed:
                    labels[curie] = label

    def show(curies):
        return "; ".join(f"{c} {labels.get(c, '')}" for c in curies)

    with open(outdir / "path-disagreements.csv", "w", newline="") as outf:
        writer = csv.writer(outf)
        writer.writerow(["cui", "cui_label", "direct_ncbitaxon", "mesh", "ncbitaxon_via_mesh"])
        for r in rows:
            writer.writerow(
                [r["cui"], labels.get(r["cui"], ""), show(r["direct"]), show(r["mesh"]), show(r["via_mesh"])]
            )

    lines = [
        "# UMLS CUIs whose direct and MeSH paths to NCBITaxon disagree",
        "",
        textwrap.fill(
            f"{len(rows):,} of the {with_both:,} UMLS CUIs that reach NCBITaxon both directly and through MeSH "
            "reach a different NCBITaxon each way (all of them are in `path-disagreements.csv`). "
            f"A random sample of {min(SAMPLE_SIZE, len(rows))}:",
            width=100,
        ),
        "",
        "| CUI | direct NCBITaxon | MeSH | NCBITaxon via MeSH |",
        "|---|---|---|---|",
    ]
    random.seed(0)
    for r in random.sample(rows, min(SAMPLE_SIZE, len(rows))):
        lines.append(
            f"| {r['cui']} {labels.get(r['cui'], '')} | {show(r['direct'])} | {show(r['mesh'])} | {show(r['via_mesh'])} |"
        )
    (outdir / "path-disagreements.md").write_text("\n".join(lines) + "\n")


if __name__ == "__main__":
    main(*sys.argv[1:4])
