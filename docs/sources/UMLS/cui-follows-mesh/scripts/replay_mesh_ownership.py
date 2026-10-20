"""Replay "a CUI follows its MeSH descriptor" (umls.compute_mesh_owners) over a published build's ids files.

It writes the umls_mesh_ownership rule's file from the build's ``ids/MESH`` files and a local UMLS download,
then runs every pipeline's production ``write_umls_ids`` twice -- by semantic type alone, and with MeSH
ownership -- and reports what the rule drops and adds, and that every dropped CUI lands in exactly one other
pipeline. It then predicts which of the
build's duplicate clique leaders the rule resolves: a MESH leader is resolved when no CUI that a
``<pipeline>/concords/UMLS`` concord links to it is still claimed by a pipeline other than the descriptor's
owner. glom() is transitive, so this is a prediction; confirm it with babel-clique-diff after a build.

Inputs: the build's ids files (``<ids-dir>/<pipeline>/{MESH,UMLS}``, e.g. exported from the build's
Identifier.parquet), its ``duplicate_clique_leaders.tsv`` and ``duplicate_curies.tsv``, its Concord.parquet
(path or https URL) and a local UMLS download. Example (about a minute, mostly the two MRCONSO passes):

    uv run python docs/sources/UMLS/cui-follows-mesh/scripts/replay_mesh_ownership.py \\
        --ids-dir data/duplicate-curies/ids --reports-dir data/duplicate-curies \\
        --concord-parquet https://stars.renci.org/var/babel_outputs/2026jul22/duckdb/Concord.parquet \\
        --output docs/sources/UMLS/cui-follows-mesh/2026jul22.md
"""

import argparse
import csv
import os
import sys
import tempfile
import textwrap
from collections import Counter, defaultdict

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "..")))

from src.createcompendia import (  # noqa: E402
    anatomy,
    chemicals,
    diseasephenotype,
    gene,
    processactivitypathway,
    protein,
    taxon,
)
from src.datahandlers import umls  # noqa: E402
from src.reports.duckdb_reports import _parse_filenames  # noqa: E402
from src.util import get_config  # noqa: E402

WRITERS = {
    "anatomy": anatomy.write_umls_ids,
    "chemicals": chemicals.write_umls_ids,
    "disease": diseasephenotype.write_umls_ids,
    "protein": protein.write_umls_ids,
    "taxon": taxon.write_umls_ids,
    # Gene and process write no ids/MESH, so the rule can only drop CUIs from them.
    "gene": gene.write_umls_ids,
    "process": processactivitypathway.write_umls_ids,
}


def read_ids(path):
    with open(path) as inf:
        return dict(line.rstrip("\n").split("\t", 1) for line in inf if line.strip())


def read_stn(mrsty):
    """CUI -> sorted list of 'STN name' strings, for describing what the rule moved."""
    stn = defaultdict(set)
    with open(mrsty) as inf:
        for line in inf:
            x = line.split("|")
            stn[f"UMLS:{x[0]}"].add(f"{x[2]} {x[3]}")
    return stn


def run_writer(pipeline, umls_dir, tmpdir, mesh_ownership):
    """Run one pipeline's write_umls_ids into tmpdir (with MeSH ownership if mesh_ownership is given) and return
    its output as a dict."""
    out = os.path.join(tmpdir, f"{pipeline}_{'mesh' if mesh_ownership else 'sty'}")
    mrconso = os.path.join(umls_dir, "MRCONSO.RRF")
    args = [os.path.join(umls_dir, "MRSTY.RRF"), out]
    if pipeline == "disease":
        args.append(os.path.join(get_config()["input_directory"], "badumls"))
    if pipeline == "gene":
        args.insert(0, mrconso)
    WRITERS[pipeline](*args, mesh_ownership=mesh_ownership)
    return read_ids(out)


def load_concord_edges(concord_parquet, targets):
    """{(pipeline, cui, target)} for every <pipeline>/concords/UMLS edge whose object is one of targets."""
    import duckdb

    con = duckdb.connect()
    if concord_parquet.startswith("http"):
        con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("CREATE TEMP TABLE targets(curie VARCHAR)")
    con.executemany("INSERT INTO targets VALUES (?)", [(t,) for t in targets])
    rows = con.execute(
        f"""
        SELECT regexp_extract(filename, 'intermediate/([^/]+)/concords/UMLS$', 1) AS pipeline, subj, obj
        FROM read_parquet('{concord_parquet}') c JOIN targets t ON c.obj = t.curie
        WHERE filename LIKE '%/concords/UMLS'
        """
    ).fetchall()
    return {(p, s, o) for p, s, o in rows if p}


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--umls-dir", default=os.path.join(get_config()["download_directory"], "UMLS"))
    ap.add_argument("--ids-dir", required=True, help="Directory with <pipeline>/MESH and <pipeline>/UMLS ids files")
    ap.add_argument(
        "--reports-dir", required=True, help="Directory with duplicate_clique_leaders.tsv and duplicate_curies.tsv"
    )
    ap.add_argument("--concord-parquet", required=True, help="Path or URL of the build's Concord.parquet")
    ap.add_argument("--build", default="2026jul22")
    ap.add_argument("--output", required=True)
    args = ap.parse_args()

    owners = get_config()["umls_mesh_owning_pipelines"]
    pipelines = owners + ["gene", "process"]
    stn = read_stn(os.path.join(args.umls_dir, "MRSTY.RRF"))
    mesh_owner = defaultdict(set)
    for p in owners:
        for curie in read_ids(os.path.join(args.ids_dir, p, "MESH")):
            mesh_owner[curie].add(p)

    lines = [f"# Replaying MeSH ownership over the {args.build} build", ""]
    lines.append(
        "Generated by `docs/sources/UMLS/cui-follows-mesh/scripts/replay_mesh_ownership.py` (see its docstring). "
        f"UMLS from `{args.umls_dir}`; ids files and duplicate reports from the {args.build} build."
    )
    lines += ["", "## What the rule changes in each pipeline's `ids/UMLS`", ""]
    lines.append("| pipeline | build's ids/UMLS | by semantic type (this UMLS) | dropped | added | after |")
    lines.append("|---|---:|---:|---:|---:|---:|")
    after = {}
    dropped_all = {}
    added_all = {}
    with tempfile.TemporaryDirectory() as tmpdir:
        # The same call rule umls_mesh_ownership makes.
        ownership = os.path.join(tmpdir, "mesh_ownership.tsv")
        badumls = diseasephenotype.read_badumls(os.path.join(get_config()["input_directory"], "badumls"))
        umls.write_mesh_ownership(
            os.path.join(args.umls_dir, "MRCONSO.RRF"),
            {p: os.path.join(args.ids_dir, p, "MESH") for p in owners},
            ownership,
            never_add={"disease": {f"UMLS:{cui}" for cui in badumls}},
        )
        for p in pipelines:
            build_ids = read_ids(os.path.join(args.ids_dir, p, "UMLS"))
            sty = run_writer(p, args.umls_dir, tmpdir, None)
            mesh = run_writer(p, args.umls_dir, tmpdir, ownership)
            after[p] = mesh
            dropped_all[p] = {c: sty[c] for c in sty if c not in mesh}
            added_all[p] = {c: mesh[c] for c in mesh if c not in sty}
            lines.append(
                f"| {p} | {len(build_ids):,} | {len(sty):,} | {len(dropped_all[p]):,} | {len(added_all[p]):,} | {len(mesh):,} |"
            )
    lines += [
        "",
        "Dropped CUIs go to the pipeline that owns their descriptor; the table below says which. A dropped CUI "
        "that no pipeline claims afterwards would be listed as going to (nobody); there should be none.",
        "",
    ]
    lines.append("| from | to (descriptor owner) | CUIs | most common semantic types |")
    lines.append("|---|---|---:|---|")
    # A CUI dropped from P is claimed by the pipeline(s) whose ids/UMLS list it after the rule.
    for p in pipelines:
        by_dest = defaultdict(list)
        for c in dropped_all[p]:
            by_dest[",".join(sorted(q for q in pipelines if c in after[q])) or "(nobody)"].append(c)
        for dest, cuis in sorted(by_dest.items(), key=lambda kv: -len(kv[1])):
            types = Counter(t for c in cuis for t in stn[c]).most_common(3)
            lines.append(f"| {p} | {dest} | {len(cuis):,} | {'; '.join(f'{t} ({n:,})' for t, n in types)} |")
    lines += ["", "Added CUIs by pipeline and the type they get (from the owned descriptor):", ""]
    lines.append("| pipeline | type | CUIs | most common semantic types | examples |")
    lines.append("|---|---|---:|---|---|")
    for p in pipelines:
        by_type = defaultdict(list)
        for c, t in added_all[p].items():
            by_type[t].append(c)
        for t, cuis in sorted(by_type.items(), key=lambda kv: -len(kv[1])):
            types = Counter(s for c in cuis for s in stn[c]).most_common(3)
            lines.append(
                f"| {p} | {t} | {len(cuis):,} | {'; '.join(f'{s} ({n:,})' for s, n in types)} | {', '.join(sorted(cuis)[:3])} |"
            )

    # Predict the leader outcome.
    leaders_path = os.path.join(args.reports_dir, "duplicate_clique_leaders.tsv")
    with open(leaders_path) as inf:
        leaders = [row for row in csv.DictReader(inf, delimiter="\t") if row["clique_leader"].startswith("MESH:")]
    edges = load_concord_edges(args.concord_parquet, [row["clique_leader"] for row in leaders])
    edges_by_target = defaultdict(set)
    for p, cui, target in edges:
        edges_by_target[target].add((p, cui))
    claimed_by = defaultdict(set)
    for p, ids in after.items():
        for c in ids:
            claimed_by[c].add(p)
    resolved = Counter()
    residual = defaultdict(list)
    cross_listed = defaultdict(list)
    for row in leaders:
        leader = row["clique_leader"]
        pair = ", ".join(_parse_filenames(row["filenames"]))
        owners_of_leader = mesh_owner.get(leader, set())
        # A dragging edge is one whose CUI a pipeline other than the descriptor's owner still claims.
        dragging = sorted(
            (p, cui) for p, cui in edges_by_target[leader] if p not in owners_of_leader and p in claimed_by[cui]
        )
        if len(owners_of_leader) > 1:
            # Both pipelines list the descriptor in their own ids/MESH (a descriptor cross-listed in two MeSH
            # trees), so the duplicate does not come through UMLS at all and the rule cannot resolve it.
            cross_listed[pair].append((leader, sorted(owners_of_leader)))
        elif edges_by_target[leader] and not dragging:
            resolved[pair] += 1
        else:
            residual[pair].append((leader, dragging, sorted(owners_of_leader)))
    lines += ["", "## Predicted effect on duplicate clique leaders (MESH leaders only)", ""]
    lines.append(
        f"{len(leaders):,} MESH CURIEs lead a clique in two compendia in {args.build}. ENSEMBL leaders are the yeast gene/protein ID clash fixed separately."
    )
    lines += [
        "",
        "| compendia | leaders | resolved: no non-owner pipeline still claims a concorded CUI "
        "| cross-listed in two ids/MESH | residual |",
        "|---|---:|---:|---:|---:|",
    ]
    for pair in sorted({", ".join(_parse_filenames(r["filenames"])) for r in leaders}):
        total = sum(1 for r in leaders if ", ".join(_parse_filenames(r["filenames"])) == pair)
        lines.append(
            f"| {pair} | {total:,} | {resolved[pair]:,} | {len(cross_listed[pair]):,} | {len(residual[pair]):,} |"
        )
    lines += [
        "",
        "Leaders whose descriptor two pipelines' `ids/MESH` both list (a MeSH descriptor cross-listed in two "
        "trees): the duplicate is between ids files, not through UMLS, and needs a stated priority between the "
        "pipelines (#730, #1123). These are the rows of `input_data/known_duplicate_clique_leaders.tsv`:",
        "",
    ]
    for pair, rows in sorted(cross_listed.items()):
        if not rows:
            continue
        lines.append(f"- **{pair}** ({len(rows)}): " + ", ".join(f"`{ldr}` ({'+'.join(own)})" for ldr, own in rows))
    lines += [
        "",
        "Residual leaders (up to 10 per pair) with the concord edges that still drag them and the descriptor's owner:",
        "",
    ]
    for pair, rows in sorted(residual.items()):
        if not rows:
            continue
        lines.append(f"- **{pair}** ({len(rows)}):")
        for leader, dragging, owner in rows[:10]:
            drag = (
                "; ".join(f"{p}/concords/UMLS {cui}" for p, cui in dragging)
                or "no <pipeline>/concords/UMLS edge (reached another way)"
            )
            lines.append(f"  - `{leader}` owned by {owner or 'nobody'}: {drag}")
    if not any(residual.values()):
        lines.append("None.")

    # Wrap prose and bullets to the repository's 100-column Markdown rule; tables and headings are exempt.
    def wrap(line):
        if not line or line.startswith(("|", "#")):
            return line
        indent = line[: len(line) - len(line.lstrip())]
        hanging = indent + ("  " if line.lstrip().startswith("- ") else "")
        return textwrap.fill(line, width=100, subsequent_indent=hanging, break_long_words=False, break_on_hyphens=False)

    wrapped = [wrap(line) for line in lines]
    with open(args.output, "w") as outf:
        outf.write("\n".join(wrapped).rstrip("\n") + "\n")
    print(f"Wrote {args.output}")


if __name__ == "__main__":
    main()
