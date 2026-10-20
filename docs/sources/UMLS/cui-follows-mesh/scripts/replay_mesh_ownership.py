"""Replay "a CUI follows its MeSH descriptor" (umls.compute_mesh_owners) over a published build's ids files.

It runs gene's and process's production ``write_umls_ids`` (which MeSH ownership never changes), writes the
umls_mesh_ownership rule's file from those and the build's ``ids/MESH`` files and a local UMLS download, then
runs every MeSH-owning pipeline's ``write_umls_ids`` twice -- by semantic type alone, and with MeSH ownership --
and reports what the rule drops and adds, and that every dropped CUI lands in exactly one other pipeline. It
counts the CUIs another pipeline owns that a pipeline's other concords still name, which its compendium build
now skips (umls.read_cuis_owned_elsewhere). It then predicts which of the build's duplicate clique leaders the
rule resolves: a MESH leader is resolved when no CUI that a ``<pipeline>/concords/UMLS`` concord links to it
is still claimed by a pipeline other than the descriptor's owner. glom() is transitive, so this is a
prediction; confirm it with babel-clique-diff after a build.

Inputs: the build's ids files (``<ids-dir>/<pipeline>/{MESH,UMLS}``) and its ``duplicate_clique_leaders.tsv``
and ``duplicate_curies.tsv``, all of which ``fetch_build_inputs.py`` (beside this script) exports from a
published build; its Concord.parquet (path or https URL); and a local UMLS download. Example (about a minute,
mostly the two MRCONSO passes; ``<build-url>`` is the published build's root, e.g. ``.../var/babel/<build>``):

    uv run python docs/sources/UMLS/cui-follows-mesh/scripts/fetch_build_inputs.py \\
        --build-url <build-url> --output-dir data/duplicate-curies
    uv run python docs/sources/UMLS/cui-follows-mesh/scripts/replay_mesh_ownership.py \\
        --ids-dir data/duplicate-curies/ids --reports-dir data/duplicate-curies \\
        --concord-parquet <build-url>/duckdb/Concord.parquet \\
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
    # Gene and process write no ids/MESH and take no MeSH ownership: their CUIs have no owner.
    "gene": gene.write_umls_ids,
    "process": processactivitypathway.write_umls_ids,
}

# A pipeline's concords built from its own ids/UMLS, which apply_mesh_ownership() already rewrites. Every other
# concord is built from something else, so a CUI another pipeline owns can still appear in it.
CONCORDS_FROM_IDS_UMLS = {"UMLS", "NCIT_UMLS"}


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
    kwargs = {"mesh_ownership": mesh_ownership} if pipeline in get_config()["umls_mesh_owning_pipelines"] else {}
    WRITERS[pipeline](*args, **kwargs)
    return out


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


def load_concords_naming_owned_cuis(concord_parquet, owners):
    """{(pipeline, concord, owner): set of CUIs} for every concord of a MeSH-owning pipeline, other than those built
    from its own ids/UMLS, that names a CUI another pipeline owns: the CUIs its compendium build now skips."""
    import duckdb

    con = duckdb.connect()
    if concord_parquet.startswith("http"):
        con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("CREATE TEMP TABLE owned(curie VARCHAR, owner VARCHAR)")
    con.executemany("INSERT INTO owned VALUES (?, ?)", [(cui, o[0]) for cui, o in owners.items()])
    rows = con.execute(
        f"""
        WITH c AS (
            SELECT regexp_extract(filename, 'intermediate/([^/]+)/concords/(.+)$', 1) AS pipeline,
                   regexp_extract(filename, 'intermediate/([^/]+)/concords/(.+)$', 2) AS concord, subj, obj
            FROM read_parquet('{concord_parquet}')
        )
        SELECT DISTINCT c.pipeline, c.concord, o.owner, o.curie
        FROM c JOIN owned o ON o.curie = c.subj OR o.curie = c.obj
        WHERE c.pipeline <> o.owner
        """
    ).fetchall()
    found = defaultdict(set)
    for pipeline, concord, owner, cui in rows:
        if pipeline in get_config()["umls_mesh_owning_pipelines"] and concord not in CONCORDS_FROM_IDS_UMLS:
            found[(pipeline, concord, owner)].add(cui)
    return found


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
        non_mesh = {p: run_writer(p, args.umls_dir, tmpdir, None) for p in ["gene", "process"]}
        # The same call rule umls_mesh_ownership makes.
        ownership = os.path.join(tmpdir, "mesh_ownership.tsv")
        umls.write_mesh_ownership(
            os.path.join(args.umls_dir, "MRCONSO.RRF"),
            {p: os.path.join(args.ids_dir, p, "MESH") for p in owners},
            ownership,
            blocklist_files_by_pipeline={"disease": os.path.join(get_config()["input_directory"], "badumls")},
            non_mesh_umls_ids_files=list(non_mesh.values()),
        )
        mesh_owners = umls.read_mesh_ownership(ownership)
        for p in pipelines:
            build_ids = read_ids(os.path.join(args.ids_dir, p, "UMLS"))
            if p in non_mesh:
                sty = mesh = read_ids(non_mesh[p])
            else:
                sty = read_ids(run_writer(p, args.umls_dir, tmpdir, None))
                mesh = read_ids(run_writer(p, args.umls_dir, tmpdir, ownership))
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

    lines += [
        "",
        "## CUIs another pipeline owns that a pipeline's other concords still name",
        "",
        "Concords built from a pipeline's own `ids/UMLS` (`"
        + "`, `".join(sorted(CONCORDS_FROM_IDS_UMLS))
        + "`) lose a dropped CUI with it, but a concord built from anything else still names it, and glom() "
        "would put it into that pipeline's compendium as well as its owner's. Each MeSH-owning pipeline's "
        "compendium build skips these pairs (`umls.read_cuis_owned_elsewhere()`). Counted over the build's "
        "Concord.parquet:",
        "",
    ]
    lines.append("| pipeline | concord | owner | CUIs | examples |")
    lines.append("|---|---|---|---:|---|")
    leaks = load_concords_naming_owned_cuis(args.concord_parquet, mesh_owners)
    for (p, concord, owner), cuis in sorted(leaks.items(), key=lambda kv: (kv[0][0], -len(kv[1]), kv[0])):
        lines.append(f"| {p} | {concord} | {owner} | {len(cuis):,} | {', '.join(sorted(cuis)[:3])} |")
    if not leaks:
        lines.append("| (none) | | | 0 | |")

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
