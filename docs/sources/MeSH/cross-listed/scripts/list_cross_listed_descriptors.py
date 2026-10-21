"""List the MeSH records that two MeSH-owning pipelines' ``ids/MESH`` files both claim.

MeSH files some descriptors in two trees (``MESH:D013171`` "Spores, Bacterial" is at both A11.870.700 and
B05.775.700), and each pipeline's ``write_mesh_ids()`` claims whole trees, so such a descriptor lands in two
pipelines' ``ids/MESH`` and in two compendia. Neither "a CUI follows its MeSH descriptor" (which leaves a CUI
alone when two pipelines claim its descriptor) nor any concord filter can settle these: they need a stated
priority between the pipelines (#730, #1123). This script lays out the facts each decision needs -- the label,
every tree number with the headings above it, and the type each pipeline gives -- so the priority can be ruled
on record by record.

Inputs, all but mesh.nt exported from or read in a published build:

- every MeSH-owning pipeline's ``ids/MESH`` (``<ids-dir>/<pipeline>/MESH``, config.yaml's
  ``umls_mesh_owning_pipelines``), as ``docs/sources/UMLS/cui-follows-mesh/scripts/fetch_build_inputs.py``
  exports them;
- ``babel_downloads/MESH/mesh.nt``, loaded through the production ``mesh.Mesh`` class (about 2 GB, twenty
  seconds), for labels and tree numbers;
- optionally the build's ``duplicate_clique_leaders.tsv``, to mark the records that led a clique in two
  compendia;
- optionally its ``duckdb/parquet/`` directory, to list what else each record's clique held in each compendium.
  Those partners are the evidence for a priority: the side whose clique holds the record's ontology and UMLS
  partners is the side that loses equivalences if it gives the record up;
- optionally its ``duckdb/Concord.parquet``, to list every concord edge that reaches each record. glom() merges
  concord pairs whether or not an ids file lists both CURIEs, so a record a pipeline gives up stays in that
  pipeline's cliques as a member if one of its concords still names it.

Example (about five minutes over https, mostly reading the Edge tables and Concord.parquet):

    uv run python docs/sources/UMLS/cui-follows-mesh/scripts/fetch_build_inputs.py \\
        --build-url <build-url> --output-dir data/duplicate-curies
    uv run python docs/sources/MeSH/cross-listed/scripts/list_cross_listed_descriptors.py \\
        --ids-dir data/duplicate-curies/ids --leaders data/duplicate-curies/duplicate_clique_leaders.tsv \\
        --parquet-dir <build-url>/duckdb/parquet --concord-parquet <build-url>/duckdb/Concord.parquet \\
        --build 2026jul22 --output-dir docs/sources/MeSH/cross-listed

``<build-url>`` is the published build's root (``.../var/babel/<build>``; read its ``VERSION.txt``).
"""

import argparse
import csv
import os
import sys
import textwrap
from collections import defaultdict

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..", "..", "..", "..")))

from src.datahandlers.mesh import Mesh, get_mesh_id_from_iri  # noqa: E402
from src.util import get_config  # noqa: E402


def read_ids(path):
    with open(path) as inf:
        return dict(line.rstrip("\n").split("\t", 1) for line in inf if line.strip())


def labels_for(mesh, iris_by_id):
    """MeSH id (descriptor/SCR id or tree number) -> label, in one SPARQL query per kind."""
    out = {}
    descriptor_values = " ".join(f"mesh:{i}" for i in iris_by_id["records"])
    for row in mesh.m.query(
        f"""PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX mesh: <http://id.nlm.nih.gov/mesh/>
            SELECT ?d ?label WHERE {{ VALUES ?d {{ {descriptor_values} }} ?d rdfs:label ?label }}"""
    ):
        out[get_mesh_id_from_iri(row["d"])] = row["label"].value
    tree_values = " ".join(f"mesh:{t}" for t in iris_by_id["trees"])
    for row in mesh.m.query(
        f"""PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX meshv: <http://id.nlm.nih.gov/mesh/vocab#>
            PREFIX mesh: <http://id.nlm.nih.gov/mesh/>
            SELECT ?t ?label WHERE {{ VALUES ?t {{ {tree_values} }} ?d meshv:treeNumber ?t . ?d rdfs:label ?label }}"""
    ):
        out[get_mesh_id_from_iri(row["t"])] = row["label"].value
    return out


def record_classes(mesh, record_ids):
    """MeSH id -> its rdf:type local name (TopicalDescriptor, SCR_Chemical, SCR_Organism, ...)."""
    values = " ".join(f"mesh:{i}" for i in record_ids)
    out = {}
    for row in mesh.m.query(
        f"""PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX mesh: <http://id.nlm.nih.gov/mesh/>
            SELECT ?d ?type WHERE {{ VALUES ?d {{ {values} }} ?d rdf:type ?type }}"""
    ):
        out[get_mesh_id_from_iri(row["d"])] = row["type"].value.split("#")[-1]
    return out


def clique_partners(parquet_dir, curies):
    """CURIE -> {biolink type: "leader (PREFIX:n, ...)"} for every clique holding it, from the build's Edge tables."""
    import duckdb

    con = duckdb.connect()
    if parquet_dir.startswith("http"):
        con.execute("INSTALL httpfs; LOAD httpfs;")
    sep = "%3D" if parquet_dir.startswith("http") else "="
    # Only the compendia a MeSH-owning pipeline writes; DuckDB cannot glob over https.
    compendia = [
        "AnatomicalEntity", "Cell", "CellularComponent", "GrossAnatomicalStructure", "ChemicalEntity",
        "ComplexMolecularMixture", "Polypeptide", "SmallMolecule", "Disease", "PhenotypicFeature",
        "OrganismTaxon", "Protein",
    ]  # fmt: skip
    files = ", ".join(f"'{parquet_dir.rstrip('/')}/filename{sep}{c}/Edge.parquet'" for c in compendia)
    con.execute(
        f"CREATE TEMP TABLE edge AS SELECT clique_leader, curie, curie_prefix, biolink_type FROM read_parquet([{files}])"
    )
    con.execute("CREATE TEMP TABLE wanted(curie VARCHAR)")
    con.executemany("INSERT INTO wanted VALUES (?)", [(c,) for c in curies])
    rows = con.execute(
        """
        WITH hit AS (SELECT DISTINCT e.curie, e.clique_leader, e.biolink_type FROM edge e JOIN wanted USING (curie)),
             counts AS (SELECT clique_leader, biolink_type, curie_prefix, count(*) AS n FROM edge
                        WHERE clique_leader IN (SELECT clique_leader FROM hit) GROUP BY ALL)
        SELECT hit.curie, hit.biolink_type, hit.clique_leader,
               string_agg(c.curie_prefix || ':' || c.n, ', ' ORDER BY c.curie_prefix) AS members
        FROM hit JOIN counts c USING (clique_leader, biolink_type)
        GROUP BY ALL ORDER BY ALL
        """
    ).fetchall()
    out = defaultdict(dict)
    for curie, biolink_type, leader, members in rows:
        out[curie][biolink_type] = f"{leader} ({members})"
    return out


def concord_edges(concord_parquet, curies):
    """CURIE -> sorted ["<pipeline>/<concord> <partner>", ...] for every concord edge with the CURIE on either side."""
    import duckdb

    con = duckdb.connect()
    if concord_parquet.startswith("http"):
        con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("CREATE TEMP TABLE wanted(curie VARCHAR)")
    con.executemany("INSERT INTO wanted VALUES (?)", [(c,) for c in curies])
    rows = con.execute(
        f"""
        SELECT w.curie, regexp_extract(c.filename, 'intermediate/([^/]+/concords/[^/]+)$', 1) AS concord,
               CASE WHEN c.subj = w.curie THEN c.obj ELSE c.subj END AS partner
        FROM read_parquet('{concord_parquet}') c JOIN wanted w ON c.subj = w.curie OR c.obj = w.curie
        """
    ).fetchall()
    out = defaultdict(set)
    for curie, concord, partner in rows:
        out[curie].add(f"{concord.replace('/concords/', '/')} {partner}")
    return {curie: sorted(edges) for curie, edges in out.items()}


def tree_context(tree_number, labels):
    """'A11.870.700' -> 'A11 Cells > A11.870 Spores > A11.870.700' (the headings two levels down, then the leaf)."""
    parts = tree_number.split(".")
    heads = [".".join(parts[: i + 1]) for i in range(min(2, len(parts) - 1))]
    return " > ".join([f"{h} {labels.get(h, '?')}" for h in heads] + [tree_number])


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--ids-dir", required=True, help="Directory with <pipeline>/MESH ids files")
    ap.add_argument("--leaders", help="A build's reports/duckdb/duplicate_clique_leaders.tsv")
    ap.add_argument("--parquet-dir", help="A build's duckdb/parquet directory (path or https URL)")
    ap.add_argument("--concord-parquet", help="A build's duckdb/Concord.parquet (path or https URL)")
    ap.add_argument("--build", default="2026jul22", help="Name of the build the ids files and leaders come from")
    ap.add_argument("--output-dir", required=True)
    args = ap.parse_args()

    pipelines = get_config()["umls_mesh_owning_pipelines"]
    types = {p: read_ids(os.path.join(args.ids_dir, p, "MESH")) for p in pipelines}
    claimants = defaultdict(list)
    for p in pipelines:
        for curie in types[p]:
            claimants[curie].append(p)
    shared = sorted(c for c, ps in claimants.items() if len(ps) > 1)

    leaders = {}
    if args.leaders:
        with open(args.leaders) as inf:
            for row in csv.DictReader(inf, delimiter="\t"):
                leaders[row["clique_leader"]] = row["filenames"]

    mesh = Mesh()
    ids = [c.split(":")[1] for c in shared]
    trees = {c: mesh.get_tree_numbers(c) for c in shared}
    tree_heads = {
        ".".join(t.split(".")[: i + 1]) for ts in trees.values() for t in ts for i in range(min(2, t.count(".")))
    }
    labels = labels_for(mesh, {"records": ids, "trees": sorted(tree_heads)})
    # Priority.md weighs dropping MeSH B05 "Organism Forms" from the taxon pipeline instead of deciding the
    # anatomy-and-taxon records one by one; this is how many other records that would also move.
    b05_only = set(mesh.get_terms_in_tree("B05"))
    for tree in ("B01", "B02", "B03", "B04"):
        b05_only -= set(mesh.get_terms_in_tree(tree))
    b05_only_taxon = (b05_only & set(types.get("taxon", {}))) - set(shared)
    classes = record_classes(mesh, ids)

    partners = clique_partners(args.parquet_dir, shared) if args.parquet_dir else {}
    edges = concord_edges(args.concord_parquet, shared) if args.concord_parquet else {}

    rows = []
    for curie in shared:
        local = curie.split(":")[1]
        rows.append(
            {
                "curie": curie,
                "label": labels.get(local, ""),
                "record_class": classes.get(local, ""),
                "pipelines": "+".join(claimants[curie]),
                "types": "; ".join(f"{p}={types[p][curie]}" for p in claimants[curie]),
                "tree_numbers": "; ".join(trees[curie]),
                "led_duplicate_clique": leaders.get(curie, ""),
                "cliques": "; ".join(f"{t}: {v}" for t, v in sorted(partners.get(curie, {}).items())),
                "concord_edges": "; ".join(edges.get(curie, [])),
            }
        )

    os.makedirs(args.output_dir, exist_ok=True)
    csv_path = os.path.join(args.output_dir, f"{args.build}.csv")
    with open(csv_path, "w", newline="") as outf:
        writer = csv.DictWriter(outf, fieldnames=list(rows[0].keys()) if rows else ["curie"])
        writer.writeheader()
        writer.writerows(rows)

    by_pair = defaultdict(list)
    for row in rows:
        by_pair[row["pipelines"]].append(row)
    lines = [
        f"# MeSH records two pipelines claim ({args.build})",
        "",
        "Generated by `scripts/list_cross_listed_descriptors.py` (see its docstring) from the "
        f"{args.build} build's `ids/MESH` files and the local `mesh.nt`. Every row is also in "
        f"[`{args.build}.csv`]({args.build}.csv). The proposed priority is in [`Priority.md`](Priority.md).",
        "",
        f"{len(rows)} MeSH records are in two pipelines' `ids/MESH`; "
        f"{sum(1 for r in rows if r['led_duplicate_clique'])} of them led a clique in two compendia in "
        f"{args.build} (the rest were members of a clique another identifier led, in each of two compendia). "
        "The cliques column says what each compendium's clique holding the record contained in that build.",
        "",
        f'Dropping MeSH B05 "Organism Forms" from the taxon pipeline would also remove {len(b05_only_taxon)} records '
        "that taxon claims only through B05 and that no other pipeline claims.",
        "",
        "| pipelines | records | led a duplicate clique |",
        "|---|---:|---:|",
    ]
    for pair, pair_rows in sorted(by_pair.items(), key=lambda kv: -len(kv[1])):
        lines.append(f"| {pair} | {len(pair_rows)} | {sum(1 for r in pair_rows if r['led_duplicate_clique'])} |")
    for pair, pair_rows in sorted(by_pair.items(), key=lambda kv: -len(kv[1])):
        lines += ["", f"## {pair.replace('+', ' and ')}", ""]
        lines.append(
            "| record | types | tree numbers | cliques holding it (leader and member prefixes) "
            "| concord edges reaching it (pipeline/concord partner) |"
        )
        lines.append("|---|---|---|---|---|")
        for r in pair_rows:
            local = r["curie"].split(":")[1]
            link = f'[`{r["curie"]}`](https://meshb.nlm.nih.gov/record/ui?ui={local}) "{r["label"]}"'
            if r["record_class"] != "TopicalDescriptor":
                link += f" ({r['record_class']})"
            tree_cell = "; ".join(tree_context(t, labels) for t in r["tree_numbers"].split("; ") if t) or "(none)"
            led = " **(led a duplicate clique)**" if r["led_duplicate_clique"] else ""
            cliques = r["cliques"] or "-"
            lines.append(f"| {link}{led} | {r['types']} | {tree_cell} | {cliques} | {r['concord_edges'] or '-'} |")

    def wrap(line):
        # The repository's 100-column Markdown rule; tables and headings are exempt.
        if not line or line.startswith(("|", "#")):
            return line
        return textwrap.fill(line, width=100, break_long_words=False, break_on_hyphens=False)

    md_path = os.path.join(args.output_dir, f"{args.build}.md")
    with open(md_path, "w") as outf:
        outf.write("\n".join(wrap(line) for line in lines) + "\n")
    print(f"Wrote {md_path} and {csv_path}")  # noqa: T201


if __name__ == "__main__":
    main()
