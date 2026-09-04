"""Tabulate MONDO's ``oboInOwl:hasDbXref`` target namespaces against what Babel actually ingests.

Babel builds MONDO's concords from ``skos:exactMatch``/``closeMatch`` only, with one deliberate
exception (``MONDO_GARD``). This script regenerates the evidence table in
``docs/sources/MONDO/README.md``: for every namespace MONDO xrefs, how many rows it has, whether
the built ``MONDO`` concord already covers it via exactMatch, and -- the question that decides
whether a namespace *could* be ingested -- how close the mapping is to 1:1.

A namespace whose targets are claimed by many subjects names disease *families*, and gloming it
fuses every subtype that cites one (``ICD9:759.89`` is claimed by 167 MONDO terms). A namespace
that is 1:1 has been disambiguated by someone, which is the property that made MONDO's GARD
mappings safe to take unfiltered.

Usage::

    curl -sL -o data/mondo.json http://purl.obolibrary.org/obo/mondo.json
    uv run python docs/sources/MONDO/scripts/analyze_xref_namespaces.py \
        --mondo-json data/mondo.json \
        --concord babel_outputs/intermediate/disease/concords/MONDO \
        --out docs/sources/MONDO/xref-namespaces.csv

Imports ``get_xref_prefix_map``/``norm`` from the production code so the exactMatch column is
compared under the same prefix renames the build applies, and cannot drift from it.
"""

import argparse
import collections
import csv
import json

from src.babel_utils import norm
from src.createcompendia.diseasephenotype import get_xref_prefix_map
from src.prefixes import MONDO

MONDO_IRI_PREFIX = "http://purl.obolibrary.org/obo/MONDO_"


def collect_xrefs(mondo_json_path):
    """Return {namespace: {"rows": n, "by_target": {target: {subjects}}}} for non-deprecated terms."""
    with open(mondo_json_path, encoding="utf8") as inf:
        graph = json.load(inf)["graphs"][0]
    namespaces = collections.defaultdict(lambda: {"rows": 0, "by_target": collections.defaultdict(set)})
    for node in graph["nodes"]:
        if not node["id"].startswith(MONDO_IRI_PREFIX):
            continue
        meta = node.get("meta") or {}
        if meta.get("deprecated"):
            continue
        for xref in meta.get("xrefs") or []:
            value = xref["val"]
            namespace = value.split(":", 1)[0] if ":" in value else value
            entry = namespaces[namespace]
            entry["rows"] += 1
            entry["by_target"][value].add(node["id"])
    return namespaces


def exactmatch_counts(concord_path, prefix_map):
    """Rows per target prefix in the built MONDO concord, under the build's own prefix renames."""
    counts = collections.Counter()
    with open(concord_path, encoding="utf8") as inf:
        for line in inf:
            _subject, _predicate, target = line.rstrip("\n").split("\t")
            counts[norm(target, prefix_map).split(":", 1)[0]] += 1
    return counts


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mondo-json", default="data/mondo.json")
    parser.add_argument("--concord", default="babel_outputs/intermediate/disease/concords/MONDO")
    parser.add_argument("--out", default="docs/sources/MONDO/xref-namespaces.csv")
    args = parser.parse_args()

    prefix_map = get_xref_prefix_map(MONDO)
    namespaces = collect_xrefs(args.mondo_json)
    exact = exactmatch_counts(args.concord, prefix_map)

    rows = []
    for namespace, entry in namespaces.items():
        by_target = entry["by_target"]
        fanouts = [len(subjects) for subjects in by_target.values()]
        # The namespace as the build would see it, so the exactMatch column lines up.
        renamed = norm(f"{namespace}:x", prefix_map).split(":", 1)[0]
        rows.append(
            {
                "namespace": namespace,
                "namespace_after_norm": renamed,
                "hasdbxref_rows": entry["rows"],
                "distinct_subjects": len({s for subjects in by_target.values() for s in subjects}),
                "distinct_targets": len(by_target),
                "targets_claimed_by_2plus": sum(1 for f in fanouts if f > 1),
                "worst_fanout": max(fanouts, default=0),
                "exactmatch_rows_in_concord": exact.get(renamed, 0),
            }
        )
    rows.sort(key=lambda r: -r["hasdbxref_rows"])

    with open(args.out, "w", encoding="utf8", newline="") as outf:
        # lineterminator="\n": the default is CRLF, which git rewrites on commit.
        writer = csv.DictWriter(outf, fieldnames=list(rows[0]), lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)

    total = sum(r["hasdbxref_rows"] for r in rows)
    print(f"{total} hasDbXref rows across {len(rows)} namespaces -> {args.out}")
    print(f"{'namespace':22}{'rows':>8}{'targets':>9}{'2+claimed':>11}{'worst':>7}{'exactMatch':>12}")
    for r in rows[:22]:
        print(
            f"{r['namespace']:22}{r['hasdbxref_rows']:8}{r['distinct_targets']:9}"
            f"{r['targets_claimed_by_2plus']:11}{r['worst_fanout']:7}{r['exactmatch_rows_in_concord']:12}"
        )


if __name__ == "__main__":
    main()
