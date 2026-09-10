"""Summarise the ChEBI role property file produced by make_chebi_roles().

Writes a Markdown report ranking roles by how many chemicals carry them, and a CSV holding the
count for every role. See docs/sources/CHEBI/roles/README.md for what the numbers mean.

Imports CHEBI_ROLE_ROOT and HAS_ROLE from the production module rather than restating them, so this
audit cannot drift from the ingest it is describing.

Usage:
    uv run python docs/sources/CHEBI/roles/scripts/audit_chebi_roles.py \
        babel_outputs/intermediate/chemicals/properties/chebi_roles.jsonl.gz \
        babel_downloads/CHEBI/labels
"""

import argparse
import csv
import gzip
import json
from collections import defaultdict

from src.createcompendia.chemicals import CHEBI_ROLE_ROOT
from src.predicates import HAS_ROLE

# How many roles the Markdown report lists. The full ranking goes to the CSV beside it; the report
# is an argument a person reads top to bottom, so it gets a sample rather than 1,377 rows.
REPORT_TOP_N = 25

# How many example chemicals to show per role. Spread across the role's chemicals rather than taken
# from the front, so the sample is not just "the lowest ChEBI identifiers".
EXAMPLES_PER_ROLE = 3


def read_labels(labels_filename):
    """Read a Babel labels file (CURIE\\tlabel) into a dict."""
    labels = {}
    with open(labels_filename) as f:
        for line in f:
            curie, _, label = line.rstrip("\n").partition("\t")
            labels.setdefault(curie, label)
    return labels


def read_roles(properties_filename):
    """Read the role property file into {role CURIE: sorted list of chemical CURIEs}."""
    chemicals_by_role = defaultdict(set)
    with gzip.open(properties_filename, "rt") as f:
        for line in f:
            prop = json.loads(line)
            if prop["predicate"] != HAS_ROLE:
                continue
            chemicals_by_role[prop["value"]].add(prop["curie"])
    return {role: sorted(chemicals) for role, chemicals in chemicals_by_role.items()}


def spread_examples(items, count):
    """Take `count` items spread evenly across `items`, rather than the first `count`."""
    if len(items) <= count:
        return items
    step = len(items) / count
    return [items[int(i * step)] for i in range(count)]


def describe(curie, labels):
    label = labels.get(curie)
    return f'`{curie}` "{label}"' if label else f"`{curie}`"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("properties", help="chebi_roles.jsonl.gz, as written by make_chebi_roles().")
    parser.add_argument("labels", help="babel_downloads/CHEBI/labels, for role and chemical names.")
    parser.add_argument("--report", default="docs/sources/CHEBI/roles/role_audit.md")
    parser.add_argument("--csv", default="docs/sources/CHEBI/roles/role_counts.csv")
    args = parser.parse_args()

    labels = read_labels(args.labels)
    chemicals_by_role = read_roles(args.properties)

    ranked = sorted(chemicals_by_role.items(), key=lambda kv: (-len(kv[1]), kv[0]))
    total_assertions = sum(len(chemicals) for chemicals in chemicals_by_role.values())
    chemicals_with_a_role = {c for chemicals in chemicals_by_role.values() for c in chemicals}

    with open(args.csv, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["role_curie", "role_label", "chemical_count"])
        for role, chemicals in ranked:
            writer.writerow([role, labels.get(role, ""), len(chemicals)])

    with open(args.report, "w") as f:
        f.write("# ChEBI role audit\n\n")
        f.write(f"Source file: `{args.properties}`\n\n")
        f.write(f"Roles are collected for descendants of {describe(CHEBI_ROLE_ROOT, labels)}.\n\n")
        f.write(f"- Role assertions: {total_assertions:,}\n")
        f.write(f"- Chemicals carrying at least one role: {len(chemicals_with_a_role):,}\n")
        f.write(f"- Distinct roles: {len(chemicals_by_role):,}\n\n")
        f.write(f"## The {REPORT_TOP_N} most-used roles\n\n")
        f.write("Full ranking in [`role_counts.csv`](./role_counts.csv).\n\n")
        f.write("| Role | Chemicals | Examples |\n| --- | ---: | --- |\n")
        for role, chemicals in ranked[:REPORT_TOP_N]:
            examples = ", ".join(describe(c, labels) for c in spread_examples(chemicals, EXAMPLES_PER_ROLE))
            f.write(f"| {describe(role, labels)} | {len(chemicals):,} | {examples} |\n")

    print(f"Wrote {args.report} and {args.csv}.")


if __name__ == "__main__":
    main()
