"""Characterize the DOIs and PMCIDs that more than one record of a pubmed2db export carries.

Two cliques must never share an identifier, so ``parse_pubmed2db_into_tsvs()`` keeps each shared identifier on
the *lowest* PMID that carries it. This script measures what that rule is choosing between. It streams every
record of the export through the production ``_read_shard_records()``, compares identifiers with the
production ``shared_identifier_key()`` (so a DOI is compared case-insensitively), and classifies each shared
identifier by the records carrying it:

- ``correspondence`` -- every record is typed ``Letter`` or ``Comment``: typically a journal's letters on one
  article, and the author's reply, published under a single DOI and indexed as separate PMIDs.
- ``same_title`` -- otherwise, every record has the same title (ignoring case and punctuation): usually one
  article indexed more than once, or a Cochrane review reissued under its unversioned DOI.
- ``other`` -- the rest: several articles published under one DOI (a symposium, a conference's preface and
  proceedings), and unrelated articles given the same DOI by mistake.

For each, it records whether the lowest PMID has the earliest ``pub_year`` (ties count), whether the highest
PMID's title is an author's reply ("... Reply."), whether the records spell the identifier in more than one
case, and whether it is a Cochrane review DOI -- the one kind where the highest PMID is the newer version.
Outputs:

- ``summary.json`` -- the counts, and a few examples of each kind, spread across the sorted list.
- ``shared_identifiers.csv`` -- every shared identifier, one row each.

Regenerate with (``--export-dir`` defaults to the download of the export pinned in config.yaml):

    uv run python docs/sources/PubMed/shared_identifiers/characterize_shared_identifiers.py [--export-dir DIR]

It makes two passes over the export; the first holds a set of every DOI and PMCID, as
``parse_pubmed2db_into_tsvs()`` does (~10 GB for a whole export). See "Identifiers shared by more than one PMID"
in docs/sources/PubMed/README.md for what the counts mean for Babel.
"""

import argparse
import csv
import json
import os
import re
from collections import Counter
from pathlib import Path

from src.createcompendia.publications import (
    _lower_pmid,
    _read_shard_records,
    expected_shard_counts,
    shared_identifier_key,
)
from src.util import get_config

EXAMPLES_PER_KIND = 3
CORRESPONDENCE_TYPES = {"Letter", "Comment"}
# The Cochrane Database of Systematic Reviews' DOI prefix. A review's unversioned DOI (…cd000036) can sit on
# every PMID the review was published under, oldest first.
COCHRANE_DOI_PREFIX = "doi:10.1002/14651858."
OUTPUT_DIR = Path(__file__).parent


def records(export_dir, shards):
    """Every record of the export, through the production reader."""
    for shard in shards:
        yield from _read_shard_records(os.path.join(export_dir, shard))


def pmid_number(pmid):
    return int(pmid.split(":")[1])


def normalized_title(title):
    return re.sub(r"[\W_]+", "", title.casefold())


def classify(carriers):
    """The kind (see the module docstring) of one shared identifier, from the records carrying it."""
    if all(set(c["publication_types"]) & CORRESPONDENCE_TYPES for c in carriers):
        return "correspondence"
    if len({normalized_title(c["title"]) for c in carriers}) == 1:
        return "same_title"
    return "other"


def describe(key, carriers):
    """One shared identifier as a row: its kind, its carriers in PMID order, and the two measurements."""
    carriers = sorted(carriers, key=lambda c: pmid_number(c["pmid"]))
    lowest, highest = carriers[0], carriers[-1]
    assert lowest["pmid"] == _lower_pmid(lowest["pmid"], highest["pmid"])
    years = [int(c["pub_year"]) for c in carriers if c["pub_year"].isdigit()]
    return {
        "identifier": key,
        "kind": classify(carriers),
        "records": len(carriers),
        "spellings": sorted({c["spelling"] for c in carriers}),
        "lowest_is_earliest": bool(years) and lowest["pub_year"].isdigit() and int(lowest["pub_year"]) == min(years),
        "highest_is_reply": highest["title"].rstrip(" .").casefold().endswith("reply"),
        "carriers": carriers,
    }


def main():
    config = get_config()
    default_export_dir = os.path.join(config["download_directory"], "PubMed2DB", config["pubmed2db_version"])
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--export-dir", default=default_export_dir, help="A downloaded pubmed2db export.")
    parser.add_argument("--output-dir", type=Path, default=OUTPUT_DIR)
    args = parser.parse_args()
    shards = list(expected_shard_counts(args.export_dir))

    # Pass 1: which keys more than one record carries.
    seen, duplicated, n_records = set(), set(), 0
    for _, _, _, others, _ in records(args.export_dir, shards):
        n_records += 1
        for other in others:
            key = shared_identifier_key(other)
            (duplicated if key in seen else seen).add(key)
    del seen

    # Pass 2: the records carrying them.
    carriers = {key: [] for key in duplicated}
    for record, pmid, title, others, _ in records(args.export_dir, shards):
        for other in others:
            key = shared_identifier_key(other)
            if key in carriers:
                carriers[key].append(
                    {
                        "pmid": pmid,
                        "spelling": other,
                        "pub_year": record["pub_year"],
                        "publication_types": sorted(t["name"] for t in record["publication_types"]),
                        "title": title,
                    }
                )
    rows = [describe(key, carriers[key]) for key in sorted(carriers)]

    kinds = Counter(row["kind"] for row in rows)
    summary = {
        # Shard names, not the local path: this output is committed, and may cover only part of an export.
        "shards": shards,
        "records": n_records,
        "shared_identifiers": len(rows),
        "by_prefix": dict(Counter(row["identifier"].split(":")[0] for row in rows)),
        "by_record_count": {str(n): c for n, c in sorted(Counter(row["records"] for row in rows).items())},
        "spelled_in_more_than_one_case": sum(len(row["spellings"]) > 1 for row in rows),
        "lowest_is_earliest": sum(row["lowest_is_earliest"] for row in rows),
        "highest_is_reply": sum(row["highest_is_reply"] for row in rows),
        "cochrane_reviews": sum(row["identifier"].startswith(COCHRANE_DOI_PREFIX) for row in rows),
        "kinds": {
            kind: {
                "count": kinds[kind],
                "lowest_is_earliest": sum(row["lowest_is_earliest"] for row in rows if row["kind"] == kind),
                "highest_is_reply": sum(row["highest_is_reply"] for row in rows if row["kind"] == kind),
                # Spread across the sorted list, not its head, which is one publisher's DOIs.
                "examples": spread([row for row in rows if row["kind"] == kind]),
            }
            for kind in ("correspondence", "same_title", "other")
        },
        "case_examples": spread([row for row in rows if len(row["spellings"]) > 1]),
    }
    args.output_dir.mkdir(parents=True, exist_ok=True)
    with open(args.output_dir / "summary.json", "w") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)
        f.write("\n")
    with open(args.output_dir / "shared_identifiers.csv", "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "identifier",
                "kind",
                "records",
                "spellings",
                "pmids",
                "pub_years",
                "lowest_is_earliest",
                "highest_is_reply",
            ]
        )
        for row in rows:
            writer.writerow(
                [
                    row["identifier"],
                    row["kind"],
                    row["records"],
                    " ".join(row["spellings"]),
                    " ".join(c["pmid"] for c in row["carriers"]),
                    " ".join(c["pub_year"] for c in row["carriers"]),
                    row["lowest_is_earliest"],
                    row["highest_is_reply"],
                ]
            )
    print(json.dumps({k: v for k, v in summary.items() if k not in ("kinds", "case_examples")}, indent=2))
    print(
        json.dumps(
            {
                k: {m: v[m] for m in ("count", "lowest_is_earliest", "highest_is_reply")}
                for k, v in summary["kinds"].items()
            }
        )
    )


def spread(rows):
    """Up to EXAMPLES_PER_KIND rows, evenly spaced through ``rows``, with titles cut short for reading."""
    if not rows:
        return []
    step = max(1, len(rows) // EXAMPLES_PER_KIND)
    return [
        {
            "identifier": row["identifier"],
            "carriers": [
                {
                    k: (c[k][:100] if k == "title" else c[k])
                    for k in ("pmid", "spelling", "pub_year", "publication_types", "title")
                }
                for c in row["carriers"]
            ],
        }
        for row in rows[::step][:EXAMPLES_PER_KIND]
    ]


if __name__ == "__main__":
    main()
