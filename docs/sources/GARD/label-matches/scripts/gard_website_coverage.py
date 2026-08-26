"""Why the GARD website browses 6,265 diseases while the list Babel ingests holds 16,214.

The question this answers is whether the ~9,949 terms with no page are deprecated entries Babel
should be filtering out. They are not: the CSV's own ``URL`` column marks which terms have a public
page, the partition is exact, and the page-less majority is overwhelmingly mapped by MONDO.

Three checks, all of them cheap and all of them re-runnable against next month's download:

1. Are the website's ids a subset of the CSV's, and how do the two sets sit by id range?
2. Does the CSV's ``URL`` column separate them? (It does, with no exceptions either way.)
3. Are the page-less terms corroborated by a curated ontology, or orphaned?

The website's browsable set is read from ``/assets/diseases.trimmed.json``, the single data asset
its single-page app loads -- there is no public GARD API. Note that ``?gard_id=`` returns HTTP 200
for any value, since the app serves the same shell either way, so a status code cannot stand in for
check 2.

Run from the repo root against a disease build (needs network for the website asset):

    uv run python docs/sources/GARD/label-matches/scripts/gard_website_coverage.py

Last result (2026-08-26, "GARD Rare Disease List Jun2026.csv"): 6,265 browsable of 16,214 published,
partitioned exactly by the URL column; 9,677 of the 9,949 page-less terms are mapped by MONDO.
"""

import collections
import csv
import json
import sys
import urllib.request

from src.datahandlers.gard import normalize_gard_curie
from src.util import get_config

# The single data asset the GARD single-page app loads; it *is* the browsable set.
WEBSITE_ASSET = "https://rarediseases.info.nih.gov/assets/diseases.trimmed.json"
ID_BUCKET = 5000  # id-range granularity for the "newer, not retired" table


def fetch_website_ids(url=WEBSITE_ASSET):
    """The GARD ids the website can browse, as unpadded CURIEs."""
    with urllib.request.urlopen(url) as response:
        payload = json.load(response)
    return {normalize_gard_curie(f"GARD:{entry['id']}") for entry in payload}


def main(gard_csv=None, concords=None):
    config = get_config()
    gard_csv = gard_csv or f"{config['download_directory']}/GARD/gard.csv"
    concords = concords or f"{config['intermediate_directory']}/disease/concords"

    website = fetch_website_ids()
    with open(gard_csv, encoding="utf-8-sig") as inf:
        rows = {normalize_gard_curie(row["ID"]): row for row in csv.DictReader(inf)}

    print(f"website browsable: {len(website):,}   published CSV: {len(rows):,}")
    print(f"  on the website but not in the CSV: {len(website - set(rows))}")
    print(f"  in the CSV but not on the website: {len(set(rows) - website):,}")

    # 1. Where the page-less terms sit by id range: high ids mean newer, not retired.
    print("\nid range          browsable   page-less")
    ranges = collections.Counter()
    for curie in rows:
        local = int(curie.split(":")[1])
        ranges[(local // ID_BUCKET * ID_BUCKET, curie in website)] += 1
    for start in sorted({start for start, _ in ranges}):
        label = f"{start:,}-{start + ID_BUCKET - 1:,}"
        print(f"  {label:15s} {ranges[(start, True)]:>8,}   {ranges[(start, False)]:>9,}")

    # 2. The URL column is GARD's own "has a public page" marker.
    url_column = collections.Counter((curie in website, bool(row["URL"].strip())) for curie, row in rows.items())
    print("\n                        URL set   URL empty")
    for browsable, label in ((True, "browsable"), (False, "page-less")):
        print(f"  {label:20s} {url_column[(browsable, True)]:>8,}   {url_column[(browsable, False)]:>9,}")
    exact = url_column[(True, False)] == 0 and url_column[(False, True)] == 0
    print(f"  -> URL column partitions the two sets exactly: {exact}")

    # 3. Corroboration: a term MONDO maps is a current disease, not a retired registry entry.
    claimed = {}
    for name in ("MONDO_GARD", "DOID", "GARD_label"):
        with open(f"{concords}/{name}") as inf:
            claimed[name] = {c for line in inf for c in line.rstrip("\n").split("\t") if c.startswith("GARD:")}

    def source_of(curie):
        for name, label in (("MONDO_GARD", "MONDO"), ("DOID", "DOID"), ("GARD_label", "label match")):
            if curie in claimed[name]:
                return label
        return "nothing"

    corroboration = collections.Counter((curie in website, source_of(curie)) for curie in rows)
    kinds = ("MONDO", "DOID", "label match", "nothing")
    print(f"\n{'':22s}" + "".join(f"{k:>14s}" for k in kinds))
    for browsable, label in ((True, "browsable"), (False, "page-less")):
        counts = [corroboration[(browsable, k)] for k in kinds]
        print(f"  {label:20s}" + "".join(f"{n:>14,}" for n in counts))
    page_less = sum(corroboration[(False, k)] for k in kinds)
    print(f"  -> MONDO maps {corroboration[(False, 'MONDO')] / page_less:.1%} of the page-less terms")
    return len(website), len(rows)


if __name__ == "__main__":
    main(*sys.argv[1:])
