"""Trace how a finished build's duplicated CURIEs reached each of the compendia they sit in.

Two build reports say *that* a CURIE landed in more than one compendium:

- ``reports/umls/duplicate-curies.csv`` (written by ``leftover_umls``) -- every UMLS CUI found in
  more than one (compendium, clique) pair, with its semantic types and each occurrence's leader.
- ``reports/duckdb/duplicate_curies.tsv`` (rule ``check_for_duplicate_curies``) -- every CURIE of
  any prefix that appears under more than one clique leader in the Parquet ``Edge`` tables.

Neither says *how* it got there, and the concords that fed the build cannot answer clique
questions on their own (see AGENTS.md, "Debugging"). This module joins the duplicate lists to the
build's intermediate-file exports -- ``Identifier.parquet`` (one row per ids-file entry) and
``Concord.parquet`` (one row per cross-reference edge), see ``docs/DataFormats.md`` -- to say,
for each duplicated CURIE, which pipelines' ids files claimed it and which concord files pulled it
in. The Parquet files are read in place from the published build over HTTPS (DuckDB ``httpfs``
with predicate pushdown), so it works on a machine with no local build: about 6 s for
``Identifier.parquet`` and 100 s for ``Concord.parquet`` against the 2026jul22 exports.

The report is scoped to one compendium pair -- by default ``Protein`` against the chemical
compendia (``config.yaml: chemical_outputs``), the pair behind Babel issues #308 and #513 -- and
covers both the UMLS CUIs and the MeSH descriptors shared across that pair, since the MeSH half
turned out to be thirty times larger than the UMLS half in 2026jul22
(``docs/sources/UMLS/ProteinChemicalDuplicates.md``).

Regenerate the committed 2026jul22 analysis with:

    uv run python -m src.reports.duplicate_curie_routes --build 2026jul22 \\
        --output docs/sources/UMLS/protein-chemical-duplicates/2026jul22.md
"""

import argparse
import csv
import os
import re
import textwrap
from collections import Counter, defaultdict

import duckdb
import requests

from src.util import ensure_parent_dir, get_config, get_logger

logger = get_logger(__name__)

# Where deployed builds are published; ``<base>/<build>/`` mirrors that build's ``babel_outputs/``.
DEFAULT_BASE_URL = "https://stars.renci.org/var/babel_outputs"

# The two duplicate reports this module reads, relative to a build directory.
UMLS_DUPLICATES_REPORT = "reports/umls/duplicate-curies.csv"
DUCKDB_DUPLICATES_REPORT = "reports/duckdb/duplicate_curies.tsv"

# ``occurrences`` in the UMLS report looks like
# ``Protein.txt[biolink:Protein, leader=UMLS:C0000578, name=5-hydroxytryptophan]; SmallMolecule.txt[...]``.
# Names can contain anything, so match up to the leader only.
_OCCURRENCE_RE = re.compile(r"(\w+)\.txt\[biolink:\w+, leader=([^,\]]+), name=")

# How many example edges to show per concord file in the rendered report.
SAMPLE_EDGES_PER_FILE = 3


def chemical_compendia():
    """The chemical compendium names (``ChemicalEntity``, ``SmallMolecule``, ...) from ``config.yaml``."""
    return {os.path.splitext(name)[0] for name in get_config()["chemical_outputs"]}


def fetch_build_file(build_dir, relative_path, base_url, build):
    """Return the local path of ``relative_path`` under ``build_dir``, downloading it from the build if absent."""
    local = os.path.join(build_dir, relative_path)
    if not os.path.exists(local):
        url = f"{base_url}/{build}/{relative_path}"
        logger.info(f"Downloading {url} to {local}")
        response = requests.get(url, timeout=120)
        response.raise_for_status()
        ensure_parent_dir(local)
        with open(local, "wb") as f:
            f.write(response.content)
    return local


def parse_umls_occurrences(occurrences):
    """Parse the UMLS report's ``occurrences`` column into ``{compendium: clique_leader}``."""
    return {compendium: leader for compendium, leader in _OCCURRENCE_RE.findall(occurrences)}


def load_umls_duplicates(path):
    """Read ``reports/umls/duplicate-curies.csv``; adds a parsed ``compendia`` dict to every row."""
    with open(path, newline="") as f:
        rows = list(csv.DictReader(f))
    for row in rows:
        row["compendia"] = parse_umls_occurrences(row["occurrences"])
    return rows


def load_duckdb_duplicates(path):
    """Read ``reports/duckdb/duplicate_curies.tsv``; adds parsed ``compendia`` and ``leaders`` lists to every row.

    The ``filenames`` and ``clique_leaders`` columns are Python-list reprs written by DuckDB
    (``[SmallMolecule, Protein]``, ``['CHEBI:17780', 'UMLS:C0000578']``).
    """
    with open(path, newline="") as f:
        rows = list(csv.DictReader(f, delimiter="\t"))
    for row in rows:
        row["compendia"] = re.findall(r"[A-Za-z]+", row["filenames"])
        row["leaders"] = re.findall(r"'([^']+)'", row["clique_leaders"])
    return rows


def spans_pair(compendia, side_a, side_b):
    """True when ``compendia`` (an iterable of compendium names) includes ``side_a`` and one of ``side_b``."""
    names = set(compendia)
    return side_a in names and bool(names & side_b)


def _prefix(curie):
    return curie.split(":", 1)[0]


def _relative_intermediate_path(filename):
    """``/abs/path/babel_outputs/intermediate/protein/ids/UMLS`` -> ``protein/ids/UMLS``."""
    return re.sub(r"^.*/intermediate/", "", filename)


def trace_routes(curie_sets, identifier_parquet, concord_parquet, connection=None):
    """Look up every CURIE in ``curie_sets`` in a build's ``Identifier.parquet`` and ``Concord.parquet``.

    :param curie_sets: ``{label: set_of_curies}``; each set is traced separately but all are looked
        up in a single pass over each Parquet file, which matters when the files are remote.
    :param identifier_parquet: path or URL of the build's ``Identifier.parquet``.
    :param concord_parquet: path or URL of the build's ``Concord.parquet``.
    :param connection: an existing DuckDB connection (tests pass one); a new in-memory one otherwise.
    :return: ``{label: {"curies": set, "ids_files": Counter(file -> CURIEs listed), "concord_files": Counter(file -> edges),
        "concord_curies": {file: set(CURIEs)}, "touched": {curie: set(files)}, "sample_edges": {file: [(subj, pred, obj)]}}}``
    """
    con = connection or duckdb.connect()
    if any(str(p).startswith(("http://", "https://")) for p in (identifier_parquet, concord_parquet)):
        con.execute("INSTALL httpfs; LOAD httpfs;")

    con.execute("CREATE OR REPLACE TEMP TABLE probe(label VARCHAR, curie VARCHAR)")
    con.executemany(
        "INSERT INTO probe VALUES (?, ?)", [(label, curie) for label, curies in curie_sets.items() for curie in curies]
    )
    results = {
        label: {
            "curies": set(curies),
            "ids_files": Counter(),
            "concord_files": Counter(),
            "concord_curies": defaultdict(set),
            "touched": defaultdict(set),
            "sample_edges": defaultdict(list),
        }
        for label, curies in curie_sets.items()
    }

    logger.info(f"Looking up {sum(len(c) for c in curie_sets.values())} CURIEs in {identifier_parquet}")
    ids_rows = con.execute(
        f"""SELECT p.label, i.filename, i.curie
            FROM read_parquet('{identifier_parquet}') i JOIN probe p ON i.curie = p.curie"""
    ).fetchall()
    for label, filename, curie in ids_rows:
        results[label]["ids_files"][_relative_intermediate_path(filename)] += 1

    logger.info(f"Looking up the same CURIEs in {concord_parquet}")
    concord_rows = con.execute(
        f"""SELECT filename, subj, pred, obj
            FROM read_parquet('{concord_parquet}')
            WHERE subj IN (SELECT curie FROM probe) OR obj IN (SELECT curie FROM probe)"""
    ).fetchall()
    for filename, subj, pred, obj in concord_rows:
        relative = _relative_intermediate_path(filename)
        for label, curies in curie_sets.items():
            hits = {c for c in (subj, obj) if c in curies}
            if not hits:
                continue
            r = results[label]
            r["concord_files"][relative] += 1
            r["concord_curies"][relative] |= hits
            for curie in hits:
                r["touched"][curie].add(relative)
            if len(r["sample_edges"][relative]) < SAMPLE_EDGES_PER_FILE:
                r["sample_edges"][relative].append((subj, pred, obj))
    return results


def analyze(umls_rows, duckdb_rows, identifier_parquet, concord_parquet, side_a, side_b, connection=None):
    """Select the ``side_a``-vs-``side_b`` duplicates from both reports and trace their routes.

    Returns a dict of everything ``render_report()`` needs, so the numbers can be asserted directly.
    """
    umls_pairs = Counter(tuple(sorted(row["compendia"])) for row in umls_rows)
    umls_selected = [row for row in umls_rows if spans_pair(row["compendia"], side_a, side_b)]
    side_a_leaders = Counter(_prefix(row["compendia"][side_a]) for row in umls_selected)
    side_b_leaders = Counter(
        _prefix(leader) for row in umls_selected for name, leader in row["compendia"].items() if name in side_b
    )
    tui_sets = Counter(row["tui_set_labels"] for row in umls_selected)

    mesh_selected = [
        row for row in duckdb_rows if _prefix(row["curie"]) == "MESH" and spans_pair(row["compendia"], side_a, side_b)
    ]
    duckdb_by_prefix = Counter(
        _prefix(row["curie"]) for row in duckdb_rows if spans_pair(row["compendia"], side_a, side_b)
    )

    routes = trace_routes(
        {"umls": {row["umls_curie"] for row in umls_selected}, "mesh": {row["curie"] for row in mesh_selected}},
        identifier_parquet,
        concord_parquet,
        connection=connection,
    )
    return {
        "side_a": side_a,
        "side_b": sorted(side_b),
        "umls_total": len(umls_rows),
        "umls_pairs": umls_pairs,
        "umls_selected": umls_selected,
        "side_a_leaders": side_a_leaders,
        "side_b_leaders": side_b_leaders,
        "tui_sets": tui_sets,
        "duckdb_by_prefix": duckdb_by_prefix,
        "mesh_selected": mesh_selected,
        "routes": routes,
    }


def _side_of(path, side_a, side_b_dirs):
    """Which side of the pair an intermediate path like ``chemicals/concords/DrugCentral`` belongs to."""
    top = path.split("/", 1)[0]
    if top == side_a.lower():
        return "a"
    if top in side_b_dirs:
        return "b"
    return None


def render_report(analysis, build, identifier_parquet, concord_parquet):
    """Render ``analyze()``'s result as Markdown."""
    side_a, side_b = analysis["side_a"], analysis["side_b"]
    # Intermediate directory names for side B: the chemical compendia all come from ``chemicals/``.
    side_b_dirs = {"chemicals"} if set(side_b) & chemical_compendia() else {b.lower() for b in side_b}
    umls_selected, mesh_selected, routes = analysis["umls_selected"], analysis["mesh_selected"], analysis["routes"]
    side_b_label = "the chemical compendia" if set(side_b) == chemical_compendia() else ", ".join(side_b)
    lines = [
        f"# Duplicate CURIE routes: {side_a} vs {side_b_label} in {build}",
        "",
        _wrap(
            f"Generated by `src/reports/duplicate_curie_routes.py` from `{UMLS_DUPLICATES_REPORT}`, "
            f"`{DUCKDB_DUPLICATES_REPORT}`, `{identifier_parquet}` and `{concord_parquet}`."
        ),
        "",
        "## UMLS CUIs in more than one compendium",
        "",
        f"{analysis['umls_total']} UMLS CUIs are in two or more compendia; by compendium pair:",
        "",
        "| Compendia | CUIs |",
        "|---|---:|",
    ]
    lines += [f"| {' + '.join(pair)} | {n} |" for pair, n in analysis["umls_pairs"].most_common()]
    lines += [
        "",
        f"### The {len(umls_selected)} CUIs shared between {side_a} and {side_b_label}",
        "",
        _wrap(f"Clique-leader prefix on the {side_a} side: {_counts(analysis['side_a_leaders'])}."),
        "",
        _wrap(f"Clique-leader prefix on the other side: {_counts(analysis['side_b_leaders'])}."),
        "",
        "| UMLS semantic types | CUIs |",
        "|---|---:|",
    ]
    # tui_set_labels joins the semantic types with '|', which a Markdown table cell cannot hold.
    lines += [f"| {t.replace('|', '; ')} | {n} |" for t, n in analysis["tui_sets"].most_common(10)]
    lines += _render_routes(routes["umls"], "CUIs", side_a, side_b_dirs)
    lines += [
        "",
        f"## MeSH descriptors shared between {side_a} and {side_b_label}",
        "",
        _wrap(
            f"`{DUCKDB_DUPLICATES_REPORT}` lists every CURIE in more than one clique. Those spanning {side_a} "
            f"and {side_b_label}, by prefix: {_counts(analysis['duckdb_by_prefix'])}."
        ),
        "",
        f"### The {len(mesh_selected)} MeSH descriptors",
    ]
    lines += _render_routes(routes["mesh"], "descriptors", side_a, side_b_dirs)
    return "\n".join(lines) + "\n"


def _counts(counter):
    return ", ".join(f"{key} {n}" for key, n in counter.most_common())


def _wrap(paragraph):
    """Wrap a prose paragraph at the repository's 100-column Markdown line length (rumdl MD013)."""
    return textwrap.fill(paragraph, width=100, break_long_words=False, break_on_hyphens=False)


def _render_routes(route, noun, side_a, side_b_dirs):
    lines = [
        "",
        "Which ids files claimed them (an identifier can be listed by several pipelines' ids files):",
        "",
        f"| ids file | {noun} |",
        "|---|---:|",
    ]
    lines += [f"| `{f}` | {n} |" for f, n in route["ids_files"].most_common()]
    lines += [
        "",
        "Which concord files have an edge touching them:",
        "",
        f"| concord file | edges | distinct {noun} |",
        "|---|---:|---:|",
    ]
    lines += [f"| `{f}` | {n} | {len(route['concord_curies'][f])} |" for f, n in route["concord_files"].most_common()]
    touched = route["touched"]
    via = {
        "a": sum(1 for files in touched.values() if any(_side_of(f, side_a, side_b_dirs) == "a" for f in files)),
        "b": sum(1 for files in touched.values() if any(_side_of(f, side_a, side_b_dirs) == "b" for f in files)),
    }
    untouched = len(route["curies"] - set(touched))
    lines += [
        "",
        f"Touched by a `{side_a.lower()}/` concord: {via['a']}; by a `{'/'.join(sorted(side_b_dirs))}/` concord:"
        f" {via['b']}; by no concord at all: {untouched}.",
        "",
        "Example edges per concord file:",
        "",
    ]
    for f, _ in route["concord_files"].most_common():
        for subj, pred, obj in route["sample_edges"][f]:
            lines.append(f"- `{f}`: `{subj}` {pred} `{obj}`")
    return lines


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--build", default="2026jul22", help="Build name on the publication server (default 2026jul22)."
    )
    parser.add_argument(
        "--base-url", default=DEFAULT_BASE_URL, help=f"Publication server (default {DEFAULT_BASE_URL})."
    )
    parser.add_argument(
        "--build-dir",
        help="Local directory holding (or receiving) the build's report files; default data/<build>.",
    )
    parser.add_argument("--identifier-parquet", help="Override the Identifier.parquet path or URL.")
    parser.add_argument("--concord-parquet", help="Override the Concord.parquet path or URL.")
    parser.add_argument("--side-a", default="Protein", help="Compendium on one side of the pair (default Protein).")
    parser.add_argument(
        "--side-b",
        nargs="*",
        help="Compendia on the other side (default: the chemical compendia from config.yaml chemical_outputs).",
    )
    parser.add_argument("--output", help="Write the Markdown report here instead of stdout.")
    args = parser.parse_args(argv)

    build_dir = args.build_dir or os.path.join("data", args.build)
    umls_report = fetch_build_file(build_dir, UMLS_DUPLICATES_REPORT, args.base_url, args.build)
    duckdb_report = fetch_build_file(build_dir, DUCKDB_DUPLICATES_REPORT, args.base_url, args.build)
    identifier_parquet = args.identifier_parquet or f"{args.base_url}/{args.build}/duckdb/Identifier.parquet"
    concord_parquet = args.concord_parquet or f"{args.base_url}/{args.build}/duckdb/Concord.parquet"
    side_b = set(args.side_b) if args.side_b else chemical_compendia()

    analysis = analyze(
        load_umls_duplicates(umls_report),
        load_duckdb_duplicates(duckdb_report),
        identifier_parquet,
        concord_parquet,
        args.side_a,
        side_b,
    )
    report = render_report(analysis, args.build, identifier_parquet, concord_parquet)
    if args.output:
        ensure_parent_dir(args.output)
        with open(args.output, "w") as f:
            f.write(report)
        logger.info(f"Wrote {args.output}")
    else:
        print(report)  # noqa: T201


if __name__ == "__main__":
    main()
