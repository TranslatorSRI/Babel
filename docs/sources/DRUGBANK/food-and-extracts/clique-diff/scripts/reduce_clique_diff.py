#!/usr/bin/env python
"""Reduce a ``babel-clique-diff`` CSV to the artifacts committed beside this script.

A whole-pipeline clique diff is too big to commit: the PR #948 run over all eight
``chemical_outputs`` produced 31,567 rows / 9.2 MB, of which 27,290 were near-identical
PubChem leader flips. ``docs/sources/CLAUDE.md`` ("Commit a ranked sample, not a huge derived
table" and "Aggregate when rows are not the unit of interest") says what to keep instead, and
this script is that reduction, kept so the slices can be re-derived and so the *next* big diff
can be reduced the same way::

    python reduce_clique_diff.py path/to/clique-diff.csv --out-dir .

It writes, into ``--out-dir``:

- ``<compendium>-moves.csv`` per compendium that lost cliques to another file — every ``moved``
  row when the group fits under ``--cap``, otherwise the top ``--cap`` ranked rows in a file
  named for its cap (``drug-moves-top-100.csv``).
- ``leader-churn[-top-N].csv`` — the ``leader_changed`` rows, same treatment.

and prints the aggregate tables that belong in the prose page rather than in a CSV: the
per-compendium kind breakdown, the type transitions, the leader prefix pairs, the clique-size
distribution, a member-prefix census, and the label-emptiness counts. **Redirect that output to
``reduce_clique_diff.txt`` beside the slices and commit it** — as ``replay_type_vote.txt`` already
is in the parent directory — because those aggregates are computed over the *full* CSV, which is
not committed, so without the capture they stop being checkable once it is deleted.

**Ranking before truncation matters** (same reason as ``write_new_cliques_csv``): a blind
``rows[:N]`` over a CURIE-sorted file keeps an arbitrary identifier range and hides exactly the
rows the artifact exists to surface. So:

- ``moved`` rows are ranked with the *unusual destination first* — a compendium that received
  only a handful of cliques is where a mis-typing would hide, and the 4,269-row Drug group has
  exactly one such row (a vaccine that went to ``ComplexMolecularMixture`` while 4,268 siblings
  went to ``ChemicalEntity``) — then by clique size descending, then by CURIE.
- ``leader_changed`` rows are ranked with the rows whose *label also changed* first, because
  those are the ones where the flip changes the name a user sees, then by clique size
  descending, then by CURIE.

This is documentation, not production code: it is meant to be rewritten by whoever next needs
it, and per ``docs/sources/CLAUDE.md`` it deliberately has no unit tests.
"""

import argparse
import collections
import csv
import pathlib
import re

# Rows to keep when a group is too big to commit whole. 100 matches the source-impact report's
# NEW_CLIQUES_TOP_N, so the two committed samples are read the same way.
DEFAULT_CAP = 100


def read_rows(path):
    """Read a clique-diff CSV, returning (rows, fieldnames)."""
    with open(path, newline="") as fh:
        reader = csv.DictReader(fh)
        return list(reader), reader.fieldnames


def write_slice(rows, fieldnames, out_dir, stem, cap):
    """Write ``rows`` to ``out_dir``, capped and named for the cap when it truncates.

    Returns the path written. The name carries the cap (``-top-100``) only when rows were
    actually dropped, so a reader can tell a complete artifact from a sample without opening it.
    """
    truncated = len(rows) > cap
    name = f"{stem}-top-{cap}.csv" if truncated else f"{stem}.csv"
    path = pathlib.Path(out_dir) / name
    with open(path, "w", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows[:cap])
    print(f"  {path.name}: {min(len(rows), cap):,} of {len(rows):,} rows")
    return path


def rank_moved(rows):
    """Rank ``moved`` rows so a rare destination compendium sorts first.

    A destination that received only a few cliques is where a bad retype hides; the bulk
    destination is by definition the expected case. Ties break on clique size (bigger cliques
    affect more identifiers) then CURIE, so the output is stable across runs.
    """
    per_destination = collections.Counter(r["destination_compendium"] for r in rows)
    return sorted(
        rows,
        key=lambda r: (per_destination[r["destination_compendium"]], -int(r["before_size"]), r["before_leader"]),
    )


def rank_leader_churn(rows):
    """Rank ``leader_changed`` rows so a flip that also changed the label sorts first.

    A flip between two identically-labelled identifiers is invisible to a reader of the output;
    one that changes the label changes the name a consumer sees, so it is the row worth showing.
    """
    return sorted(
        rows,
        key=lambda r: (
            r["before_leader_label"] == r["destination_label"],
            -int(r["before_size"]),
            r["before_leader"],
        ),
    )


def prefix(curie):
    """The prefix of a CURIE, or the whole string if it has no colon."""
    return curie.split(":", 1)[0]


def print_aggregates(rows):
    """Print the tables that belong in the prose page rather than in a committed CSV."""
    print("\n## Rows by compendium and kind\n")
    for (compendium, kind), n in sorted(
        collections.Counter((r["compendium"], r["destination_kind"]) for r in rows).items()
    ):
        print(f"  {compendium:28} {kind:16} {n:,}")

    print("\n## Type transitions\n")
    for (compendium, before, after), n in sorted(
        collections.Counter((r["compendium"], r["before_leader_type"], r["destination_type"]) for r in rows).items()
    ):
        print(f"  {compendium:28} {before} -> {after}: {n:,}")

    print("\n## Leader prefix pairs\n")
    for (compendium, before, after), n in sorted(
        collections.Counter(
            (r["compendium"], prefix(r["before_leader"]), prefix(r["destination"])) for r in rows
        ).items()
    ):
        print(f"  {compendium:28} {before} -> {after}: {n:,}")

    print("\n## Clique sizes (before_size), by kind\n")
    for kind in sorted({r["destination_kind"] for r in rows}):
        sizes = collections.Counter(int(r["before_size"]) for r in rows if r["destination_kind"] == kind)
        print(f"  {kind}: " + ", ".join(f"{size}={n:,}" for size, n in sorted(sizes.items())))

    # example_members lists at most five members, so this census is only exhaustive for a group whose
    # cliques are all that small -- which is why the largest before_size is printed beside it. For the
    # #948 Drug group (max 3) it is a complete answer to "does any demoted clique hold a structural
    # identifier?"; for a group with bigger cliques, read it as a sample.
    print("\n## Member prefixes seen in example_members (<=5 members shown per row)\n")
    for compendium in sorted({r["compendium"] for r in rows}):
        group = [r for r in rows if r["compendium"] == compendium]
        prefixes = collections.Counter(
            prefix(m) for r in group for m in re.findall(r'([^ ;]+) "', r["example_members"])
        )
        biggest = max(int(r["before_size"]) for r in group)
        census = ", ".join(f"{p}={n:,}" for p, n in prefixes.most_common()) or "(none listed)"
        print(f"  {compendium:28} largest clique={biggest}: {census}")

    print("\n## Integrity checks\n")
    size_mismatch = [r for r in rows if r["before_size"] != r["after_size"] or r["before_size"] != r["member_count"]]
    print(f"  rows where before_size != after_size or != member_count: {len(size_mismatch):,}")
    print(f"  rows with an empty before_leader_label: {sum(1 for r in rows if not r['before_leader_label']):,}")
    print(f"  rows with an empty destination_label:   {sum(1 for r in rows if not r['destination_label']):,}")
    leaders = [r["before_leader"] for r in rows]
    print(f"  distinct before_leader values: {len(set(leaders)):,} of {len(leaders):,} rows")
    churn = [r for r in rows if r["destination_kind"] == "leader_changed"]
    relabelled = sum(1 for r in churn if r["before_leader_label"] != r["destination_label"])
    print(f"  leader_changed rows whose label also changed: {relabelled:,} of {len(churn):,}")


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("csv_path", type=pathlib.Path, help="A babel-clique-diff --out-csv file.")
    parser.add_argument("--out-dir", type=pathlib.Path, default=pathlib.Path("."), help="Where to write the slices.")
    parser.add_argument("--cap", type=int, default=DEFAULT_CAP, help=f"Rows per slice (default {DEFAULT_CAP}).")
    args = parser.parse_args()

    rows, fieldnames = read_rows(args.csv_path)
    print(f"Read {len(rows):,} change rows from {args.csv_path}.\n")

    print("Wrote:")
    moved = [r for r in rows if r["destination_kind"] == "moved"]
    for compendium in sorted({r["compendium"] for r in moved}):
        group = [r for r in moved if r["compendium"] == compendium]
        stem = pathlib.Path(compendium).stem.lower() + "-moves"
        write_slice(rank_moved(group), fieldnames, args.out_dir, stem, args.cap)

    churn = [r for r in rows if r["destination_kind"] == "leader_changed"]
    if churn:
        write_slice(rank_leader_churn(churn), fieldnames, args.out_dir, "leader-churn", args.cap)

    print_aggregates(rows)


if __name__ == "__main__":
    main()
