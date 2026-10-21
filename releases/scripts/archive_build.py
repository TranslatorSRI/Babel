#!/usr/bin/env python3
"""Archive a build's summary reports into ``releases/<build>/``.

A finished build directory is hundreds of gigabytes and lives on a cluster that will eventually be
cleaned. A small part of it -- the summary tables, the per-compendium content reports, and the
provenance metadata -- is what people actually read afterwards, and what the next release is
compared against. This copies that subset (~420 KB) into the repository.

**The archive mirrors the build directory's own layout**, so every relative path is the same on both
sides and ``releases/2026jul22`` is itself a usable ``--build-dir``::

    uv run python releases/scripts/draft_release_notes.py 2026jul22 --build-dir releases/2026jul22

which is what keeps a note re-draftable years after the build directory is gone. Adding a
translation layer between "path in a build" and "path in the archive" would buy nothing and cost
that property, so don't.

What is deliberately *not* archived: ``benchmarks/`` and ``reports/slurm/`` (operational, only
useful while sizing that run), ``logs/``, ``reports/umls/`` and the loose ``reports/<Type>.txt``
(diagnostics rather than summaries), and ``reports/duckdb/*.tsv{,.gz}`` -- one of which is 200 MB,
which is why :data:`MAX_FILE_MB` exists. See ``releases/ARTIFACTS.md``.

Deliberately imports nothing from ``src/``: this is release engineering, not pipeline code (the same
reasoning as ``draft_release_notes.py``).
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]

# Fixed-name files every healthy build has exactly one of. A missing one means the directory is not
# a complete build, so archiving refuses outright rather than committing a half-populated release.
REQUIRED_FILES = [
    "reports/tables/prefix_table.csv",
    "reports/tables/cliques_table.csv",
    "reports/tables/mapping_sources_table.csv",
    "reports/tables/prefix_comparison.md",
    "reports/tables/prefix_comparison_overall.csv",
    "reports/tables/prefix_comparison_by_clique_prefix.csv",
    "reports/content/compendia_report.json",
    "reports/duckdb/prefix_report.json",
    # Builds before 2026jul22 lack it, but those are archived already; every build since has it.
    "reports/umls_mesh_skew.json",
]

# Per-compendium files, globbed rather than listed: the set follows config.yaml's compendium list,
# and enumerating that here would mean importing from src/.
GLOB_PATTERNS = ["metadata/*.yaml", "reports/content/compendia/*.json"]

# The largest archived file today is ~50 KB. The cap is not tuned to that -- it is a backstop for a
# widened glob or an upstream format change: `reports/duckdb/identically_labeled_cliques.tsv.gz` is
# 200 MB and sits one directory away from a manifest entry.
MAX_FILE_MB = 5.0

# The prefix report carries the release name it was built under, and it is the field that labels the
# baseline in the *next* release's comparison. See check_prefix_report_name().
PREFIX_REPORT = "reports/duckdb/prefix_report.json"


def build_name_for(release: str, manifest_path: Path) -> str:
    """Resolve a release id from ``releases.yaml`` to its build name, or pass a build name through.

    Archives are named by build, not release id, because for the older Translator-named releases the
    two differ (id ``TranslatorFuguJuly2024``, build ``2024jul13``) and every artifact -- including
    ``config.yaml``'s ``previous_release`` pin -- already uses the build name.
    """
    releases = yaml.safe_load(manifest_path.read_text())["releases"]
    for entry in releases:
        if entry["id"] == release:
            build = entry.get("build")
            if not build:
                raise ValueError(f"Release {release!r} records no build in {manifest_path}; nothing to archive.")
            return str(build)
    known_builds = {str(entry.get("build")) for entry in releases}
    if release in known_builds:
        return release
    raise ValueError(f"{release!r} is neither a release id nor a build name in {manifest_path}.")


def resolve_manifest(build_dir: Path) -> list[str]:
    """The build-relative paths to archive, sorted.

    :raises FileNotFoundError: if any of :data:`REQUIRED_FILES` is absent, naming all of them -- one
        report at a time would mean re-running the whole thing per missing file.
    :raises ValueError: if either glob matches nothing, which means the build shape has changed and
        the manifest needs revisiting rather than silently archiving less.
    """
    missing = [name for name in REQUIRED_FILES if not (build_dir / name).is_file()]
    if missing:
        raise FileNotFoundError(f"{build_dir} is missing {len(missing)} required file(s): " + ", ".join(missing))

    paths = list(REQUIRED_FILES)
    for pattern in GLOB_PATTERNS:
        matched = sorted(str(p.relative_to(build_dir)) for p in build_dir.glob(pattern))
        if not matched:
            raise ValueError(f"{build_dir} has no files matching {pattern!r}; has the build layout changed?")
        paths.extend(matched)
    return sorted(set(paths))


def check_prefix_report_name(build_dir: Path, build: str) -> None:
    """Fail unless the prefix report's ``name`` is the build being archived.

    ``name`` is stamped from ``config.yaml``'s ``release_name`` when the build runs, so a run that
    started before the pin was moved carries the *previous* build's name -- and that field is what
    labels the baseline in the next release's comparison, so a wrong value propagates forward
    silently. The 2026jul22 build shipped stamped ``2026jul15`` for exactly this reason. Checking it
    here makes it a hard failure instead of a step someone has to remember.

    :raises ValueError: if the field disagrees with ``build``.
    """
    stamped = json.loads((build_dir / PREFIX_REPORT).read_text()).get("name")
    if stamped != build:
        raise ValueError(
            f"{build_dir / PREFIX_REPORT} is stamped name={stamped!r}, but this is the {build!r} "
            f"archive. `release_name` in config.yaml was probably not moved to {build!r} before the "
            f"build ran. Correct the field (and check which release the value names) before archiving."
        )


def plan_copy(build_dir: Path, paths: list[str], max_bytes: int) -> tuple[list[str], list[str]]:
    """Split the manifest into what will be copied and what is over the size cap."""
    oversize = [p for p in paths if (build_dir / p).stat().st_size > max_bytes]
    return [p for p in paths if p not in set(oversize)], oversize


def copy_files(build_dir: Path, dest_dir: Path, paths: list[str]) -> int:
    """Copy each build-relative path into ``dest_dir``, overwriting. Returns the bytes written."""
    total = 0
    for path in paths:
        target = dest_dir / path
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(build_dir / path, target)
        total += target.stat().st_size
    return total


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("release", help="Release id or build name, e.g. 2026jul22 (as in releases/releases.yaml).")
    parser.add_argument("--build-dir", type=Path, required=True, help="The build's output directory, or a copy.")
    parser.add_argument(
        "--dest-root",
        type=Path,
        default=REPO_ROOT / "releases",
        help="Where release directories live (default: releases/ in the repo root).",
    )
    parser.add_argument(
        "--manifest", type=Path, default=None, help="Path to releases.yaml (default: releases/releases.yaml)."
    )
    parser.add_argument(
        "--max-file-size-mb",
        type=float,
        default=MAX_FILE_MB,
        help=f"Refuse to archive any file larger than this (default: {MAX_FILE_MB}).",
    )
    parser.add_argument("--dry-run", action="store_true", help="Report what would be copied; write nothing.")
    args = parser.parse_args(argv)

    if not args.build_dir.is_dir():
        parser.error(f"--build-dir {args.build_dir} is not a directory")

    try:
        build = build_name_for(args.release, args.manifest or REPO_ROOT / "releases" / "releases.yaml")
        paths = resolve_manifest(args.build_dir)
        # Pre-flight, before anything is written: a failure here must leave no partial archive.
        check_prefix_report_name(args.build_dir, build)
    except (FileNotFoundError, ValueError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1

    to_copy, oversize = plan_copy(args.build_dir, paths, int(args.max_file_size_mb * 1000 * 1000))
    for path in oversize:
        size_mb = (args.build_dir / path).stat().st_size / 1e6
        print(f"warning: skipping {path} ({size_mb:.1f} MB, over the {args.max_file_size_mb} MB cap)", file=sys.stderr)

    dest_dir = args.dest_root / build
    total = sum((args.build_dir / p).stat().st_size for p in to_copy)
    if args.dry_run:
        print(f"Would copy {len(to_copy)} files ({total / 1000:.0f} KB) into {dest_dir}:")
        for path in to_copy:
            print(f"  {path}")
    else:
        total = copy_files(args.build_dir, dest_dir, to_copy)
        print(f"Archived {len(to_copy)} files ({total / 1000:.0f} KB) into {dest_dir}.", file=sys.stderr)

    # Non-zero when anything was left out, so a widened glob or a grown file is noticed rather than
    # quietly producing a smaller archive than the manifest promises.
    return 1 if oversize else 0


if __name__ == "__main__":
    sys.exit(main())
