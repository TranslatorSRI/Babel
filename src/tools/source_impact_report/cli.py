"""CLI for the per-source impact report.

Invocation:
    uv run source-impact-report --source EMAPA
    uv run source-impact-report --source GARD --concord GARD --concord GARD_label

The CLI discovers where the source contributes (across one or more pipelines),
runs a synthetic re-glom with and without the source for each registered pipeline,
diffs the resulting cliques, scans the final compendia to see which type the source's
CURIEs ended up under, and writes a markdown (and optionally JSON) report.

See ``docs/sources/<SOURCE>/impact-report.md`` for the rendered output; ``--output``
overrides the path.
"""

from __future__ import annotations

import argparse
import datetime
import logging
import pathlib
import subprocess
import sys
from collections.abc import Callable, Iterator

import src.createcompendia.anatomy as anatomy
import src.createcompendia.diseasephenotype as diseasephenotype
from src.categories import (
    ANATOMICAL_ENTITY,
    CELL,
    CELLULAR_COMPONENT,
    DISEASE,
    GROSS_ANATOMICAL_STRUCTURE,
    PHENOTYPIC_FEATURE,
)
from src.model.compendium_diff import load_compendium
from src.model.glom_diff import (
    SourceImpactDiff,
    cliques_from_compendia,
    diff_cliques,
)
from src.model.source import SourceContribution, discover_source, scan_concords_for_curies, summarize_xref_groups
from src.reports.source_impact import (
    EXPANDED_SAMPLE_LIMIT,
    PURE_NEW_SAMPLE_LIMIT,
    SAMPLE_LIMIT,
    LookupContext,
    load_labels_for_prefixes,
    render_json,
    render_markdown,
)
from src.reports.source_impact_details import NEW_CLIQUES_CSV, NEW_XREFS_SUMMARY_CSV, write_detail_files
from src.util import get_config, get_logger

logger = get_logger(__name__)


# Signature of a pipeline's clique-computation function as registered in PIPELINE_CONFIG.
# Each implementation accepts ``(concordances, identifiers, *, excluded_sources=None)``
# and returns ``(clique_dict, types_dict)`` — a union-find clique mapping and a
# CURIE → declared biolink type map built from the ids files.  The CLI calls it twice
# per source (once with all inputs, once with the source excluded) to produce the
# synthetic before/after diff.
ComputeCliquesFn = Callable[..., tuple[dict, dict]]


# Per-pipeline configuration:
#
# - ``compute_fn``: returns ``(glom-dict, types-dict)`` for one call to glom over the
#   pipeline's intermediate files. Called twice per source (with and without the
#   source) to build the synthetic before/after diff.
# - ``compendium_files``: list of final compendium files to scan when counting how many
#   source CURIEs ended up where (after build).
# - ``compendium_prefixes``: prefixes that may appear in cliques for this pipeline;
#   labels for these are loaded from ``babel_downloads/<PREFIX>/labels`` to enrich the
#   rendered samples.
# - ``clique_classifier``: callable that picks a biolink type for one clique given the
#   types map. Used by the renderer to apply the right prefix-priority list when picking
#   the preferred CURIE for a sample.
# - ``biolink_types``: biolink types whose ``id_prefixes`` we look up to determine the
#   preferred CURIE per clique.
# The biolink types anatomy cliques can carry, in the named-constant form mandated by the
# repo conventions (never hard-code "biolink:..." strings). The compendium file names are
# derived from these (``<BareType>.txt``) so the two lists can never drift apart. They must
# also match config.yaml's ``anatomy_outputs`` (the same set, as file names, which is what
# Snakemake builds); test_compendium_files_match_config_outputs asserts that.
_ANATOMY_BIOLINK_TYPES = [ANATOMICAL_ENTITY, CELL, CELLULAR_COMPONENT, GROSS_ANATOMICAL_STRUCTURE]
# The biolink types disease/phenotype cliques can carry. As with anatomy, the compendium file
# names (``Disease.txt``, ``PhenotypicFeature.txt``) are derived from these so they can't drift,
# and the same test pins them to config.yaml's ``disease_outputs``.
_DISEASE_BIOLINK_TYPES = [DISEASE, PHENOTYPIC_FEATURE]

PIPELINE_CONFIG: dict[str, dict] = {
    "anatomy": {
        "compute_fn": anatomy.compute_cliques_for_impact_report,
        "compendium_files": [f"{bt.split(':')[-1]}.txt" for bt in _ANATOMY_BIOLINK_TYPES],
        "compendium_prefixes": get_config()["anatomy_prefixes"],
        "clique_classifier": anatomy.classify_anatomy_clique,
        "biolink_types": _ANATOMY_BIOLINK_TYPES,
    },
    "disease": {
        "compute_fn": diseasephenotype.compute_cliques_for_impact_report,
        "compendium_files": [f"{bt.split(':')[-1]}.txt" for bt in _DISEASE_BIOLINK_TYPES],
        "compendium_prefixes": get_config()["disease_labelsandsynonyms"],
        "clique_classifier": diseasephenotype.classify_disease_clique,
        "biolink_types": _DISEASE_BIOLINK_TYPES,
    },
}


def _list_source_files(directory: pathlib.Path) -> list[pathlib.Path]:
    """Return non-metadata files in a `concords/` or `ids/` directory."""
    if not directory.exists():
        return []
    return sorted(
        p
        for p in directory.iterdir()
        if p.is_file() and not p.name.startswith("metadata-") and not p.name.endswith(".yaml")
    )


def _compute_synthetic_diff(
    pipeline: str,
    source_name: str,
    intermediate_root: pathlib.Path,
    source_curies: frozenset[str],
) -> tuple[SourceImpactDiff, dict[str, str]]:
    """Run a registered compute_fn twice and return the clique diff for one pipeline.

    Returns a ``(diff, types)`` tuple where ``types`` is the after-state CURIE -> declared
    biolink type map (so callers can pass it into the renderer to classify cliques).
    """
    cfg = PIPELINE_CONFIG[pipeline]
    compute_fn: ComputeCliquesFn = cfg["compute_fn"]

    ids_dir = intermediate_root / pipeline / "ids"
    concords_dir = intermediate_root / pipeline / "concords"
    identifiers = [str(p) for p in _list_source_files(ids_dir)]
    concordances = [str(p) for p in _list_source_files(concords_dir)]

    logger.info("synthetic diff for %s: %d ids files, %d concord files", pipeline, len(identifiers), len(concordances))

    after_dicts, after_types = compute_fn(concordances, identifiers)
    before_dicts, _ = compute_fn(concordances, identifiers, excluded_sources={source_name})

    diff = diff_cliques(before_dicts, after_dicts, source_curies, babel_pipeline=pipeline)
    return diff, after_types


def _iter_pipeline_contributions(
    contribution: SourceContribution,
    pipelines: list[str],
) -> Iterator[tuple[str, dict, frozenset[str]]]:
    """Yield ``(pipeline, config, source_curies)`` for each in-scope pipeline.

    Skips pipelines that have no registered ``PIPELINE_CONFIG`` entry or no contribution
    from this source, so the per-pipeline consumers don't each re-derive that filter.
    """
    for st in pipelines:
        cfg = PIPELINE_CONFIG.get(st)
        if cfg is None:
            continue
        stc = contribution.by_pipeline.get(st)
        if stc is None:
            continue
        yield st, cfg, stc.all_curies


def _final_compendium_breakdown(
    contribution: SourceContribution,
    pipelines: list[str],
    compendia_root: pathlib.Path,
) -> dict[str, dict[str, int]]:
    """Count how many source CURIEs land in each compendium file per pipeline."""
    breakdown: dict[str, dict[str, int]] = {}
    for st, cfg, source_curies in _iter_pipeline_contributions(contribution, pipelines):
        per_file: dict[str, int] = {}
        for fname in cfg["compendium_files"]:
            path = compendia_root / fname
            if not path.exists():
                continue
            count = sum(
                len({ident["i"] for ident in clique.get("identifiers", [])} & source_curies)
                for clique in load_compendium(path)
            )
            per_file[fname] = count
        breakdown[st] = per_file
    return breakdown


def _remote_comparison_summary(
    contribution: SourceContribution,
    pipelines: list[str],
    remote_url: str,
    remote_cache_dir: pathlib.Path,
    compendia_root: pathlib.Path,
) -> dict[str, dict[str, int]]:
    """Download previous-build compendia and count cliques present/missing for this source.

    Returns a per-pipeline breakdown with keys ``remote_total_cliques``,
    ``remote_cliques_with_source_curies``, ``current_cliques_with_source_curies``,
    ``current_only_with_source_curies``, and ``remote_compendium_files_missing``. The
    "current only" count is the number of cliques in the current build that contain any
    source CURIE *and* are not present (by identifier-set) in the remote build — a useful
    first-pass estimate of net-new clique structure introduced by adding this source. Only
    compendium files whose remote counterpart was successfully fetched are compared, so a
    failed remote download reduces coverage (recorded in ``remote_compendium_files_missing``)
    rather than inflating ``current_only_with_source_curies``.
    """
    import requests  # imported lazily so synthetic-only runs don't need it

    remote_cache_dir.mkdir(parents=True, exist_ok=True)
    summary: dict[str, dict[str, int]] = {}
    base = remote_url.rstrip("/")

    for st, cfg, source_curies in _iter_pipeline_contributions(contribution, pipelines):
        remote_paths: list[pathlib.Path] = []
        current_paths: list[pathlib.Path] = []
        missing = 0
        for fname in cfg["compendium_files"]:
            current = compendia_root / fname
            if not current.exists():
                continue
            cached = remote_cache_dir / fname
            if not cached.exists():
                url = f"{base}/compendia/{fname}"
                logger.info("downloading remote compendium %s", url)
                with requests.get(url, stream=True, timeout=60) as resp:
                    if not resp.ok:
                        logger.warning("remote download failed for %s: %s %s", url, resp.status_code, resp.reason)
                        missing += 1
                        continue
                    with cached.open("wb") as out:
                        for chunk in resp.iter_content(chunk_size=64 * 1024):
                            out.write(chunk)
            # Only compare a current compendium against its remote counterpart when we
            # actually obtained that counterpart; otherwise a missing remote file would make
            # every current clique in it look "current only" and inflate the net-new estimate.
            remote_paths.append(cached)
            current_paths.append(current)

        remote_cliques = cliques_from_compendia(remote_paths) if remote_paths else frozenset()
        current_cliques = cliques_from_compendia(current_paths) if current_paths else frozenset()

        remote_with_source = sum(1 for c in remote_cliques if c & source_curies)
        current_with_source = 0
        current_only = 0
        for c in current_cliques:
            if c & source_curies:
                current_with_source += 1
                if c not in remote_cliques:
                    current_only += 1

        summary[st] = {
            "remote_total_cliques": len(remote_cliques),
            "remote_cliques_with_source_curies": remote_with_source,
            "current_cliques_with_source_curies": current_with_source,
            "current_only_with_source_curies": current_only,
            "remote_compendium_files_missing": missing,
        }

    return summary


def _scan_xref_rows(
    contribution: SourceContribution,
    intermediate_root: pathlib.Path,
) -> dict[str, list[tuple[str, str, str, str]]]:
    """Scan every pipeline's concord tree once for rows touching a source CURIE.

    Three consumers need these rows — the label-needed CURIE set, the markdown's join-pathway
    table, and the xref detail files — so the walk happens here rather than inside each of them.
    """
    return {
        st: scan_concords_for_curies(intermediate_root / st / "concords", source_curies)
        for st, _cfg, source_curies in _iter_pipeline_contributions(contribution, list(contribution.pipelines))
    }


def _curies_needing_labels(
    diffs_by_pipeline: dict[str, SourceImpactDiff],
    contribution: SourceContribution,
    xref_rows_by_pipeline: dict[str, list[tuple[str, str, str, str]]],
) -> set[str]:
    """Collect every CURIE whose label the report or detail files will render.

    This is the exact set ``load_labels_for_prefixes`` needs to keep, so it can stream the
    (potentially huge) per-prefix ``labels`` files once and discard everything else. It
    covers the members of every diffed clique (rendered in the markdown samples and the
    new-/modified-clique detail files) plus the concord-row endpoints in the xref detail files
    and the source's own CURIEs.
    """
    needed: set[str] = set()
    for diff in diffs_by_pipeline.values():
        for clique in diff.pure_new_cliques:
            needed.update(clique)
        for ec in diff.expanded_cliques:
            needed.update(ec.before_clique)
            needed.update(ec.after_clique)
        for mc in diff.merged_cliques:
            needed.update(mc.after_clique)
            for bc in mc.before_cliques:
                needed.update(bc)
    for _st, _cfg, source_curies in _iter_pipeline_contributions(contribution, list(contribution.pipelines)):
        needed.update(source_curies)
    for rows in xref_rows_by_pipeline.values():
        for subject, _predicate, obj, _asserted_by in rows:
            needed.add(subject)
            needed.add(obj)
    return needed


def _build_lookup_context(
    *,
    pipelines: list[str],
    types_by_pipeline: dict[str, dict[str, str]],
    downloads_root: pathlib.Path,
    skip_biolink: bool,
    needed_curies: set[str] | None = None,
) -> LookupContext:
    """Assemble the per-pipeline helpers the renderer needs to enrich CURIE samples.

    Loads label files for the prefixes registered for each pipeline, looks up the
    OBO PURL converter and the biolink prefix-priority lists, and registers the
    per-pipeline clique classifier callable. When ``skip_biolink`` is set, the
    Biolink prefix-map / toolkit lookups are skipped — useful for offline tests that
    don't need OBO PURLs or preferred-identifier annotations.
    """
    all_prefixes: set[str] = set()
    classifiers: dict[str, Callable] = {}
    biolink_types_needed: set[str] = set()
    for st in pipelines:
        cfg = PIPELINE_CONFIG.get(st)
        if cfg is None:
            continue
        all_prefixes.update(cfg.get("compendium_prefixes", []))
        if "clique_classifier" in cfg:
            classifiers[st] = cfg["clique_classifier"]
        biolink_types_needed.update(cfg.get("biolink_types", []))

    labels = load_labels_for_prefixes(sorted(all_prefixes), downloads_root, needed_curies)

    expander = None
    prefix_priority_by_type: dict[str, list[str]] = {}
    if not skip_biolink:
        # Lazy imports so --no-biolink-lookup avoids the network fetches these trigger.
        try:
            from src.util import get_biolink_prefix_map  # noqa: PLC0415

            converter = get_biolink_prefix_map()
            expander = converter.expand
        except Exception as exc:
            logger.warning("could not load Biolink prefix map; rendering plain CURIEs: %s", exc)
        try:
            from src.util import get_biolink_model_toolkit  # noqa: PLC0415

            tk = get_biolink_model_toolkit(get_config()["biolink_version"])
            for bt in biolink_types_needed:
                # ``get_element`` accepts the mapped form, the camel-case name, or the
                # human-readable name, but the toolkit returns a ClassDefinition object
                # (not a dict), so we read id_prefixes via attribute access.
                bare_name = bt.removeprefix("biolink:")
                element = tk.get_element(bare_name)
                if element is None:
                    continue
                prefs = getattr(element, "id_prefixes", None) or []
                prefix_priority_by_type[bt] = list(dict.fromkeys(prefs))
        except Exception as exc:
            logger.warning("could not load Biolink toolkit prefix orders: %s", exc)

    return LookupContext(
        types_by_pipeline=types_by_pipeline,
        labels_by_prefix=labels,
        curie_expander=expander,
        clique_classifier=classifiers,
        prefix_priority_by_type=prefix_priority_by_type,
    )


def _run_rumdl_fmt(path: pathlib.Path) -> None:
    """Apply ``rumdl fmt`` to the generated report so committed output is lint-clean.

    URL + label entries push individual list items past the repo's 100-char line cap;
    rather than having the renderer try to safely wrap inside markdown links, we
    delegate line wrapping to the project's existing markdown formatter. Failures
    (missing binary, unrelated error) only emit a warning so the report still lands
    on disk.
    """
    try:
        subprocess.run(["rumdl", "fmt", str(path)], check=False, capture_output=True)
    except FileNotFoundError:
        logger.warning(
            "rumdl not found on PATH; report may have line-length lint warnings. Run `uv run rumdl fmt %s` to fix.",
            path,
        )


def _git_commit_sha() -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            check=True,
            capture_output=True,
            text=True,
        )
        return result.stdout.strip()
    except (subprocess.CalledProcessError, FileNotFoundError):
        return "unknown"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="source_impact_report",
        description="Generate an impact report for adding a new Babel data source.",
    )
    parser.add_argument(
        "--source",
        required=True,
        help="Name of the source (matches the basename used in intermediate/<type>/{ids,concords}/<name>).",
    )
    parser.add_argument(
        "--concord",
        action="append",
        dest="concords",
        default=None,
        metavar="NAME",
        help="Basename of a concord file this source writes, repeatable. Default: the source name. "
        "Pass it when a source's concord is named something else -- GARD's label-match concord is "
        "GARD_label, so: --source GARD --concord GARD --concord GARD_label. Without it that concord "
        "is treated as another source's data and the report says zero cross-references were added. "
        "The names used are recorded in the report header.",
    )
    parser.add_argument(
        "--pipelines",
        default=None,
        help="Comma-separated list of pipelines to analyse. Default: auto-detect from filesystem.",
    )
    parser.add_argument(
        "--mode",
        choices=("synthetic", "remote", "both"),
        default="synthetic",
        help="Comparison mode (default: synthetic).",
    )
    parser.add_argument(
        "--remote-url",
        default=None,
        help="Base URL of a previous Babel build (e.g. "
        "https://stars.renci.org/var/babel/2025dec11/). Required for "
        "remote/both modes.",
    )
    parser.add_argument(
        "--remote-cache-dir",
        default="babel_downloads/remote_compendia",
        help="Where to cache downloaded remote compendia.",
    )
    parser.add_argument("--intermediate-root", default="babel_outputs/intermediate")
    parser.add_argument("--compendia-root", default="babel_outputs/compendia")
    parser.add_argument(
        "--downloads-root",
        default="babel_downloads",
        help="Root directory holding per-prefix label files for enriching sample "
        "CURIEs with preferred labels (default: babel_downloads).",
    )
    parser.add_argument(
        "--no-biolink-lookup",
        action="store_true",
        help="Skip Biolink prefix-map / model toolkit lookups; render samples without "
        "OBO PURL links and without preferred-identifier annotations.",
    )
    parser.add_argument(
        "--output", default=None, help="Output path for the report. Default: docs/sources/<SOURCE>/impact-report.md"
    )
    parser.add_argument(
        "--no-detail-files",
        action="store_true",
        help=f"Skip writing the CSV/JSON detail files ({NEW_CLIQUES_CSV}, {NEW_XREFS_SUMMARY_CSV} and the "
        "full new-cliques.csv / new-xrefs.csv / modified-cliques.{csv,json} they reduce) into the "
        "<output-stem>/ subdirectory beside the markdown report.",
    )
    parser.add_argument("--format", choices=("md", "json", "both"), default="md")
    parser.add_argument(
        "--sample-limit",
        type=int,
        default=None,
        help="Maximum number of sample merges to show per pipeline (default: 3).",
    )
    parser.add_argument(
        "--pure-new-sample-limit",
        type=int,
        default=None,
        help="Maximum number of sample pure-new cliques to show per pipeline (default: 3).",
    )
    parser.add_argument(
        "--expanded-sample-limit",
        type=int,
        default=None,
        help="Maximum number of sample expanded cliques to show per pipeline (default: 3).",
    )
    parser.add_argument("--verbose", "-v", action="store_true")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    # get_logger() (called at import) already installed the shared stderr handler/formatter
    # that Snakemake captures; here we only raise the verbosity when --verbose is passed.
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)

    sample_limit = args.sample_limit if args.sample_limit is not None else SAMPLE_LIMIT
    pure_new_sample_limit = (
        args.pure_new_sample_limit if args.pure_new_sample_limit is not None else PURE_NEW_SAMPLE_LIMIT
    )
    expanded_sample_limit = (
        args.expanded_sample_limit if args.expanded_sample_limit is not None else EXPANDED_SAMPLE_LIMIT
    )

    intermediate_root = pathlib.Path(args.intermediate_root)
    compendia_root = pathlib.Path(args.compendia_root)

    contribution = discover_source(args.source, intermediate_root, concord_names=args.concords)
    if not contribution.by_pipeline:
        logger.error(
            "no intermediate files found for source %r under %s",
            args.source,
            intermediate_root,
        )
        return 1

    if args.pipelines:
        requested = [s.strip() for s in args.pipelines.split(",") if s.strip()]
        unknown = [s for s in requested if s not in contribution.pipelines]
        if unknown:
            logger.error("source %r has no files for pipeline(s): %s", args.source, ", ".join(unknown))
            return 1
        pipelines = requested
    else:
        pipelines = sorted(contribution.pipelines)

    if (args.mode in ("remote", "both")) and not args.remote_url:
        logger.error("--remote-url is required for mode=%s", args.mode)
        return 1

    diffs_by_pipeline: dict[str, SourceImpactDiff] = {}
    types_by_pipeline: dict[str, dict[str, str]] = {}
    if args.mode in ("synthetic", "both"):
        for st in pipelines:
            if st not in PIPELINE_CONFIG:
                logger.warning(
                    "pipeline %r has no synthetic compute_fn registered; skipping clique diff for this type",
                    st,
                )
                continue
            stc = contribution.by_pipeline[st]
            diff, types = _compute_synthetic_diff(
                pipeline=st,
                source_name=args.source,
                intermediate_root=intermediate_root,
                source_curies=stc.all_curies,
            )
            diffs_by_pipeline[st] = diff
            types_by_pipeline[st] = types

    remote_summary: dict[str, dict[str, int]] = {}
    if args.mode in ("remote", "both"):
        remote_summary = _remote_comparison_summary(
            contribution=contribution,
            pipelines=pipelines,
            remote_url=args.remote_url,
            remote_cache_dir=pathlib.Path(args.remote_cache_dir),
            compendia_root=compendia_root,
        )

    final_breakdown = _final_compendium_breakdown(contribution, pipelines, compendia_root)

    # Scanned once here and threaded through: the label-needed set, the markdown's join-pathway
    # table and the two xref detail files all read the same rows.
    xref_rows_by_pipeline = _scan_xref_rows(contribution, intermediate_root)
    xref_groups = [
        group
        for st in sorted(xref_rows_by_pipeline)
        for group in summarize_xref_groups(xref_rows_by_pipeline[st], st, contribution.concord_names)
    ]

    lookup = _build_lookup_context(
        pipelines=pipelines,
        types_by_pipeline=types_by_pipeline,
        downloads_root=pathlib.Path(args.downloads_root),
        skip_biolink=args.no_biolink_lookup,
        needed_curies=_curies_needing_labels(diffs_by_pipeline, contribution, xref_rows_by_pipeline),
    )

    generated_at = datetime.datetime.now(datetime.UTC).strftime("%Y-%m-%d %H:%M:%S UTC")
    babel_commit = _git_commit_sha()

    if args.output:
        output_path = pathlib.Path(args.output)
    else:
        output_path = pathlib.Path("docs") / "sources" / args.source / "impact-report.md"

    output_path.parent.mkdir(parents=True, exist_ok=True)

    write_details = not args.no_detail_files
    # Detail files live in a subdirectory named after the report's stem, e.g.
    # docs/sources/EMAPA/impact-report/ beside docs/sources/EMAPA/impact-report.md.
    details_dirname = output_path.stem if write_details else None

    if args.format in ("md", "both"):
        md = render_markdown(
            contribution,
            diffs_by_pipeline,
            final_breakdown,
            mode=args.mode,
            generated_at=generated_at,
            babel_commit=babel_commit,
            remote_url=args.remote_url,
            remote_summary=remote_summary,
            lookup=lookup,
            details_dirname=details_dirname,
            xref_groups=xref_groups,
            sample_limit=sample_limit,
            pure_new_sample_limit=pure_new_sample_limit,
            expanded_sample_limit=expanded_sample_limit,
        )
        output_path.write_text(md)
        logger.info("wrote markdown report to %s", output_path)
        _run_rumdl_fmt(output_path)

    if write_details:
        details_dir = output_path.parent / details_dirname
        counts = write_detail_files(
            details_dir,
            contribution,
            diffs_by_pipeline,
            xref_rows_by_pipeline,
            xref_groups,
            lookup,
        )
        logger.info("wrote detail files to %s: %s", details_dir, counts)
    if args.format in ("json", "both"):
        json_payload = render_json(
            contribution,
            diffs_by_pipeline,
            final_breakdown,
            mode=args.mode,
            generated_at=generated_at,
            babel_commit=babel_commit,
            remote_url=args.remote_url,
            remote_summary=remote_summary,
            sample_limit=sample_limit,
            pure_new_sample_limit=pure_new_sample_limit,
            expanded_sample_limit=expanded_sample_limit,
        )
        json_path = output_path.with_suffix(".json")
        json_path.write_text(json_payload)
        logger.info("wrote json report to %s", json_path)

    return 0


if __name__ == "__main__":
    sys.exit(main())
