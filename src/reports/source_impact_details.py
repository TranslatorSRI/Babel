"""Detail-file writers for the source-impact report.

While ``src.reports.source_impact`` renders the human-readable markdown (and a summary
JSON capped at a few samples), this module writes the detail files an SME reviews in a PR —
one subdirectory (``<output-stem>/``) beside the markdown report:

- ``new-cliques-top-<N>.csv`` — the ``NEW_CLIQUES_TOP_N`` most review-worthy pure-new cliques
  the source introduces (the common case is a brand-new single-identifier clique; multi-member
  pure-new cliques carry ``member_count``). The **only capped file** here: see
  ``NEW_CLIQUES_TOP_N`` for why, and ``write_new_cliques_csv`` for the ranking. Re-run the tool
  for the full list.
- ``modified-cliques.json`` / ``modified-cliques.csv`` — every existing clique the source
  expands or merges. The JSON keeps the full before/after structure; the CSV has one row
  per source identifier landing in the clique, flagged ``added`` (structurally new) or
  ``preexisting`` (already pulled in via another source's xref, now a typed identifier).
- ``new-xrefs.csv`` — every cross-reference row touching a source CURIE, with the predicate
  (which for SSSOM-style sources distinguishes exact/close match) and the concord file that
  asserted it.
- ``new-xrefs-summary.csv`` — the committed aggregate of the above: one row per *join pathway*
  (predicate over a canonical prefix pair, per asserting concord file) with its total and a handful
  of example rows. See ``write_new_xrefs_summary_csv`` and ``src.model.source.summarize_xref_groups``.

All files are deterministically sorted so re-running the tool yields byte-identical output
(clean git diffs). The tool cannot see downstream Biolink-class prefix filtering directly,
so the clique/xref *counts* are an *upper bound* of what could land in the build — but the
``new-cliques-top-<N>.csv`` / ``modified-cliques.csv`` rows carry per-identifier survival columns
(``would_be_added`` / ``needs_biolink_registration``) that predict that filtering by
checking each identifier's prefix against the Biolink Model ``id_prefixes`` for the
*clique's* assigned biolink type (the single ``node_type`` ``NodeFactory.create_node()``
filters every member against), so a reviewer can see which "added" identifiers would
actually be emitted and which require a Biolink Model registration first.
"""

from __future__ import annotations

import csv
import json
import pathlib
from collections.abc import Collection, Iterable
from dataclasses import dataclass

from src.model.glom_diff import SourceImpactDiff
from src.model.source import SourceContribution, XrefGroup
from src.reports.source_impact import (
    LookupContext,
    biolink_registration_note,
    clique_leader,
    curie_label,
    preferred_curie,
    prefix_of,
    prefix_survives,
    sort_clique_for_display,
)

# Detail-file names, written inside the report's per-source subdirectory.
#
# Two of these are committed and four are generated locally and gitignored. The convention is that
# the *unqualified* name is the full table and the *qualified* name is the committed reduction:
#
#   new-cliques.csv          full, gitignored   <->  new-cliques-top-100.csv   committed
#   new-xrefs.csv            full, gitignored   <->  new-xrefs-summary.csv     committed
#   modified-cliques.{csv,json}                       gitignored, no reduction
#
# Both full tables run to hundreds of KB or more and go stale on the next build, so they are not
# worth a place in the diff; both reductions are permanent records of what the ingest looked like on
# first commit. The full tables are still always written, so "regenerate and upload for SME review"
# is a copy rather than a rebuild — see docs/AddingNewSources.md.
#
# The new-cliques reduction is a *ranked slice* (see write_new_cliques_csv) and embeds its limit in
# the filename so a reader knows it is a sample without opening it. The new-xrefs reduction is an
# *aggregate* (see write_new_xrefs_summary_csv), because what matters about a source's xrefs is which
# join pathways it opens, not which individual rows carry them.
NEW_CLIQUES_TOP_N = 100
NEW_CLIQUES_CSV = f"new-cliques-top-{NEW_CLIQUES_TOP_N}.csv"
NEW_CLIQUES_FULL_CSV = "new-cliques.csv"
MODIFIED_CLIQUES_JSON = "modified-cliques.json"
MODIFIED_CLIQUES_CSV = "modified-cliques.csv"
NEW_XREFS_SUMMARY_CSV = "new-xrefs-summary.csv"
NEW_XREFS_FULL_CSV = "new-xrefs.csv"

PIPE = "|"


@dataclass(frozen=True)
class _ModifiedClique:
    """Normalised view of one expanded-or-merged clique for the detail writers."""

    pipeline: str
    change_kind: str  # "expanded" | "merged"
    biolink_type: str | None
    after_clique: frozenset[str]
    before_cliques: tuple[frozenset[str], ...]
    added: frozenset[str]
    preexisting: frozenset[str]
    after_preferred: str
    before_preferred: str | None


def _biolink_type_for(clique, st, lookup: LookupContext) -> str | None:
    classifier = lookup.clique_classifier.get(st)
    types = lookup.types_by_pipeline.get(st, {})
    return classifier(clique, types) if classifier else None


def _modified_cliques(
    diffs: dict[str, SourceImpactDiff],
    lookup: LookupContext,
) -> list[_ModifiedClique]:
    """Flatten every expanded and merged clique across pipelines, sorted for output."""
    out: list[_ModifiedClique] = []
    for st in sorted(diffs):
        diff = diffs[st]
        for ec in diff.expanded_cliques:
            biolink_type = _biolink_type_for(ec.after_clique, st, lookup)
            out.append(
                _ModifiedClique(
                    pipeline=st,
                    change_kind="expanded",
                    biolink_type=biolink_type,
                    after_clique=ec.after_clique,
                    before_cliques=(ec.before_clique,),
                    added=ec.added_source_curies,
                    preexisting=ec.preexisting_source_curies,
                    after_preferred=preferred_curie(ec.after_clique, biolink_type, lookup.prefix_priority_by_type),
                    before_preferred=preferred_curie(ec.before_clique, biolink_type, lookup.prefix_priority_by_type),
                )
            )
        for mc in diff.merged_cliques:
            biolink_type = _biolink_type_for(mc.after_clique, st, lookup)
            before_union: frozenset[str] = frozenset().union(*mc.before_cliques)
            out.append(
                _ModifiedClique(
                    pipeline=st,
                    change_kind="merged",
                    biolink_type=biolink_type,
                    after_clique=mc.after_clique,
                    before_cliques=mc.before_cliques,
                    added=mc.source_curies_involved - before_union,
                    preexisting=mc.source_curies_involved & before_union,
                    after_preferred=preferred_curie(mc.after_clique, biolink_type, lookup.prefix_priority_by_type),
                    before_preferred=None,
                )
            )
    out.sort(key=lambda m: (m.pipeline, clique_leader(m.after_clique)))
    return out


def _fmt_bool(value: bool | None) -> str:
    """Render a tri-state survival flag for a CSV cell ("" when unknown)."""
    if value is None:
        return ""
    return "true" if value else "false"


def _write_rows(path: pathlib.Path, header: list[str], rows: Iterable[list], *, delimiter: str = ",") -> None:
    # lineterminator="\n" overrides csv's default "\r\n" so committed files use LF and stay
    # byte-identical across re-runs regardless of the platform's git autocrlf setting.
    with path.open("w", newline="") as f:
        writer = csv.writer(f, delimiter=delimiter, lineterminator="\n")
        writer.writerow(header)
        writer.writerows(rows)


def write_new_cliques_csv(
    path: pathlib.Path,
    diffs: dict[str, SourceImpactDiff],
    lookup: LookupContext,
    limit: int | None = NEW_CLIQUES_TOP_N,
) -> int:
    """Write one row per pure-new clique, ranked, and capped at *limit* rows (``None`` for all).

    The same ranking applies either way, so the committed capped file is always a prefix of the full
    one — the two calls in ``write_detail_files`` differ only in *limit*.

    Returns the number of rows actually written (so up to the cap, *not* the total number of
    pure-new cliques — the report's headline count comes from the diff, not from here).

    Survival columns predict downstream Biolink prefix filtering. ``preferred_id_would_survive``
    judges the preferred (highest-priority) identifier; ``needs_biolink_registration`` is set
    when that prefix is positively absent from the clique type's ``id_prefixes``. Because
    ``create_node`` keeps a clique as long as *any* member's prefix survives,
    ``unsupported_prefixes`` lists the member prefixes that would be dropped even if the clique
    itself survives — for a single-identifier pure-new clique (the common case) an unsupported
    prefix means the whole clique is dropped.
    """
    header = [
        "pipeline",
        "preferred_id",
        "preferred_label",
        "biolink_type",
        "member_count",
        "equivalent_ids",
        "preferred_id_would_survive",
        "needs_biolink_registration",
        "unsupported_prefixes",
    ]
    rows: list[list] = []
    for st in sorted(diffs):
        for clique in diffs[st].pure_new_cliques:
            biolink_type = _biolink_type_for(clique, st, lookup)
            ordered = sort_clique_for_display(clique, biolink_type, lookup.prefix_priority_by_type)
            preferred = ordered[0]
            would_survive, needs_reg = prefix_survives(preferred, biolink_type, lookup.prefix_priority_by_type)
            unsupported = sorted(
                {prefix_of(c) for c in clique if prefix_survives(c, biolink_type, lookup.prefix_priority_by_type)[1]}
            )
            rows.append(
                [
                    st,
                    preferred,
                    curie_label(preferred, lookup.labels_by_prefix) or "",
                    biolink_type or "",
                    len(clique),
                    PIPE.join(ordered),
                    _fmt_bool(would_survive),
                    _fmt_bool(needs_reg) if would_survive is not None else "",
                    PIPE.join(unsupported),
                ]
            )
    # Rank before capping, so the rows an SME must not miss are the ones kept.
    # Key, by column: preferred_id_would_survive ("false" = the Biolink prefix filter drops this
    # clique) first, then member_count descending (the structurally interesting merges — most
    # pure-new cliques are singletons), then (pipeline, preferred_id) so the retained set and its
    # git diff are stable across re-runs.
    rows.sort(key=lambda r: (r[6] != "false", -r[4], r[0], r[1]))
    if limit is not None:
        del rows[limit:]
    _write_rows(path, header, rows)
    return len(rows)


def write_modified_cliques_csv(
    path: pathlib.Path,
    diffs: dict[str, SourceImpactDiff],
    lookup: LookupContext,
) -> int:
    """Write one row per source identifier landing in a modified clique.

    Returns the number of (identifier) rows written. ``added_kind`` is ``added`` for a
    structurally-new identifier and ``preexisting`` for one that was already pulled into the
    clique via another source's cross-reference and is now a typed identifier.
    """
    header = [
        "pipeline",
        "clique_preferred_id",
        "clique_preferred_label",
        "clique_biolink_type",
        "change_kind",
        "added_kind",
        "added_id",
        "added_id_label",
        "added_id_biolink_type",
        "would_be_added",
        "needs_biolink_registration",
        "biolink_registration_note",
        "equivalent_ids",
    ]
    rows: list[list] = []
    for m in _modified_cliques(diffs, lookup):
        types = lookup.types_by_pipeline.get(m.pipeline, {})
        ordered = sort_clique_for_display(m.after_clique, m.biolink_type, lookup.prefix_priority_by_type)
        equivalent_ids = PIPE.join(ordered)
        preferred_label = curie_label(m.after_preferred, lookup.labels_by_prefix) or ""
        for added_kind, curie_set in (("added", m.added), ("preexisting", m.preexisting)):
            for curie in curie_set:
                # Judge survival on the *clique's* assigned biolink type, since that is the
                # single node_type NodeFactory.create_node() filters every member's prefix
                # against (not each identifier's own declared type). When the clique type is
                # unknown (no classifier), prefix_survives returns (None, False) -> blank.
                # added_id_biolink_type still records the identifier's own declared type for
                # context.
                own_type = types.get(curie)
                would_be_added, needs_reg = prefix_survives(curie, m.biolink_type, lookup.prefix_priority_by_type)
                note = biolink_registration_note(curie, m.biolink_type) if needs_reg else ""
                rows.append(
                    [
                        m.pipeline,
                        m.after_preferred,
                        preferred_label,
                        m.biolink_type or "",
                        m.change_kind,
                        added_kind,
                        curie,
                        curie_label(curie, lookup.labels_by_prefix) or "",
                        own_type or "",
                        _fmt_bool(would_be_added),
                        _fmt_bool(needs_reg) if would_be_added is not None else "",
                        note,
                        equivalent_ids,
                    ]
                )
    rows.sort(key=lambda r: (r[0], r[1], r[6]))
    _write_rows(path, header, rows)
    return len(rows)


def write_modified_cliques_json(
    path: pathlib.Path,
    diffs: dict[str, SourceImpactDiff],
    lookup: LookupContext,
) -> int:
    """Write the full before/after structure of every modified clique. Returns the count."""

    def members(clique: frozenset[str]) -> list[dict]:
        return [{"i": curie, "label": curie_label(curie, lookup.labels_by_prefix)} for curie in sorted(clique)]

    entries: list[dict] = []
    for m in _modified_cliques(diffs, lookup):
        types = lookup.types_by_pipeline.get(m.pipeline, {})
        # Per-added-identifier survival, judged on the *clique's* assigned biolink type —
        # the single node_type create_node() filters every member's prefix against. The
        # ``declared_biolink_type`` field records the identifier's own declared type for
        # context. The flat added/preexisting lists are kept for back-compat; this enriches them.
        added_curie_details = []
        for kind, curie_set in (("added", m.added), ("preexisting", m.preexisting)):
            for curie in sorted(curie_set):
                own_type = types.get(curie)
                would_be_added, needs_reg = prefix_survives(curie, m.biolink_type, lookup.prefix_priority_by_type)
                added_curie_details.append(
                    {
                        "i": curie,
                        "added_kind": kind,
                        "declared_biolink_type": own_type,
                        "clique_biolink_type": m.biolink_type,
                        "would_be_added": would_be_added,
                        "needs_biolink_registration": needs_reg if would_be_added is not None else None,
                        "note": biolink_registration_note(curie, m.biolink_type) if needs_reg else None,
                    }
                )
        added_curie_details.sort(key=lambda d: d["i"])
        entries.append(
            {
                "pipeline": m.pipeline,
                "change_kind": m.change_kind,
                "biolink_type": m.biolink_type,
                "preferred_id_before": m.before_preferred,
                "preferred_id_after": m.after_preferred,
                "before_clique_leaders": sorted(clique_leader(bc) for bc in m.before_cliques),
                "before_cliques": [sorted(bc) for bc in m.before_cliques],
                "added_source_curies": sorted(m.added),
                "preexisting_source_curies": sorted(m.preexisting),
                "added_curie_details": added_curie_details,
                "after_members": members(m.after_clique),
            }
        )
    path.write_text(json.dumps(entries, indent=2, sort_keys=True) + "\n")
    return len(entries)


def write_new_xrefs_csv(
    path: pathlib.Path,
    xref_rows_by_pipeline: dict[str, list[tuple[str, str, str, str]]],
    own_concords: Collection[str],
    lookup: LookupContext,
) -> int:
    """Write one row per concord row touching a source CURIE, across all concord files.

    This is the **full** table, generated locally and gitignored; ``write_new_xrefs_summary_csv``
    writes the aggregate that gets committed. *xref_rows_by_pipeline* holds the already-scanned
    ``scan_concords_for_curies`` output, keyed by pipeline, so the concord tree is walked once per
    report rather than once per consumer.

    ``status`` reflects *which* concord file asserts the row, not before/after novelty
    (this writer does not diff the pre-source glom state, so it cannot tell whether the
    row newly becomes a clique edge): ``added`` means one of *own_concords* -- the files
    ``discover_source`` was told belong to this source --
    asserts it (a brand-new bridge), and ``from_other_source`` means another source's
    concord file asserts a row that happens to touch a source CURIE — such a row may have
    already existed before this source was added. Returns the number of rows written.
    """
    header = [
        "pipeline",
        "subject",
        "subject_label",
        "predicate",
        "object",
        "object_label",
        "asserted_by",
        "status",
    ]
    rows: list[list] = []
    for st in sorted(xref_rows_by_pipeline):
        for subject, predicate, obj, asserted_by in xref_rows_by_pipeline[st]:
            status = "added" if asserted_by in own_concords else "from_other_source"
            rows.append(
                [
                    st,
                    subject,
                    curie_label(subject, lookup.labels_by_prefix) or "",
                    predicate,
                    obj,
                    curie_label(obj, lookup.labels_by_prefix) or "",
                    asserted_by,
                    status,
                ]
            )
    rows.sort(key=lambda r: (r[0], r[1], r[4], r[6]))
    _write_rows(path, header, rows)
    return len(rows)


def write_new_xrefs_summary_csv(
    path: pathlib.Path,
    groups: Iterable[XrefGroup],
    lookup: LookupContext,
) -> int:
    """Write the committed xref aggregate: one row per example, group columns repeated.

    The long layout (rather than one row per group with ``example_1..example_n`` columns) keeps the
    file a flat table that GitHub renders and sorts, reuses the ``subject``/``object`` column
    vocabulary of the full table, and avoids nesting quoted example strings inside quoted CSV
    fields. The aggregate view is this table deduplicated on its first seven columns; ``xref_count``
    is the group's total, so it repeats across that group's example rows.

    Note that ``subject``/``object`` keep the orientation the concord file wrote, which may not match
    the sorted ``prefix_1``/``prefix_2`` order — ``asserted_by`` is what says which side asserted the
    mapping. Returns the number of rows written.
    """
    header = [
        "pipeline",
        "predicate",
        "prefix_1",
        "prefix_2",
        "asserted_by",
        "status",
        "xref_count",
        "subject",
        "subject_label",
        "object",
        "object_label",
    ]
    rows: list[list] = []
    for group in groups:
        for subject, obj in group.examples:
            rows.append(
                [
                    group.pipeline,
                    group.predicate,
                    group.prefix_1,
                    group.prefix_2,
                    group.asserted_by,
                    group.status,
                    group.count,
                    subject,
                    curie_label(subject, lookup.labels_by_prefix) or "",
                    obj,
                    curie_label(obj, lookup.labels_by_prefix) or "",
                ]
            )
    # summarize_xref_groups already orders groups (biggest pathway first) and their examples, so the
    # rows are emitted in that order rather than re-sorted here.
    _write_rows(path, header, rows)
    return len(rows)


def write_detail_files(
    details_dir: pathlib.Path,
    contribution: SourceContribution,
    diffs: dict[str, SourceImpactDiff],
    xref_rows_by_pipeline: dict[str, list[tuple[str, str, str, str]]],
    xref_groups: Iterable[XrefGroup],
    lookup: LookupContext,
) -> dict[str, int]:
    """Write all six detail files into ``details_dir``; return a {filename: row_count} map.

    The two committed reductions and their four locally-generated companions — see the filename
    constants at the top of this module for which is which.
    """
    details_dir.mkdir(parents=True, exist_ok=True)
    return {
        NEW_CLIQUES_CSV: write_new_cliques_csv(details_dir / NEW_CLIQUES_CSV, diffs, lookup),
        NEW_CLIQUES_FULL_CSV: write_new_cliques_csv(details_dir / NEW_CLIQUES_FULL_CSV, diffs, lookup, limit=None),
        MODIFIED_CLIQUES_CSV: write_modified_cliques_csv(details_dir / MODIFIED_CLIQUES_CSV, diffs, lookup),
        MODIFIED_CLIQUES_JSON: write_modified_cliques_json(details_dir / MODIFIED_CLIQUES_JSON, diffs, lookup),
        NEW_XREFS_SUMMARY_CSV: write_new_xrefs_summary_csv(details_dir / NEW_XREFS_SUMMARY_CSV, xref_groups, lookup),
        NEW_XREFS_FULL_CSV: write_new_xrefs_csv(
            details_dir / NEW_XREFS_FULL_CSV, xref_rows_by_pipeline, contribution.concord_names, lookup
        ),
    }
