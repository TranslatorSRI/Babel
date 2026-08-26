"""Discover how a Babel source contributes to the build outputs.

Given a source name like "EMAPA", walks every
``<intermediate_root>/<pipeline>/ids/<name>`` and
``<intermediate_root>/<pipeline>/concords/<name>`` and assembles a structured
description that the source-impact report tool can render.

A Babel source can vary along three axes simultaneously, so every aggregate is a
collection:

- multiple pipelines (e.g., MESH contributes to anatomy, chemical, disease)
- multiple biolink types within a pipeline (UBERON declares both
  ``biolink:AnatomicalEntity`` and ``biolink:GrossAnatomicalStructure`` in one ids file)
- multiple prefixes per ids file (rare but supported)
"""

from __future__ import annotations

import pathlib
from collections import defaultdict
from collections.abc import Collection, Iterable, Sequence
from dataclasses import dataclass
from functools import cached_property


def _prefix_of(curie: str) -> str:
    return curie.split(":", 1)[0]


def scan_concords_for_curies(
    concords_dir: pathlib.Path | str,
    source_curies: Iterable[str],
) -> list[tuple[str, str, str, str]]:
    """Scan every concord file in a directory tree for rows touching any source CURIE.

    Returns ``(subject, predicate, object, asserted_by)`` tuples where ``asserted_by`` is
    the path of the concord file relative to ``concords_dir`` — the source that *declared*
    the cross-reference. A source's cross-references frequently live in *another* source's
    concord file (e.g. EMAPA's own concord is empty, but UBERON's concord carries
    ``UBERON:… xref EMAPA:…`` rows), so this scans every file in the directory tree rather
    than only the source's own concord. Subdirectories are included (e.g.
    ``chemicals/concords/UNICHEM/UNICHEM_*`` files). Metadata sidecars
    (``metadata-*`` / ``*.yaml``) are skipped.
    """
    concords_dir = pathlib.Path(concords_dir)
    source_set = frozenset(source_curies)
    rows: list[tuple[str, str, str, str]] = []
    if not concords_dir.exists():
        return rows
    for path in sorted(concords_dir.rglob("*")):
        if not path.is_file() or path.name.startswith("metadata-") or path.name.endswith(".yaml"):
            continue
        asserted_by = str(path.relative_to(concords_dir))
        with path.open() as f:
            for line in f:
                parts = line.rstrip("\n").split("\t")
                if len(parts) < 3:
                    continue
                subject, predicate, obj = parts[0], parts[1], parts[2]
                if subject in source_set or obj in source_set:
                    rows.append((subject, predicate, obj, asserted_by))
    return rows


# Number of example xrefs kept per join pathway in the committed summary. Sized so that even a
# source with many partner prefixes stays small: the row count is bounded by
# examples_per_group x distinct pathways, which is independent of how many xrefs the source has
# (EMAPA's 4,336 rows are one pathway). 10 is enough for a reviewer to judge whether a pathway
# asserts equivalence or an "is about" relation, which is what the xref audit in
# docs/AddingNewSources.md asks of it.
# ponytail: no cap on the *number* of pathways — enumerating them all is the point of the file. A
# source with hundreds of nested concord files (UNICHEM-style) could reach a few thousand rows; if
# that happens, rank groups by count and cap, logging how many were dropped.
XREF_EXAMPLES_PER_GROUP = 10


@dataclass(frozen=True)
class XrefGroup:
    """One join pathway: a predicate over a canonical prefix pair, asserted by one concord file.

    ``prefix_1``/``prefix_2`` are **sorted**, so the pair does not depend on which side of the
    relation a given concord file happened to write — matching how ``src/metadata/provenance.py``
    keys its metadata counts (``xref(CHEBI, DrugCentral)``). Direction is not lost: ``asserted_by``
    names the file that made the assertion, and each example keeps its subject/object as written.
    That is why ``asserted_by`` and ``status`` are part of the group identity and not derived from
    the pair — ``MP -> HP`` asserted by MP (a new bridge) and ``HP -> MP`` asserted by HP (a mapping
    that already existed) share a prefix pair but are different facts, and collapsing them would
    hide the distinction the impact report exists to draw.
    """

    pipeline: str
    predicate: str
    prefix_1: str
    prefix_2: str
    asserted_by: str
    status: str
    count: int
    # (subject, object) as written in the concord. Labels are attached by callers, since they live in
    # the report's LookupContext and src/model must not depend on src/reports.
    examples: tuple[tuple[str, str], ...]


def summarize_xref_groups(
    rows: Iterable[tuple[str, str, str, str]],
    pipeline: str,
    own_concords: Collection[str],
    examples_per_group: int = XREF_EXAMPLES_PER_GROUP,
) -> list[XrefGroup]:
    """Group scanned concord rows into join pathways with counts and example rows.

    *rows* are ``(subject, predicate, object, asserted_by)`` tuples as returned by
    ``scan_concords_for_curies``. ``status`` is ``added`` when the row comes from one of
    *own_concords* -- the concord files this source writes, which ``discover_source`` was told
    about -- and ``from_other_source`` otherwise, the same test the detail-file writer applies.

    Returned groups are sorted with the biggest pathway first, so a reader sees the dominant join
    route before the long tail. Identical triples asserted by two different files are deliberately
    *not* deduplicated: they are two assertions and land in two groups.
    """
    grouped: dict[tuple[str, str, str, str, str], list[tuple[str, str]]] = defaultdict(list)
    for subject, predicate, obj, asserted_by in rows:
        prefix_1, prefix_2 = sorted((_prefix_of(subject), _prefix_of(obj)))
        status = "added" if asserted_by in own_concords else "from_other_source"
        grouped[(predicate, prefix_1, prefix_2, asserted_by, status)].append((subject, obj))

    groups = [
        XrefGroup(
            pipeline=pipeline,
            predicate=predicate,
            prefix_1=prefix_1,
            prefix_2=prefix_2,
            asserted_by=asserted_by,
            status=status,
            count=len(pairs),
            examples=_pick_examples(pairs, examples_per_group),
        )
        for (predicate, prefix_1, prefix_2, asserted_by, status), pairs in grouped.items()
    ]
    groups.sort(key=lambda g: (-g.count, g.prefix_1, g.prefix_2, g.asserted_by, g.status, g.predicate))
    return groups


def _pick_examples(pairs: list[tuple[str, str]], limit: int) -> tuple[tuple[str, str], ...]:
    """Pick up to *limit* examples spread evenly across *pairs*, sorted for determinism.

    Even spacing rather than the first *limit* rows, so the examples span the source's identifier
    range instead of clustering on its lowest IDs (UBERON's first ten xrefs to EMAPA are all
    ``UBERON:00000xx``). The cost is that adding one xref upstream re-strides the whole sample, so
    the committed rows churn more than a ``[:limit]`` slice would — an acceptable trade in a file
    that is ten rows long and regenerated wholesale whenever the source is refreshed.
    """
    ordered = sorted(pairs)
    if len(ordered) <= limit:
        return tuple(ordered)
    step = len(ordered) / limit
    return tuple(ordered[int(i * step)] for i in range(limit))


@dataclass
class PipelineContribution:
    """One source's contribution within a single babel_pipeline directory."""

    pipeline: str
    ids_path: pathlib.Path | None
    concords_paths: list[pathlib.Path]

    @cached_property
    def _ids_rows(self) -> list[tuple[str, str | None]]:
        if self.ids_path is None or not self.ids_path.exists():
            return []
        rows: list[tuple[str, str | None]] = []
        with self.ids_path.open() as f:
            for line in f:
                parts = line.rstrip("\n").split("\t")
                if not parts or not parts[0]:
                    continue
                curie = parts[0]
                declared_type = parts[1] if len(parts) > 1 and parts[1] else None
                rows.append((curie, declared_type))
        return rows

    @cached_property
    def all_curies(self) -> frozenset[str]:
        return frozenset(curie for curie, _ in self._ids_rows)

    @cached_property
    def curies_by_prefix(self) -> dict[str, frozenset[str]]:
        buckets: dict[str, set[str]] = defaultdict(set)
        for curie, _ in self._ids_rows:
            buckets[_prefix_of(curie)].add(curie)
        return {k: frozenset(v) for k, v in buckets.items()}

    @cached_property
    def declared_types_by_curie(self) -> dict[str, str | None]:
        return {curie: declared for curie, declared in self._ids_rows}

    @cached_property
    def declared_biolink_types(self) -> frozenset[str]:
        return frozenset(t for t in self.declared_types_by_curie.values() if t)

    @cached_property
    def declared_type_counts(self) -> dict[str, int]:
        """How many CURIEs in this source's ids file declare each biolink type.

        Rows without a declared type are bucketed under the empty string so callers can
        report "undeclared" explicitly.
        """
        counts: dict[str, int] = defaultdict(int)
        for declared in self.declared_types_by_curie.values():
            counts[declared or ""] += 1
        return dict(counts)

    @cached_property
    def concord_pairs(self) -> list[tuple[str, str, str]]:
        """Every row of every concord file this source contributes, concatenated."""
        triples: list[tuple[str, str, str]] = []
        for concords_path in self.concords_paths:
            if not concords_path.exists():
                continue
            with concords_path.open() as f:
                for line in f:
                    parts = line.rstrip("\n").split("\t")
                    if len(parts) < 3:
                        continue
                    triples.append((parts[0], parts[1], parts[2]))
        return triples

    @cached_property
    def concord_partner_prefix_counts(self) -> dict[str, int]:
        """Count of partner-prefix occurrences across the concord file.

        For each row in the concord file, contributes one count to whichever endpoint's
        prefix is *not* one of the source's own (as declared in the ids file). This
        isolates how many bridges go to each external vocabulary.
        """
        own_prefixes = set(self.curies_by_prefix.keys())
        counts: dict[str, int] = defaultdict(int)
        for c1, _, c2 in self.concord_pairs:
            for c in (c1, c2):
                prefix = _prefix_of(c)
                if prefix not in own_prefixes:
                    counts[prefix] += 1
        return dict(counts)


@dataclass
class SourceContribution:
    """Aggregated description of a source across every babel_pipeline it touches."""

    name: str
    by_pipeline: dict[str, PipelineContribution]

    @property
    def pipelines(self) -> frozenset[str]:
        return frozenset(self.by_pipeline.keys())

    @property
    def prefixes(self) -> frozenset[str]:
        out: set[str] = set()
        for pc in self.by_pipeline.values():
            out.update(pc.curies_by_prefix.keys())
        return frozenset(out)

    @property
    def declared_biolink_types(self) -> frozenset[str]:
        out: set[str] = set()
        for pc in self.by_pipeline.values():
            out.update(pc.declared_biolink_types)
        return frozenset(out)

    @property
    def declared_type_counts(self) -> dict[str, int]:
        """Total CURIEs declaring each biolink type, summed across all pipelines.

        Rows without a declared type are bucketed under the empty string (mirroring
        ``PipelineContribution.declared_type_counts``).
        """
        counts: dict[str, int] = defaultdict(int)
        for pc in self.by_pipeline.values():
            for declared, count in pc.declared_type_counts.items():
                counts[declared] += count
        return dict(counts)

    @property
    def total_identifier_count(self) -> int:
        return sum(len(pc.all_curies) for pc in self.by_pipeline.values())

    @property
    def total_concord_row_count(self) -> int:
        return sum(len(pc.concord_pairs) for pc in self.by_pipeline.values())

    @property
    def concord_names(self) -> frozenset[str]:
        """Basenames of the concord files counted as this source's own (see discover_source)."""
        return frozenset(path.name for pc in self.by_pipeline.values() for path in pc.concords_paths)


def discover_source(
    name: str,
    intermediate_root: pathlib.Path | str,
    concord_names: Sequence[str] | None = None,
) -> SourceContribution:
    """Discover where a named source contributes across the intermediate build outputs.

    Walks ``<intermediate_root>/<pipeline>/ids/<name>`` and
    ``<intermediate_root>/<pipeline>/concords/<concord_name>`` for every pipeline subdirectory
    and records a PipelineContribution wherever the source has any of those files. Returns a
    SourceContribution; callers can check ``by_pipeline`` to detect a source name that is
    not present anywhere.

    ``concord_names`` defaults to ``[name]``, which is the common case: a source whose concord
    file is named after it. A source is free to write more than one concord, or to name one
    something other than its own name -- ``GARD_label`` is GARD's label-match concord -- and no
    naming rule can reliably tell those apart from another source's concord *about* this one
    (``MONDO_GARD`` is MONDO's data about GARD, and belongs to MONDO). So the caller names them:
    ``source-impact-report --source GARD --concord GARD_label``. The concords used are recorded
    in the report header, so a regeneration that forgets the flag is visible rather than silent.
    """
    intermediate_root = pathlib.Path(intermediate_root)
    if not intermediate_root.exists():
        raise FileNotFoundError(f"Intermediate root does not exist: {intermediate_root}")
    if concord_names is None:
        concord_names = [name]
    by_pipeline: dict[str, PipelineContribution] = {}
    for pipeline_dir in sorted(intermediate_root.iterdir()):
        if not pipeline_dir.is_dir():
            continue
        ids_path = pipeline_dir / "ids" / name
        concords_paths = [
            path for path in (pipeline_dir / "concords" / concord for concord in concord_names) if path.exists()
        ]
        has_ids = ids_path.exists()
        if not (has_ids or concords_paths):
            continue
        by_pipeline[pipeline_dir.name] = PipelineContribution(
            pipeline=pipeline_dir.name,
            ids_path=ids_path if has_ids else None,
            concords_paths=concords_paths,
        )
    return SourceContribution(name=name, by_pipeline=by_pipeline)
