# MONDO — the Mondo Disease Ontology as a disease source

MONDO is the backbone of Babel's `disease` pipeline: it supplies identifiers, labels and synonyms,
it is first in the type-precedence order used by `create_typed_sets`, and it is one of the three
prefixes in `DISEASE_UNIQUE_PREFIXES`, so a clique may hold at most one MONDO identifier.

This page is about the part that is easy to get wrong: **which of MONDO's mappings Babel reads.**

## Babel reads MONDO's exact and close matches, not its `hasDbXref`s

`build_disease_obo_relationships()` in `src/createcompendia/diseasephenotype.py` builds MONDO's
concords from UberGraph with `build_sets(..., set_type="exact")` and `set_type="close"` — that is,
`skos:exactMatch` and `skos:closeMatch` — walking down from
[`MONDO:0000001`](http://purl.obolibrary.org/obo/MONDO_0000001) "disease" and
[`MONDO:0042489`](http://purl.obolibrary.org/obo/MONDO_0042489) "susceptibility to disease":

| concord | predicate | rows | fed to `glom()` as |
| --- | --- | --- | --- |
| `MONDO` | `skos:exactMatch` | 108,230 | equivalence pairs |
| `MONDO_close` | `skos:closeMatch` | 1,478 | `close={MONDO: ...}` |
| `MONDO_GARD` | `oboInOwl:hasDbXref`, GARD only | 16,212 | equivalence pairs |

MONDO also carries **145,433 `hasDbXref` rows across 39 namespaces** on its non-deprecated terms,
and with one exception Babel reads none of them. That is deliberate: an OBO `hasDbXref` often means
"this term is *about* that thing" rather than "this term *is* that thing" (see
[`docs/sources/CLAUDE.md`](../CLAUDE.md), "An OBO `hasDbXref` is not an equivalence"). Ingesting
them wholesale would merge on ICD family codes exactly the way DOID's did before
[#1031](https://github.com/NCATSTranslator/Babel/issues/1031).

## The exception: `MONDO_GARD`

GARD is the one namespace that is mapped at scale, has a Babel prefix, is perfectly 1:1, and has
**no exactMatch route at all** (MedDRA nearly qualifies — see below — but is not 1:1).
All 15,930 of MONDO's GARD mappings are `hasDbXref`; not one is a `skos:exactMatch` or `closeMatch`
(confirmed both against UberGraph and against the `mondo.json` release). GARD itself is a registry
and asserts no mappings, and DOID's xrefs reach only 2,195 of its 16,214 terms — so without this
concord 98% of the registry lands in single-identifier cliques duplicating concepts MONDO already
names. See [`docs/AddingNewSources.md`](../../AddingNewSources.md) ("Prefer joining an existing
clique") for why that outcome is worse than either alternative.

Three things scope the exception:

1. **A separate concord file.** `MONDO_GARD` is listed in `config.yaml: disease_concords` alongside
   `MONDO`, so the exception is visible where the pipeline's inputs are declared, and it can be
   filtered, overuse-scoped or dropped without touching MONDO's exact matches. It carries its own
   `metadata-MONDO_GARD.yaml` recording how the rows were selected.
2. **A fail-closed allowlist of exactly one prefix.** `allowed_prefixes={GARD}` means a namespace
   MONDO newly starts emitting is dropped rather than silently trusted.
3. **The 1:1 property, enforced at build time.** The 15,936 pairs are one-to-one in both
   directions today — no GARD id claimed by two MONDO terms, no MONDO term claiming two GARD ids —
   and `tests/pipeline/test_mondo_gard.py` pins that against the real built concord. But MONDO is
   a unique prefix, so a future release mapping one GARD id to two MONDO terms would make `glom()`
   hand it to whichever row sorts first, silently. `OVERUSE_FILTERED_CONCORDS["MONDO_GARD"] =
   [GARD]` turns that into "neither claims it" instead. (The file is deduplicated when written —
   a term under both MONDO roots would otherwise be a doubled row, which the row-counting filter
   would read as a double claim.)
4. **Glommed before DOID.** 148 of DOID's GARD xref rows disagree with MONDO about which MONDO
   clique a GARD id belongs to (`docs/sources/GARD/scripts/count_doid_mondo_gard_disagreements.py`);
   listing `MONDO_GARD` before `DOID` in `disease_concords` is what makes MONDO the authority. A
   unit test pins the order.

MONDO writes GARD in the registry's zero-padded form (`GARD:0022702`); the `GARD: GARD` entry of
`disease_xref_prefixes[MONDO]` in `config.yaml` resolves to `gard.normalize_gard_curie` via
`LOCAL_ID_DEPENDENT_RENAMES` and strips it. Without that these rows would join neither GARD's own
ids file nor DOID's unpadded xrefs. See [`docs/sources/GARD/README.md`](../GARD/README.md).

## The `hasDbXref` namespaces Babel does not read

Recorded here so that "MONDO has more mappings than we use" stays a known fact rather than a
rediscovery, and so that a future decision to ingest one of them starts from evidence. Regenerate
with:

```bash
curl -sL -o data/mondo.json http://purl.obolibrary.org/obo/mondo.json
uv run python docs/sources/MONDO/scripts/analyze_xref_namespaces.py --mondo-json data/mondo.json
```

which writes the full 39-namespace table to [`xref-namespaces.csv`](xref-namespaces.csv). The
`exactMatch` column counts rows in the built `MONDO` concord; where MONDO spells a vocabulary
differently in its xrefs than in its exact matches, the concord's spelling is in brackets.
**Worst fan-out** is the largest number of MONDO terms claiming one target — the number that decides
whether a namespace could be glommed unfiltered. Counts are from the MONDO release of 2026-08-19.

| namespace | `hasDbXref` | distinct targets | targets claimed 2+ | worst fan-out | `exactMatch` | status |
| --- | ---: | ---: | ---: | ---: | ---: | --- |
| MEDGEN | 21,658 | 21,658 | 0 | 1 | 21,738 | covered by exactMatch |
| UMLS | 21,658 | 21,658 | 0 | 1 | 21,738 | covered by exactMatch |
| GARD | 15,930 | 15,930 | 0 | 1 | 0 | **ingested as `MONDO_GARD`** |
| DOID | 11,944 | 11,944 | 0 | 1 | 11,921 | covered by exactMatch |
| OMIM | 9,904 | 9,904 | 0 | 1 | 9,960 | covered by exactMatch |
| Orphanet | 9,163 | 9,163 | 0 | 1 | 8,551 | covered by exactMatch |
| SCTID (`SNOMEDCT`) | 9,013 | 9,011 | 2 | 2 | 9,025 | covered by exactMatch |
| MESH | 8,091 | 8,088 | 3 | 2 | 8,121 | covered by exactMatch |
| NCIT | 7,378 | 7,378 | 0 | 1 | 7,329 | covered by exactMatch |
| ICD9 | 5,554 | 3,908 | 419 | 167 | 0 | not read |
| VeNom | 5,193 | 5,193 | 0 | 1 | 0 | not read |
| icd11.foundation | 4,569 | 4,568 | 1 | 2 | 0 | not read |
| OMIA | 3,196 | 3,183 | 6 | 4 | 0 | not read |
| EFO | 2,387 | 2,387 | 0 | 1 | 2,379 | covered by exactMatch |
| NANDO | 2,244 | 2,087 | 143 | 3 | 0 | not read |
| ICD10CM | 2,114 | 2,086 | 20 | 5 | 2,030 | covered by exactMatch |
| MedDRA (`MEDDRA`) | 1,471 | 1,443 | 26 | 3 | 3 | not read |
| NORD | 908 | 907 | 1 | 2 | 0 | not read |
| ICDO (`ICD0`) | 768 | 731 | 28 | 6 | 0 | not read |
| OMIMPS (`OMIM.PS`) | 614 | 614 | 0 | 1 | 624 | covered by exactMatch |
| HP | 557 | 555 | 2 | 2 | 0 | not read |
| ONCOTREE | 555 | 555 | 0 | 1 | 0 | not read |
| 17 more, ≤206 rows each | 564 | | | | 0 | not read |

Reading that table: **every namespace MONDO maps at scale is already covered by an exactMatch,
except GARD.** What is left unread is either not a Babel prefix, or is a namespace we would decline
on purpose — but two of them are worth revisiting rather than assuming:

- **The ICD family** (ICD9, ICDO, ICD10WHO, icd11.foundation) — the table shows exactly why an ICD
  code is not an equivalence: `ICD9:759.89` is claimed by **167** different MONDO terms, and 419 of
  ICD9's 3,908 targets are claimed more than once. This is the failure
  [`docs/sources/DOID/mappings.md`](../DOID/mappings.md) documents at length. If these are ever
  ingested they need `remove_overused_xrefs` scoping from the start, not an allowlist entry.
  `icd11.foundation` is the interesting outlier — 4,568 targets with a single 2-way collision — so
  its poor reputation may be inherited from its siblings rather than earned.
- **MedDRA** (1,471 rows) — nearly the GARD case, and currently invisible: MONDO's exact matches
  reach only 3 MEDDRA rows, so these mappings are effectively absent from Babel. 26 of 1,443 targets
  are claimed twice or three times, so it is not the clean 1:1 that made GARD safe to take
  unfiltered — but a scoped overuse filter would likely make it usable, and MEDDRA is already a
  registered `biolink:Disease` prefix.
- **HP** (557 rows) — crosses the disease/phenotype boundary the pipeline deliberately keeps
  disjoint (`MUTUALLY_EXCLUSIVE_PREFIX_GROUPS`, `docs/sources/MP/disjointness.md`). Not a candidate.
- **Registries with no Babel prefix** (VeNom, OMIA, NANDO, NORD, ONCOTREE, GTR, DECIPHER) — these
  need a prefix in `src/prefixes.py` and Biolink registration before they could be emitted at all.
  NANDO (Japanese) and NORD are rare-disease registries in the same family as GARD; NANDO's 143
  multiply-claimed targets suggest it is less cleanly curated than GARD.
- **Non-vocabularies** (Wikipedia, PMID) — citations, not equivalences.

What to check on each MONDO release: whether any "not read" namespace has gained a Babel prefix,
and whether GARD has gained exactMatch mappings — in which case `MONDO_GARD` becomes redundant and
should be removed rather than left to duplicate them.
