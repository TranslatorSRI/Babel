# GARD label matches: the GARD_label concord

GARD asserts no cross-references of its own, so its terms reach Babel cliques only through MONDO's
and DOID's inbound xrefs. 277 of the 16,214 ingested registry terms are mapped by neither, and
before this concord shipped as single-identifier cliques — several of them beside an existing clique
with the identical label:

| Alone in `Disease.txt` | The clique next to it |
| --- | --- |
| [`GARD:27461`](https://rarediseases.info.nih.gov/?gard_id=27461) "Odontogenic Carcinoma" | `UMLS:C5401355` + [`NCIT:C173720`](http://purl.obolibrary.org/obo/NCIT_C173720) "Odontogenic Carcinoma" |
| [`GARD:27460`](https://rarediseases.info.nih.gov/?gard_id=27460) "Oral Cavity Langerhans Cell Histiocytosis" | `UMLS:C5420233` + [`NCIT:C173487`](http://purl.obolibrary.org/obo/NCIT_C173487) "Oral Cavity Langerhans Cell Histiocytosis" |

`build_gard_label_concord()` in `src/createcompendia/diseasephenotype.py` links them, writing the
`GARD_label` concord. This page is the evidence: what it emits, how often the same rule is wrong,
and what was checked before resorting to a label match at all. Regenerate it — and
[`label-matches.csv`](label-matches.csv), the full per-row record — with:

```bash
uv run python docs/sources/GARD/label-matches/scripts/gard_label_match_report.py
```

## No authoritative mapping exists

A label match is a last resort, so the alternatives were checked first. All four are negative:

- **The registry CSV** has columns `ID,DisplayName,Synonyms,URL` and no cross-reference column.
  GARD is a registry, not an ontology.
- **UMLS** has no `GARD` source vocabulary — none of the 190 `SAB` values in `MRCONSO.RRF` is GARD,
  so no CUI carries a GARD atom.
- **NCIt** asserts no GARD provenance. For
  [`NCIT:C173720`](http://purl.obolibrary.org/obo/NCIT_C173720) "Odontogenic Carcinoma" the EVS API
  returns `Contributing_Source = GDC` and a single `Maps_To` into GDC; there is no GARD mapping and
  no GARD contributing source.
- **GARD's own website** is a single-page app whose only data asset,
  `https://rarediseases.info.nih.gov/assets/diseases.trimmed.json`, holds 6,265 curated terms with
  names, synonyms and tags — no identifiers from any other vocabulary.

So the label is the only signal there is.

### On where these terms came from

265 of the 277 unmapped terms fall in the contiguous `GARD:27000-28999` id block, and that block
looks unlike the rest of the registry: 30.6% of its terms carry a label that exactly matches an NCIt
preferred label, against 17.7% for every GARD id below it. Of the 277, 212 match an NCIt label, and
the labels themselves read like NCIt's ("Childhood Salivary Gland Acinic Cell Carcinoma",
"Refractory Blastic Plasmacytoid Dendritic Cell Neoplasm").

That is an observation, not a provenance claim. **No source documents it.** GARD's About page names
Orphanet, OMIM, Mondo, HPO, MedGen and UMLS as its data sources and does not mention NCIt; NCIt, as
above, asserts nothing about GARD. The block is recorded here because it explains why the matches
cluster the way they do, not as evidence that anything was imported from anywhere.

## What the concord emits

265 rows, one per GARD term, against the 2026-08-25 build (GARD Jun2026; MONDO, DOID, NCIt and UMLS
2026AA):

| Target vocabulary | Rows |
| --- | --- |
| NCIT | 210 |
| MONDO | 42 |
| orphanet | 7 |
| MESH | 6 |

**Every subject is a single-identifier clique, and the median target has two non-GARD members** —
172 of the 265 join a clique of exactly two. This change overwhelmingly attaches one lonely
identifier to two lonely identifiers; it is not reaching into large curated cliques.

Twelve registry terms are left alone. Seven have no label match anywhere and are, as far as this
build can tell, genuinely new concepts:

```text
GARD:15005  Pacak-Zhung syndrome
GARD:15006  STAT5 Haploinsufficiency
GARD:15009  Monocytosis/myelocytosis, Autoimmunity, Gain of function, Immunodeficiency, Short stature
GARD:15863  Usher syndrome type 1J
GARD:24658  Heart, malformation of
GARD:27070  TUBB2A-related tubulinopathy
GARD:28300  Camurati-Engelmann disease, type 2
```

The other five match, but their target's clique is not `biolink:Disease` — see guard 3 below.

## How often the same rule is wrong

The concord's own rows cannot be checked against anything: it runs only on GARD ids nothing else
places, so there is no second opinion to compare with. The precision figure comes from a **held-out
set** — the 15,937 GARD ids MONDO and DOID *do* place, which the concord deliberately skips. Running
the same rule over them and asking whether it lands in the clique those curated xrefs already chose:

| | |
| --- | --- |
| Held-out GARD ids | 15,937 |
| Rule picks a target | 15,370 |
| Agrees with the curated clique | 15,337 |
| **Disagrees** | **33 (0.21%)** |

The 33 are upstream splits rather than sloppy matching — two curated terms for one name, and the
label cannot tell them apart:

```text
GARD:10867  "Familial multiple trichoepitheliomata"      is on MONDO:0011114; the label also names UMLS:C1275122
GARD:10957  "Iron-refractory iron deficiency anemia"     is on MONDO:0008788; the label also names UMLS:C0085576
GARD:11984  "Hereditary pheochromocytoma and paraganglioma" is on MONDO:0017366; the label also names UMLS:C4274332
GARD:12827  "Hypotrichosis-lymphedema-telangiectasia syndrome" is on MONDO:0007670; the label also names MONDO:0011914
GARD:13809  "Pigmentary retinal dystrophy"               is on MONDO:0007639; the label also names UMLS:C4551633
GARD:15142  "Congenital cleft nose"                      is on MONDO:0008866; the label also names UMLS:C4759655
GARD:1638   "Cutis laxa with osteodystrophy"             is on MONDO:0018163; the label also names UMLS:C5550995
GARD:16798  "Ptosis, hereditary congenital, 1"           is on MONDO:0008340; the label also names MONDO:0979905
```

Read this as an **upper bound**, for two reasons. The held-out population is harder — curated MONDO
cliques full of near-synonymous siblings — than the median-size-2 NCIT/UMLS pairs the concord
actually targets. And the concord skips exactly the terms this measurement is taken on, so a
disagreement here costs nothing in the build.

### Case-sensitive matching is worse, not stricter

Requiring identical casing is the obvious way to tighten a label match, and it makes the result more
than ten times worse on the same held-out set:

| Policy | Held-out decisions | Wrong |
| --- | --- | --- |
| Case-insensitive | 15,370 | 33 (0.21%) |
| Case-sensitive | 8,018 | 212 (2.64%) |

Case-sensitivity does not filter bad matches; it walks past the *right* clique on a capital letter
and then matches some other clique capitalized the same way. `normalize_label_for_matching()` folds
case for this reason.

## The three guards

Full rationale is in `build_gard_label_concord()`'s docstring; the short version, and why each one
is not optional:

1. **Skip a GARD id another concord already places.** Those 15,937 ids sit in a curated clique
   already. Without this guard the 33 disagreements above stop being an error rate and become 33
   attempts to fuse curated cliques. The check reads the concord files rather than naming
   `MONDO_GARD` and `DOID`, so a source that starts emitting GARD xrefs is covered automatically.

2. **At most one row per GARD id.** Guard 1 leaves only GARD ids that appear in no other concord, so
   each is a single-identifier clique and one pair can only union `{GARD:x}` into the target's
   clique. **This concord is therefore structurally incapable of merging two pre-existing cliques.**
   Emitting every matching identifier instead would have put 475 existing clique pairs at risk of
   fusion. The invariant is enforced by construction rather than detected by a warning downstream.

3. **Skip a target whose clique is not `biolink:Disease`.** `write_compendium()`'s per-class prefix
   filter keeps GARD alive in `Disease.txt` only (`config.yaml: disease_extra_prefixes` is a
   Disease-only allowlist, and Biolink registers GARD for no class at all), so a GARD id that joins
   a phenotype clique is *not* moved to `PhenotypicFeature.txt` — it is dropped from the build
   entirely, trading a working single-identifier clique for a vanished identifier.

Guard 3 was added because a `babel-clique-diff` of the first implementation reported five dropped
members, which is exactly what that diff exists to catch:

```text
GARD:27493  Cementoblastoma                → NCIT:C4308,     clique led by HP:0012328
GARD:28330  Ileal Atresia                  → NCIT:C101026,   clique led by HP:0011102
GARD:28364  Phocomelia of the Lower Limb   → NCIT:C35323,    clique led by HP:0009819
GARD:28368  Chilblains                     → MESH:D002647,   clique led by HP:0009710
GARD:28372  Myokymia                       → MESH:D020385,   clique led by HP:0002411
```

Each is the same concept under both names; Babel holds it as a phenotype because HP names it, while
GARD calls it a rare disease. They stay as they are until GARD is registered in the Biolink Model
([#1051](https://github.com/NCATSTranslator/Babel/issues/1051)), at which point this guard can go.

Two cheaper proxies were tried first and both failed. Vetoing labels that HP or MP also carries
catches only 3 of the 5 (HP's label for Cementoblastoma and Phocomelia differs from GARD's), and the
target's declared type in the ids file says `biolink:Disease` for all five — the type comes from the
*clique*, not the identifier. The clique that makes them phenotypes forms through UMLS, two hops
from the target, so nothing local sees it. So guard 3 asks the question directly: it reglommed the
other concords with `compute_cliques_for_impact_report()` and types the result with
`classify_disease_clique()`, the same two functions the build itself uses. It costs about five
seconds and is exact.

HP and MP are also absent from `config.yaml: disease_gard_label_match_prefixes`, for the related but
separate reason that `split_mutually_exclusive_cliques()` keeps phenotype and disease cliques
disjoint on purpose. Guard 3 covers the indirect case; the pool exclusion covers the direct one.

## Effect on the build

`babel-clique-diff` between builds that differ only in this concord
([`clique-diff.summary.json`](clique-diff.summary.json); the narrative is in
[`../clique-diff.md`](../clique-diff.md)):

| | Changed cliques | Dropped members | Moved | Leader changes |
| --- | --- | --- | --- | --- |
| `Disease.txt` | 528 | 0 | 0 | 0 |
| `PhenotypicFeature.txt` | 0 | 0 | 0 | 0 |

The 528 are the 265 GARD singletons that stop existing plus the 263 cliques that gain a member (two
cliques gain two GARD ids each, where the registry carries a label twice). `Disease.txt` goes from
365,345 to 365,080 cliques, and all 16,214 GARD identifiers still reach a compendium.

## Deliberate omissions

- **Synonyms are not matched, only preferred labels.** It would chase at most a few of the seven
  unmatched terms while widening the ambiguity surface considerably — GARD ships pipe-separated
  synonyms for much of the registry, and a synonym collision is far more common than a label one.
- **Nothing is done about the 15,937 GARD ids MONDO and DOID place.** They are the held-out set that
  makes the precision number meaningful, and re-deciding them is exactly what guard 1 forbids.
- **There is no `gard_badxrefs.txt`.** No emitted row currently needs one; if a bad match surfaces,
  the established `DEFAULT_BAD_XREFS` mechanism takes it with no new machinery.

## The better fix is upstream

MONDO already maps 15,930 registry terms with `oboInOwl:hasDbXref`, so these 265 fit its existing
practice exactly. Sending them upstream would turn a heuristic into curation, and each term MONDO
takes up drops out of this concord automatically (guard 1). That is tracked as
[#1058](https://github.com/NCATSTranslator/Babel/issues/1058).
