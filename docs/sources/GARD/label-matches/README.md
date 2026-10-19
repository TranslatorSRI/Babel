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

270 rows, one per GARD term, against the 2026-08-26 build (GARD Jun2026; MONDO, DOID, NCIt and UMLS
2026AA):

| Target vocabulary | Rows |
| --- | --- |
| NCIT | 213 |
| MONDO | 42 |
| MESH | 8 |
| orphanet | 7 |

**Every subject is a single-identifier clique, and the median target has two non-GARD members** —
172 of the 270 join a clique of exactly two. This change overwhelmingly attaches one lonely
identifier to two lonely identifiers; it is not reaching into large curated cliques.

Seven registry terms are left alone, having no label match anywhere. As far as this build can tell
they are genuinely new concepts:

```text
GARD:15005  Pacak-Zhung syndrome
GARD:15006  STAT5 Haploinsufficiency
GARD:15009  Monocytosis/myelocytosis, Autoimmunity, Gain of function, Immunodeficiency, Short stature
GARD:15863  Usher syndrome type 1J
GARD:24658  Heart, malformation of
GARD:27070  TUBB2A-related tubulinopathy
GARD:28300  Camurati-Engelmann disease, type 2
```

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

Those 33 are worth something on their own: a GARD label exactly naming a different concept than
MONDO's own xref chose is a cheap curation signal, and 21 of the 33 are MONDO against MONDO. **None
of them reaches the compendia** — guard 1 skips every one, so `GARD_label` emits no row for these
GARD ids, no concord asserts the pair, and all 33 land in the clique MONDO's or DOID's curated xref
chose rather than the one the label names. One exception is worth knowing about and is flagged in
the CSV's `pair_also_asserted_by` column: for `GARD:8433` "King Denborough syndrome" the label's
target is *also* a DOID xref (`DOID:0080990`), so that row is a live disagreement between DOID and
MONDO which `glom()` already refused because both cliques hold a MONDO identifier — stronger
evidence for an upstream report than a label coincidence, not weaker. The full list is
[`label-mismatches.csv`](label-mismatches.csv), and
[#1063](https://github.com/NCATSTranslator/Babel/issues/1063) sends them upstream — as a "please
check" list rather than asserted corrections, since a label collision says two terms are described
the same way, not which mapping is right.

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

## The guards

Full rationale is in `build_gard_label_concord()`'s docstring; the short version, and why neither is
optional:

1. **Skip a GARD id another concord already places.** Those 15,937 ids sit in a curated clique
   already. Without this guard the 33 disagreements above stop being an error rate and become 33
   attempts to fuse curated cliques. The check reads the concord files rather than naming
   `MONDO_GARD` and `DOID`, so a source that starts emitting GARD xrefs is covered automatically.

2. **At most one row per GARD id.** Guard 1 leaves only GARD ids that appear in no other concord, so
   each is a single-identifier clique and one pair can only union `{GARD:x}` into the target's
   clique. **This concord is therefore structurally incapable of merging two pre-existing cliques.**
   Emitting every matching identifier instead would have put 475 existing clique pairs at risk of
   fusion. The invariant is enforced by construction rather than detected by a warning downstream.

A third guard used to sit here and is worth knowing about, because the temptation to add it back is
real. `disease_gard_ids` types every registry term `biolink:Disease`, but five of the matched terms
name concepts HP also names — Cementoblastoma, Ileal Atresia, Phocomelia of the Lower Limb,
Chilblains, Myokymia — and the clique type vote rightly follows HP. While `config.yaml`'s
extra-prefixes allowlist named GARD for `biolink:Disease` only, joining such a clique **deleted**
the GARD identifier rather than moving it to `PhenotypicFeature.txt`, so the concord refused those
five links and shipped a duplicate single-identifier Disease clique beside the phenotype clique
naming the same thing. A `babel-clique-diff` run reporting `5 dropped members` is what surfaced it;
nothing else would have, since the impact report cannot see a dropped identifier at all — a CURIE
absent from both sides is not a difference.

The fix was not a better guard but a correct allowlist: GARD is unregistered for *every* Biolink
class, so its exemption was never one earned on disease grounds, and
`disease_extra_prefixes_by_biolink_class` now names it under `biolink:PhenotypicFeature` too. The
identifier follows its clique, the guard has nothing left to prevent, and the five links are made.

Two cheaper proxies for that guard were tried before it was deleted, and both failed, which is the
other reason not to reintroduce it: vetoing any label HP or MP also carries catches only three of
the five, because HP's labels for Cementoblastoma and Phocomelia differ from GARD's; and the
target's declared type in the ids file says `biolink:Disease` for all five, because the type is a
property of the *clique*, and the clique that makes them phenotypes forms through UMLS two hops from
the target. The working version had to reglom every other concord — by far the most expensive thing
in the module, for a question that no longer needs asking.

HP and MP are also absent from `config.yaml: disease_gard_label_match_prefixes`, for the related but
separate reason that `split_mutually_exclusive_cliques()` keeps phenotype and disease cliques
disjoint on purpose. That is about matching a GARD term *directly* onto an HP or MP term; reaching
an HP-led clique through one of its other members, as five of the 270 do, is a different thing and
is fine.

## Effect on the build

`babel-clique-diff` between builds that differ only in this concord
([`clique-diff.summary.json`](clique-diff.summary.json); the narrative is in
[`../clique-diff.md`](../clique-diff.md)):

| | Changed cliques | Dropped members | Moved | Leader changes |
| --- | --- | --- | --- | --- |
| `Disease.txt` | 533 | 0 | 5 | 0 |
| `PhenotypicFeature.txt` | 5 | 0 | 0 | 0 |

`Disease.txt` goes from 365,345 to 365,075 cliques. Its 533 rows are 265 GARD singletons that merge
into an existing disease clique, 263 cliques that gain a member (two gain two GARD ids each, where
the registry carries a label twice), and the **5 `moved`** — the terms whose match puts them in an
HP-led clique, so they retype into `PhenotypicFeature.txt` rather than staying diseases. That is the
`moved` column doing exactly what it is for: a member changing compendium is neither a merge nor a
loss, and it is the one outcome the impact report cannot express. `PhenotypicFeature.txt` gains
those five members and no cliques, and all 16,214 GARD identifiers still reach a compendium.

## Deliberate omissions

- **Synonyms are not matched, only preferred labels.** It would chase at most a few of the seven
  unmatched terms while widening the ambiguity surface considerably — GARD ships pipe-separated
  synonyms for much of the registry, and a synonym collision is far more common than a label one.
- **Nothing is done about the 15,937 GARD ids MONDO and DOID place.** They are the held-out set that
  makes the precision number meaningful, and re-deciding them is exactly what guard 1 forbids.
- **There is no `gard_badxrefs.txt`.** No emitted row currently needs one; if a bad match surfaces,
  the established `DEFAULT_BAD_XREFS` mechanism takes it with no new machinery.

## The better fix is upstream

MONDO already maps 15,930 registry terms with `oboInOwl:hasDbXref`, so these 270 fit its existing
practice exactly. Sending them upstream would turn a heuristic into curation, and each term MONDO
takes up drops out of this concord automatically (guard 1). That is tracked as
[#1058](https://github.com/NCATSTranslator/Babel/issues/1058).
