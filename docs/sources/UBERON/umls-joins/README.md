# Joining UBERON to UMLS

Many anatomical concepts used to ship as two anatomy cliques: one led by an UBERON term, and one
holding only UMLS (often a SNOMED CT "Entire X" concept, or a concept whose only source is FMA). For
example, [`UBERON:0000395`](http://purl.obolibrary.org/obo/UBERON_0000395) "cochlear ganglion" and
`UMLS:C1289388` "Entire spiral ganglion" were separate even though UBERON cross-references that
concept's SNOMED code. This page records how the anatomy pipeline joins them, what that does to the
compendia, and what was considered and rejected.

## What the pipeline does

### SNOMED CT: rename UBERON's SCTID xrefs

UBERON writes SNOMED CT xrefs as `SCTID:` (about 4,100 of them) while the UMLS concord writes
`SNOMEDCT:`, so none of UBERON's SNOMED xrefs could join a UMLS concept.
`config.yaml: anatomy_xref_prefixes` renames them, the same mechanism the disease pipeline uses
(see [`../../CLAUDE.md`](../../CLAUDE.md), "A missing prefix rename is the same bug, spelled
differently").

### FMA: use UBERON's FMA xrefs

`ANATOMY_OBO_IGNORE_LIST` drops FMA because CL uses FMA xrefs to mean "part of". CL terms are never
written from the UBERON root, though, and CL now comes from Wikidata, so `UBERON_OBO_IGNORE_LIST`
keeps UBERON's FMA xrefs (about 6,100). They are the only bridge to the many UMLS concepts whose
sole sources are FMA and UWDA. GO and EMAPA keep the shared list; their FMA xrefs have not been
reviewed.

### FMA as an extra prefix for gross anatomy

Biolink registers FMA for `biolink:AnatomicalEntity` but not `biolink:GrossAnatomicalStructure`, so
`write_compendium()` drops an FMA CURIE from any clique typed gross anatomy. Joining UBERON to UMLS
moves thousands of UMLS+FMA cliques into UBERON gross anatomy cliques, which dropped 3,027 FMA
CURIEs until `config.yaml: anatomy_extra_prefixes_by_biolink_class` shipped FMA as a temporary extra
prefix. It is removed once
[biolink/biolink-model#1827](https://github.com/biolink/biolink-model/issues/1827) is released
([#1134](https://github.com/NCATSTranslator/Babel/issues/1134)).

### Four wrong UBERON xrefs

Reviewing the clique diff turned up four single wrong UBERON xrefs that the new joins exposed. They
are in `input_data/anatomy_badxrefs.txt` with the reason for each:

- [`UBERON:0012456`](http://purl.obolibrary.org/obo/UBERON_0012456) "Merkel nerve ending" →
  `MESH:D018862` "Merkel Cells" (already merged the nerve ending with
  [`CL:0000242`](http://purl.obolibrary.org/obo/CL_0000242) "Merkel cell" on main)
- [`UBERON:0008883`](http://purl.obolibrary.org/obo/UBERON_0008883) "osteoid" → `FMA:66830`
  "Bone matrix"
- [`UBERON:0003691`](http://purl.obolibrary.org/obo/UBERON_0003691) "epidural space" →
  `FMA:71228` "Cranial epidural space"
- [`UBERON:0002464`](http://purl.obolibrary.org/obo/UBERON_0002464) "nerve trunk" →
  `SNOMEDCT:281248001` "Structure of nervous system of trunk"

### UberGraph query filter

UberGraph infers every NCBITaxon class to be a subclass of
[`UBERON:0000465`](http://purl.obolibrary.org/obo/UBERON_0000465) "material anatomical entity", so
the unfiltered xref query from the UBERON root returned about 860,000 taxa and timed out.
`build_sets()` now filters descendents to the root's ontology in the query. This does not change
the concords; it only makes the anatomy build run.

## What it does to the compendia

A local anatomy build from the same downloads (UMLS 2026AA, UberGraph of 2026-10-09), compared with
`babel-clique-diff`. The summaries are in [`clique-diff/`](./clique-diff/):
`main-vs-branch` is the whole change, `main-vs-sctid` isolates the SNOMED rename, and
`sctid-vs-branch` isolates UBERON's FMA xrefs.

| | main | this change |
|---|---|---|
| UMLS CUIs in UBERON cliques | 3,534 | 10,219 |
| `AnatomicalEntity.txt` cliques | 147,548 | 140,879 |
| `GrossAnatomicalStructure.txt` cliques | 12,693 | 12,693 |
| `Cell.txt` cliques | 9,207 | 9,204 |
| `CellularComponent.txt` cliques | 9,470 | 9,457 |
| CURIEs dropped from every anatomy compendium | | 5 |

- **Merges are small.** 4,192 cliques merge two main cliques, 1,253 merge three, and 16 merge four
  or five. Almost all of the larger ones are an UBERON term with several UMLS synonyms, e.g.
  [`UBERON:0005351`](http://purl.obolibrary.org/obo/UBERON_0005351) "paraflocculus" or
  [`UBERON:0016925`](http://purl.obolibrary.org/obo/UBERON_0016925) "juxtaductal region of aortic
  arch".
- **Retyping is expected.** 5,073 UMLS-only cliques move from `AnatomicalEntity.txt` to
  `GrossAnatomicalStructure.txt` because they now carry UBERON's type. About 20 more move between
  anatomy and `Cell.txt`/`CellularComponent.txt`, mostly UMLS concepts that UMLS types as "Cell
  Component" (brain nuclei, nerve endings) joining their UBERON term.
- **Five CURIEs are dropped**, all from correct merges that changed a clique's type to one Biolink
  does not register the prefix for: `FMA:62977` joins
  [`GO:0043209`](http://purl.obolibrary.org/obo/GO_0043209) "myelin sheath", and four `SNOMEDCT:`
  CURIEs leave `Cell.txt`/`CellularComponent.txt` as their UMLS concepts join UBERON structures.
- **14 UMLS CUIs move from one UBERON term to another**, often to a more specific one, e.g. "Medulla
  segment of fasciculus gracilis" to
  [`UBERON:0002653`](http://purl.obolibrary.org/obo/UBERON_0002653) "gracile fasciculus of medulla".

### How many missed joins this fixes

`scripts/missed_umls_links.py` finds likely-missed joins lexically: a UMLS-only clique whose UMLS
strings match exactly one UBERON label or exact synonym in a different clique. It is a heuristic for
measuring recall, not a mapping, and it misses many. On main it finds 8,903 candidates:

| Bridge between the pair | Candidates | Joined by the SNOMED rename | Joined by this change |
|---|---|---|---|
| UBERON's SNOMED xref is the CUI's SNOMED code | 2,904 | 2,730 | 2,730 |
| UBERON's FMA xref is the CUI's FMA code | 2,925 | 0 | 2,901 |
| both | 294 | 251 | 294 |
| neither | 2,780 | 0 | 0 |
| **total** | **8,903** | **2,981** | **5,925** |

[`missed-umls-links-sample.csv`](./missed-umls-links-sample.csv) is a sample spread across these
groups. The full per-candidate list is regenerated to `data/uberon-umls/missed-umls-links.csv`.

Of the 2,780 with neither bridge, 1,956 are SNOMED CUIs where UBERON has no SNOMED xref or points at
the other half of SNOMED's structure/entire pair (below), 223 are FMA or UWDA concepts UBERON does
not xref, 185 come only from sources such as LOINC, ICF or the veterinary SNOMED edition, and 416
are other UMLS-only concepts.

## Rejected: linking SNOMED "structure" and "entire" concepts

SNOMED CT models anatomy as Structure/Entire/Part triples: "X structure" means X *or any part of X*,
"Entire X" means the whole. UMLS keeps them as separate CUIs, and SNOMED links them with
`has_entire_anatomy_structure`. Using that relationship as an equivalence would join many more
pairs, and was tried and rejected:

- UBERON intends its SNOMED xrefs to point at the Entire concept, because the UBERON class is the
  entity and only Entire concepts can be made equivalent without incoherence
  ([obophenotype/uberon#3287](https://github.com/obophenotype/uberon/issues/3287)). Treating
  Structure as equivalent to Entire contradicts that.
- In a full build it produced wrong merges where a Structure atom sits in a broader UMLS CUI: UMLS
  files "Bone structure of knee joint region" under its general "Knee" CUI, so "Entire bone of knee
  joint region" joined [`UBERON:0001465`](http://purl.obolibrary.org/obo/UBERON_0001465) "knee".

Of UBERON's 4,118 SNOMED xrefs, 3,064 point at an Entire concept and 690 at a Structure concept.
[`sctid-structure-entire.csv`](./sctid-structure-entire.csv), from
`scripts/sctid_structure_entire.py`, proposes the Entire partner for each of the 690 (675 to
replace, 6 to remove because the term already xrefs the Entire concept, 9 for a curator). That list
is filed upstream as
[obophenotype/uberon#3786](https://github.com/obophenotype/uberon/issues/3786); once applied, the
SNOMED rename picks it up with no change here.

## Also rejected: Wikidata UBERON↔UMLS mappings

Wikidata pairs UBERON IDs (P1554) with UMLS CUIs (P2892), the same source the pipeline uses for CL.
Filtered to one-to-one pairs it would add about 2,600 joins against the 2026jul22 release, but
nearly all of them are pairs the FMA xrefs already join; it added almost nothing for the pairs with
no bridge.

## Open: UBERON terms with several SNOMED xrefs

77 UBERON terms xref more than one SNOMED concept, listed in
[`multi-snomed-xrefs.csv`](./multi-snomed-xrefs.csv). Each joins all of its SNOMED concepts into one
clique. That is fine when they are synonyms or the two halves of a structure/entire pair, and wrong
when one is narrower: [`UBERON:0011877`](http://purl.obolibrary.org/obo/UBERON_0011877) "margin of
tongue" xrefs both "Entire left margin of tongue" and "Entire right margin of tongue", and
[`UBERON:0004319`](http://purl.obolibrary.org/obo/UBERON_0004319) "distal phalanx of pedal digit 5"
xrefs both "Structure of distal phalanx of little toe" and "Entire distal phalanx of lesser toe"
(the lesser toes are toes 2-5). Requiring one SNOMED xref per UBERON term would stop these but also
drop the correct joins among the 77, so it is left for review rather than applied.

## Regenerating

From the repository root, after `uv run snakemake anatomy`:

```bash
uv run python docs/sources/UBERON/umls-joins/scripts/sctid_structure_entire.py
uv run python docs/sources/UBERON/umls-joins/scripts/missed_umls_links.py --before <main compendia> --after babel_outputs/compendia
uv run babel-clique-diff --before <main compendia> --after babel_outputs/compendia --files AnatomicalEntity.txt GrossAnatomicalStructure.txt Cell.txt CellularComponent.txt --out-json main-vs-branch.summary.json --out-csv main-vs-branch.csv
```

The per-row clique-diff CSVs (2–3 MB each) are not committed; regenerate them with the last command.
