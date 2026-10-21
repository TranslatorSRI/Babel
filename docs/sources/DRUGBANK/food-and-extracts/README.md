# DrugBank foods and extracts: typing DrugBank's foods as `biolink:Food`

[Issue #828](https://github.com/NCATSTranslator/Babel/issues/828)

DrugBank ships a large family of **food-and-extract products** — whole foods, pollens, plant
barks/roots/leaves, animal danders, molds, and dust used for allergy skin-testing and immunotherapy.
In Babel these default to `biolink:ChemicalEntity`, which is accurate but uselessly broad: the food
ones ([`DRUGBANK:DB10626`](https://go.drugbank.com/drugs/DB10626) "Trout",
[`DRUGBANK:DB10500`](https://go.drugbank.com/drugs/DB10500) "Almond") should be `biolink:Food`, and
the *extracts* ([`DRUGBANK:DB16536`](https://go.drugbank.com/drugs/DB16536) "Birch bark extract")
are processed materials, not defined chemicals.

The goal is to type **as many DrugBank food entries as `biolink:Food` as we safely can**. The two
signals we have (NCIt's Food/Seed classification and the UNII botanical flags) do that for 685
entries, plant and non-plant alike — NCIt Food covers
[`DRUGBANK:DB10623`](https://go.drugbank.com/drugs/DB10623) "Scallop",
[`DRUGBANK:DB10918`](https://go.drugbank.com/drugs/DB10918) "Venison",
[`DRUGBANK:DB14694`](https://go.drugbank.com/drugs/DB14694) "Beef liver" and
[`DRUGBANK:DB14233`](https://go.drugbank.com/drugs/DB14233) "Egg phospholipids" just as it covers
strawberries. What we deliberately leave behind is the set whose *only* organism evidence is an NCBI
Taxonomy cross-reference: that set contains real foods (lobster, flounder, mushroom) but also immune
globulins, antivenins and CAR-T therapies, and no signal we have separates them, so typing it would
declare things food that are definitely not food. It is deferred, with the evidence needed to sort
it out, to [#930](https://github.com/NCATSTranslator/Babel/issues/930).

Two further follow-ups: [#926](https://github.com/NCATSTranslator/Babel/issues/926) for the
food-vs-taxon question, and [#929](https://github.com/NCATSTranslator/Babel/issues/929) for a
`biolink:ProcessedMaterial` output to replace the interim extract type.

## Why the obvious approaches don't reach these

- **Food ontologies (FoodOn/FooDB) don't help.** These extracts carry only a UNII and no chemical
  structure. FoodOn has no UNII/DrugBank cross-references; FooDB *foods* carry only
  ITIS/Wikipedia/NCBITaxon (only FooDB *compounds* carry UNII/DrugBank, and an extract is a food,
  not a compound). Ingesting a food ontology builds a "trout" clique keyed on NCBITaxon with no
  edge of any kind to `DRUGBANK:DB10626`.
- **Structure-based joins never reach them.** With no InChI Key there is no UniChem structure
  match, so these ids are absent from `ids/DRUGBANK` entirely. They enter chemical cliques only
  through the UMLS and RXNORM concords and inherit `ChemicalEntity` from the clique.
- **DrugBank's rich category data is temporarily un-downloadable**. We have only the
  CC-0 `drugbank vocabulary.csv` (`DrugBank ID, Accession Numbers, Common name, CAS, UNII,
  Synonyms, Standard InChI Key`).

## The signal we use

Each DrugBank entry carries a `UNII`, and the FDA UNII records (`Latest_UNII_Records.txt`) give each
UNII both an NCIt classification and cross-references to organism databases. Those are the bridge.

A DrugBank row is considered only if it has **no `Standard InChI Key`** (an extract/material, not a
defined molecule — a defined molecule extracted from a plant keeps its structure and stays a
chemical). Then:

1. It is treated as **food material** when either signal fires:
   - its UNII's NCIt class is under [`NCIT:C1949`](http://purl.obolibrary.org/obo/NCIT_C1949) "Food"
     or [`NCIT:C73913`](http://purl.obolibrary.org/obo/NCIT_C73913) "Seed" (Seed covers the
     nuts/grains NCIt files there — almond, cashew, sesame, wheat, oat). NCIt calls a food a food
     regardless of what it came from, so this signal is **not** plant-specific: scallop, perch,
     venison, beef liver and egg phospholipids all arrive this way;
   - **or** its UNII carries a botanical-database flag (USDA PLANTS, GRIN, or MPNS — see "Why the
     botanical flags but not NCBI" below), which reaches the plant materials NCIt has no food class
     for (barks, roots, pollens, herbs).

   Anything else — NCBI-only organisms and unflagged rows — is left as `ChemicalEntity` and
   deferred.
2. Among that material, a row whose name/synonyms contain an `extract` marker
   (`config.yaml: drugbank_extract_markers`) is a processed extract →
   **`biolink:ComplexMolecularMixture`** (bark/root/pollen extracts, herbal tinctures).
3. A botanical flag says "plant material", not "food", so on its own it must not overrule an NCIt
   class that says the entry is a **drug**. A row whose NCIt class is under
   `config.yaml: nonfood_ncit_roots` —
   [`NCIT:C1966`](http://purl.obolibrary.org/obo/NCIT_C1966) "Imaging Agent",
   [`NCIT:C274`](http://purl.obolibrary.org/obo/NCIT_C274) "Antineoplastic Agent" — is left as
   `ChemicalEntity`. Without this veto, [`DRUGBANK:DB00965`](https://go.drugbank.com/drugs/DB00965)
   "Ethiodized oil" (an iodinated poppy-seed-oil contrast agent) and
   [`DRUGBANK:DB05051`](https://go.drugbank.com/drugs/DB05051) "BZL101" (an antineoplastic) are
   typed `Food`.
4. Any other food material → **`biolink:Food`** (whole fruits, roots, seeds, herbs, meats).

The `extract` check is applied **after** material identification and takes precedence over Food, so
an NCIt-food that is sold as an extract still lands in `ComplexMolecularMixture`. The never-food
veto comes after *that*, and only guards the `Food` branch: an explicit NCIt Food/Seed
classification still wins, so a food that is also a diagnostic agent
([`NCIT:C61506`](http://purl.obolibrary.org/obo/NCIT_C61506) "Inulin", used to measure GFR) is
unaffected.

Keep `nonfood_ncit_roots` **narrow**.
[`NCIT:C1909`](http://purl.obolibrary.org/obo/NCIT_C1909) "Pharmacologic Substance" is deliberately
*not* on it: NCIt files cocoa butter, garlic oil, rice bran and green tea under it, so vetoing on it
would throw out real foods.

`biolink:ComplexMolecularMixture` here is an **interim** type. The right long-term home for an
"extracted from a living organism" material is
[`biolink:ProcessedMaterial`](https://biolink.github.io/biolink-model/ProcessedMaterial/); adding it
as a Babel chemical output is tracked in
[#929](https://github.com/NCATSTranslator/Babel/issues/929), after which the extract rows flip from
`ComplexMolecularMixture` to `ProcessedMaterial`. "allergen" is **not** used as a signal (it was too
broad — it swept in NCBI-only danders/molds/insects and biologics); `extract` is the reliable
marker.

### Why the botanical flags but not NCBI

The plant-material signal keys on the **botanical** UNII columns only — `PLANTS` / `GRIN` / `MPNS`.
A UNII in any of those denotes plant material (a whole plant or a plant part/extract), and a
DrugBank plant material with no structure is safely `Food`/extract.

The broader `NCBI` organism flag is deliberately **excluded** — not because NCBI-flagged entries
aren't food (many are), but because the flag says nothing either way. NCBI Taxonomy covers animals,
bacteria, fungi *and* the source organisms of **biologic drugs**, so retyping on it would declare
immune globulins, vaccine antigens, antivenins and CAR-T cell therapies to be food. That is a worse
error than leaving a food as `ChemicalEntity`, so the 481 NCBI-only structureless entries are listed
separately (see Snapshot) and deferred to
[#930](https://github.com/NCATSTranslator/Babel/issues/930), which has to recover the genuine foods
hiding there ([`DRUGBANK:DB10541`](https://go.drugbank.com/drugs/DB10541) "Lobster",
[`DRUGBANK:DB10531`](https://go.drugbank.com/drugs/DB10531) "Flounder",
[`DRUGBANK:DB10516`](https://go.drugbank.com/drugs/DB10516) "Casein" and
[`DRUGBANK:DB10544`](https://go.drugbank.com/drugs/DB10544) "Cultivated mushroom" are NCBI-flagged
animals/fungi, and no NCIt class they carry sits under Food/Seed). The snapshot file carries the
per-entry evidence for that triage.

### Why the veto keys on NCIt subtrees, not UMLS semantic types

Babel otherwise leans on UMLS semantic types (STY) rather than NCIt classes, so the obvious question
is whether `nonfood_ncit_roots` could be a list of never-food STYs instead. It cannot, for
three reasons, each measured against this snapshot:

- **STYs are too coarse.** The tightest semantic type that catches ethiodized oil is `T130`
  "Indicator, Reagent, or Diagnostic Aid" — which also covers
  [`NCIT:C61506`](http://purl.obolibrary.org/obo/NCIT_C61506) "Inulin" (a real GFR diagnostic, so
  UMLS is not wrong) and black pepper. `T121` "Pharmacologic Substance" is far worse: it covers 71
  of the 304 `Food` rows. Every STY veto that catches the contrast agent takes real foods with it,
  whereas NCIt's "Imaging Agent" catches it and nothing else in the set.
- **STYs are too sparse here.** A semantic type is only reachable through the entry's NCIt code, and
  83 of the 304 `Food` rows have no NCIt class at all — they are typed by the botanical flag alone.
  There is also no Seed concept: barley and cotton seed come back `T002` "Plant", not `T168` "Food".
- **The tempting wrong route.** UNIIs *do* map into UMLS directly (`MRCONSO`, `SAB=MTHSPL`, UNII as
  `CODE` — 1071 of the 1168 UNIIs here). Do **not** type from it: in `MTHSPL` these products are all
  FDA drug ingredients, so it stamps `T121` "Pharmacologic Substance" on 122 of the 130
  NCIt-*foods*.

The semantic types are still worth having in front of you, so both snapshot CSVs carry them (see
Snapshot below) — they are how you decide what the *next* veto root should be.

## How it is wired

- `chemical_ncit_food_codes` (rule) queries UberGraph for the NCIt Food/Seed subtrees
  (`config.yaml: food_ncit_roots`) → `ncit/food_codes`.
- `chemical_ncit_nonfood_codes` (rule) does the same for the never-food subtrees
  (`config.yaml: nonfood_ncit_roots`) → `ncit/nonfood_codes`.
- `chemical_drugbank_food_extracts` (rule) reads the DrugBank vocabulary CSV + the UNII
  records + those NCIt codes + `config.yaml: drugbank_extract_markers`, and writes
  `ids/DRUGBANK_food_extracts` (`DRUGBANK:xxx\tbiolink:Type`) —
  `datahandlers/drugbank.py:write_drugbank_food_extract_types`. The plant-flag set comes
  from the same UNII records file (`unii.py:read_unii_flags`), so the rule needs no extra input.
- `chemicals.create_typed_sets` adds that type to the clique's type vote as an extra **candidate**
  (the entries carry no Babel type of their own, so a vote over their members alone would leave them
  as `ChemicalEntity`). The most preferred of the voted type and this evidence wins, ranked by
  `config.yaml: chemical_type_order` — where `biolink:Food` sits below every structure-bearing type,
  the mixture types included, and above only the uninformative `ChemicalEntity` it exists to improve
  on. So a clique that votes nothing but `ChemicalEntity` becomes `Food`, and one that votes
  `SmallMolecule` stays a `SmallMolecule`.

  This started out as a clique-level *override* that skipped the vote entirely, on the assumption
  that a food clique never also contains a genuine chemical. That assumption was derived from the
  concords and was wrong: see "The override that had to become a vote" below (issue #935).
- `Food.txt` and the pre-existing `ComplexMolecularMixture.txt` are both in
  `config.yaml: chemical_outputs`, so **no new output type is needed** for this change. `RXCUI`
  members survive because the chemical build already passes `extra_prefixes=[RXCUI]`. These cliques
  flow through DrugChemical conflation and every export like any other chemical output, and
  conflation never re-types them, so the retype survives downstream.

## Snapshot

Two committed files in this directory, regenerated by `scripts/generate_csvs.py` (which imports the
production `classify_food_or_extract`, so the CSVs can't drift from the pipeline). They are built
from DrugBank vocabulary `5-1-13` (pinned — DrugBank downloads are currently blocked) and the
current FDA UNII records.

- **`food-and-extracts.csv`** — the retype changes, **685 entries** = **304 `biolink:Food`** (130
  via NCIt Food/Seed, 174 via a botanical flag) + **381 `biolink:ComplexMolecularMixture`** (the
  extracts). Columns:
  `drugbank_curie, label, unii, ncit, ncit_label, ncit_parents, ncit_umls_semantic_types,
  biolink_type, future_biolink_type, signal`.
  `future_biolink_type` is `biolink:ProcessedMaterial` on the extract rows (the #929 target), blank
  on Food rows; `signal` is the evidence that fired — `ncit-food`, `botanical-flag`, or `extract`.
- **`ncbi-only-drugbank-entries.csv`** — the **481** NCBI-only structureless entries left as
  `ChemicalEntity` for now (the review set for
  [#930](https://github.com/NCATSTranslator/Babel/issues/930)). Columns:
  `drugbank_curie, label, unii, unii_preferred_name, ncbitaxon, ncbitaxon_label, unii_ncit,
  unii_ncit_label, unii_ncit_parents, unii_ncit_semantic_types, has_extract_marker`.

  The `ncbitaxon` columns are the point of the file: every one of the 481 has an NCBI taxon, and the
  taxon is the evidence that separates the three groups #930 has to tell apart — a food
  (`NCBITaxon:6706` "*Homarus americanus* (American lobster)"), a plant/animal material sold as an
  extract (`has_extract_marker` = `True`), and a biologic, whose taxon is its *source* organism and
  is usually `NCBITaxon:9606` "*Homo sapiens* (human)" or a livestock species. `unii_ncit` is often
  empty here (275 of 481 rows) — precisely the entries NCIt gives us no class for, hence no food
  signal. Fourteen taxon ids have no label, which is the usual sign of an id NCBI has since merged.

Both files carry the same two audit columns, and they exist to be *acted on*: `*_parents` is the
NCIt class's direct superclasses (`NCIT:C390 (Contrast Agent)`) and `*_semantic_types` its UMLS
semantic types (`T130=Indicator, Reagent, or Diagnostic Aid`). Reading a `Food` row whose semantic
type is `T121`/`T130`-ish — or whose parent is plainly an agent class — is how you find the next
`nonfood_ncit_roots` entry. That is also how ethiodized oil was found.

To regenerate after a UNII refresh: run the `chemical_ncit_food_codes` and
`chemical_ncit_nonfood_codes` rules (or `ncit.write_ncit_descendant_codes`) to produce
`ncit_food_codes` / `ncit_nonfood_codes`, then run `scripts/generate_csvs.py`. It also needs
`babel_downloads/NCIT/labels` and `babel_downloads/NCBITaxon/labels` for the label columns, and
`babel_downloads/UMLS/MRCONSO.RRF` / `MRSTY.RRF` for the semantic types; its module docstring has
the exact commands. Because DrugBank is pinned but the UNII records are re-downloaded fresh, exact
membership can drift slightly between refreshes.

## The override that had to become a vote

The original wiring **forced** any clique holding one of these CURIEs to `Food` or
`ComplexMolecularMixture`, skipping the type vote. That was justified here with a claim derived from
the concords: *672 of the 685 cliques have concord partners (1326 links), and every partner is typed
`ChemicalEntity`, so there is nothing to clobber.* A warning was added to catch it if that ever
stopped being true.

It was never true of a real build. The `babel-1.18` run fired that warning **seven** times, and
shipped seven small molecules in `Food.txt`. The clearest case is D-glucose:
[`DRUGBANK:DB09341`](https://go.drugbank.com/drugs/DB09341) "Dextrose, unspecified form" is a
structureless DrugBank row that NCIt correctly classifies as a food — but its UMLS/RxNorm concords
glom it into the real D-glucose clique, next to
[`CHEBI:17234`](http://purl.obolibrary.org/obo/CHEBI_17234) "glucose",
`PUBCHEM.COMPOUND:107526` (typed `biolink:SmallMolecule`) and an InChI Key. The override then
discarded that `SmallMolecule` and typed the whole clique `Food`.

The lesson is about the evidence, not the rule: a claim about clique membership cannot be checked
against the *concords*, because glom builds cliques transitively — a food CURIE and a small molecule
that share no direct concord edge still land in one clique via a third identifier.

Issue [#935](https://github.com/NCATSTranslator/Babel/issues/935) fixes this by making the evidence
a vote (see "How it is wired"). `scripts/replay_type_vote.py` replays `create_typed_sets` over a
finished build's `Food.txt` to measure the effect without a rebuild; `replay_type_vote.txt` is its
output for `babel-1.18`:

| Type after the vote | Cliques |
| --- | --- |
| `biolink:Food` | 285 |
| `biolink:SmallMolecule` | 5 |
| `biolink:MolecularMixture` | 2 |
| `biolink:ComplexMolecularMixture` | 1 |

Only the eight broken cliques move, and no identifier is dropped — `DRUGBANK:DB09341` is still a
member of the D-glucose clique, which is now a `SmallMolecule` again. Cliques that vote nothing more
specific stay `Food`, including the ones that *should*: inulin
([`CHEBI:15443`](http://purl.obolibrary.org/obo/CHEBI_15443) "inulin") is both a food and a GFR
diagnostic.

**A real build confirmed the replay exactly**: 8 cliques left `Food.txt`, holding 102 identifiers,
into those same four types, with nothing dropped. See [clique-diff.md](./clique-diff.md) for the
build-vs-build diff, which also covers the ~4,269 `biolink:Drug` cliques the reordered
`chemical_type_order` demotes to `ChemicalEntity` — a much larger effect than the food change, and
the one to look at when this ships in a release.

Note which direction the ordering runs: `ChemicalMixture` outranks `Food`, so a clique that *voted*
`ChemicalMixture` would keep it rather than become a food. None of the 293 does — the two
`ChemicalMixture` members in the set sit in cliques whose majority vote is `ChemicalEntity`, which
`Food` does outrank.

The script also reports the one pairing the ordering leaves exposed: `chemical_type_order` ranks
`biolink:Drug` *last*, below `Food`, so a clique that votes `Drug` and carries food evidence is
typed `Food`. That is mildly wrong — a drug formulation is not a food — but it is an **accepted
tradeoff**, not an oversight. No clique does it today (none of the 674 typed members is a `Drug`,
and in the build-vs-build diff no clique anywhere moves *into* `Food.txt` — `Food` only loses),
`Drug` is last for its own good reasons, and both a reorder and a special case would cost more than
the error does. `test_food_evidence_beats_a_drug_vote` pins the behaviour so it cannot change
silently; invert that assertion rather than deleting it if `Food` ever starts appearing where a drug
formulation belongs.

## Known limitations

- **The `extract` marker is a proxy, so one category can split on wording.** A pollen listed as
  "*Genus species* pollen **extract**" lands in `ComplexMolecularMixture` while a bare "*Genus
  species* pollen" lands in `Food`, purely on incidental wording — and a plain food does the same:
  [`DRUGBANK:DB14242`](https://go.drugbank.com/drugs/DB14242) "Honey" is an extract only because one
  of its DrugBank synonyms is "Honey Extract". Tightening this (and the whole
  Food-vs-`biolink:OrganismTaxon` question) is deferred to
  [#926](https://github.com/NCATSTranslator/Babel/issues/926).
- **Non-food plant materials still default to `Food` unless NCIt calls them a drug.** The never-food
  veto only catches what NCIt has *classified* — it caught the contrast agent and the
  antineoplastic, but a botanically-flagged material NCIt says nothing useful about still becomes
  `Food`. The 21 pollen entries are what remains of this:
  [`DRUGBANK:DB10346`](https://go.drugbank.com/drugs/DB10346) "Acacia longifolia pollen" is `Food`,
  though NCIt files it under [`NCIT:C79660`](http://purl.obolibrary.org/obo/NCIT_C79660) "Pollen" →
  "Plant Part" and its semantic type is `T002` "Plant". A `Pollen` veto root was considered and
  **rejected**: pollen is arguably edible, so `ChemicalEntity` would be a less informative answer,
  not a more correct one. What pollen actually wants is a type for a *raw organism part used as a
  substance*, which Biolink has no class for — the nearest are `ComplexMolecularMixture` and the
  [#929](https://github.com/NCATSTranslator/Babel/issues/929) `ProcessedMaterial` (raw pollen is not
  processed; pollen *extracts* already go there). Tracked on
  [#926](https://github.com/NCATSTranslator/Babel/issues/926).
- **NCBI-only entries are not retyped** (#930), including the genuine foods among them (lobster,
  flounder, casein, mushroom). This is the deliberate trade in the other direction: leave ~481
  entries under-typed rather than call an immune globulin a food.
- **Seven unflagged entries regress.** A handful of entries previously typed
  `ComplexMolecularMixture` via the old "allergen" match carry **no** organism flag at all and are
  not NCIt-food — "Pyrethrum extract", "Allergenic extract- beef liver", "Penicillium glaucum", and
  four `-lerbart` names. They fall outside both files and revert to `ChemicalEntity`; they can be
  picked up in #930 or by manual mapping.
