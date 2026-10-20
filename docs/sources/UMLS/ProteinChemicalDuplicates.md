# UMLS and MeSH identifiers duplicated between Protein and the chemical compendia

Babel issue [#308] (2024) reported UMLS CUIs appearing in both a chemical compendium and
`Protein.txt`, so that a CUI and its MeSH descriptor no longer normalized to the same clique. This
document records what is fixed, what is still broken in the 2026jul22 build, and exactly which
build inputs the remaining duplicates come through. It is the analysis behind the rewritten #308
and the fix direction in [#849]; [#513] describes the same residual from the other end. The numbers
are regenerable: `2026jul22.md` in `protein-chemical-duplicates/` is the output of
`src/reports/duplicate_curie_routes.py`, which needs no local build (see
[Regenerating the analysis](#regenerating-the-analysis)).

## What is fixed: the ids files are disjoint

PR #444 (May 2025) made `chemicals.write_umls_ids()` blocklist the three protein semantic-type
trees — `A1.4.1.2.1.7` "Amino Acid, Peptide, or Protein" (T116), Enzyme (T126) and Receptor
(T192) — that `protein.write_umls_ids()` claims. `tests/pipeline/test_umls.py` and
`tests/pipeline/test_vocabulary_partitioning.py` guard that split (both pass against UMLS 2026AA
with the current code), and the 2026jul22 build's `Identifier.parquet` confirms it: no duplicated
CUI traced below is listed by both `protein/ids/UMLS` and `chemicals/ids/UMLS`. That was #308's
preferred option 1, and it removed the duplicates that come from typing a CUI twice. The issue's
first example, `UMLS:C0106132` "Chorionic Gonadotropin, beta Subunit, Human", now resolves as one
Protein clique with `MESH:D018997` and `UniProtKB:P01233`.

## What is still broken: cross-references re-join what the ids files separated

The build's own report `reports/umls/duplicate-curies.csv` (written by `leftover_umls`; it counts
and reports, it never fails the build) lists 577 UMLS CUIs in more than one compendium in
2026jul22. 542 of them are Protein against a chemical compendium:

| Compendia                  | CUIs |
|----------------------------|-----:|
| Protein + SmallMolecule    |  268 |
| ChemicalEntity + Protein   |  252 |
| MolecularMixture + Protein |   21 |
| Food + Protein             |    1 |

They arrive by two routes, in opposite directions:

- **526 CUIs claimed by `protein/ids/UMLS`, pulled into a chemical clique by DrugCentral.** On the
  Protein side these are 522 UMLS-led cliques with no UniProtKB member (plus a few UniProtKB-led
  ones). Every one carries T116 plus a chemical semantic type (T121 "Pharmacologic Substance" above
  all), and many are plainly small molecules — 5-hydroxytryptophan, acetylcysteine, 6-aminocaproic
  acid — because T116 *includes amino acids by definition*. `build_drugcentral_relations()`
  (`src/createcompendia/chemicals.py`) writes DrugCentral's `UMLSCUI` column as `xref` edges without
  looking at the CUI's semantic types, and DrugCentral entries are approved drugs with UNII/ChEBI
  structures, so the CUI lands in a UNII- or CHEBI-led chemical clique (leaders on the chemical
  side: UNII 284, CHEBI 226, MESH 15, others 17).
- **16 CUIs claimed by `chemicals/ids/UMLS`, pulled into a UniProtKB-led Protein clique by
  `protein/concords/UMLS_UniProtKB`** (the UMLS→UniProtKB mapping from PR #361, `oio:closeMatch`);
  `chemicals/concords/UMLS` links the same 15 CUIs to MeSH supplementary records on the chemical
  side. These are the protein drugs (`UMLS:C1144523` "Glucarpidase" and the like) whose home is
  the design question in [#849].

### The larger half: MeSH descriptors dragged into Protein

PR #495 added `protein.build_umls_relationships()`, which maps every CUI in `protein/ids/UMLS` to
its MeSH descriptor and DrugBank ID through `MRCONSO.RRF` — with a `TODO` noting it does not check
whether those partners are chemicals. They usually are: the DuckDB report
`reports/duckdb/duplicate_curies.tsv` has 17,631 MeSH descriptors (and 1,145 DrugBank IDs) in both
Protein and a chemical compendium, and tracing the MeSH descriptors shows

| Where the MeSH descriptor is listed | Descriptors |
|-------------------------------------|------------:|
| `chemicals/ids/MESH`                |      15,658 |
| `protein/ids/MESH`                  |       1,967 |
| reaches Protein only through `protein/concords/UMLS` | 15,664 |

So the protein compendium contains ~15.6k MeSH descriptors that the MeSH ingest deliberately
routed to chemicals (`docs/sources/MESH/Ingestion.md`), each one hauled in by a T116 CUI. That a
CUI's own MeSH descriptor sits in a chemical D-tree is also the best available evidence that the
CUI is a chemical, which is what the fix in [#849]'s plan builds on: re-home T116 CUIs whose MeSH
descriptor the chemical pipeline claims, so the CUI, its MeSH descriptor and its DrugBank ID all
leave Protein together.

### What NodeNorm serves as a result

NodeNorm keeps one clique per CURIE, so which side wins is arbitrary per identifier. Against the
2026jul22 build (dev NodeNorm, September 2026):

- `UMLS:C0000608` "6-aminocaproic acid" → a **Protein** clique `[UMLS:C0000608, NCIT:C47391,
  MESH:D015119, DRUGBANK:DB00513]`, while `CHEBI:16586` → a SmallMolecule clique that also lists
  those four identifiers.
- `MESH:D000111` "acetylcysteine" and `DRUGBANK:DB06151` → a Protein clique made only of UMLS CUIs;
  `CHEBI:28939` is the SmallMolecule.
- `MESH:D006916` "5-hydroxytryptophan" → SmallMolecule `CHEBI:17780`, but `NCIT:C52181` → Protein
  `UMLS:C0000578`.
- #308's second example, `UMLS:C0079633` "interleukin-8", is now a lone Protein clique: UMLS 2026AA
  maps `MESH:D016209` to `UMLS:C5886868` instead, and PubChem's MeSH concord takes the descriptor
  to `PUBCHEM.COMPOUND:74974005`.

The BabelTests in #308 assert the small-molecule cases; protein drugs (botulinum toxin,
interferons, hyaluronidase) are the design question in [#849], not this bug.

## Regenerating the analysis

```bash
uv run python -m src.reports.duplicate_curie_routes --build 2026jul22 \
    --output docs/sources/UMLS/protein-chemical-duplicates/2026jul22.md
```

It downloads the two duplicate reports into `data/2026jul22/reports/` if they are not there and
queries the build's `Identifier.parquet` and `Concord.parquet` in place on stars.renci.org (see
"Querying a published build in place" in [`docs/DataFormats.md`](../../DataFormats.md)); about
two minutes in all. `--side-a`/`--side-b` retarget it at another compendium pair.
`tests/pipeline/test_compendia_duplicates.py` runs the same counts against a build directory and
xfails while the duplicates are there, failing outright if they grow past the 2026jul22 baseline.

[#308]: https://github.com/NCATSTranslator/Babel/issues/308
[#513]: https://github.com/NCATSTranslator/Babel/issues/513
[#849]: https://github.com/NCATSTranslator/Babel/issues/849
