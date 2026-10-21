# A UMLS CUI follows its MeSH descriptor

Babel types UMLS CUIs and MeSH descriptors independently: each pipeline's `write_umls_ids()`
selects CUIs by UMLS semantic type (`umls.write_umls_ids()`, `src/datahandlers/umls.py`), and each
pipeline's `write_mesh_ids()` selects descriptors by MeSH tree number (`mesh.write_ids()`, see
[`MeSH/Ingestion.md`](../MeSH/Ingestion.md)). Each pipeline's UMLS concord (`umls.build_sets()`)
then links every CUI it claims to that CUI's MeSH descriptor and DrugBank ingredient. When the two
selections disagree about a CUI, the concord dragged another pipeline's descriptor into this
pipeline's clique, and the same identifiers ended up in two compendia, often leading a clique in
both. Two examples from the 2026jul22 build (issues [#276], [#308], [#1123]):

- [`UMLS:C0000608`](https://uts.nlm.nih.gov/uts/umls/concept/C0000608) "Aminocaproic Acid" is
  T116 "Amino Acid, Peptide, or Protein", so protein claimed it; its descriptor
  [`MESH:D015119`](https://meshb.nlm.nih.gov/record/ui?ui=D015119) "Aminocaproic Acid" is in a
  chemical D-tree, so chemicals claimed that. `protein/concords/UMLS` linked the two, and the
  descriptor (with `DRUGBANK:DB00513`) was in both `Protein.txt` and `SmallMolecule.txt`. 15,658
  MeSH descriptors and 1,145 DrugBank IDs were duplicated this way, and 891 MeSH descriptors led a
  clique in both `ChemicalEntity.txt` and `Protein.txt`.
- [`UMLS:C0242726`](https://uts.nlm.nih.gov/uts/umls/concept/C0242726) "Plant Roots" is T002
  "Plant", so taxon claimed it; [`MESH:D018517`](https://meshb.nlm.nih.gov/record/ui?ui=D018517)
  "Plant Roots" is in anatomy's A18 tree. 36 descriptors led a clique in both `AnatomicalEntity.txt`
  and `OrganismTaxon.txt`, and 18 MeSH `SCR_Chemical` virus records in `ChemicalEntity.txt` and
  `OrganismTaxon.txt`.

Downstream, Redis NodeNorm served whichever compendium loaded last, and NodeNorm-ES merged the two
cliques into one document whose `type` listed both Biolink classes
([biothings/NodeNormalizationAPI#41](https://github.com/biothings/NodeNormalizationAPI/issues/41)).

## The rule

MeSH placement is the tie-breaker. `rule umls_mesh_ownership` (`src/snakefiles/umls.snakefile`)
reads the `ids/MESH` of every pipeline in `config.yaml`'s `umls_mesh_owning_pipelines` (anatomy,
chemicals, disease, protein, taxon), and gene's and process's `ids/UMLS`, and writes
`intermediate/umls/mesh_ownership.tsv`: each CUI that has an *owner*, with the owner and the
descriptor that decided it (`umls.compute_mesh_owners()`). The five MeSH-owning pipelines read that
one file twice. `umls.apply_mesh_ownership()` runs inside `write_umls_ids()` after the
semantic-type blocklist, and the compendium build filters its concords:

1. A CUI is **owned** by a pipeline when that pipeline's `ids/MESH` is the only one that claims any
   of the CUI's descriptors, the pipeline does not refuse the CUI (disease's `badumls`), and
   neither gene nor process selects it.
2. The owner **adds** the CUI, whether its semantic types put it elsewhere or nowhere. Every other
   MeSH-owning pipeline **drops** it. The CUI takes the type of the descriptor it stands for, which
   is the descriptor rather than a supplementary concept record when it has both:
   [`UMLS:C0020388`](https://uts.nlm.nih.gov/uts/umls/concept/C0020388) "hydroxyproline" has
   [`MESH:D006909`](https://meshb.nlm.nih.gov/record/ui?ui=D006909) "Hydroxyproline" (typed
   Polypeptide) and [`MESH:C000716210`](https://meshb.nlm.nih.gov/record/ui?ui=C000716210)
   "trans-4-hydroxy-L-proline" (typed ChemicalEntity), and takes Polypeptide.
3. A CUI with no owner is left as its semantic types decided: its descriptors are claimed by no
   pipeline, by two or more (a descriptor MeSH cross-lists in two trees, such as
   [`MESH:D013171`](https://meshb.nlm.nih.gov/record/ui?ui=D013171) "Spores, Bacterial" in B05 and
   A11, or a CUI with descriptors on two sides), or only by a pipeline that refuses it, or gene or
   process selects it.
4. Each MeSH-owning pipeline's compendium build **skips every concord pair that names a CUI another
   pipeline owns** (`umls.read_cuis_owned_elsewhere()`). Dropping the CUI from `ids/UMLS` removes it
   from the concords built from that file (`UMLS`, protein's `NCIT_UMLS`), but not from those built
   from anything else: MONDO, HP and DOID xrefs in disease, DrugCentral in chemicals, the downloaded
   `UMLS_UniProtKB` mapping in protein. Without this, glom() put the CUI into the dropping
   pipeline's compendium as well as its owner's.

Three earlier drafts failed in ways worth not repeating:

- The first added a CUI to every pipeline that claimed its descriptor, which put 47 CUIs into two
  pipelines' `ids/UMLS` and failed `test_no_id_in_multiple_compendia`. Rule 3 is why that cannot
  happen now.
- The second let each pipeline decide alone, from its own `ids/MESH` and the union of the others'.
  A pipeline dropped a CUI whenever another pipeline claimed its descriptor, but only a *sole*
  claimant added it. So a CUI whose descriptors two other pipelines claimed, or whose only claimant
  refused it, was dropped and added nowhere, and it fell to the leftover compendium as a lone
  clique. Against 2026jul22 that lost one chemical and one protein CUI. Deciding ownership once,
  in one rule, makes "drop" safe: a pipeline only drops a CUI that the file gives to an owner, and
  the owner always adds it. `apply_mesh_ownership()` raises if the file gives a pipeline a CUI on
  that pipeline's own blocklist.
- The third had gene and process, which write no `ids/MESH`, drop any CUI a MeSH-owning pipeline
  owned, for the owner to add. But gene concords UMLS only to HGNC and process only to GO, so
  neither ever dragged a descriptor, and the rule *created* a duplicate it then resolved: 30 CUIs
  against UMLS 2026AA, e.g. `UMLS:C1413234` "CD68 gene", which UMLS also gives the protein
  descriptor `MESH:D000097382` "CD68 Molecule", moved from Gene into Protein. Their CUIs now have no
  owner (rule 3), and gene and process do not read the ownership file at all.

"Descriptor" means the MeSH atoms `build_sets()` concords: English, unsuppressed `SAB=MSH` atoms
whose term type is in `umls.MESH_CONCORD_TTYS` (`MH`, `NM`, `HT`, `QAB`). Both functions read that
constant, so a CUI moves exactly when its descriptor would be dragged; a CUI with only entry-term
atoms (`C0000005` → `D012711` via `PEP`/`ET`) is neither concorded nor moved.

`rule umls_mesh_ownership` parses `MRCONSO.RRF` once for every pipeline. It depends on all five
`ids/MESH` files, which depend only on `mesh.nt`, and on gene's and process's `ids/UMLS`, which
depend only on the UMLS download, so nothing waits on another pipeline's compendium. But a
single-target run of a MeSH-owning pipeline (`snakemake anatomy`) now builds all of those (see
"Per-target sizing" in [`docs/RunningBabel.md`](../../RunningBabel.md)).

A smaller-looking alternative was considered and rejected: pass each pipeline's own `ids/MESH` to
its UMLS concord as `build_sets()`'s `acceptable_identifiers`, the mechanism disease uses for OMIM
and NCIT, so that a pipeline never concords a CUI to another pipeline's descriptor. That stops the
drag without a cross-pipeline rule, but it leaves the CUI in the pipeline its semantic type chose,
cut off from the descriptor it stands for: `UMLS:C0000608` "Aminocaproic Acid" would stay a
Protein clique of its own while `MESH:D015119` and `DRUGBANK:DB00513` form a SmallMolecule clique,
two answers in NodeNorm for one concept. It would also need the same restriction on the DRUGBANK
concord, which drags DrugBank partners the same way.

Rule 2 is deliberately unconditional. A CUI that no pipeline's semantic types selected but whose
descriptor a pipeline owns used to fall to the leftover compendium (`umls.txt`) as a lone clique,
separated from the descriptor it stands for; now it joins that descriptor's clique. Against
2026jul22 this adds about 4,900 such CUIs to chemicals (T121 "Pharmacologic Substance" and T123
"Biologically Active Substance" CUIs of chemical descriptors) and 178 to disease, of which 150 are
T033 "Finding" CUIs of C-tree descriptors. The disease pipeline still does not map T033 by
semantic type (see the comment in `diseasephenotype.write_umls_ids()` and issue #569): only a
Finding CUI that *is* a disease descriptor's own concept joins it.

## What it changes, measured before a build

`scripts/replay_mesh_ownership.py` in `cui-follows-mesh/` runs the production writers with and
without the rule against a local UMLS download and a published build's ids files, and predicts which
duplicate leaders the rule resolves by finding the `<pipeline>/concords/UMLS` edge that dragged each
one. Its output for 2026jul22 is [`cui-follows-mesh/2026jul22.md`](cui-follows-mesh/2026jul22.md).
In short:

| pipeline  | dropped | added  | net     |
|-----------|--------:|-------:|--------:|
| anatomy   |      25 |    138 |    +113 |
| chemicals |     963 | 20,682 | +19,719 |
| disease   |      14 |    178 |    +164 |
| protein   |  15,748 |  1,098 | -14,650 |
| taxon     |      53 |     15 |     -38 |

Gene and process are unchanged. Rule 4 keeps 254 protein-owned CUIs out of chemicals' DrugCentral
concord, 73 chemical-owned CUIs out of protein's `UMLS_UniProtKB`, and a dozen or so out of
disease's MONDO, HP and DOID xrefs.

Of the 969 MeSH descriptors that led a clique in two compendia, the rule resolves 949. The other
20 are descriptors cross-listed in two trees (rule 3), which no UMLS rule can decide; they are the
rows of `input_data/known_duplicate_clique_leaders.tsv`, awaiting a stated priority between the
pipelines ([#730], [#1123]). glom() is transitive, so the prediction is confirmed with
`babel-clique-diff` after the first build.

2026jul22's `ids/MESH` came from the top-level `mesh.nt`, which NLM stopped updating after MeSH
2024. The next build reads the MeSH year `config.yaml` names ([#1144]), so
[`cui-follows-mesh/2026jul22-mesh2026.md`](cui-follows-mesh/2026jul22-mesh2026.md) repeats the
replay with every MeSH-owning pipeline's `ids/MESH` rebuilt from MeSH 2026
(`snakemake babel_outputs/intermediate/<pipeline>/ids/MESH --forcerun=get_mesh`) and 2026jul22's
`ids/UMLS`. MeSH 2026 adds about 1,700 records to those files and removes about 4,000 that NLM has
deleted. Six records move between chemicals and protein. The rule moves 46 more CUIs than above,
33 of them from protein to chemicals. The records two pipelines' `ids/MESH` share are unchanged, so
the allowlist still covers the cross-listed leaders, and the rule still resolves all 949 of the
others.

The replay needs the build's `ids/MESH` and `ids/UMLS` files and its duplicate reports, which
`scripts/fetch_build_inputs.py` exports from a published build's `Identifier.parquet` and
`reports/duckdb/` in under a minute; its `Concord.parquet`, read over https (see "Querying a
published build without downloading it" in [`docs/DataFormats.md`](../../DataFormats.md)); and a
local UMLS download. The replay itself takes about a minute. Read `VERSION.txt` before assuming
`var/babel/latest/` is the build you want.

## What it does not fix

- `protein/concords/UMLS_UniProtKB` is a downloaded mapping, not keyed on `protein/ids/UMLS`. Rule 4
  keeps a re-homed CUI out of the UniProtKB-led Protein clique it maps to, so the CUI and its
  descriptor are a chemical clique and the accession stays a Protein clique. Whether a protein drug
  belongs in one clique with its accession (the 16 in [#308]) is [#849].
- Identifiers no ids file claims that two pipelines' concords both reach (NCIT through UMLS xrefs
  into Anatomy and Protein, OMIM through Gene and Disease) are the member-level residue in
  `reports/duckdb/duplicate_curies.tsv`.
- The 18 MeSH `SCR_Chemical` virus records (e.g.
  [`MESH:C000719044`](https://meshb.nlm.nih.gov/record/ui?ui=C000719044) "H3N1 virus") now take
  their T005 CUIs into chemicals, because that is where MeSH's own record class puts them. If that
  reads as wrong in the clique diff, the fix is in `taxon.write_mesh_ids()`, not here.

## The control

`assert_no_unexpected_duplicate_clique_leaders()` (`src/reports/duckdb_reports.py`) fails the
build when `reports/duckdb/duplicate_clique_leaders.tsv` has a row that is not in
`input_data/known_duplicate_clique_leaders.tsv`; see "Build controls" in
[`docs/Architecture.md`](../../Architecture.md).

[#276]: https://github.com/NCATSTranslator/Babel/issues/276
[#308]: https://github.com/NCATSTranslator/Babel/issues/308
[#730]: https://github.com/NCATSTranslator/Babel/issues/730
[#849]: https://github.com/NCATSTranslator/Babel/issues/849
[#1123]: https://github.com/NCATSTranslator/Babel/issues/1123
[#1144]: https://github.com/NCATSTranslator/Babel/pull/1144
