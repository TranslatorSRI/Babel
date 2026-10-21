# Babel 2026jul22

- Babel: [2026jul22](https://stars.renci.org/var/babel_outputs/2026jul22/)
  ([tagged 2026jul22](https://github.com/NCATSTranslator/Babel/releases/tag/2026jul22),
  approx [Babel v1.18.1](https://github.com/NCATSTranslator/Babel/releases/tag/v1.18.1), branch
  `babel-1.18.1`)
  - Built against
    [Biolink Model v4.4.3](https://github.com/biolink/biolink-model/releases/tag/v4.4.3)
  - [Summary tables](./reports/tables/)
  - [CURIE summary](./reports/content/compendia_report.json)
  - [Prefix report](./reports/duckdb/prefix_report.json)
- NodeNorm: [v2.5.0](https://github.com/NCATSTranslator/NodeNormalization/releases/tag/v2.5.0),
  [v2.5.1](https://github.com/NCATSTranslator/NodeNormalization/releases/tag/v2.5.1)
- NameRes: [v1.7.0](https://github.com/NCATSTranslator/NameResolution/releases/tag/v1.7.0)

Next release: *None as yet*
Previous release: [Babel 2025sep1](../2025sep1/README.md)

## Bugfixes

<!-- Wrong behaviour a consumer may have relied on, now corrected: "you used to see X, you now see
     Y -- check whether X affected your downstream analyses, and whether Y is the behaviour you
     want." -->

- **Junk NCBIGene synonyms are gone**
  ([Babel #853](https://github.com/NCATSTranslator/Babel/pull/853)).
  NCBI splits a comma-containing alias across `gene_info.gz`'s pipe-delimited synonym field, and
  earlier builds emitted the fragments as synonyms in their own right: 912 of them across 276
  genes, including bare `MET` and `CYS`, which are real human gene symbols. A NameRes search for
  one of those names against 2025sep1 or earlier could match the wrong gene; re-check any result
  that relied on a short symbol. Genuine double-prime nomenclature (`U2B''`, RNA polymerase
  `beta''`) is deliberately kept -- narrowing the fix to protect those ~4,000 synonyms is what
  [Babel #917](https://github.com/NCATSTranslator/Babel/pull/917)'s full-file analysis established.
- **ChEBI secondary identifiers and PubChem xrefs are back**
  ([Babel #951](https://github.com/NCATSTranslator/Babel/pull/951)). Upstream tag renames in the
  ChEBI SDF silently emptied both ingests, so ChEBI cliques in earlier builds were missing xrefs
  they should have had. Cliques that look newly merged against 2025sep1 are mostly this.
- **The `biolink:Food` clique-level retype is corrected**
  ([Babel #948](https://github.com/NCATSTranslator/Babel/pull/948)) -- seven cliques including
  D-glucose and tocopherol shipped as `biolink:Food` in babel-1.18 and are back to their own types.
  Written up in full under Known issues below; it is the clearest "you saw X, you now see Y" case in
  this release.

## Areas that changed substantially

Explanations for the movements in the `Compendium size comparison` table at the end of this note.
This is a large release -- 605.9M CURIEs against
689.0M in 2025sep1, a net 12.1% drop -- and consumers should expect several compendia to look very
different.

- [MAJOR] **Protein is down 38.2%** (275.5M -> 170.2M CURIEs), essentially all of it UniProtKB
  (253.7M -> 149.9M identifiers, \-40.9%). **This is an upstream UniProt change, not a Babel one.**
  UniProt now keeps only proteins from reference proteomes: the
  [2026-06-10 release notes](https://www.uniprot.org/release-notes/2026-06-10-release) say "the
  number of UniProtKB accessions has been reduced by 43%", and
  [the change announcement](https://www.uniprot.org/help/refprot_only_changes) gives "removal of
  approximately 57 million protein entries, which will result in an estimated total of 150 million
  protein entries in UniProtKB". Babel ended at 149.9M UniProtKB identifiers, which matches. No PR
  in this release touches the UniProtKB ingest, the protein completeness report shows
  `Missing identifiers: 0`, and the run logs record no UniProt download or parse errors.
  - Nothing to do for now. If a specific protein people care about turns out to have been dropped,
    that is the point at which to work out whether Babel should retain non-reference-proteome
    entries from another source.
- [MAJOR] **PhenotypicFeature is down 78.5%** (483,108 -> 103,707) and `umls` is up 41.9%
  (897,846 -> 1,274,014). These are the same movement: UMLS semantic types T033/T034 were excluded
  from disease-phenotype so that leftover-UMLS re-types them
  ([Babel #818](https://github.com/NCATSTranslator/Babel/pull/818),
  [Babel issue #569](https://github.com/NCATSTranslator/Babel/issues/569)), and the UMLS identifiers
  in PhenotypicFeature (365,930 -> 28,761) carried their SNOMEDCT (47,823 -> 6,164) and MEDDRA
  (22,809 -> 6,409) clique partners with them. Consumers resolving a phenotype by UMLS CUI will now
  get a leftover UMLS clique instead.
- [MAJOR] **ChemicalEntity is down 87.2%** (4,046,131 -> 518,554), which unwinds the 510% jump
  reported in [2025sep1](../2025sep1/README.md). The identifiers moved to SmallMolecule (+4.5%,
  +9.9M): INCHIKEY in ChemicalEntity went 1,776,876 -> 55,437 and PUBCHEM.COMPOUND 1,647,448 ->
  14,116, while the corresponding SmallMolecule rows grew by about the same amounts. Chemicals that
  were previously typed only as the generic `biolink:ChemicalEntity` now get a definite type.
- **MacromolecularComplex is up 1,536%** (1,258 -> 20,579) -- all ComplexPortal species are now
  ingested ([Babel #831](https://github.com/NCATSTranslator/Babel/pull/831)).
- **A new `Food` compendium** (932 CURIEs) from the DrugBank retype
  ([Babel #918](https://github.com/NCATSTranslator/Babel/pull/918)); see the known issue above.
- **4,269 `Drug` cliques are now `ChemicalEntity`.** Moving the chemical type precedence into
  `config.yaml: chemical_type_order`
  ([Babel #948](https://github.com/NCATSTranslator/Babel/pull/948)) ranked `biolink:Drug` below
  `biolink:ChemicalEntity`, which flips every clique whose type vote was tied between the two. All
  4,269 are two- or three-member RxNorm formulation stubs (`RXCUI`+`UMLS`, occasionally `MESH`) with
  no structural identifier, so a consumer normalizing one of those RXCUIs now gets
  `biolink:ChemicalEntity` where 2025sep1 returned `biolink:Drug`. The net `Drug` row in the table
  below is only -0.7% because other changes added cliques over the same period; the retype itself
  was measured directly by
  [the clique diff](../../docs/sources/DRUGBANK/food-and-extracts/clique-diff.md).
- **ComplexMolecularMixture is up 432.6%** (276 -> 1,470) -- the same DrugBank retype
  ([Babel #828](https://github.com/NCATSTranslator/Babel/pull/828)) moved plant, fruit and animal
  extracts here rather than into `Food`.
- **MP arrives in PhenotypicFeature** (31 -> 14,750), kept disjoint from HP
  ([Babel #886](https://github.com/NCATSTranslator/Babel/pull/886)).
- **EMAPA arrives in AnatomicalEntity and GrossAnatomicalStructure** (+64.7%, 15,709 -> 25,867)
  ([Babel #781](https://github.com/NCATSTranslator/Babel/pull/781)).
- **Gene is up 11.7%** (+9.3M, mostly NCBIGene +8.2M), **MGI is up 1,352%** (+537,947), and
  **Publication is up 5.3%** (+4.2M).
- **Polypeptide is down 97.0%** (166 -> 5) -- an incidental number, not a lost ingest. Babel assigns
  `biolink:Polypeptide` only from ChEBI's peptide subclasses
  ([`CHEBI:16670`](http://purl.obolibrary.org/obo/CHEBI_16670) "peptide") and MeSH `D12.125` /
  `D12.644`, but the Biolink Model registers only `UniProtKB`, `PR`, `ENSEMBL`, `FB` and `UMLS` as
  `id_prefixes` for that class (identical in 4.2.6-rc5 and 4.4.3, so the model update is not the
  cause). `write_compendium()` therefore drops every CHEBI and MESH member, and `Polypeptide.txt`
  holds only the residue: cliques that happened to pick up a UMLS CURIE as well. With chemical
  cliques rearranged this much and UMLS refreshed 2025AA -> 2026AA, that residue moved. Across the
  whole build, cliques *typed* `biolink:Polypeptide` went 380 -> 219 and the portion outside
  `Polypeptide.txt` (leftover UMLS) is 214 in both releases -- the whole change is in this one file.
  The underlying prefix-registration mismatch predates this release and is worth an issue in its own
  right; it is written up in
  [Architecture.md](../../docs/Architecture.md#polypeptidetxt-is-a-residue-not-a-compendium).

## Babel changes

### Updates

- Updated the Biolink Model from 4.2.6-rc5 to 4.4.3.
- Updated UMLS from 2025AA to 2026AA.
- Updated RxNorm from 07072025 to 07062026.

### New features and identifier/mapping additions

- Added the Mammalian Phenotype Ontology (MP,
  [Babel #886](https://github.com/NCATSTranslator/Babel/pull/886)) and the Mouse Developmental
  Anatomy Ontology (EMAPA, [Babel #781](https://github.com/NCATSTranslator/Babel/pull/781)).
- Expanded the ComplexPortal ingest to include all species, not just *Saccharomyces cerevisiae*
  [Babel #831](https://github.com/NCATSTranslator/Babel/pull/831).
- Created a Food compendium by identifying DrugBank identifiers that are really plant/fruit/animal
  extracts [Babel #828](https://github.com/NCATSTranslator/Babel/pull/828),
  [Babel #918](https://github.com/NCATSTranslator/Babel/pull/918),
  [Babel #948](https://github.com/NCATSTranslator/Babel/pull/948).
- Added HGNC gene symbols from mim2gene.txt as OMIM labels
  [Babel #801](https://github.com/NCATSTranslator/Babel/pull/801).
- Added an obsolete label file, allowing them to be excluded
  [Babel #806](https://github.com/NCATSTranslator/Babel/pull/806).
- Added a source impact report tool for evaluating the effect of adding a new concord to an existing
  clique [Babel #742](https://github.com/NCATSTranslator/Babel/pull/742).
- Babel releases are now published to Zenodo at
  [doi:10.5281/zenodo.18489042](https://doi.org/10.5281/zenodo.18489042)
  [Babel #660](https://github.com/NCATSTranslator/Babel/pull/660).

### Improvements to Babel's output

- Reworked the DrugChemical conflation order. The more complex system introduced in
  [Babel #506](https://github.com/NCATSTranslator/Babel/pull/506) gave consistently bad results, so
  it was simplified to a custom prefix order based on the ChemicalEntity prefix order
  ([Babel #626](https://github.com/NCATSTranslator/Babel/pull/626)), which gave much better results.
- Reorganized UMLS and MeSH tree mappings to the Biolink Model, including in:
  - Proteins: [Babel #668](https://github.com/NCATSTranslator/Babel/pull/668), building on the
    UMLS concord added in [Babel #495](https://github.com/NCATSTranslator/Babel/pull/495)
  - Disease/phenotype: [Babel #818](https://github.com/NCATSTranslator/Babel/pull/818)
- Narrowed UniChem overused-xref filtering to UNII, KEGG.COMPOUND and DrugCentral, so every other
  prefix -- CHEBI above all -- keeps its complete set of mappings
  [Babel #508](https://github.com/NCATSTranslator/Babel/pull/508).
- Fixed ChEBI SDF tag renames [Babel #951](https://github.com/NCATSTranslator/Babel/pull/951) and
  reading its database accession file
  [Babel #955](https://github.com/NCATSTranslator/Babel/pull/955).
- Improved preferred labels:
  - Updated preferred label prefix overrides:
    [Babel #657](https://github.com/NCATSTranslator/Babel/pull/657)
  - Label demotion for long labels is now limited to chemicals:
    [Babel #725](https://github.com/NCATSTranslator/Babel/pull/725)
- Added a `taxon_specific` boolean flag to the synonyms output, so Solr can identify cliques that
  aren't taxon-specific [Babel #604](https://github.com/NCATSTranslator/Babel/pull/604).
- Added support for switching UMLS to the level 0 vocabularies only, if needed
  [Babel #605](https://github.com/NCATSTranslator/Babel/pull/605).

### Development and infrastructure

- Moved Babel from the TranslatorSRI org to the
  [NCATSTranslator org](https://github.com/NCATSTranslator/Babel). Old URLs still redirect, but
  bookmarks and scripts are worth updating.
- Improved download robustness [Babel #861](https://github.com/NCATSTranslator/Babel/pull/861).
  - Babel downloads now set a custom User-Agent, which is necessary for some downloads
    [Babel #797](https://github.com/NCATSTranslator/Babel/pull/797).
  - Including for SPARQL queries [Babel #876](https://github.com/NCATSTranslator/Babel/pull/876).
- Updated Babel to work on Slurm using the
  [Snakemake Slurm executor plugin](https://snakemake.github.io/snakemake-plugin-catalog/plugins/executor/slurm.html)
  [Babel #594](https://github.com/NCATSTranslator/Babel/pull/594), then tuned the per-rule resource
  requests against real runs [Babel #860](https://github.com/NCATSTranslator/Babel/pull/860),
  [Babel #869](https://github.com/NCATSTranslator/Babel/pull/869).
  - Added
    [Snakemake benchmarking](https://snakemake.readthedocs.io/en/stable/tutorial/additional_features.html#benchmarking)
    [Babel #679](https://github.com/NCATSTranslator/Babel/pull/679) as well as a script for
    comparing benchmarks to the resources set for each rule.
- Replaced requirements.txt with [uv](https://docs.astral.sh/uv/) for managing packages
  [Babel #598](https://github.com/NCATSTranslator/Babel/pull/598), and added ruff, snakefmt
  [Babel #599](https://github.com/NCATSTranslator/Babel/pull/599) and rumdl
  [Babel #614](https://github.com/NCATSTranslator/Babel/pull/614) for formatting source files.
- Significantly improved Babel's documentation
  [Babel #614](https://github.com/NCATSTranslator/Babel/pull/614),
  [Babel #685](https://github.com/NCATSTranslator/Babel/pull/685), including per-source notes such
  as MeSH [Babel #808](https://github.com/NCATSTranslator/Babel/pull/808). This covers both prose
  for human readers and the reference material coding agents work from — `AGENTS.md`, the
  per-directory `CLAUDE.md` files, and long-form guides like `docs/AddingNewSources.md`.
- Significantly improved testing
  [Babel #756](https://github.com/NCATSTranslator/Babel/pull/756),
  [Babel #810](https://github.com/NCATSTranslator/Babel/pull/810), which is broken up into:
  - Unit tests: small, fast tests that can be run after every change and on every PR.
  - Network tests: tests that access online resources. These are scheduled to run weekly on GitHub
    Actions.
  - Pipeline tests: tests that test an entire Babel pipeline or a part of it, usually by invoking
    Snakemake. This means that already downloaded/generated files won't be redownloaded/regenerated,
    making these tests fast after their initial run. Not feasible for larger pipelines that require
    large memory.
    - This includes identifier partition tests
      [Babel #692](https://github.com/NCATSTranslator/Babel/pull/692), which check whether the same
      UMLS/MeSH identifier is ending up in multiple pipeline outputs.
  - Slow tests: tests from any of the above categories which take a long time to run -- they are
    excluded by default.
- Reorganized some code, including testing code, creating another shared file for utility functions,
  tools [Babel #896](https://github.com/NCATSTranslator/Babel/pull/896) and file for predicates
  [Babel #778](https://github.com/NCATSTranslator/Babel/pull/778).
- Built tools for:
  - Comparing each build's prefix counts against the previous release
    [Babel #889](https://github.com/NCATSTranslator/Babel/pull/889)
  - Generating release notes across Babel/NodeNorm Redis/NameRes Solr
    [Babel #983](https://github.com/NCATSTranslator/Babel/pull/983)

### Minor changes and fixes

- Fixed Chembl label bug [Babel #585](https://github.com/NCATSTranslator/Babel/pull/585).
- The leftover UMLS file no longer generates its own custom compendium file, but rather assembles
  one-identifier cliques in memory and passes them to write_compendia()
  [Babel #809](https://github.com/NCATSTranslator/Babel/pull/809).
- Various leftover UMLS improvements:
  [Babel #816](https://github.com/NCATSTranslator/Babel/pull/816),
  [Babel #863](https://github.com/NCATSTranslator/Babel/pull/863)
- Various UniChem fixes: [Babel #865](https://github.com/NCATSTranslator/Babel/pull/865)
- Various DrugChemical improvements: [Babel #864](https://github.com/NCATSTranslator/Babel/pull/864)
- Fixed the NCBIGene source alias field
  [Babel #853](https://github.com/NCATSTranslator/Babel/pull/853), characterized against all 70.5M
  rows of `gene_info.gz` in [Babel #917](https://github.com/NCATSTranslator/Babel/pull/917)
- Added tools for:
  - Reporting the status of the current Slurm run
    [Babel #862](https://github.com/NCATSTranslator/Babel/pull/862)
  - Comparing two Babel compendia: [Babel #877](https://github.com/NCATSTranslator/Babel/pull/877)
    [Babel #885](https://github.com/NCATSTranslator/Babel/pull/885)

### Known issues and caveats

- DrugBank downloads are currently disabled
  (<https://github.com/cthoyt/drugbank-downloader/issues/23>), so I used the previously downloaded
  5-1-13.
- HMDB is now behind a Cloudflare bot challenge, so the pipeline can't download it (see
  `docs/sources/DownloadPatterns.md`). Unlike DrugBank, this is not a reused old download -- I
  downloaded it fresh in a browser on my laptop and copied it to the HPC.
- The `biolink:Food` retype in [Babel #918](https://github.com/NCATSTranslator/Babel/pull/918)
  applied at the clique level, so cliques that reach DrugBank food evidence through RXCUI/UMLS
  -- including D-glucose and tocopherol -- shipped as `biolink:Food` in babel-1.18
  ([Babel issue #935](https://github.com/NCATSTranslator/Babel/issues/935)). The fix
  ([Babel #948](https://github.com/NCATSTranslator/Babel/pull/948), commit `43157fa7`) is in the
  2026jul22 tag, and `Food.txt` is down to 285 cliques: a build-vs-build
  [clique diff](../../docs/sources/DRUGBANK/food-and-extracts/clique-diff.md) puts **eight** cliques
  leaving `Food` (293 -> 285) holding 102 identifiers -- the seven the babel-1.18 warning caught,
  five of which return to `biolink:SmallMolecule` and two to `biolink:MolecularMixture`, plus
  "Cantaloupe", which goes to `biolink:ComplexMolecularMixture` by the extract rule. No identifier
  was dropped. Confirmed fixed in the deployed build:
  [`CHEBI:4167`](http://purl.obolibrary.org/obo/CHEBI_4167) "D-glucose" and
  `DRUGBANK:DB09341` now normalize to a single `biolink:SmallMolecule` clique, so the DrugBank food
  identifier takes its clique's type rather than imposing `biolink:Food` on it.

## NodeNorm Redis

- NodeNorm now uses the preferred name provided by Babel rather than duplicating its selection logic
  [NodeNorm #386](https://github.com/NCATSTranslator/NodeNormalization/pull/386)
  - This required increasing the size of one of the databases so that we could include the preferred
    names for all cliques
- Enormous improvements to the Redis database loader
  [NodeNorm #384](https://github.com/NCATSTranslator/NodeNormalization/pull/384),
  [NodeNorm #391](https://github.com/NCATSTranslator/NodeNormalization/pull/391),
- Added to /status: NodeNorm version
  [NodeNorm #361](https://github.com/NCATSTranslator/NodeNormalization/pull/361) and backend type
  [NodeNorm #392](https://github.com/NCATSTranslator/NodeNormalization/pull/392)

## NameRes Solr

- Enormous improvements to Solr data loading
  [NameRes #278](https://github.com/NCATSTranslator/NameResolution/pull/278)
- Use `taxon_specific` flag from Babel so that taxon-specific queries don't filter out
  non-taxon-specific cliques
  [NameRes #214](https://github.com/NCATSTranslator/NameResolution/pull/214)
- Stop storing names_exactish -- we only need the index
  [NameRes #225](https://github.com/NCATSTranslator/NameResolution/pull/225)
- Added logging to track request times
  [NameRes #230](https://github.com/NCATSTranslator/NameResolution/pull/230)
- Improved /status [NameRes #246](https://github.com/NCATSTranslator/NameResolution/pull/246)
- Improved documentation [NameRes #215](https://github.com/NCATSTranslator/NameResolution/pull/215)
  [NameRes #271](https://github.com/NCATSTranslator/NameResolution/pull/271), particularly
  deployment instructions [NameRes #240](https://github.com/NCATSTranslator/NameResolution/pull/240)
- Updated defaults: autocomplete defaults to false
  [NameRes #210](https://github.com/NCATSTranslator/NameResolution/pull/210)

## Deployed database sizes

Sizes of the deployed databases, read from `https://nodenormalization-exp.apps.renci.org/status`
and `https://name-resolution-exp.apps.renci.org/status`. Both report Babel 2026jul22 (NodeNorm
v2.5.1, NameRes v1.7.0).

| Database name    | Database ID         | Number of keys | Memory used |
|------------------|---------------------|----------------|-------------|
| id-id            | eq_id_to_id_db      | 605,837,726    | 53.78G      |
| id-eq-id         | id_to_eqids_db      | 398,664,426    | 94.61G      |
| id-categories    | id_to_type_db       | 398,664,426    | 30.21G      |
| semantic-count   | curie_to_bl_type_db | 135            | 30.33M      |
| info-content     | info_content_db     | 398,664,426    | 57.54G      |
| conflation-db    | gene_protein_db     | 35,199,861     | 4.49G       |
| chemical-drug-db | chemical_drug_db    | 104,863        | 210.54M     |
| Solr             | name_lookup         | 331,513,708    | 109.75 GB   |

## Compendium size comparison

| Filename                               | 2025sep1    | 2026jul22   | Diff          | % Diff    |
| -------------------------------------- | ----------- | ----------- | ------------- | --------- |
| Count of CURIEs in all files           | 688,983,999 | 605,864,191 | \-83,119,808  | \-12.1%   |
| Count of cliques in all files (approx) | 490,293,340 | 388,490,111 | \-101,803,229 | \-20.8%   |
| AnatomicalEntity                       | 249,584     | 252,287     | +2,703        | +1.1%     |
| BiologicalProcess                      | 67,929      | 65,256      | \-2,673       | \-3.9%    |
| Cell                                   | 13,175      | 13,952      | +777          | +5.9%     |
| CellLine                               | 38,810      | 38,896      | +86           | +0.2%     |
| CellularComponent                      | 14,696      | 14,818      | +122          | +0.8%     |
| ChemicalEntity                         | 4,046,131   | 518,554     | \-3,527,577   | \-87.2%   |
| ChemicalMixture                        | 530         | 609         | +79           | +14.9%    |
| ComplexMolecularMixture                | 276         | 1,470       | +1,194        | +432.6%   |
| Disease                                | 632,330     | 639,398     | +7,068        | +1.1%     |
| Drug                                   | 360,925     | 358,459     | \-2,466       | \-0.7%    |
| Food                                   | 0           | 932         | +932          | Infinity% |
| Gene                                   | 79,427,652  | 88,740,328  | +9,312,676    | +11.7%    |
| GeneFamily                             | 28,050      | 28,463      | +413          | +1.5%     |
| GrossAnatomicalStructure               | 15,709      | 25,867      | +10,158       | +64.7%    |
| MacromolecularComplex                  | 1,258       | 20,579      | +19,321       | +1535.9%  |
| MolecularActivity                      | 206,636     | 213,714     | +7,078        | +3.4%     |
| MolecularMixture                       | 21,879,355  | 23,892,388  | +2,013,033    | +9.2%     |
| OrganismTaxon                          | 3,543,867   | 3,745,133   | +201,266      | +5.7%     |
| Pathway                                | 53,125      | 53,772      | +647          | +1.2%     |
| PhenotypicFeature                      | 483,108     | 103,707     | \-379,401     | \-78.5%   |
| Polypeptide                            | 166         | 5           | \-161         | \-97.0%   |
| Protein                                | 275,514,857 | 170,218,499 | \-105,296,358 | \-38.2%   |
| Publication                            | 79,773,973  | 83,969,833  | +4,195,860    | +5.3%     |
| SmallMolecule                          | 221,734,011 | 231,673,258 | +9,939,247    | +4.5%     |
| umls                                   | 897,846     | 1,274,014   | +376,168      | +41.9%    |
