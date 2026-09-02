# Per-source documentation

This folder holds documentation specific to an individual upstream data source — how Babel
downloads, parses, types, and routes that source's identifiers — as opposed to the
pipeline-wide documentation in the parent `docs/` folder.

## Layout

One directory per data source, named by the source's CURIE prefix (the same prefix used in
`src/prefixes.py` and in `babel_downloads/<PREFIX>/`). A source directory may contain multiple
files when there is enough to say (ingestion, synonyms, known issues, etc.).

When you learn something non-obvious about how a source is ingested, add it here rather than
letting it accumulate in `AGENTS.md` — `AGENTS.md` should point here, not duplicate the detail.

## Sources documented so far

- **PubMed** ([PubMed/README.md](./PubMed/README.md)) — the Publication compendium is built from
  the [pubmed2db](https://github.com/TranslatorSRI/pubmed2db) NDJSON export pinned by
  `config.yaml: pubmed2db_url`: the record contract Babel relies on (one record per PMID, deletions
  applied), the validation-report count check, how a DOI shared by several PMIDs is resolved, and
  the open `PMCID:` vs `PMC:` question (#1044).
- **COMPLEXPORTAL** ([COMPLEXPORTAL/Ingestion.md](./COMPLEXPORTAL/Ingestion.md)) — the
  `MacromolecularComplex` source: which ComplexTAB columns Babel reads, downloading all species
  files, the manifest-as-download-sentinel pattern, and the per-output cross-species
  deduplication rules (labels, IDs, synonyms, taxa, descriptions).
- **CHEBI** ([CHEBI/README.md](./CHEBI/README.md)) — which files ChEBI is ingested from, and why
  PubChem *substance* xrefs are deliberately left on the floor while compound xrefs are taken. Also
  ([CHEBI/sdf_tags/README.md](./CHEBI/sdf_tags/README.md)) the data-item tags Babel reads out of
  `ChEBI_complete.sdf`, the renames that silently emptied the secondary-ID and PubChem ingests in
  `babel-1.18`, the checks that now catch a rename, and how to re-audit a new SDF.
- **DOID** ([DOID/mappings.md](./DOID/mappings.md)) — why DOID's ICD xrefs are dropped where the
  concord is built: an ICD code names a whole disease family, so one code merged every subtype
  citing it (61 hereditary spastic paraplegia subtypes in a single clique), and *no* DOID→ICD row
  is an equivalence. Includes the measurement script, the full record of the 6,420 dropped rows,
  and the still-open question of DOID's remaining overused xref targets.
- **DRUGBANK** ([DRUGBANK/food-and-extracts/README.md](./DRUGBANK/food-and-extracts/README.md))
  — retyping DrugBank food-and-extract products (foods, pollens, danders) out of
  `biolink:ChemicalEntity`: foods become `biolink:Food` via their UNII's NCIt class, non-food
  allergens become `biolink:ComplexMolecularMixture`.
- **EMAPA** ([EMAPA/README.md](./EMAPA/README.md)) — the Mouse Developmental Anatomy Ontology as
  an anatomy source: extracting identifiers from UberGraph via a `part_of` traversal, exporting
  xref concords, and how its terms are typed and routed into the anatomy compendia. The worked
  example for an OBO-from-UberGraph source, with an auto-generated source-impact report.
- **ENSEMBL** ([ENSEMBL/Download.md](./ENSEMBL/Download.md)) — how Ensembl identifiers are
  downloaded via the BioMart API: per-dataset retry logic, permanently broken datasets and how to
  skip them, the attribute-batching workaround, and how partial progress is preserved across
  failed runs.
- **HP** ([HP/README.md](./HP/README.md)) — the Human Phenotype Ontology as a disease/phenotype
  source: extracting identifiers from the [`HP:0000118`](http://purl.obolibrary.org/obo/HP_0000118)
  "Phenotypic abnormality" subtree, and tagging every ingested term with the taxon
  [`NCBITaxon:9606`](http://purl.obolibrary.org/obo/NCBITaxon_9606) "Homo sapiens".
- **MESH** ([MESH/Ingestion.md](./MESH/Ingestion.md)) — how MeSH is partitioned across compendia
  by tree letter, how Supplementary Concept Records (SCRs) are typed and routed, the
  chemical/protein D-tree split, and which MeSH branches/SCR classes we deliberately skip.
- **MP** ([MP/README.md](./MP/README.md)) — the Mammalian Phenotype Ontology as a
  disease/phenotype source: extracting identifiers from UberGraph via a `subClassOf` walk from
  [`MP:0000001`](http://purl.obolibrary.org/obo/MP_0000001) "mammalian phenotype", typing every
  term as `biolink:PhenotypicFeature`, tagging each with the taxon
  [`NCBITaxon:40674`](http://purl.obolibrary.org/obo/NCBITaxon_40674) "Mammalia", exporting xref
  concords, and routing them into the disease compendia.
- **UMLS** ([UMLS/Leftover.md](./UMLS/Leftover.md)) — the "leftover UMLS" compendium: how
  unclaimed UMLS concepts are swept up and typed, the manual STY→Biolink override tables and the
  drift test that keeps them honest, and the coverage report under `reports/umls/`.
- **NCBIGene** ([NCBIGene/quoting/README.md](./NCBIGene/quoting/README.md)) — an investigation into
  how the two free-text synonym columns (`Synonyms`/`otheraliases`, `Other_designations`/
  `otherdesignations`) in `gene_info.gz` are quoted, prompted by issue #744's `''…''` fragments and
  the discovery that a trailing `''` is legitimate "double-prime" gene nomenclature.

## Cross-cutting patterns

- **Download patterns** ([DownloadPatterns.md](./DownloadPatterns.md)) — HTTP directory listing
  vs FTP for file discovery, and when to use each approach.

See the data handlers in `src/datahandlers/` and the compendium builders in
`src/createcompendia/` for the code behind each source.
