# MeSH ingestion and tree partitioning

This document describes how Babel ingests Medical Subject Headings (MeSH) data: how MeSH's
hierarchy is split across Babel's compendia, how Supplementary Concept Records are typed, and
which parts of MeSH we deliberately do not incorporate.

The MeSH data handler is `src/datahandlers/mesh.py`; the per-compendium routing lives in each
compendium's `write_mesh_ids()` function (in `src/createcompendia/`).

## Source data

The MeSH release is pinned by `mesh_year` in `config.yaml`, next to `umls_version`. Keep the two in
step: UMLS 20XXAA carries MeSH 20XX codes, so an older MeSH dump leaves UMLS MSH codes (mostly new
Supplementary Concept Records) with no MeSH record. Each build writes
`babel_outputs/reports/umls_mesh_skew.json`, which counts the MSH codes in UMLS's `MRCONSO.RRF`
(column 14 of the `SAB=MSH` rows) that have no `rdfs:label` in `mesh.nt`. With UMLS 2026AA and
MeSH 2026 the only missing codes are the 22 `U` "(MeSH Category)" headings that UMLS adds itself;
with the stale MeSH 2024 dump there were 1,878, on 2,162 CUIs with no other MSH code. The report
is informational and does not fail the build.

Two downloads come from `ftp.nlm.nih.gov`, both for the configured year. The same files are served
under the same paths over HTTPS from <https://nlmpubs.nlm.nih.gov/projects/mesh/>, the site NLM's
[MeSH download page](https://www.nlm.nih.gov/databases/download/mesh.html) links to; open
<https://nlmpubs.nlm.nih.gov/projects/mesh/rdf/> in a browser to see whether a newer year is out. We
intend to download from it instead of FTP, which has been the less reliable of the two:
[#1161](https://github.com/NCATSTranslator/Babel/pull/1161) makes that switch, so if the FTP
downloads fail before it lands, that PR is the fix to reach for.

- **RDF**: `/online/mesh/rdf/{mesh_year}/mesh{mesh_year}.nt.gz`, checked against NLM's `.sha1`
  and decompressed to `babel_downloads/MESH/mesh.nt` (a few GB). The per-year directory also has
  `mesh{mesh_year}-dups.nt`, which is the same data before deduplication, so we don't download
  it. Do not use the top-level `/online/mesh/rdf/mesh.nt.gz`: NLM stopped updating it after
  MeSH 2024.
- **XML**: `desc{mesh_year}.gz` and `supp{mesh_year}.gz`, saved as
  `babel_downloads/MESH/desc.xml.gz` and `supp.xml.gz`. These are used only for registry numbers
  (see below). The current year is in `/online/mesh/MESH_FILES/xmlmesh/`; once a year is superseded
  its final files are archived in `/online/mesh/{mesh_year}/xmlmesh/`. `mesh_xml_dir()` picks
  whichever has the year's files.

If NLM has not published the configured year, both downloads fail. Neither falls back to another
year. `tests/datahandlers/test_mesh.py::test_configured_mesh_year_is_published` (a `network`
test) checks this ahead of a build.

`get_mesh` takes `mesh_year` as a param, so changing the year reruns it, but only for a `mesh.nt`
Snakemake recorded with that param. A `babel_downloads/MESH/mesh.nt` downloaded before `mesh_year`
existed (the stale top-level dump) has no such record and is silently reused: replace it with
`--forcerun=get_mesh`, which also rebuilds every pipeline's `ids/MESH`.

### Per-year RDF IRIs are normalized

The per-year dumps name records with the year in the IRI
(`<http://id.nlm.nih.gov/mesh/2026/D002839>`), while the old top-level dump and everything in
`src/datahandlers/mesh.py` use the unversioned form (`<http://id.nlm.nih.gov/mesh/D002839>`).
`normalize_mesh_nt()` rewrites `<http://id.nlm.nih.gov/mesh/{mesh_year}/` to
`<http://id.nlm.nih.gov/mesh/` during decompression (the `vocab#` namespace is unversioned and left
alone). It raises if the dump has no IRIs with that prefix, which would mean the dump is for some
other year or NLM changed its IRI scheme.

### Registry numbers come from the XML

Registry numbers (CAS numbers, UNIIs, EC numbers and NCBI Taxon IDs written as `txid...`) feed
the chemical `mesh_cas` / `mesh_unii` concords and the taxon `NCBI_MESH` concord. NLM's per-year
RDF dumps dropped `meshv:registryNumber` starting with MeSH 2025: `mesh2024.nt` has about 384,000
of these triples, while `mesh2025.nt` and `mesh2026.nt` have none. So `pull_mesh_registry()` reads
the `RegistryNumber`s of each record's preferred concept (`PreferredConceptYN="Y"`) from the XML,
skipping MeSH's `0` placeholder and ignoring `RelatedRegistryNumber`. This gives the same pairs the
old RDF query gave. Against MeSH 2024 RDF, MeSH 2026 XML shares 158,927 pairs. Of the 2,327
pairs that only the 2024 RDF has, all but 11 are on records NLM has since deleted.
`pull_mesh_registry()` raises if it finds no registry numbers at all.

Compared with the 2024 dump, `mesh2026.nt` also lacks `meshv:active`, `dateEstablished`,
`dateRevised`, `lastActiveYear` and `sortVersion`, and adds `dateIntroduced` and `lastUpdated`
(MeSH 2026 changed its record-level dates; see `readme.txt` in the year directory). Babel uses
none of these.

### RDF vocabulary

`mesh.nt` uses the MeSH RDF vocabulary (`meshv:`, `<http://id.nlm.nih.gov/mesh/vocab#>`), and
these predicates matter most for ingestion:

- `meshv:treeNumber` — a descriptor's position(s) in the MeSH tree (e.g. `D02.455...`). A
  descriptor can sit in multiple trees.
- `meshv:mappedTo` / `meshv:preferredMappedTo` — link a Supplementary Concept Record (SCR) to the
  main-heading descriptor(s) it is filed under. SCRs do **not** have tree numbers of their own.
- `rdf:type` — for SCRs, this is the SCR class (see below).
- `rdfs:label` — the human-readable name.

If `babel_downloads/MESH/mesh.nt` is present locally, it is directly greppable for ad hoc lookups
(a CURIE's `rdf:type`, `treeNumber`, `mappedTo`, `#label`) without running the pipeline — much
faster than re-deriving from the handler for one-off questions. For example, to inspect a single
record:

```bash
grep -E "mesh/C038967>" babel_downloads/MESH/mesh.nt \
  | grep -E "#type|mappedTo|#label|treeNumber"
```

## How MeSH is split across compendia

MeSH is partitioned by its top-level tree letter. Each consuming compendium declares its own
`meshmap` (tree-prefix → Biolink type) and calls `mesh.write_ids()`, which expands each tree
prefix to all descriptors under it. The current routing:

- **A01–A20 Anatomy** → anatomy (`ANATOMICAL_ENTITY`; `A11` Cells → `CELL`,
  `A11.284` → `CELLULAR_COMPONENT`). See `anatomy.write_mesh_ids()`.
- **B01–B05 Organisms** → taxon (`ORGANISM_TAXON`). See `taxon.write_mesh_ids()`.
- **C Diseases** (C01, C04–C22, C24–C26) → disease (`DISEASE`); **C23** → `PHENOTYPIC_FEATURE`.
  See `diseasephenotype.write_mesh_ids()`. Note C02 (Virus Diseases) and C03 (Parasitic Diseases)
  are intentionally absent pending verification of MONDO/HPO cross-references.
- **D01–D26 Chemicals & Drugs** → chemical (`CHEMICAL_ENTITY`, with mixture and polypeptide
  overrides); protein subtrees (`D05.500`, `D05.875`, `D08.244`, `D08.622`, `D08.811`, `D12.776`)
  are split off to protein (`PROTEIN`). See `chemicals.write_mesh_ids()` and
  `protein.write_mesh_ids()`.

When the same descriptor appears in multiple trees with conflicting types, `mesh.write_ids()`
resolves the conflict using the `order` argument (an explicit type priority list) and an
`EXCLUDE` sentinel.

### Chemical and protein partition the same D-trees independently

`chemicals.write_mesh_ids()` and `protein.write_mesh_ids()` each declare their own view of the D
subtrees (chemicals marks the protein subtrees `EXCLUDE`; protein claims them as `PROTEIN`). These
two lists are maintained separately (there is a `TODO` in `chemicals.write_mesh_ids()` about
unifying them). When changing a D-tree assignment in one, check the other so an identifier does
not end up in both compendia or in neither.

### UMLS CUIs follow the descriptor's pipeline

Where a pipeline's MeSH ids and its UMLS semantic-type selection disagree about a concept, the MeSH
placement wins: a CUI is claimed by the pipeline whose `ids/MESH` lists the CUI's descriptor, and
dropped from any other. So a change to a D-tree assignment here also moves the CUIs of the
descriptors it moves. See [`UMLS/CuiFollowsMesh.md`](../UMLS/CuiFollowsMesh.md).

## Supplementary Concept Records (SCRs)

SCRs are the `MESH:C…` records (as opposed to `MESH:D…` main-heading descriptors). They have **no
tree numbers of their own**. Instead they are:

1. Typed by their RDF class — one of `meshv:SCR_Chemical`, `meshv:SCR_Organism`,
   `meshv:SCR_Disease`, `meshv:SCR_Protocol`, `meshv:SCR_Population`, `meshv:SCR_Anatomy`.
2. Filed under one or more main-heading descriptors via `meshv:mappedTo` /
   `meshv:preferredMappedTo`.

`mesh.write_ids()` ingests SCRs through its `extra_vocab` argument (`{SCR_class: biolink_type}`)
and routes them by gating their mapped descriptors against a tree list:

- `scr_include_trees` — keep only SCRs whose mapped descriptors fall under these trees.
- `scr_exclude_trees` — drop SCRs whose mapped descriptors fall under these trees.

The two options are mutually exclusive. `get_scr_terms_mapped_to_trees()` does the
descriptor → tree lookup.

Because SCRs are typed by class and not by tree, **a "MeSH X is missing" bug is frequently an
unconsumed SCR class rather than a typing error** — the record is dropped before any type is
assigned.

### SCR class coverage (as of 2026 dump)

The approximate record counts and current handling:

- **`SCR_Chemical`** (~250,000) — consumed by chemical and protein (split via
  `scr_include_trees` / `scr_exclude_trees` on the protein D subtrees).
- **`SCR_Organism`** (~66,000) — consumed by taxon.
- **`SCR_Disease`** (~6,800) — **not consumed.** Specific/rare diseases (OMIM-style), preferred-
  mapped to C-tree descriptors. High-value gap for the disease compendium.
- **`SCR_Population`** (~1,800) — **not consumed.** Ethnic/national/tribal groups; tied to the
  Population and Community Ontology (PCO) work, not chemicals/diseases.
- **`SCR_Protocol`** (~1,200) — **not consumed.** ~98% chemotherapy drug-combination regimens
  (preferred-mapped to `D000971` Antineoplastic Combined Chemotherapy Protocols); the rest are
  questionnaires/diagnostic/vaccination protocols.
- **`SCR_Anatomy`** (~24) — **not consumed.** Tiny anatomy tail.

## Coverage audit

Babel does not ingest every MeSH branch. Whole top-level categories (E procedures/equipment, F
psychiatry/psychology except disease-like F03, G phenomena/processes, H–N, V, Z) and the D27
Chemical Actions and Uses tree are currently out of scope, along with the four unconsumed SCR
classes above. The standing audit of what we do and don't incorporate — with a per-branch
keep/skip recommendation — is tracked in
[issue #807](https://github.com/NCATSTranslator/Babel/issues/807). Consult it before adding or
removing a MeSH branch.

## Related code and issues

Code:

- `src/datahandlers/mesh.py` — handler: `pull_mesh()`, `normalize_mesh_nt()`, `pull_mesh_xml()`,
  `pull_mesh_registry()`, `Mesh` class, `write_ids()`, `get_scr_terms_mapped_to_trees()`,
  `get_mesh_id_from_iri()`, `_mesh_id()`.
- `src/reports/umls_mesh_skew.py` — the UMLS/MeSH version skew report.
- `src/createcompendia/{anatomy,taxon,diseasephenotype,chemicals,protein}.py` — `write_mesh_ids()`.

Issues:

- [#807](https://github.com/NCATSTranslator/Babel/issues/807) — MeSH branch/SCR coverage audit.
- [#520](https://github.com/NCATSTranslator/Babel/issues/520) — add chemotherapy protocols
  (`SCR_Protocol`).
- [#189](https://github.com/NCATSTranslator/Babel/issues/189) — add PCO (relevant to
  `SCR_Population`).
- [#735](https://github.com/NCATSTranslator/Babel/issues/735) — centralize per-vocabulary
  tree-partition registries.
- [#583](https://github.com/NCATSTranslator/Babel/issues/583) — add MeSH concept codes (`M#`)
  alongside descriptor codes.
