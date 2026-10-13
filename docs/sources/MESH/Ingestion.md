# MeSH ingestion and tree partitioning

This document describes how Babel ingests Medical Subject Headings (MeSH) data: how MeSH's
hierarchy is split across Babel's compendia, how Supplementary Concept Records are typed, and
which parts of MeSH we deliberately do not incorporate.

The MeSH data handler is `src/datahandlers/mesh.py`; the per-compendium routing lives in each
compendium's `write_mesh_ids()` function (in `src/createcompendia/`).

## Source data

MeSH is downloaded as an RDF N-Triples dump to `babel_downloads/MESH/mesh.nt` (a few GB). It
uses the MeSH RDF vocabulary (`meshv:`, `<http://id.nlm.nih.gov/mesh/vocab#>`) and these
predicates matter most for ingestion:

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
- **D27 Chemical Actions and Uses**, only for descriptors in none of the trees above (A, B, C,
  D01–D26) → chemical role (`CHEMICAL_ROLE`). See `chemical_role.write_mesh_ids()` and "Chemical
  roles: D27" below.

When the same descriptor appears in multiple trees with conflicting types, `mesh.write_ids()`
resolves the conflict using the `order` argument (an explicit type priority list) and an
`EXCLUDE` sentinel.

### Chemical and protein partition the same D-trees independently

`chemicals.write_mesh_ids()` and `protein.write_mesh_ids()` each declare their own view of the D
subtrees (chemicals marks the protein subtrees `EXCLUDE`; protein claims them as `PROTEIN`). These
two lists are maintained separately (there is a `TODO` in `chemicals.write_mesh_ids()` about
unifying them). When changing a D-tree assignment in one, check the other so an identifier does
not end up in both compendia or in neither.

### Chemical roles: D27

D27 classifies chemicals by what they do ("Antidepressive Agents", "Herbicides", "Excipients")
rather than what they are. Biolink calls that a `biolink:ChemicalRole`, so these descriptors go to
the ChemicalRole compendium, not chemicals. The rule is **D27 and none of the trees another
compendium takes** (`mesh.MESH_ENTITY_TREES`: A, B, C, D01–D26), not "any D27 tree number". MeSH
also files specific substances under D27 beside their action class (Cefiderocol is in D02/D03 and
under anti-bacterial agents in D27; High Fructose Corn Syrup is in D09 and D27), and a few D27
descriptors are diseases too ("Metabolic Side Effects of Drugs and Substances", C25). Descriptors
in both stay with the other compendium, including a few headings that read like roles ("Hormones",
"Glucocorticoids", "Antioxidants"). A curated list would be the way to move those if we want them
as roles. Trees no compendium takes from MeSH (E, G, J, N) don't block a role: "Food Additives"
(G07/J02) and "Detergents" (J01) are roles.

The same line is applied to UMLS, using MeSH as the arbiter. A CUI whose MeSH atoms all point at
role descriptors (`umls.get_cuis_mapped_only_to_mesh()`) is left out of
`chemicals.write_umls_ids()`. `chemical_role.write_umls_ids()` takes those CUIs only when UMLS
types them as a substance (semantic type tree A1.4) and not as a protein. The rest are effects
filed as entry terms under the agent class ("Serotonin Effect" under "Serotonin Agents", a
Physiologic Function), findings, devices or protein classes, and process, protein or leftover UMLS
keeps them.

D27 is deliberately **not** in chemicals' `scr_exclude_trees`. That would drop every SCR mapped to
a D27 descriptor, and those are substances filed under their class (eglin c → Serpins, which is
in D12 and D27). No SCR in the 2026 dump maps only to role descriptors.

`chemical_roles/scripts/analyze_d27.py` regenerates the counts and samples behind these choices
into `chemical_roles/README-generated.md`, using the production predicates.

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
psychiatry/psychology except disease-like F03, G phenomena/processes, H–N, V, Z) are currently out
of scope (D27 Chemical Actions and Uses now goes to ChemicalRole), along with the four unconsumed
SCR classes above. The standing audit of what we do and don't incorporate — with a per-branch
keep/skip recommendation — is tracked in
[issue #807](https://github.com/NCATSTranslator/Babel/issues/807). Consult it before adding or
removing a MeSH branch.

## Related code and issues

Code:

- `src/datahandlers/mesh.py` — handler: `Mesh` class, `write_ids()`,
  `get_scr_terms_mapped_to_trees()`, `get_mesh_id_from_iri()`, `_mesh_id()`.
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
