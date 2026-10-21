# CHEBI

ChEBI is ingested from two files pulled by `src/datahandlers/chebi.py`:

- `ChEBI_complete.sdf` — the structure file, read by `make_chebi_relations()` for secondary
  identifiers, KEGG COMPOUND / PubChem Compound cross-references, and the structure and
  physical-property values described under [Structure properties](#structure-properties) below.
  (Labels come from the OBO ontology via UberGraph, not from here; the SDF's `ChEBI NAME` tag is
  read only as a canary — see `CHEBI_SDF_KEYS`.)
- `database_accession.tsv` — the flat cross-reference table, covering the ChEBI entries that have no
  structure and so never appear in the SDF.
- `source.tsv` and `status.tsv` — the lookup tables that turn `database_accession.tsv`'s numeric
  `source_id` and `status_id` into a database name and a curation state.

## Reading `database_accession.tsv`

The file has six columns:

```text
id  compound_id  accession_number  type  status_id  source_id
9   3            C06147            MANUAL_X_REF     3   45
```

### What `source_id` means, and the one type where it differs

`source_id` names the database ChEBI recorded the value from. For `MANUAL_X_REF`, `CITATION` and
`REGISTRY_NUMBER` that database is also the identifier's **namespace** — `source.tsv`'s `prefix`
column gives it (`kegg.compound`, `pubmed`, `reaxys`). A PMID belongs to PubMed, a Reaxys registry
number to Reaxys, and Agricola citations look nothing like PubMed's (`IND20495852` vs `22183881`).

**`CAS` is the exception.** A CAS registry number is a shared identifier that dozens of databases
redistribute, so there `type` fixes the namespace and `source_id` records only *who supplied it*.
`498-15-7` appears under KEGG COMPOUND, ChemIDplus and NIST Chemistry WebBook — the same number,
three sources. 27% of CAS values are shared across sources, against 2 of 106,179 for `CITATION`.

So `source_id` on its own never identifies an accession *namespace*. Reading it as "the target
database" for a CAS row is the mistake that makes `17  7  498-15-7  CAS  1  45` — a CAS number ChEBI
sourced *from* KEGG COMPOUND — look like the KEGG accession `498-15-7`.

The audit script's "Does `source_id` name the identifier's namespace?" table regenerates that
evidence; read its caveat about bare-integer namespaces colliding by coincidence.

### The filter

A row is taken as a cross-reference only when **all three** of these hold:

- `type` is `MANUAL_X_REF`, so `accession_number` is the source database's own identifier. Dropping
  this condition would emit 10,615 CAS numbers as KEGG/PubChem CURIEs (10,476 under KEGG COMPOUND,
  139 under PubChem Compound).
- `source_id` resolves, via `source.tsv`, to a name in `CHEBI_DBX_SOURCE_NAMES` — today
  `KEGG COMPOUND` (45) and `PubChem Compound` (68). Resolving by *name* rather than pinning the
  numbers means a renumbering raises instead of silently emptying the ingest.
- `status_id` resolves, via `status.tsv`, to `CHECKED` or `OK`. ChEBI's third state is `SUBMITTED`
  — a depositor's claim that has not been reviewed. Concord rows feed `glom()` as equivalences, so
  an unreviewed one merges cliques on nobody's authority; 793 KEGG COMPOUND rows and 30 of the 55
  PubChem Compound rows are `SUBMITTED`, so excluding them costs little.
  [#957](https://github.com/NCATSTranslator/Babel/issues/957) tracks checking against ChEBI's own
  documentation whether `SUBMITTED` is in fact verified by some other route.

Ingesting the `CAS`-typed rows as `CAS:` cross-references in their own right is a separate question,
tracked in [#956](https://github.com/NCATSTranslator/Babel/issues/956) — they are excluded here
because they are not KEGG or PubChem accessions, not because CAS is unwanted.

Filtered that way the file yields **17,672 KEGG COMPOUND and 25 PubChem Compound** cross-references,
and every accession matches its expected shape (`C\d+`, and all-digits respectively). Rows whose
CHEBI already appears in the SDF are skipped, since the SDF is authoritative for those.

Note that `KEGG.COMPOUND` is not in `config.yaml`'s `chemical_ids` (it was dropped from UniChem in
[#834](https://github.com/NCATSTranslator/Babel/issues/834)), so these xrefs act as *bridges* rather
than adding compendium members: 1,058 KEGG accessions are shared by more than one ChEBI entry, which
lets `glom()` merge the 2,196 ChEBI entries involved. That merging is the point — it is also the
main risk of this ingest, and worth a `babel-clique-diff` if something downstream looks wrong.

Regenerate those counts with
[`scripts/audit_database_accession.py`](./scripts/audit_database_accession.py), which imports the
same `read_chebi_lookup_ids()`, `CHEBI_DBX_ACCESSION_TYPE` and `CHEBI_DBX_ACCEPTED_STATUSES` the
build matches on, so the audit cannot drift from the pipeline. Its output for the 2026-07-21 file is
committed as
[`dbx_audit_2026-07-21.md`](./dbx_audit_2026-07-21.md):

```bash
uv run python docs/sources/CHEBI/scripts/audit_database_accession.py \
    database_accession.tsv.gz source.tsv.gz status.tsv.gz
```

### History: this half read nothing at all until #955

The code originally expected columns `ID / COMPOUND_ID / SOURCE / TYPE / ACCESSION_NUMBER`, matching
column 3 against the literal strings `KEGG COMPOUND accession` and `Pubchem accession`. After ChEBI
reshaped the file, column 3 was `type` (only ever `MANUAL_X_REF`, `CITATION`, `CAS` or
`REGISTRY_NUMBER`), so neither branch could fire — and the accession was being read from column 4,
by then `status_id`. The branch matched **0 of 422,561 rows**.

This is the same silent-upstream-reshape failure as the SDF tag renames (#951), on the other input.
Neither `check_chebi_sdf_keys()` nor the `count_xrefs` guard could catch it, because the SDF
supplies ~197,000 xrefs on its own — a reminder that a whole-output emptiness check does not protect
an individual input. `make_chebi_relations()` now counts this file's contribution separately from
the SDF's and raises if it is zero, which is what would have caught this the release it appeared.

## Structure properties

`make_chebi_relations()` also writes the SDF's structure and physical-property values to
`intermediate/chemicals/properties/chebi_structure.jsonl.gz`, one `Property` row per
(ChEBI, predicate, value):

| SDF tag | Predicate | Rows, 2026-06-29 |
| --- | --- | ---: |
| `SMILES` | `chemrof:smiles_string` | 192,445 |
| `FORMULA` | `chemrof:generalized_empirical_formula` | 192,383 |
| `MONOISOTOPIC_MASS` | `chemrof:monoisotopic_mass` | 192,256 |
| `MASS` | `chemrof:mass` | 192,249 |
| `INCHI` | `chemrof:inchi_string` | 181,048 |
| `INCHIKEY` | `chemrof:inchi_key_string` | 181,048 |
| `CHARGE` | `chemrof:charge` | 15,613 |

These are annotations, not equivalences: they are written to their own file, are not an input to
`chemical_compendia`, and take no part in concord or clique building. `make_chebi_relations()`'s
docstring explains why they are kept out of `get_chebi_concord.jsonl.gz`, which *is* loaded into
`write_compendium()`'s in-memory `PropertyList`.

The predicates are ChemROF's rather than ChEBI's own `obo/chebi/` annotation properties, because
that is the namespace UberGraph now publishes these values under — see
[#1086](https://github.com/NCATSTranslator/Babel/issues/1086), where reading them under the old
names is silently returning nothing.

### Semicolons in an InChI are not separators

Multi-valued SDF tags are semicolon-delimited, and `split_chebi_sdf_values()` exists to split them.
**Structure values must not go through it.** A multi-component InChI uses `;` to separate
per-component layers: [`CHEBI:29124`](http://purl.obolibrary.org/obo/CHEBI_29124)
"dioxouranium(1+)" is `InChI=1S/2O.U/q;;+1`. 3,997 of the 181,048 InChIs in the 2026-06-29 SDF
contain one, so splitting would shred all of them into fragments that are not InChIs — the same
shape as the NCBIGene double-prime bug, where a plausible cleanup discarded ~4,000 real values.
Every tag in `CHEBI_SDF_STRUCTURE_PROPERTIES` is single-valued, so the lines are joined and used
as-is. `tests/data/chebi_dioxouranium.sdf` pins this.

### CHARGE is exempt from the empty-input guard

`make_chebi_relations()` raises if any input it reads produces no rows, which is what catches a
value-format change that a tag-name check cannot see. `CHARGE` is deliberately excluded
(`CHEBI_SDF_SPARSE_STRUCTURE_KEYS`): it is on 8% of entries because most ChEBI entries are
uncharged and simply omit it, so guarding it would be a check that fires on legitimate data. A
`CHARGE` *rename* is still caught by `check_chebi_sdf_keys()`.

## Roles

ChEBI's `RO:0000087` "has role" assertions are ingested separately, by `make_chebi_roles()` via
UberGraph. See [`roles/README.md`](./roles/README.md).

## Deliberately ignored: PubChem substance xrefs

The SDF carries PubChem cross-references under two separate tags, and Babel reads only one of them:

| Tag | Entries in babel-1.18 | Ingested? |
| --- | --- | --- |
| `PubChem Compound Database Links` | 180,991 | yes, as `PUBCHEM.COMPOUND` |
| `PubChem Substance Database Links` | 191,573 | **no** |

This is a deliberate choice, not an oversight. A PubChem substance is a submitter-deposited record,
so it is a much weaker equivalence assertion than a PubChem compound — several substance records
routinely describe the same chemical, and what they assert is "some depositor submitted this" rather
than "this is the same chemical". Compound IDs are the normalized entries and are what we want for
clique building.

`PUBCHEM.SUBSTANCE` does exist as a prefix (`src/prefixes.py`), so ingesting these later is a small
change if we ever want them: read the substance tag in `make_chebi_relations()` the same way the
compound tag is read. Consider first whether substance-level equivalence is strong enough for
`glom()`, which treats every concord row as an equivalence.

Note that this is not a behaviour change from before the babel-1.18 tag renames. ChEBI previously
published both under a single `PubChem Database Links` tag holding `SID: nnn CID: nnn` pairs, and
the code that parsed it extracted only the CIDs. ChEBI now does that separation for us.

## SDF tag names

ChEBI renames the SDF's data-item tags between releases, and an unrecognized tag is silently
ignored rather than raising — which in `babel-1.18` emptied both the secondary-identifier and
PubChem ingests without failing the build. See
[`sdf_tags/README.md`](./sdf_tags/README.md) for what broke, the checks that now catch it, and how
to re-audit the tags against a fresh download.
