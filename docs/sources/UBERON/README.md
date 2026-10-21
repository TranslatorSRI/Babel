# UBERON source notes

UBERON (Uber-anatomy ontology, <https://obofoundry.org/ontology/uberon.html>) is the backbone of the
anatomy pipeline: its terms lead most anatomy cliques, and their Biolink types come from where a
term sits under [`UBERON:0001062`](http://purl.obolibrary.org/obo/UBERON_0001062) "anatomical
entity" (see `ANATOMY_OBO_SOURCES` in `src/createcompendia/anatomy.py`).

UBERON's cross-references are read from UberGraph by `build_anatomy_obo_relationships()` and written
to the `UBERON` anatomy concord:

- targets in `UBERON_OBO_IGNORE_LIST` are dropped;
- `config.yaml: anatomy_xref_prefixes` renames UBERON's spellings of other vocabularies (`SCTID` →
  `SNOMEDCT`);
- individually wrong pairs are dropped by `input_data/anatomy_badxrefs.txt`.

## Further notes

- [`umls-joins/README.md`](./umls-joins/README.md) — how UBERON terms are joined to UMLS concepts
  through their SNOMED CT and FMA xrefs, what that does to the anatomy compendia, why SNOMED's
  structure and entire concepts are not treated as equivalent, and the UBERON terms with several
  SNOMED xrefs that still need review.
