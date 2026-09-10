# ChEBI SDF data-item tags

`make_chebi_relations()` reads `ChEBI_complete.sdf` for the ChEBI ID and name, the compound's
secondary (obsoleted) identifiers, its KEGG COMPOUND and PubChem Compound cross-references, and its
structure and physical-property values (`SMILES`, `INCHI`, `INCHIKEY`, `FORMULA`, `MASS`,
`MONOISOTOPIC_MASS`, `CHARGE` — see [the structure properties
section](../README.md#structure-properties)). It finds each of them by matching the SDF's data-item
tags — `> <ChEBI ID>`, `> <SECONDARY_ID>` and so on — against a fixed list of expected names in
`CHEBI_SDF_KEYS`.

The tag audit below predates the structure properties, so its "Requested?" column marks only the
seven keys that were consumed at the time; `inchikey` and `smiles` were then carried as canaries.

## Why this needs watching

`read_sdf()` matches tags by exact string and silently omits any tag it does not recognize. So when
ChEBI renames a tag, nothing fails: the ingest runs to completion, the build succeeds, and the field
is simply empty for every compound in ChEBI.

That is what happened in `babel-1.18`. Two tags had been renamed:

| Previously | In babel-1.18 |
| --- | --- |
| `Secondary ChEBI ID` | `SECONDARY_ID` |
| `PubChem Database Links` | `PubChem Compound Database Links` / `PubChem Substance Database Links` |

Both of our keys stopped matching anything. The secondary-ID property file was written empty, so
every ChEBI secondary identifier disappeared from the release —
[`CHEBI:520984`](http://purl.obolibrary.org/obo/CHEBI_520984), a secondary ID of
[`CHEBI:421707`](http://purl.obolibrary.org/obo/CHEBI_421707) "abacavir", is the reported example —
and the SDF's ~181,000 PubChem compound xrefs went with them.

The value format changed at the same time. Multiple values for one tag are now semicolon-delimited
on a single line, so abacavir's `SECONDARY_ID` reads
`CHEBI:193608;CHEBI:441792;CHEBI:2360;CHEBI:525912;CHEBI:520984`. Code that treats the line as one
value produces a CURIE like `KEGG.COMPOUND:C00001;C00002` that matches nothing downstream — this
was already happening to 77 multi-value KEGG entries before anyone noticed.

## What now prevents a silent recurrence

`check_chebi_sdf_keys()` raises if any expected tag appears in no SDF entry at all, naming the tags
that vanished. A second check then counts the rows written **per tag** and raises if any of the
three tags we consume produced none, which catches the failure modes a tag-name check cannot see —
a value-format change, a truncated download, a parse bug.

Counting per tag rather than in aggregate is the point. A whole-output check does not protect an
individual input: the SDF's ~181,000 PubChem xrefs could vanish entirely and KEGG's ~16,000 would
keep a total-count guard quiet — which is the exact shape of the bug being fixed here.

## Re-auditing against a new SDF

`audit_sdf_tags.py` tabulates every tag actually present in an SDF, normalized the way the parser
normalizes it, and flags any expected key that is missing:

```bash
uv run python docs/sources/CHEBI/sdf_tags/audit_sdf_tags.py babel_downloads/CHEBI/ChEBI_complete.sdf
```

[`tag_audit_babel-1.18.md`](./tag_audit_babel-1.18.md) is its output for the `babel-1.18` download,
confirming that the other five keys were unaffected by this rename.
