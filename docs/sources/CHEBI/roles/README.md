# ChEBI roles

ChEBI asserts what a chemical *does* — its role — separately from what it *is*. Aspirin
([`CHEBI:15365`](http://purl.obolibrary.org/obo/CHEBI_15365) "acetylsalicylic acid") is a
salicylate by structure and, by role, a cyclooxygenase inhibitor, an antipyretic, a
teratogenic agent and ten other things. `make_chebi_roles()` records those assertions as
properties.

## What is collected

Every [`RO:0000087`](http://purl.obolibrary.org/obo/RO_0000087) "has role" assertion on a
descendant of [`CHEBI:24431`](http://purl.obolibrary.org/obo/CHEBI_24431) "chemical entity", written
to `intermediate/chemicals/properties/chebi_roles.jsonl.gz` as
`Property(curie=<chemical>, predicate=RO:0000087, value=<role>)`.

These are annotations, not equivalences. They are deliberately **not** an input to
`chemical_compendia`, so they take no part in concord building, `glom()` or typing — see
`make_chebi_roles()`'s docstring for why they also sit in their own file rather than in
`get_chebi_concord.jsonl.gz`.

## The non-redundant graph, and why

UberGraph publishes role assertions in both its redundant and non-redundant graphs. We query the
**non-redundant** one, so each chemical carries only its directly asserted roles. Aspirin has 13
there against 45 in the redundant graph; the extra 32 are the ancestors of those 13
(`EC 1.14.99.1 (prostaglandin-endoperoxide synthase) inhibitor` implies
`EC 1.14.* ... inhibitor` implies `EC 1.* (oxidoreductase) inhibitor`, and so on). Storing the
closure would multiply the row count several-fold to say nothing new. A consumer that wants
ancestors can walk the role hierarchy itself.

## The roles are not themselves normalized

The values are ChEBI CURIEs that Babel does not currently give cliques of their own. Role terms sit
under [`CHEBI:50906`](http://purl.obolibrary.org/obo/CHEBI_50906) "role", which is a separate ChEBI
root — it is *not* a descendant of `CHEBI:24431` — so `write_chebi_ids()` never gives them a row in
`ids/CHEBI`, and no role term appears as a *subject* in this file.

Some of them nevertheless reach `ChemicalEntity.txt` today, as passengers: MeSH's D-tree is typed
`biolink:ChemicalEntity` wholesale, and a MeSH↔ChEBI concord pulls the ChEBI role term into that
clique. [`CHEBI:35482`](http://purl.obolibrary.org/obo/CHEBI_35482) "opioid analgesic" normalizes
today as a `biolink:ChemicalEntity` clique with `MESH:D000701` and `UMLS:C0002772`.

Typing them properly — Biolink has a `biolink:ChemicalRole` class with `id_prefixes: ['CHEBI']` — is
[#101](https://github.com/NCATSTranslator/Babel/issues/101), and the UMLS side of the same question
is [#390](https://github.com/NCATSTranslator/Babel/issues/390). Both change existing cliques, which
this ingest does not.

## Re-auditing

[`scripts/audit_chebi_roles.py`](./scripts/audit_chebi_roles.py) ranks roles by how many chemicals
carry them. It imports `CHEBI_ROLE_ROOT` and `HAS_ROLE` from the production module, so the audit
cannot drift from the ingest.

```bash
uv run python docs/sources/CHEBI/roles/scripts/audit_chebi_roles.py \
    babel_outputs/intermediate/chemicals/properties/chebi_roles.jsonl.gz \
    babel_downloads/CHEBI/labels
```

[`role_audit.md`](./role_audit.md) is its output for the 2026-06-29 ChEBI download — 42,918 role
assertions over 24,807 chemicals and 1,377 distinct roles — with the full per-role ranking in
[`role_counts.csv`](./role_counts.csv).

## The guard

`make_chebi_roles()` raises if UberGraph returns no role assertions at all. A SPARQL query against a
renamed predicate returns an empty result set rather than an error, which is how the `CHEBIP:smiles`
lookup in `get_subclasses_and_smiles()` went quiet for ~194,000 terms
([#1086](https://github.com/NCATSTranslator/Babel/issues/1086)). ChEBI curates tens of thousands of
role assertions, so zero means the query is broken, not that the source is empty.
