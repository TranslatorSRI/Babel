# NCBITaxon

NCBI Taxonomy is the backbone of the `OrganismTaxon` compendium (`src/createcompendia/taxon.py`).
Its identifiers come from the `names.dmp` in `taxdump.tar` (`src/datahandlers/ncbitaxon.py`), and
two other sources link to it:

- **MeSH**, through registry numbers of the form `txid<N>` (the `NCBI_MESH` concord).
- **UMLS**, through NCBI atoms in MRCONSO (the `UMLS` concord, which also links UMLS to MeSH).

NCBITaxon, MESH and UMLS are all unique prefixes in `glom()`, so a taxon clique holds at most one of
each. A bad link therefore never grows a large clique. It shows up instead as an identifier that
loses, or swaps, its NCBITaxon partner. That is what the checks in
[`umls-and-obsolete-ids/`](./umls-and-obsolete-ids/) measure.

## UMLS calls NCBI Taxonomy "NCBI"

`umls.build_sets()` matches on the MRCONSO source abbreviation (SAB). For NCBI Taxonomy that is
`NCBI`, and the CODE column is the bare taxon ID. Until this was fixed, the taxon pipeline asked for
`NCBITaxon`, which matched nothing. The UMLS concord held only UMLS → MeSH links, and roughly
745,000 UMLS taxa shipped as single-identifier cliques beside their NCBITaxon twins. For example,
[`NCBITaxon:3704`](http://purl.obolibrary.org/obo/NCBITaxon_3704) "Armoracia rusticana" and
`UMLS:C1110641` "Armoracia rusticana" were separate cliques.

## Merged and deleted taxon IDs

NCBI regularly merges taxa (often two species names found to be one species) and deletes others,
recording these in `merged.dmp` and `delnodes.dmp`. MeSH and UMLS lag behind and still cite the old
IDs: 1,874 merged and 36 deleted IDs reached the compendium before this was handled, each as an
unlabeled NCBITaxon identifier.

`taxon.update_obsolete_ncbitaxon_ids()` rewrites both taxon concords after they are built:

- **Merged IDs are replaced** with the ID they were merged into
  (`ncbitaxon.read_obsolete_taxa()`, which fails if NCBI ever stops flattening merge chains).
- **Rows with deleted IDs are dropped.** There is no current taxon to point at.
- **A replaced row loses to a direct link.** When NCBI merges two names, MeSH and UMLS usually keep
  a record for each, so after remapping both records point at the same taxon and only one can join
  it. If another record of the same prefix already links to that taxon directly, the remapped row is
  dropped. Example: MeSH registry `MESH:C000668355` "Collybia dryophila" → merged `NCBITaxon:71877`
  → [`NCBITaxon:206318`](http://purl.obolibrary.org/obo/NCBITaxon_206318) "Gymnopus dryophilus",
  which `MESH:C000672803` "Gymnopus dryophilus" links to directly. Without this rule the choice fell
  to row order, and a full build lost 1,329 correct direct links.

Counts of each are recorded under `obsolete_ncbitaxon` in the concord metadata YAMLs.

A merge target can be broader than the old taxon in name, because the merge reflects NCBI's current
classification: UMLS "Crustacea" cites `NCBITaxon:6657`, which NCBI merged into
[`NCBITaxon:197562`](http://purl.obolibrary.org/obo/NCBITaxon_197562) "Pancrustacea". We follow
NCBI.

## Effect on the compendium

Measured by building `taxon` on main (`f6fbc10b`) and on this change from the same downloads (UMLS
2026AA, NCBI taxdump and MeSH of 2026-10-09). Artifacts are in
[`umls-and-obsolete-ids/`](./umls-and-obsolete-ids/).

- **Cliques:** 3,768,902 → 3,055,807 (`clique-diff.summary.json`). Almost all of the difference is
  UMLS-only cliques joining NCBITaxon cliques. UMLS-only cliques went from 745,560 to 33,191.
- **No clique holds two identifiers with the same prefix.** The largest clique is still three
  identifiers.
- **Dropped identifiers:** 1,911. These are the 1,910 merged or deleted NCBITaxon IDs, plus one MeSH
  ID no longer in MeSH that only reached the build through UMLS.
- **Partner changes** ([`partner-changes.md`](./umls-and-obsolete-ids/partner-changes.md)): 712,491
  UMLS identifiers gained an NCBITaxon partner, and 1,384 MeSH/UMLS identifiers had an obsolete
  partner replaced by the current one. 81 UMLS identifiers lost or swapped a current partner. Almost
  all of these are a second CUI for the same species, created from MeSH with MeSH's spelling, giving
  way to the CUI that carries NCBI's own atom (e.g. `UMLS:C5434345` "Colletotrichum caudasporum" vs
  `UMLS:C3747940` "Colletotrichum caudisporum").
- **Labels:** of the 780,600 cliques holding both an NCBITaxon and a UMLS identifier, 722,101 have
  the same label on both. Of the other 58,499, 48,988 have the UMLS label among NCBITaxon's own
  synonyms (usually an older name). A sample of the rest is mostly UMLS formatting ("Genus Moschus",
  "Stevia <Eupatorieae>").

## Known remaining disagreements

48 UMLS CUIs reach one NCBITaxon directly and a different one through their MeSH heading
([`path-disagreements.md`](./umls-and-obsolete-ids/path-disagreements.md)). Only one link can win.
In the sample the direct link is more often right (`UMLS:C0993566` "Daboia" directly reaches the
genus, while MeSH's registry number points at the species *Daboia russelii*), but not always: MeSH
lists Oncothecales as an entry term of Magnoliopsida, and UMLS attached NCBI's Oncothecales atom to
its Magnoliopsida CUI. These are left to `glom()` and listed rather than handled by another rule.

Some separate-looking pairs are correct as they are. MeSH "Plants" (`MESH:D010944`) is linked by
UMLS to Viridiplantae, not to [`NCBITaxon:3193`](http://purl.obolibrary.org/obo/NCBITaxon_3193)
"Embryophyta". `UMLS:C0591833` "Murine" is the eye-drop brand, not
[`NCBITaxon:39107`](http://purl.obolibrary.org/obo/NCBITaxon_39107) "Murinae". Others, such as
`UMLS:C1704307` "Fly (organism)" (SNOMED CT only), have no source link to NCBI Taxonomy at all.

## Regenerating

Build both sides of the comparison, then run the two scripts in
[`umls-and-obsolete-ids/scripts/`](./umls-and-obsolete-ids/scripts/) (usage is in each docstring)
and `babel-clique-diff` (see [`docs/tools/CliqueDiff.md`](../../tools/CliqueDiff.md)).
`tests/pipeline/test_taxon.py` checks the two invariants over a real build: the UMLS concord links
to NCBITaxon, and every NCBITaxon ID in the taxon concords is current.
