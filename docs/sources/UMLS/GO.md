# GO atoms in UMLS

GO has no UMLS cross-references of its own: none in `go.obo`, no `umls2go` in external2go, none in
Biomappings. So the GO atoms that UMLS keeps in `MRCONSO.RRF` are Babel's only source of UMLS-GO
mappings, and they also become UMLS labels and synonyms. Two parts of `src/datahandlers/umls.py`
decide which GO atoms count, and this page records what each choice does to a real build.

[`GO/scripts/go_atoms_report.py`](GO/scripts/go_atoms_report.py) regenerates every number below from
MRCONSO and a finished build's `intermediate/` directory. It writes
[`GO/go_atoms_report.json`](GO/go_atoms_report.json), which was produced from the 2026AA MRCONSO and
the 2026jul22 intermediates. The full command is in the script's docstring.

## Which GO atoms map a CUI to a GO term

UMLS keeps each GO synonym atom (term types `SY`, `ET`, ...) under the GO code of the term it came
from, but often files it in a different CUI from the GO term's preferred term (`PT`). For example,
the `PT` of [`GO:0045943`](http://purl.obolibrary.org/obo/GO_0045943) "positive regulation of
transcription by RNA polymerase I" is in `C1158785`, while its entry term "activation of
transcription from RNA polymerase I promoter" is a CUI of its own, `C2249862`. Taking every atom
maps one GO term to several CUIs, and the process/activity pipeline's overused-xref filter then
drops every pair for that GO term.

So the process/activity concord (`build_process_umls_relationships()`) passes
`acceptable_ttys={**DEFAULT_ACCEPTABLE_TTYS, "GO": GO_PREFERRED_TTYS}` to `umls.build_sets()`, which
keeps only the GO `PT` and `MTH_PT` atoms. Replayed over 2026jul22, this raises the number of CUIs
with GO atoms that end up in a clique with a GO term from 26,407 to 35,846.

### Considered and rejected: falling back to synonym atoms

Restricting to preferred terms loses the GO terms whose `PT` is in a CUI outside the process
semantic types, but which also have a `SY` or `ET` atom in a process CUI. Falling back to those
atoms would add 172 pairs, and 65 of those CUIs would end up in a GO clique. Most are narrow entry
terms rather than equivalents, and some are wrong:

- `C3272179` "inhibition of synapse assembly" →
  [`GO:0051964`](http://purl.obolibrary.org/obo/GO_0051964) "negative regulation of synapse
  assembly"
- `C2611850` "regulation of circadian rhythm phase" →
  [`GO:0009649`](http://purl.obolibrary.org/obo/GO_0009649) "entrainment of circadian clock"
- `C0031845` "Physiological Processes" →
  [`GO:0008150`](http://purl.obolibrary.org/obo/GO_0008150) "biological_process"
- `C1325880` "cellular process" → [`GO:0042995`](http://purl.obolibrary.org/obo/GO_0042995) "cell
  projection"

These CUIs stay as UMLS cliques of their own.

### Not changed: anatomy

Anatomy (cellular component) uses the same `build_sets()` with the default `acceptable_ttys`, so it
still takes every GO atom. There, CUIs also link to MESH, SNOMEDCT, NCIT and FMA, so restricting GO
would add mappings that join existing cliques, and the ones reviewed so far need work first:

- [`GO:0005634`](http://purl.obolibrary.org/obo/GO_0005634) "nucleus" would join a clique that
  already wrongly merges [`UBERON:0000125`](http://purl.obolibrary.org/obo/UBERON_0000125) "neural
  nucleus".
- One CUI holds the `PT`s of both Golgi stack and Golgi cisterna, so which one merges depends on
  order.
- Cell projection membrane maps to the MeSH term for cell surface extensions.

Switching anatomy over is tracked in
[#1137](https://github.com/NCATSTranslator/Babel/issues/1137), which lists every join it would make
and the CUIs that carry more than one GO preferred term.

## Which GO atoms become UMLS synonyms

`pull_umls()` still uses GO atoms for labels, because many process CUIs have no other source. For
synonyms, it skips a GO atom whose GO code is not one of the CUI's own preferred-term
(`PT`/`MTH_PT`) codes. UMLS often files another GO term's entry terms under a CUI, and they would
become synonyms of the wrong clique: `C1152464` "cardiolipin synthase activity"
([`GO:0008808`](http://purl.obolibrary.org/obo/GO_0008808)) carries "cardiolipin synthase" from both
[`GO:0043337`](http://purl.obolibrary.org/obo/GO_0043337) "cardiolipin synthase (CMP-forming)
activity" and [`GO:0090483`](http://purl.obolibrary.org/obo/GO_0090483)
"phosphatidylglycerol-phosphatidylethanolamine phosphatidyltransferase activity". A CUI with no GO
preferred term, like `C2249862`, keeps all its GO atoms, since those are its own names.

Against 2026jul22, this drops 163 strings that are names of a different GO term from CUIs that join
a GO clique (of 197 such strings), and 18 strings from CUIs that don't. The rule is
`umls.is_other_go_term_atom()`, which the report script imports.

It keeps the other 34, because MRCONSO alone can't identify them:

- 21 are on CUIs with more than one GO preferred term. All of those terms count as the CUI's own,
  but it can join only one of them. For example, `C0021721` "Intercellular Junctions" joins
  [`GO:0030054`](http://purl.obolibrary.org/obo/GO_0030054) "cell junction" and keeps "cell-cell
  junction". Most of these CUIs are in anatomy, where
  [#1137](https://github.com/NCATSTranslator/Babel/issues/1137) covers them.
- 13 are filed under the CUI's own GO code, but current GO gives that name only to another term.
  Catching them would need GO's current names in `pull_umls()`, which reads only MRCONSO.

Labels need no such check. `input_data/umls_precedence.txt` ranks GO's `PT` above every other GO
term type, so a CUI with a GO preferred term never takes its label from an atom this rule skips. The
report confirms this: 0 such CUIs.

### Considered and rejected: dropping every GO atom

GO's own synonyms reach a clique from UberGraph (`common/ubergraph/synonyms.jsonl`), so dropping
every GO atom from UMLS synonyms looks harmless. It only is for CUIs that end up in a GO clique, and
`pull_umls()` runs long before anyone knows which those are. Against 2026jul22, dropping every GO
atom removes 40,399 strings from CUIs that never join a GO clique: leftover CUIs, CUIs whose GO term
is obsolete, CUIs whose pair the overuse filter dropped. For many of those CUIs, those strings are
their only names. On CUIs that do join, it removes only 197 wrong-term strings and 2,554 strings
that are no longer GO names; the other 102,957 were already names of that GO term.
