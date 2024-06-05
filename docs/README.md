# Babel Documentation

This directory contains several pieces of Babel documentation.

Note that both [Node Normalization](https://github.com/TranslatorSRI/NodeNormalization) and
[Name Resolution](https://github.com/TranslatorSRI/NameResolution) have their own GitHub repositories
with their own documentation, but this directory is intended to include all the basic instructions
needed to get started with Babel and its tools.

## What does Babel do?

Babel was built as part of the [NCATS Translator project](https://ui.transltr.io/) to solve the problem
of multiple databases using different identifiers (specifically, [CURIEs](https://en.wikipedia.org/wiki/CURIE)) to
refer to the same concept, such as [CHEBI:15377 "water"](https://www.ebi.ac.uk/chebi/searchId.do?chebiId=15377) and
[PUBCHEM.COMPOUND:962 "water"](https://pubchem.ncbi.nlm.nih.gov/compound/962). Babel downloads many online 
databases of identifiers and uses their cross-reference information to identify _cliques_ of identifiers
that refer to the same concept. Each clique has a type taken from the
[Biolink Model](https://github.com/biolink/biolink-model), which controls which identifier prefixes are allowed
and the order in which the identifiers are sorted. While generating these clique, Babel also tracks the synonyms
for each clique, and collects this information to allow the cliques to be looked up by name for autocomplete or
named entity linkage purposes. Finally, we produce [conflation](./Conflation.md) files that allows cliques to
be combined on the basis of various criteria.

## How can I access Babel cliques?

There are several ways of accessing Babel cliques:
* The NCATS Translator project provides the [Node Normalization](https://nodenorm.transltr.io/docs) frontend to
  "normalize" identifiers -- any member of a particular clique will be normalized to the same _preferred identifier_,
  allowing identifiers referring to the same concept to be merged. You can find out more about this frontend in
  [its GitHub repository](https://github.com/TranslatorSRI/NodeNormalization).
* The NCATS Translator project also provides the [Name Lookup (Name Resolution)](https://name-lookup.transltr.io/)
  frontends for searching for concepts by labels or synonyms. You can find out more about this frontend at
  [its GitHub repository](https://github.com/TranslatorSRI/NameResolution).
* Members of the Translator consortium can also request access to the [Babel outputs](./BabelOutputs.md), which
  are currently available in JSONL or [KGX](https://github.com/biolink/kgx) formats.

## I've found a "split" clique: two identifiers that should be considered identical are in separate cliques.

Please report this as an issue to the [Babel GitHub repository](https://github.com/TranslatorSRI/Babel/issues).
At a minimum, please include the identifiers (CURIEs) for the identifiers that should be combined. Links to
a NodeNorm instance showing the two cliques are very helpful. Evidence supporting the lumping, such as a link to an
external database that makes it clear that these identifiers refer to the same concept, are also very helpful: while we
have some ability to combine cliques manually if needed urgently for some application, we prefer to find a source of
mappings that would combine the two identifiers, allowing us to improve cliquing across Babel.

## I've found a "lumped" clique: two identifiers that are combined in a single clique refer to different concepts.

Please report this as an issue to the [Babel GitHub repository](https://github.com/TranslatorSRI/Babel/issues).
At a minimum, please include the identifiers (CURIEs) for the identifiers that should be split. Links to
a NodeNorm instance showing the lumped clique is very helpful. Evidence, such as a link to an external database
that makes it clear that these identifiers refer to the same concept, are also very helpful: while we have some
ability to combine cliques manually if needed urgently for some application, we prefer to find a source of mappings
that would combine the two identifiers, allowing us to improve cliquing across Babel.

## How can I build Babel?

Babel is difficult to build, primarily because of its inefficient memory handling -- we currently need around 500G of
memory to build the largest compendia (Protein and DrugChemical conflated information), although the smaller
compendia should be buildable with far less memory. We are working on reducing these restrictions as far as possible.
You can read more about [Babel's build process](./Build.md), and please do contact us if you run into any problems or
would like some assistance.

## Who should I contact for more information about Babel?

You can find out more about Babel by [opening an issue on this repository](https://github.com/TranslatorSRI/Babel/issues),
contacting one of the [Translator SRI PIs](https://ncats.nih.gov/research/research-activities/translator/projects), or
contacting the [NCATS Translator team](https://ncats.nih.gov/research/research-activities/translator/about).