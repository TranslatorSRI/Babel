# Babel Translator July 2024 Release

- Babel: [2024jul13](https://stars.renci.org/var/babel_outputs/2024jul13/) (approx
  [Babel 2024jul13](https://github.com/TranslatorSRI/Babel/releases/tag/2024jul13))
  - [CURIE summary](./summaries/2024jul13.json)

Next release: _None as yet_

## New features
* Added manual disease concords, and used that to do a better job of combining opioid use disorder and
  alcohol use disorder.
* Moved `ensembl_datasets_to_skip` into the config file.

## Bugfixes
* Eliminated preferred prefix overrides in Babel; we now trust the preferred prefixes from the Biolink Model.
* DrugChemical conflation generation now removes CURIEs that can't be normalized.
* Replaced `http://nihilism.com/` with `http://example.org/` as a base IRI.
* Updated mappings from Reactome types to Biolink types.

## Updates
* Updated Biolink from 4.1.6 to 4.2.1.
* Updated UMLS from 2023AB to 2024AA.
* Updated RxNorm from 03042024 to 07012024.
* Updated PANTHER_Sequence_Classification from PTHR18.0_human to PTHR19.0_human.
* Updated PANTHER pathways from SequenceAssociationPathway3.6.7.txt to SequenceAssociationPathway3.6.8.txt.

## Releases since [May 2024](TranslatorMay2024)
* No official releases
