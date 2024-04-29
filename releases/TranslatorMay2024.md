# Babel Translator May 2024 Release

- Babel: [2024mar24](https://stars.renci.org/var/babel_outputs/2024mar24/) (approx
  [Babel v1.5.0](https://github.com/TranslatorSRI/Babel/releases/tag/v1.5.0))

Next release: _None as yet_

## New features
* [New identifiers] 36.9 million PubMed IDs (e.g. `PMID:25061375`) have been added as `biolink:JournalArticle`, as well as
  the mappings to PMC (e.g. `PMC:PMC4109484`) and DOIs (e.g. `doi:10.3897/zookeys.420.7089`) that are included in PubMed.
  Details in [TranslatorSRI/Babel#227](https://github.com/TranslatorSRI/Babel/pull/227).
* Fixed type determination for DrugChemical conflation. Details in
  [TranslatorSRI/Babel#266](https://github.com/TranslatorSRI/Babel/pull/266).
* Synonym files now include the clique identifier count (the number of identifiers in each clique) in synonyms file.
* Minor fixes.

## Releases since [December 2023](TranslatorDecember2023)
* [Babel v1.4.0](https://github.com/TranslatorSRI/Babel/releases/tag/v1.4.0):
  * Upgraded Biolink Model to v4.1.6.
  * Upgraded RxNorm to 03042024.
  * Upgraded Dockerfile to Python 3.12.
