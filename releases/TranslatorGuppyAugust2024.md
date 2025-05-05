# Babel Translator "Guppy" August 2024 Release

- Babel: [2024aug18](https://stars.renci.org/var/babel_outputs/2024aug18/) (approx
  [Babel 1.8.0](https://github.com/TranslatorSRI/Babel/releases/tag/v1.8.0))
  - [CURIE summary](./summaries/2024aug18.json)

Next release: [Translator "Hammerhead" November 2024](./TranslatorHammerheadNovember2024.md)
Previous release: [TranslatorFuguJuly2024](./TranslatorFuguJuly2024.md)

## New features
* Added support for generating DuckDB and Parquet files from the compendium and synonym files,
  allowing us to run queries such as looking for all the identically labeled cliques across
  all the compendia. Increased Babel Outputs file size to support DuckDB.
* Added labels from DrugBank (https://github.com/TranslatorSRI/Babel/pull/335).
* Improved cell anatomy concords using Wikidata (https://github.com/TranslatorSRI/Babel/pull/329).
* Added manual concords for the DrugChemical conflation (https://github.com/TranslatorSRI/Babel/pull/337).
* Wrote a script for comparing between two summary files (https://github.com/TranslatorSRI/Babel/pull/320).
* Added timestamping as an option to Wget.
* Reorganized primary label determination so that we can include it in compendia files as well.
  * This isn't currently used by the loader, but might be in the future. For now, this is only
    useful in helping track what labels are being chosen as the preferred label.

## Bugfixes
* Added additional ENSEMBL datasets to skip (https://github.com/TranslatorSRI/Babel/pull/297).
* Fixed a bug in recognizing the end of file when reading the PubChem ID and SMILES files.
* Fixed the lack of `clique_identifier_count` in leftover UMLS output.
* Fixed unraised exception in Ensembl BioMart download.
* Updated PubChem Compound download from FTP to HTTPS.
* Updated method for loading a prefix map.

## Updates
* Added additional Ubergraph IRI stem prefixes.
* Changed DrugBank ID types from 'ChemicalEntity' to 'Drug'.
