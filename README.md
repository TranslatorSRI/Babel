[![Build Status](https://travis-ci.com/TranslatorIIPrototypes/Babel.svg?branch=master)](https://travis-ci.com/TranslatorIIPrototypes/Babel)

# Babel

## Introduction

The [Biomedical Data Translator](https://ncats.nih.gov/translator) integrates data across many data sources.  One
source of difficulty is that different data sources use different vocabularies.
One source may represent water as MESH:D014867, while another may use the
identifier DRUGBANK:DB09145.   When integrating, we need to recognize that 
both of these identifiers are identifying the same concept.

Babel integrates the specific naming systems used in the Translator, 
creating equivalent sets across multiple semantic types, and following the
conventions established by the [biolink model](https://github.com/biolink/biolink-model).  It checks these conventions
at runtime by querying the [Biolink Model service](https://github.com/TranslatorIIPrototypes/bl_lookup).  Each semantic type (such as 
chemical substance) requires specialized processing, but in each case, a 
JSON-formatted compendium is written to disk.  This compendium can be used 
directly, but it can also be served via the [Node Normalization service](https://github.com/TranslatorIIPrototypes/NodeNormalization).

We anticipate that the simple approach taken here will soon be overtaken by
more advanced probabilistic procedures, so caution should be taken in building
strong dependencies against the Babel code.

## Configuration

Before running, edit `config.json` and provide a path relative to the config file.
This path will be used to store downloaded an intermediate files.  If all compendia 
are built, this directory will end up holding approximately 80GB of files.

Also, if building the disease/phenotype compendia, there are two files that 
must be obtained with the user's UMLS license.  In particular `MRCONSO.RRF` 
and `MRSTY.RRF` should be placed in `/babel/input_data`.

## Building Compendia

From the root directory, the following command can be used to generate the compendia.
```
export PYTHONPATH=.; python babel/anatomy.py
export PYTHONPATH=.; python babel/disease_phenotype.py
export PYTHONPATH=.; python babel/process_and_activity.py
export PYTHONPATH=.; python babel/chemicals.py
```
Each generates one or more files in `babel/compendia`.  These files have a 
standard format of one entity per row.  Each entity is a JSON string, containing an
identifier, a label, the semantic types of the object, and its equivalent
identifiers.

## Compendia Notes

Different semantic types have different scripts, because the algorithms applied
vary.  

### Process and Activity

`process_and_activity.py` is the simplest script.  It first queries UberGraph to
find all entities that descend from the term `GO:0008150` (biological_process) and
their db_xref values.  The (oversimple) assumption is made that each
entity's db_xref values are equivalent identifiers. These equivalent identifiers
are used to write a compendium for biological process, and then the process is
repeated starting with `GO:0003674` (molecular_activity)

### Anatomy

The `anatomy.py` script queries UberGraph to find all entities that descend from 
`UBERON:0001062` and their db_xref values.  UberGraph contains numerous ontologies, 
so this query returns not only entities from UBERON, but also cell types from CL,
and cellular components from GO.  The (oversimple) assumption is made that each
entity's db_xref values are equivalent identifiers.  Types are assigned simply:
if the entity has an UBERON identifier, it is an anatomic_entity, otherwise if it
has a CL identifier it is a cell, otherwise if it has a GO identifier, it is a
cellular_component.  Otherwise, the entity is ignored. Individual compendia are written
for each type.

### Disease and Phenotypic Feature

`disease_phenotype.py` handles disease and phenotypic feature types jointly, 
because there are multiple entities that are considered as diseases by some
vocabularies and phenoptyic features by others.   We rely heavily on the MONDO
ontology in two ways.  First, MONDO provides strong equivalence statements, which
we use to drive equivalence for diseases.  Second, we take MONDO as authoritative
where diseases are concerned.  If an entity is in MONDO, we consider it a disease
independent of what other ontologies say about it.

The actual procedure is more complicated than above.   First, we pull all 
descendents of `HP:0000118`, and their db_xrefs from UberGraph. Here, we 
deliberately ignore xrefs of type `ICD` and `NCIT` as these tend to lead to 
unwanted merges.  We then filter out any identifier that occurs as a db_xref
in more than one of these HP terms.  Next, we merge into this set, all descendents
of `MONDO:0000001` along with their exactMatch and equivalentTo identifiers.
Finally, we merge in identifiers from MEDDRA and UMLS.  Once these groups are
allowed to merge (if they share any equivalent identifiers), we assign a type,
either disease or phenotypic feature to each.  If a term has a MONDO identifier,
it is a disease.  If it does not have a MONDO, but does have an HP identifier,
it is a phenotypic feature. If it does not have either a MONDO or HP identifier,
then we check its UMLS identifier, and infer from UMLS whether it is a disease
or a phenotypic feature.  If it has none of the above, then we consider it a 
phenotypic feature.

### Chemical Substance

The most complicated equivalent set creation is for chemical substance.  The full
description is beyond the scope of this readme, but the basic outline is as follows.
We assume that for chemicals that have inchikeys, the inchikey defines equivalence.
In general, we rely on EBI's UniChem to provide inchikeys and related equivalence.

However, some entities that we consider chemical substances do not have an inchikey,
but may have a SMILES.  For these entities, we join on these SMILES.  

For entities with neither an InchiKey nor a SMILES, we rely on db_xref annotations
or similar relationships from other databases as above.
