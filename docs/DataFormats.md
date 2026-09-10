# Data formats

There are three custom formats used within Babel outputs.

## Compendia files

Compendia files are JSON Lines (JSONL) files in the `compendia/` directory. Each line consists of a
single "clique" -- a set of identifiers that Babel believes represents the same concept. Here is an
example from `compendia/Gene.txt` for the
[glucose-6-phosphatase catalytic subunit 1 (G6PC1)](https://www.ncbi.nlm.nih.gov/gene/2538) gene.

```json
{
  "ic": "100",
  "identifiers": [
    {
      "i": "NCBIGene:2538",
      "l": "G6PC1",
      "d": [],
      "t": [
        "NCBITaxon:9606"
      ]
    },
    {
      "i": "ENSEMBL:ENSG00000131482",
      "l": "G6PC1 (Hsap)",
      "d": [],
      "t": []
    },
    {
      "i": "HGNC:4056",
      "l": "G6PC1",
      "d": [],
      "t": []
    },
    {
      "i": "OMIM:613742",
      "d": [],
      "t": []
    },
    {
      "i": "UMLS:C1414892",
      "l": "G6PC1 gene",
      "d": [],
      "t": []
    }
  ],
  "preferred_name": "G6PC1",
  "taxa": [
    "NCBITaxon:9606"
  ],
  "type": "biolink:Gene"
}
```

This entry consists of the following fields:

| Field            | Value                                                       | Meaning                                                                                                                                                                                                                                                               |
|------------------|-------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| ic               | 100                                                         | Information content value (see [Understanding.md](./Understanding.md#what-are-information-content-values)). They are decimal values that range from 0.0 (high-level broad term with many subclasses) to 100.0 (very specific term with no subclasses).                 |
| identifiers      | _See below_                                                 | A list of identifiers for this clique. This is arranged in the same order as the valid ID prefixes for this type in the Biolink Model, e.g. [starting with NCBIGene and ENSEMBL for `biolink:Gene`](https://biolink.github.io/biolink-model/Gene/#valid-id-prefixes). |
| identifiers[0].i | NCBIGene:2538                                               | A CURIE representing this identifier. You can use the [Biolink Model prefixmap](https://github.com/biolink/biolink-model/tree/master/project/prefixmap) to expand this into a full concept IRI.                                                                       |
| identifiers[0].l | G6PC1                                                       | A label for this identifier. This will almost always be from the source of the CURIE (in this case, the label is from the NCBI Gene database).                                                                                                                        |
| identifiers[0].d | (blank in this example, but usually 1-3 sentences)          | A description of this identifier or concept from this source.                                                                                                                                                                                                         |
| identifiers[0].t | ["NCBITaxon:9606"]                                          | A list of taxa that this concept is found in as NCBITaxon CURIEs. NCBITaxon:9606 refers to the species _Homo sapiens_.                                                                                                                                                |
| preferred_name   | G6PC1                                                       | The preferred name for this clique. NodeNorm returns this as the `label` of the normalized clique. Note that it is not necessarily the label of the clique leader — see below.                                                                                        |
| taxa             | ["NCBITaxon:9606"]                                          | A list of taxa that this concept is found in as NCBITaxon CURIEs. This is combined from all the individual taxa from each identifier.                                                                                                                                 |
| type             | biolink:Gene                                                | The Biolink type of this concept. Must be a class from the [Biolink model](https://biolink.github.io/biolink-model/) with a `biolink:` prefix.                                                                                                                        |

The first identifier in the `identifiers` list is considered the "clique leader" or "preferred ID"
for the clique. When normalizing an identifier, that identifier is used to represent the entire
clique. The preferred name is not necessarily the label for the clique leader -- another name may be
chosen to clarify the meaning of the clique or to provide a better label for displaying in the
Translator UI.

## Synonym files

Synonym files are JSONL files, where each entry is a JSON document describing a concept and all its
synonyms.

```json
{
  "clique_identifier_count": 5,
  "curie": "NCBIGene:2538",
  "curie_suffix": 2538,
  "names": [
    "GSD1",
    "G6PC",
    "G6PT",
    "GSD1a",
    "G6PC1",
    "G6Pase",
    "G-6-Pase",
    "G6PC gene",
    "G6PC1 gene",
    "G6Pase-alpha",
    "G6PC1 (Hsap)",
    "G6PT, FORMERLY",
    "glucose-6-phosphatase alpha",
    "GLUCOSE-6-PHOSPHATASE, CATALYTIC",
    "GLUCOSE-6-PHOSPHATASE, CATALYTIC, 1",
    "glucose-6-phosphatase catalytic subunit",
    "glucose-6-phosphatase catalytic subunit 1",
    "glycogen storage disease type I, von Gierke disease",
    "glucose-6-phosphatase, catalytic (glycogen storage disease type I, von Gierke disease)"
  ],
  "preferred_name": "G6PC1",
  "shortest_name_length": 4,
  "taxa": [
    "NCBITaxon:9606"
  ],
  "types": [
    "Gene",
    "GeneOrGeneProduct",
    "GenomicEntity",
    "ChemicalEntityOrGeneOrGeneProduct",
    "PhysicalEssence",
    "OntologyClass",
    "BiologicalEntity",
    "ThingWithTaxon",
    "NamedThing",
    "Entity",
    "PhysicalEssenceOrOccurrent",
    "MacromolecularMachineMixin"
  ]
}
```

This entry consists of the following fields:

| Field                   | Value                              | Meaning                                                                                                                                                                                                                                     |
|-------------------------|------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| clique_identifier_count | 5                                  | The number of identifiers in the corresponding clique (i.e. for `NCBIGene:2538`).                                                                                                                                                           |
| curie                   | NCBIGene:2538                      | The CURIE for this entry. Note that the equivalent identifiers are not included.                                                                                                                                                            |
| curie_suffix            | 2538                               | If the CURIE suffix is completely numerical, it will be stored in this field as a number. This is used to sort search results, with lower CURIE suffixes appearing first.                                                                   |
| names                   | [ "GD1", "G6PC", ... ]"            | A list of synonyms for this concept. It is usually arranged from shortest to longest, except for conflated cliques, which has all the synonyms for the first identifier, followed by all the synonyms for the second identifier, and so on. |
| preferred_name          | G6PC1                              | The preferred name for this clique.                                                                                                                                                                                                         |
| shortest_name_length    | 4                                  | The length of the shortest synonym in the `names` list, in order to sort results for the shortest name.                                                                                                                                     |
| taxa                    | ["NCBITaxon:9606"]                 | The list of taxa that this concept is found in. This should be identical to the entry in the corresponding Compendia file.                                                                                                                  |
| taxon_specific          | true or false                      | True if this concept is associated with one or more specific taxa; false if it is not taxon-specific.                                                                                                                                       |
| types                   | ["Gene", "GeneOrGeneProduct", ...] | A list of Biolink types (without the `biolink:` prefix) for this concept. This is arranged in the same order provided by the Biolink Model Toolkit, starting with the narrowest concept, expanding to the broadest, followed by mixins.     |

Note that the per-type synonym files are generated with DrugChemical conflation turned on, but
GeneProtein conflation turned off. A separate `synonyms/GeneProteinConflated.txt.gz` is also
produced, containing the same concepts with GeneProtein conflation applied, so that NameRes can be
loaded with that conflation turned on.

## Conflation files

There are only two conflation files: `GeneProtein.txt` and `DrugChemical.txt`, corresponding to the
two currently supported conflation methods. Both files have the same format: a JSONL file where each
entry is a list of clique leaders (i.e. the first identifier for a clique in the
[Compendia files](#compendia-files)) that should be combined under that conflation. For example, the
following entry indicates that if either `NCBIGene:2538` or `UniProtKB:P35575` is queried with
GeneProtein conflation turned on, then a combined clique of both identifiers should be returned.

```json
["NCBIGene:2538", "UniProtKB:P35575"]
```

Here is the response when normalizing `UniProtKB:P35575` from NodeNorm when both GeneProtein
conflation and individual types are turned on:

```json
{
  "UniProtKB:P35575": {
    "id": {
      "identifier": "NCBIGene:2538",
      "label": "G6PC1"
    },
    "equivalent_identifiers": [
      {
        "identifier": "NCBIGene:2538",
        "label": "G6PC1",
        "type": "biolink:Gene"
      },
      {
        "identifier": "ENSEMBL:ENSG00000131482",
        "label": "G6PC1 (Hsap)",
        "type": "biolink:Gene"
      },
      {
        "identifier": "HGNC:4056",
        "label": "G6PC1",
        "type": "biolink:Gene"
      },
      {
        "identifier": "OMIM:613742",
        "type": "biolink:Gene"
      },
      {
        "identifier": "UMLS:C1414892",
        "label": "G6PC1 gene",
        "type": "biolink:Gene"
      },
      {
        "identifier": "UniProtKB:P35575",
        "label": "G6PC1_HUMAN Glucose-6-phosphatase catalytic subunit 1 (sprot)",
        "type": "biolink:Protein"
      },
      {
        "identifier": "PR:P35575",
        "label": "glucose-6-phosphatase catalytic subunit 1 (human)",
        "type": "biolink:Protein"
      },
      {
        "identifier": "UMLS:C4549614",
        "label": "G6PC1 protein, human",
        "type": "biolink:Protein"
      }
    ],
    "type": [
      "biolink:Gene",
      "biolink:GeneOrGeneProduct",
      "biolink:GenomicEntity",
      "biolink:ChemicalEntityOrGeneOrGeneProduct",
      "biolink:PhysicalEssence",
      "biolink:OntologyClass",
      "biolink:BiologicalEntity",
      "biolink:ThingWithTaxon",
      "biolink:NamedThing",
      "biolink:PhysicalEssenceOrOccurrent",
      "biolink:MacromolecularMachineMixin",
      "biolink:Protein",
      "biolink:GeneProductMixin",
      "biolink:Polypeptide",
      "biolink:ChemicalEntityOrProteinOrPolypeptide"
    ],
    "information_content": 88.2
  }
}
```

Note that this includes both biolink:Gene identifiers (such as
[HGNC:4056](https://alliancegenome.org/gene/HGNC:4056)) and biolink:Protein identifiers (such as
[UniProtKB:P35575](http://www.uniprot.org/uniprot/P35575)).

## DuckDB and Parquet exports

The `babel_outputs/duckdb/` directory contains the same information as the JSONL files above,
reformatted for analytical queries. The intended use case is index-wide queries that would be
impractical against the raw JSONL — for example, finding synonyms shared across multiple cliques,
computing synonym-length distributions, or checking prefix coverage across semantic types. There
are no fixed downstream consumers yet; the schema is evolving.

The export is produced by `src/snakefiles/duckdb.snakefile` via
`src/exporters/duckdb_exporters.py`. Each semantic type gets a subdirectory under
`babel_outputs/duckdb/parquet/filename={Type}/` containing one or more Parquet files. A
transient DuckDB database (`.duckdb`) is written alongside each set of Parquet files during
export and can be used directly for interactive querying, but the Parquet files are the
durable output.

### Compendium tables (`filename={Type}/Node.parquet`, `Clique.parquet`, `Edge.parquet`)

These three tables are derived from the compendia JSONL for each semantic type.

`Node.parquet` — one row per identifier across all cliques:

| Column       | Type            | Meaning                                          |
|--------------|-----------------|--------------------------------------------------|
| curie        | STRING          | The identifier CURIE, e.g. `NCBIGene:2538`       |
| curie_prefix | STRING          | The prefix portion of the CURIE, e.g. `NCBIGene` |
| label        | STRING          | The label for this identifier, if any            |
| label_lc     | STRING          | Lower-cased label (for case-insensitive search)  |
| description  | STRING[]        | Description text(s) for this identifier          |
| taxa         | STRING[]        | Taxa CURIEs associated with this identifier      |

`Clique.parquet` — one row per clique:

| Column                   | Type   | Meaning                                                   |
|--------------------------|--------|-----------------------------------------------------------|
| clique_leader            | STRING | CURIE of the preferred identifier for the clique          |
| preferred_name           | STRING | Preferred display name for the clique                     |
| clique_identifier_count  | INT    | Number of identifiers in the clique                       |
| biolink_type             | STRING | Biolink type, e.g. `biolink:Gene`                         |
| information_content      | FLOAT  | Information content value (0–100)                         |

We get information content values as the [`normalizedInformationContent` from Ubergraph].

`Edge.parquet` — one row per (clique, identifier) pair; the primary way to look up which
clique contains a given CURIE:

| Column               | Type   | Meaning                                                                    |
|----------------------|--------|----------------------------------------------------------------------------|
| clique_leader        | STRING | CURIE of the clique's preferred identifier                                 |
| curie                | STRING | An identifier that belongs to this clique                                  |
| conflation           | STRING | Conflation type if applicable, otherwise `'None'`                          |
| clique_leader_prefix | STRING | Prefix of the clique leader CURIE                                          |
| curie_prefix         | STRING | Prefix of the member CURIE                                                 |
| biolink_type         | STRING | Biolink type of the owning clique (denormalized to avoid Edge→Clique join) |

### Synonym table (`filename={Type}/Synonyms.parquet`)

Derived from the synonym JSONL files. One row per (clique, synonym) pair — i.e., the `names`
array from the synonym file is unnested so each individual synonym gets its own row. This makes
synonym-frequency queries straightforward at the cost of a large row count for types with many
synonyms per concept (notably `Protein` and `GeneProteinConflated`, which have hundreds of
UniProt synonyms per entry).

| Column           | Type   | Meaning                                            |
|------------------|--------|----------------------------------------------------|
| clique_leader    | STRING | CURIE of the clique's preferred identifier         |
| preferred_name   | STRING | Preferred display name for the clique              |
| preferred_name_lc| STRING | Lower-cased preferred name                         |
| biolink_type     | STRING | Biolink type, e.g. `biolink:Gene`                  |
| label            | STRING | One synonym from the `names` list                  |
| label_lc         | STRING | Lower-cased synonym                                |

### Conflation table (`filename={ConflationName}/Conflation.parquet`)

Derived from the conflation JSONL files (`GeneProtein.txt`, `DrugChemical.txt`). One row per
(conflation group, member CURIE):

| Column             | Type   | Meaning                                              |
|--------------------|--------|------------------------------------------------------|
| conflation_type    | STRING | Conflation name, e.g. `GeneProtein`                  |
| conflation_leader  | STRING | CURIE of the conflation group's lead identifier      |
| curie              | STRING | A member CURIE of the conflation group               |
| curie_prefix       | STRING | Prefix of the member CURIE                           |

### Intermediate tables (`Concord.parquet`, `Identifier.parquet`, `Metadata.parquet`, `Property.parquet`)

Unlike the tables above, these four are not per-semantic-type; they sweep the whole
`babel_outputs/intermediate/` tree into four flat Parquet files written directly under
`babel_outputs/duckdb/` (not under `parquet/filename={Type}/`). They are produced by
`export_intermediates_to_parquet()` (rule `export_intermediate_files_to_duckdb`) and make the raw
build inputs — the cross-reference concords and per-source identifier lists — queryable without
downloading the many original TSV files. The `filename` column on every row is the absolute path of
the source file, so you can tell which semantic type and which source a row came from.

`Concord.parquet` — one row per cross-reference edge, from every non-empty concord file (including
the extension-less DrugChemical conflation concords at
`intermediate/drugchemical/concords/{RXNORM,UMLS,PUBCHEM_RXNORM}`):

| Column   | Type   | Meaning                                                          |
|----------|--------|------------------------------------------------------------------|
| filename | STRING | Absolute path of the concord file this edge came from            |
| subj     | STRING | Subject CURIE of the cross-reference, e.g. `RXCUI:203437`        |
| pred     | STRING | Relation/predicate, e.g. `eq` or `linked`                        |
| obj      | STRING | Object CURIE of the cross-reference, e.g. `PUBCHEM.COMPOUND:146037278` |

Finding every raw concord edge that touches a CURIE — the first question a conflation-regression
triage asks (see Babel [#754](https://github.com/NCATSTranslator/Babel/issues/754)) — is a
one-liner:

```sql
SELECT * FROM read_parquet('babel_outputs/duckdb/Concord.parquet')
WHERE subj = 'CHEBI:3395' OR obj = 'CHEBI:3395';
```

If the edge you expected is absent, the link was never generated upstream (a compendium-build /
RxCUI-typing problem); if it is present but the CURIEs still aren't conflated, the pair was dropped
by a conflation filter — look up the reason in the paired per-run exclusion report
`babel_outputs/reports/drugchemical/excluded_pairs.tsv.gz` (its `reason` column names the filter).

`Identifier.parquet` — one row per identifier extracted into an `ids/` file:

| Column       | Type   | Meaning                                                            |
|--------------|--------|--------------------------------------------------------------------|
| filename     | STRING | Absolute path of the ids file this CURIE came from                 |
| curie        | STRING | The identifier CURIE                                               |
| biolink_type | STRING | The Biolink type from the ids file's second column, or NULL if the file has only a CURIE column |

`Metadata.parquet` — one row per metadata YAML _inside a `concords/` or `ids/` directory_
(`metadata-<subject>.yaml` sidecars describing a sibling file, and bare `metadata.yaml` files
describing their directory). Metadata YAMLs elsewhere in the intermediate tree — e.g.
`intermediate/chemicals/partials/metadata-untyped_compendium.yaml` — are not exported, since this
table exists to describe the concord and identifier files above:

| Column            | Type   | Meaning                                                              |
|-------------------|--------|----------------------------------------------------------------------|
| filename          | STRING | Absolute path of the metadata YAML file                              |
| subject_filename  | STRING | The file the metadata describes (the `<subject>` of a sidecar, or the metadata file's own name for a bare `metadata.yaml`) |
| subject_file_path | STRING | Absolute path of the described file (or its directory, for a bare `metadata.yaml`) |
| metadata_json     | STRING | The metadata YAML contents                                          |

`Property.parquet` — one row per property, from every non-empty file in a `properties/` directory.
Property files are gzipped JSONL of `src.properties.Property`, so unlike the concord and ids files
their columns are fixed rather than sniffed:

| Column    | Type   | Meaning                                                                    |
|-----------|--------|----------------------------------------------------------------------------|
| filename  | STRING | Absolute path of the property file this row came from                      |
| curie     | STRING | The CURIE the property is about, e.g. `CHEBI:15365`                        |
| predicate | STRING | The property URI, e.g. `https://w3id.org/chemrof/generalized_empirical_formula` |
| value     | STRING | The property value, e.g. `C9H8O4`. Always a single string — a CURIE with several values for one predicate has several rows |
| source    | STRING | Free text saying where the value came from                                 |

Properties are annotations, not equivalences: nothing here took part in clique building, and most
of it does not appear in the compendia at all. This table is how you ask a structure question of a
build without one:

```sql
-- What structure does ChEBI record for aspirin?
SELECT predicate, value FROM read_parquet('babel_outputs/duckdb/Property.parquet')
WHERE curie = 'CHEBI:15365';

-- Which cliques were split apart despite sharing an InChIKey skeleton? (issues #452, #230)
SELECT split_part(value, '-', 1) AS skeleton, count(DISTINCT curie) AS chemicals
FROM read_parquet('babel_outputs/duckdb/Property.parquet')
WHERE predicate = 'https://w3id.org/chemrof/inchi_key_string'
GROUP BY 1 HAVING count(DISTINCT curie) > 1;
```

Labels are intentionally not exported here: `ids/` files carry only `CURIE\tbiolink:Type`, and the
label for any identifier that landed in a clique is already available in `Node.parquet` (join
`Identifier.curie` to `Node.curie`).

[`normalizedInformationContent` from Ubergraph]: https://github.com/INCATools/ubergraph/#graph-organization
