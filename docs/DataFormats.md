# Data formats

There are three custom formats used within Babel outputs.

## Compendia files

Compendia files are JSON Lines (JSONL) files in the `compendia/` directory. Each line consists of a single "clique" --
a set of identifiers that Babel believes represents the same concept. Here is an example from `compendia/Gene.txt` for
the [glucose-6-phosphatase catalytic subunit 1 (G6PC1)](https://www.ncbi.nlm.nih.gov/gene/2538) gene.

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

| Field            | Value | Meaning                                                                                                                                                                                                                                                               |
|------------------| ----- |-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| ic               | 100 | Information content value (see [the main README](./README.md#what-are-information-content-values)). They are decimal values that range from 0.0 (high-level broad term with many subclasses) to 100.0 (very specific term with no subclasses).                        |
| identifiers      | _See below_ | A list of identifiers for this clique. This is arranged in the same order as the valid ID prefixes for this type in the Biolink Model, e.g. [starting with NCBIGene and ENSEMBL for `biolink:Gene`](https://biolink.github.io/biolink-model/Gene/#valid-id-prefixes). |
| identifiers[0].i | NCBIGene:2358 | A CURIE representing this identifier. You can use the [Biolink Model prefixmap](https://github.com/biolink/biolink-model/tree/master/project/prefixmap) to expand this into a full concept IRI.                                                                       |
| identifiers[0].l | G6PC1 | A label for this identifier. This will almost always be from the source of the CURIE (in this case, the label is from the NCBI Gene database).                                                                                                                        |
| identifiers[0].d | (blank in this example, but usually 1-3 sentences) | A description of this identifier or concept from this source.                                                                                                                                                                                                         |
| identifiers[0].t | ["NCBITaxon:9606"] | A list of taxa that this concept is found in as NCBITaxon CURIEs. NCBITaxon:9606 refers to the species _Homo sapiens_.                                                                                                                                                |
| preferred_name | G6PC1 | The preferred name for this clique. This is not currently used by NodeNorm, but will be in the future.                                                                                                                                                                |
| taxa | ["NCBITaxon:9606"] | A list of taxa that this concept is found in as NCBITaxon CURIEs. This is combined from all the individual taxa from each identifier. |
| descriptions | (blank in this example, but usually a list of descriptions) | A list of descriptions, created by combining descriptions from all the identifiers.                                                                                                                                                                                   |
| type | biolink:Gene | The Biolink type of this concept. Must be a class from the [Biolink model](https://biolink.github.io/biolink-model/) with a `biolink:` prefix. |

## Synonym files

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

## Conflation files

```json
["NCBIGene:2538", "UniProtKB:P35575"]
```