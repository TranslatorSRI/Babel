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

| Field            | Value                                                       | Meaning                                                                                                                                                                                                                                                               |
|------------------|-------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| ic               | 100                                                         | Information content value (see [the main README](./README.md#what-are-information-content-values)). They are decimal values that range from 0.0 (high-level broad term with many subclasses) to 100.0 (very specific term with no subclasses).                        |
| identifiers      | _See below_                                                 | A list of identifiers for this clique. This is arranged in the same order as the valid ID prefixes for this type in the Biolink Model, e.g. [starting with NCBIGene and ENSEMBL for `biolink:Gene`](https://biolink.github.io/biolink-model/Gene/#valid-id-prefixes). |
| identifiers[0].i | NCBIGene:2358                                               | A CURIE representing this identifier. You can use the [Biolink Model prefixmap](https://github.com/biolink/biolink-model/tree/master/project/prefixmap) to expand this into a full concept IRI.                                                                       |
| identifiers[0].l | G6PC1                                                       | A label for this identifier. This will almost always be from the source of the CURIE (in this case, the label is from the NCBI Gene database).                                                                                                                        |
| identifiers[0].d | (blank in this example, but usually 1-3 sentences)          | A description of this identifier or concept from this source.                                                                                                                                                                                                         |
| identifiers[0].t | ["NCBITaxon:9606"]                                          | A list of taxa that this concept is found in as NCBITaxon CURIEs. NCBITaxon:9606 refers to the species _Homo sapiens_.                                                                                                                                                |
| preferred_name   | G6PC1                                                       | The preferred name for this clique. This is not currently used by NodeNorm, but will be in the future.                                                                                                                                                                |
| taxa             | ["NCBITaxon:9606"]                                          | A list of taxa that this concept is found in as NCBITaxon CURIEs. This is combined from all the individual taxa from each identifier.                                                                                                                                 |
| descriptions     | (blank in this example, but usually a list of descriptions) | A list of descriptions, created by combining descriptions from all the identifiers.                                                                                                                                                                                   |
| type             | biolink:Gene                                                | The Biolink type of this concept. Must be a class from the [Biolink model](https://biolink.github.io/biolink-model/) with a `biolink:` prefix.                                                                                                                        |

The first identifier in the `identifiers` list is considered the "clique leader" or "preferred ID" for the clique.
When normalizing an identifier, that identifier is used to represent the entire clique. The preferred name is not
necessarily the label for the clique leader -- another name may be chosen to clarify the meaning of the clique or
to provide a better label for displaying in the Translator UI.

## Synonym files

Synonym files are JSONL files, where each entry is a JSON document describing a concept and all its synonyms.

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
| clique_identifier_count | 5                                  | The number of identifiers in the corresponding clique (i.e. for `NCBIGene:2358`).                                                                                                                                                           |
| curie                   | NCBIGene:2538                      | The CURIE for this entry. Note that the equivalent identifiers are not included.                                                                                                                                                            |
| curie_suffix            | 2538                               | If the CURIE suffix is completely numerical, it will be stored in this field as a number. This is used to sort search results, with lower CURIE suffixes appearing first.                                                                   |
| names                   | [ "GD1", "G6PC", ... ]"            | A list of synonyms for this concept. It is usually arranged from shortest to longest, except for conflated cliques, which has all the synonyms for the first identifier, followed by all the synonyms for the second identifier, and so on. |
| preferred_name          | G6PC1                              | The preferred name for this clique.                                                                                                                                                                                                         |
| shortest_name_length    | 4                                  | The length of the shortest synonym in the `names` list, in order to sort results for the shortest name.                                                                                                                                     |
| taxa                    | ["NCBITaxon:9606"]                 | The list of taxa that this concept is found in. This should be identical to the entry in the corresponding Compendia file.                                                                                                                  |
| types                   | ["Gene", "GeneOrGeneProduct", ...] | A list of Biolink types (without the `biolink:` prefix) for this concept. This is arranged in the same order provided by the Biolink Model Toolkit, starting with the narrowest concept, expanding to the broadest, followed by mixins.     |

Note that the synonym files are generated with DrugChemical conflation turned on, but GeneProtein conflation turned off.

## Conflation files

There are only two conflation files: `GeneProtein.txt` and `DrugChemical.txt`, corresponding to the two currently
supported conflation methods. Both files have the same format: a JSONL file where each entry is a list of clique
leaders (i.e. the first identifier for a clique in the [Compendia files](#compendia-files)) that should be combined
under that conflation. For example, the following entry indicates that if either `NCBIGene:2538` or `UniProtKB:P35575`
is queried with DrugChemical conflation turned on, then a combined clique of both identifiers should be returned.

```json
["NCBIGene:2538", "UniProtKB:P35575"]
```

Here is the response when normalizing `UniProtKB:P35575` from NodeNorm when both DrugChemical conflation and individual
types are turned on:

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

Note that this includes both biolink:Gene identifiers (such as [HGNC:4056](https://alliancegenome.org/gene/HGNC:4056))
and biolink:Protein identifiers (such as [UniProtKB:P35575](http://www.uniprot.org/uniprot/P35575)).