# Canonical RDF predicate URI constants. Keeps full predicate strings in one place
# so that synonym/label TSV files use consistent values. Grouped by RDF URLs.
#
# SPARQL PREFIX declarations inside query strings are left as inline literals
# since the SPARQL syntax requires it and they don't benefit from centralisation.
from src.categories import OWL

# oboInOwl predicates
OBO_IN_OWL = "http://www.geneontology.org/formats/oboInOwl#"
HAS_EXACT_SYNONYM = OBO_IN_OWL + "hasExactSynonym"
HAS_RELATED_SYNONYM = OBO_IN_OWL + "hasRelatedSynonym"
HAS_ALTERNATIVE_ID = OBO_IN_OWL + "hasAlternativeId"
HAS_DB_XREF = OBO_IN_OWL + "hasDbXref"
HAS_SYNONYM = OBO_IN_OWL + "hasSynonym"
OBO_ID = OBO_IN_OWL + "id"

# SKOS predicates
SKOS = "http://www.w3.org/2004/02/skos/core#"
EXACT_MATCH = SKOS + "exactMatch"
CLOSE_MATCH = SKOS + "closeMatch"
ALT_LABEL = SKOS + "altLabel"
PREF_LABEL = SKOS + "prefLabel"

# RDF predicates
RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"

# RDFS predicates
RDFS = "http://www.w3.org/2000/01/rdf-schema#"
RDFS_LABEL = RDFS + "label"
RDFS_SUBCLASSOF = RDFS + "subClassOf"

# OWL predicates
OWL_EQUIVALENT_CLASS = OWL + "equivalentClass"

# RO (Relations Ontology) predicates
RO = "http://purl.obolibrary.org/obo/RO_"
HAS_ROLE = RO + "0000087"

# ChemROF predicates, for a chemical's structure and physical properties.
#
# These are the names UberGraph publishes ChEBI's structural annotations under. ChEBI used to
# publish them under http://purl.obolibrary.org/obo/chebi/ (smiles, inchi, inchikey, formula,
# monoisotopicmass); that namespace now has zero triples in UberGraph, which is issue #1086.
#
# We name our properties after the ChemROF URIs rather than the dead ChEBI ones, and rather than
# minting Babel-local URIs, so that a producer can be switched from the ChEBI SDF to UberGraph (or
# to another source that speaks ChemROF) without changing anything downstream of the property file.
CHEMROF = "https://w3id.org/chemrof/"
CHEMROF_SMILES = CHEMROF + "smiles_string"
CHEMROF_INCHI = CHEMROF + "inchi_string"
CHEMROF_INCHI_KEY = CHEMROF + "inchi_key_string"
CHEMROF_FORMULA = CHEMROF + "generalized_empirical_formula"
CHEMROF_MASS = CHEMROF + "mass"
CHEMROF_MONOISOTOPIC_MASS = CHEMROF + "monoisotopic_mass"
CHEMROF_CHARGE = CHEMROF + "charge"

# Biolink predicates
BIOLINK = "biolink:"
BIOLINK_SAME_AS = BIOLINK + "same_as"
