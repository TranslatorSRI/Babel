# Babel Conflation

Babel is designed to produce cliques of _identical_ identifiers, but our users would sometimes like to combine 
identifiers that are similar in some other way. Babel generates "conflations" to support this.

Babel currently generates two conflations:
1. GeneProtein conflates gene with the protein transcribed from it.
   The gene identifier will always be returned.
2. DrugChemical conflates drugs with their active ingredients as a chemical. For each conflation we attempt to
   determine a Biolink type, and arrange the identifiers in order of (1) preferred prefix order for that Biolink
   type, followed by (2) ordering identifiers from the numerically smallest suffix to the numerically largest 
   suffix.

## How are conflations generated in Babel and used in NodeNorm?

Each conflation file is a JSON-Lines (JSONL) file, where every line is a JSON list of clique identifiers, which are
stored in Redis databases in NodeNorm. If a particular conflation is turned on, NodeNorm will:
1. Normalize the input identifier to a clique identifier.
2. If the clique identifier is not part of any conflation, we return it as-is.
3. If the clique identifier is part of a conflation, we construct a new clique whose preferred identifier is the first
   identifier in the clique, and which consists of all the identifiers from all the cliques included in that conflation.

## How are types handled for conflated cliques?

Babel does not assign a type to any conflations. When NodeNorm is called with a particular conflation turned on,
it determines the types of a conflated clique by:
1. Starting with the most specific type of the first identifier in the conflation.
2. Adding all the supertypes of the most specific type for the first identifier in the conflation as determined
   by the [Biolink Model Toolkit](https://github.com/biolink/biolink-model-toolkit).
3. Add all the types and ancestors for all the other identifiers in the conflation without duplication.