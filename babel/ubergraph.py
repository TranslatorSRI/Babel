from babel.triplestore import TripleStore
from src.util import Text
from collections import defaultdict
from functools import reduce

class UberGraph:

    def __init__(self):
        self.triplestore = TripleStore("https://stars-app.renci.org/uberongraph/sparql")

    def get_subclasses_of(self,iri):
        text="""
        prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        prefix UBERON: <http://purl.obolibrary.org/obo/UBERON_>
        prefix CL: <http://purl.obolibrary.org/obo/CL_>
        prefix GO: <http://purl.obolibrary.org/obo/GO_>
        prefix CHEBI: <http://purl.obolibrary.org/obo/CHEBI_>
        prefix MONDO: <http://purl.obolibrary.org/obo/MONDO_>
        prefix HP: <http://purl.obolibrary.org/obo/HP_>
        select distinct ?descendent ?descendentLabel
        from <http://reasoner.renci.org/ontology>
        where {
            graph <http://reasoner.renci.org/ontology/closure> {
                ?descendent rdfs:subClassOf $sourcedefclass .
            }
            OPTIONAL {
                ?descendent rdfs:label ?descendentLabel .
            }
        }
        """
        rr = self.triplestore.query_template(
            inputs  = { 'sourcedefclass': iri  }, \
            outputs = [ 'descendent', 'descendentLabel' ], \
            template_text = text \
        )
        results = []
        for x in rr:
            y = {}
            y['descendent'] = Text.opt_to_curie(x['descendent'])
            y['descendentLabel'] = x['descendentLabel']
            results.append(y)
        return results


    def get_subclasses_and_xrefs(self,iri):
        text="""
        prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        prefix UBERON: <http://purl.obolibrary.org/obo/UBERON_>
        prefix CL: <http://purl.obolibrary.org/obo/CL_>
        prefix GO: <http://purl.obolibrary.org/obo/GO_>
        prefix CHEBI: <http://purl.obolibrary.org/obo/CHEBI_>
        prefix MONDO: <http://purl.obolibrary.org/obo/MONDO_>
        prefix HP: <http://purl.obolibrary.org/obo/HP_>
        select distinct ?descendent ?descendentLabel ?xref 
        from <http://reasoner.renci.org/nonredundant>
        from <http://reasoner.renci.org/ontology>
        where {
          graph <http://reasoner.renci.org/ontology/closure> {
                ?descendent rdfs:subClassOf $sourcedefclass .
          }  
          OPTIONAL {
            ?descendent rdfs:label ?descendentLabel .
          }
          OPTIONAL {
            ?descendent <http://www.geneontology.org/formats/oboInOwl#hasDbXref> ?xref .
          }
        }
        """
        resultmap = self.triplestore.query_template(
            inputs  = { 'sourcedefclass': iri  }, \
            outputs = [ 'descendent', 'descendentLabel', 'xref' ], \
            template_text = text \
        )
        results = defaultdict(list)
        for row in resultmap:
            if row['xref'] is None:
                results[(Text.opt_to_curie(row['descendent']),row['descendentLabel'])]=[]
            else:
                results[ (Text.opt_to_curie(row['descendent']),row['descendentLabel'])].\
                    append( (Text.opt_to_curie(row['xref']) ))
        return results

    def get_subclasses_and_exacts(self,iri):
        text=lambda predicate: f"""
        prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        prefix UBERON: <http://purl.obolibrary.org/obo/UBERON_>
        prefix CL: <http://purl.obolibrary.org/obo/CL_>
        prefix GO: <http://purl.obolibrary.org/obo/GO_>
        prefix CHEBI: <http://purl.obolibrary.org/obo/CHEBI_>
        prefix MONDO: <http://purl.obolibrary.org/obo/MONDO_>
        prefix HP: <http://purl.obolibrary.org/obo/HP_>
        PREFIX EXACT_MATCH: <http://www.w3.org/2004/02/skos/core#exactMatch>
        PREFIX EQUIVALENT_CLASS: <http://www.w3.org/2002/07/owl#equivalentClass>
        PREFIX ID: <http://www.geneontology.org/formats/oboInOwl#id>
        SELECT DISTINCT ?descendent ?descendentLabel ?match ?match_id
        FROM <http://reasoner.renci.org/ontology>
        WHERE {{
            graph <http://reasoner.renci.org/ontology/closure> {{
                ?descendent rdfs:subClassOf $identifier .
            }}
            ?descendent {predicate} ?match.      
            OPTIONAL {{
                ?match ID: ?match_id.
            }} 
            OPTIONAL {{
                ?descendent rdfs:label ?descendentLabel
            }}
            FILTER (!isBlank(?match)) #This sometimes returns blank nodes         
        }}
        """
        resultmap = self.triplestore.query_template(
               template_text=text('EXACT_MATCH:'),
               inputs={
                   'identifier': iri
               }, outputs=[ 'descendent', 'descendentLabel', 'match', 'match_id' ] )
        resultmap += self.triplestore.query_template(
                template_text=text('EQUIVALENT_CLASS:'),
                inputs={
                    'identifier': iri
                }, outputs=[ 'descendent', 'descendentLabel', 'match', 'match_id' ] )
        results = defaultdict(list)
        for row in resultmap:
            if row['match'] is None:
                results[(Text.opt_to_curie(row['descendent']),row['descendentLabel'])]=[]
            else:
                results[ (Text.opt_to_curie(row['descendent']),row['descendentLabel'])].\
                    append( (Text.opt_to_curie(row['match']) ))
        return results

