from src.triplestore import TripleStore
from src.util import Text
from collections import defaultdict
from src.babel_utils import norm

class UberGraph:
    #Some of these get_subclass_and_whatever things can/should be merged...

    def __init__(self):
        self.triplestore = TripleStore("https://ubergraph.apps.renci.org/sparql")

    def get_all_labels(self):
        text = """
                prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                prefix UBERON: <http://purl.obolibrary.org/obo/UBERON_>
                prefix CL: <http://purl.obolibrary.org/obo/CL_>
                prefix GO: <http://purl.obolibrary.org/obo/GO_>
                prefix CHEBI: <http://purl.obolibrary.org/obo/CHEBI_>
                prefix MONDO: <http://purl.obolibrary.org/obo/MONDO_>
                prefix HP: <http://purl.obolibrary.org/obo/HP_>
                prefix NCIT: <http://purl.obolibrary.org/obo/NCIT_>
                select distinct ?thing ?label
                from <http://reasoner.renci.org/ontology>
                where {
                    ?thing rdfs:label ?label .
                }
                """
        rr = self.triplestore.query_template(
            inputs={}, \
            outputs=['thing', 'label'], \
            template_text=text \
            )
        results = []
        for x in rr:
            y = {}
            y['iri'] = Text.opt_to_curie(x['thing'])
            y['label'] = x['label']
            results.append(y)
        return results

    def get_all_synonyms(self):
        text = """
                prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                prefix owl: <http://www.w3.org/2002/07/owl#>
                prefix oboInOwl: <http://www.geneontology.org/formats/oboInOwl#>
                prefix UBERON: <http://purl.obolibrary.org/obo/UBERON_>
                prefix CL: <http://purl.obolibrary.org/obo/CL_>
                prefix GO: <http://purl.obolibrary.org/obo/GO_>
                prefix CHEBI: <http://purl.obolibrary.org/obo/CHEBI_>
                prefix MONDO: <http://purl.obolibrary.org/obo/MONDO_>
                prefix HP: <http://purl.obolibrary.org/obo/HP_>
                prefix NCIT: <http://purl.obolibrary.org/obo/NCIT_>
                SELECT ?cls ?pred ?val
                from <http://reasoner.renci.org/ontology>
                WHERE
                { ?cls ?pred ?val ;
                    a owl:Class .
                    FILTER (
                    ?pred = oboInOwl:hasRelatedSynonym ||
                    ?pred = oboInOwl:hasNarrowSynonym ||
                    ?pred = oboInOwl:hasBroadSynonym ||
                    ?pred = oboInOwl:hasExactSynonym
                    )
                }
                """
        rr = self.triplestore.query_template(
            inputs={}, \
            outputs=['cls', 'pred', 'val'], \
            template_text=text \
            )
        results = []
        for x in rr:
            y = ( Text.opt_to_curie(x['cls']), x['pred'], x['val'])
            results.append(y)
        return results

    def get_subclasses_of(self,iri):
        text="""
        prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        prefix UBERON: <http://purl.obolibrary.org/obo/UBERON_>
        prefix CL: <http://purl.obolibrary.org/obo/CL_>
        prefix GO: <http://purl.obolibrary.org/obo/GO_>
        prefix CHEBI: <http://purl.obolibrary.org/obo/CHEBI_>
        prefix MONDO: <http://purl.obolibrary.org/obo/MONDO_>
        prefix HP: <http://purl.obolibrary.org/obo/HP_>
        prefix NCIT: <http://purl.obolibrary.org/obo/NCIT_>
        prefix PR: <http://purl.obolibrary.org/obo/PR_>
        prefix EFO: <http://www.ebi.ac.uk/efo/EFO_>
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

    def get_subclasses_and_smiles(self,iri):
        text="""
        prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        prefix UBERON: <http://purl.obolibrary.org/obo/UBERON_>
        prefix CL: <http://purl.obolibrary.org/obo/CL_>
        prefix GO: <http://purl.obolibrary.org/obo/GO_>
        prefix CHEBI: <http://purl.obolibrary.org/obo/CHEBI_>
        prefix CHEBIP: <http://purl.obolibrary.org/obo/chebi/>
        prefix MONDO: <http://purl.obolibrary.org/obo/MONDO_>
        prefix HP: <http://purl.obolibrary.org/obo/HP_>
        prefix NCIT: <http://purl.obolibrary.org/obo/NCIT_>
        prefix PR: <http://purl.obolibrary.org/obo/PR_>
        prefix EFO: <http://www.ebi.ac.uk/efo/EFO_>
        select distinct ?descendent ?descendentSmiles
        from <http://reasoner.renci.org/ontology>
        where {
            graph <http://reasoner.renci.org/ontology/closure> {
                ?descendent rdfs:subClassOf $sourcedefclass .
            }
            OPTIONAL {
                ?descendent CHEBIP:smiles ?descendentSmiles .
            }
        }
        """
        rr = self.triplestore.query_template(
            inputs  = { 'sourcedefclass': iri  }, \
            outputs = [ 'descendent', 'descendentSmiles' ], \
            template_text = text \
        )
        results = []
        for x in rr:
            y = {}
            y['descendent'] = Text.opt_to_curie(x['descendent'])
            if x['descendentSmiles'] is not None:
                y['SMILES'] = x['descendentSmiles']
            results.append(y)
        return results


    def get_subclasses_and_xrefs(self,iri):
        """Return all subclasses of iri that have an xref as well as the xref.
        Does not return subclasses that lack an xref."""
        text="""
        prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        prefix UBERON: <http://purl.obolibrary.org/obo/UBERON_>
        prefix CL: <http://purl.obolibrary.org/obo/CL_>
        prefix GO: <http://purl.obolibrary.org/obo/GO_>
        prefix CHEBI: <http://purl.obolibrary.org/obo/CHEBI_>
        prefix MONDO: <http://purl.obolibrary.org/obo/MONDO_>
        prefix HP: <http://purl.obolibrary.org/obo/HP_>
        prefix NCIT: <http://purl.obolibrary.org/obo/NCIT_>
        prefix PR: <http://purl.obolibrary.org/obo/PR_>
        select distinct ?descendent ?xref
        from <http://reasoner.renci.org/nonredundant>
        from <http://reasoner.renci.org/ontology>
        where {
          graph <http://reasoner.renci.org/ontology/closure> {
                ?descendent rdfs:subClassOf $sourcedefclass .
          }
          ?descendent <http://www.geneontology.org/formats/oboInOwl#hasDbXref> ?xref .
        }
        """
        resultmap = self.triplestore.query_template(
            inputs  = { 'sourcedefclass': iri  }, \
            outputs = [ 'descendent', 'xref' ], \
            template_text = text \
        )
        results = defaultdict(set)
        for row in resultmap:
            dcurie = Text.opt_to_curie(row['descendent'])
            #Sometimes we're getting back just strings that aren't curies, skip those (but complain)
            if ':' not in row['xref']:
                print(f'Bad XREF from {row["descendent"]} to {row["xref"]}')
                continue
            results[ dcurie ].add( (Text.opt_to_curie(row['xref']) ))
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
        prefix EFO: <http://www.ebi.ac.uk/efo/EFO_>
        prefix NCIT: <http://purl.obolibrary.org/obo/NCIT_>
        PREFIX EXACT_MATCH: <http://www.w3.org/2004/02/skos/core#exactMatch>
        PREFIX M_EXACT_MATCH: <http://purl.obolibrary.org/obo/mondo#exactMatch>
        PREFIX EQUIVALENT_CLASS: <http://www.w3.org/2002/07/owl#equivalentClass>
        PREFIX ID: <http://www.geneontology.org/formats/oboInOwl#id>
        SELECT DISTINCT ?descendent ?match
        FROM <http://reasoner.renci.org/ontology>
        WHERE {{
            graph <http://reasoner.renci.org/ontology/closure> {{
                ?descendent rdfs:subClassOf $identifier .
            }}
            OPTIONAL {{
                ?descendent {predicate} ?match.
            }}
        }}
        """
        resultmap = self.triplestore.query_template(
               template_text=text('EXACT_MATCH:'),
               inputs={
                   'identifier': iri
               }, outputs=[ 'descendent', 'match' ] )
        resultmap += self.triplestore.query_template(
               template_text=text('M_EXACT_MATCH:'),
               inputs={
                   'identifier': iri
               }, outputs=[ 'descendent', 'match' ] )
        resultmap += self.triplestore.query_template(
                template_text=text('EQUIVALENT_CLASS:'),
                inputs={
                    'identifier': iri
                }, outputs=[ 'descendent', 'match'] )
        results = defaultdict(list)
        for row in resultmap:
            desc=Text.opt_to_curie(row['descendent'])
            if row['match'] is None:
                results[desc] += []
            else:
                results[ desc ].append( (Text.opt_to_curie(row['match']) ))
        #Sometimes, if there are no exact_matches, we'll get some kind of blank node id
        # like 't19830198'. Want to filter those out.
        for k,v in results.items():
            results[k] = list(filter(lambda x: ':' in x, v))
        return results

    def get_subclasses_and_close(self,iri):
        text=lambda predicate: f"""
        prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        prefix UBERON: <http://purl.obolibrary.org/obo/UBERON_>
        prefix CL: <http://purl.obolibrary.org/obo/CL_>
        prefix GO: <http://purl.obolibrary.org/obo/GO_>
        prefix CHEBI: <http://purl.obolibrary.org/obo/CHEBI_>
        prefix MONDO: <http://purl.obolibrary.org/obo/MONDO_>
        prefix HP: <http://purl.obolibrary.org/obo/HP_>
        prefix EFO: <http://www.ebi.ac.uk/efo/EFO_>
        prefix NCIT: <http://purl.obolibrary.org/obo/NCIT_>
        PREFIX CLOSE_MATCH: <http://www.w3.org/2004/02/skos/core#closeMatch>
        PREFIX M_CLOSE_MATCH: <http://purl.obolibrary.org/obo/mondo#closeMatch>
        PREFIX EQUIVALENT_CLASS: <http://www.w3.org/2002/07/owl#equivalentClass>
        PREFIX ID: <http://www.geneontology.org/formats/oboInOwl#id>
        SELECT DISTINCT ?descendent ?match
        FROM <http://reasoner.renci.org/ontology>
        WHERE {{
            graph <http://reasoner.renci.org/ontology/closure> {{
                ?descendent rdfs:subClassOf $identifier .
            }}
            OPTIONAL {{
                ?descendent {predicate} ?match.
            }}
        }}
        """
        resultmap = self.triplestore.query_template(
               template_text=text('CLOSE_MATCH:'),
               inputs={
                   'identifier': iri
               }, outputs=[ 'descendent',  'match' ] )
        resultmap += self.triplestore.query_template(
               template_text=text('M_CLOSE_MATCH:'),
               inputs={
                   'identifier': iri
               }, outputs=[ 'descendent', 'match' ] )
        results = defaultdict(list)
        for row in resultmap:
            desc = Text.opt_to_curie(row['descendent'])
            if row['match'] is None:
                results[desc] += []
            else:
                results[ desc].append( (Text.opt_to_curie(row['match']) ))
        #Sometimes, if there are no exact_matches, we'll get some kind of blank node id
        # like 't19830198'. Want to filter those out.
        for k,v in results.items():
            results[k] = list(filter(lambda x: ':' in x, v))
        return results


def build_sets(iri, concordfiles, set_type, ignore_list = [], other_prefixes={}, hop_ontologies=False ):
    """Given an IRI create a list of sets.  Each set is a set of equivalent LabeledIDs, and there
    is a set for each subclass of the input iri.  Write these lists to concord files, indexed by the prefix"""
    prefix = Text.get_curie(iri)
    types2relations={'xref':'xref', 'exact': 'oio:exactMatch', 'close': 'oio:closeMatch'}
    if set_type not in types2relations:
        return
    uber = UberGraph()
    if set_type == 'xref':
        uberres = uber.get_subclasses_and_xrefs(iri)
    elif set_type == 'exact':
        uberres = uber.get_subclasses_and_exacts(iri)
    elif set_type == 'close':
        uberres = uber.get_subclasses_and_close(iri)
    for k,v in uberres.items():
        if not hop_ontologies:
            subclass_prefix = Text.get_curie(k)
            if subclass_prefix != prefix:
                continue
        v = set([ norm(x,other_prefixes) for x in v ])
        for x in v:
            if Text.get_curie(x) not in ignore_list:
                p = Text.get_curie(k)
                if p in concordfiles:
                    concordfiles[p].write(f'{k}\t{types2relations[set_type]}\t{x}\n')


