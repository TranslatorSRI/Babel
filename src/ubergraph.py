import logging

from src.triplestore import TripleStore
from src.util import Text
from collections import defaultdict
from src.babel_utils import norm

class UberGraph:
    #Some of these get_subclass_and_whatever things can/should be merged...

    # UberGraph stored descriptions with the RDF property IAO:0000115 ("definition")
    RDF_DESCRIPTION_PROPERTY = "http://purl.obolibrary.org/obo/IAO_0000115"

    # When the query needs to be queried in batches -- such as, for example, get_all_labels() -- this
    # constant controls how large each batch should be.
    QUERY_BATCH_SIZE = 200_000

    def __init__(self):
        self.triplestore = TripleStore("https://ubergraph.apps.renci.org/sparql")

    def get_all_labels(self):
        # Since this is a very large query, we do this in chunks.
        query_count = """
                      prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>

                      select (count (distinct *) as ?count)
                      from <http://reasoner.renci.org/ontology>
                      where {
                        ?thing rdfs:label ?label .
                      }
                      """
        rr = self.triplestore.query_template(
            inputs={},
            outputs=['count'],
            template_text=query_count
        )
        if len(rr) == 0:
            raise RuntimeError("get_all_labels() count failed: no counts returned")
        if len(rr) > 1:
            raise RuntimeError("get_all_labels() count failed: too many counts returned")

        total_count = int(rr[0]['count'])

        results = []
        for start in range(0, total_count, UberGraph.QUERY_BATCH_SIZE):
            # end = start + UberGraph.QUERY_BATCH_SIZE if UberGraph.QUERY_BATCH_SIZE < total_count else UberGraph.QUERY_BATCH_SIZE
            print(f"Querying get_all_labels() offset {start} limit {UberGraph.QUERY_BATCH_SIZE} (total count: {total_count})")

            text = """
                   prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                   select distinct ?thing ?label
                   from <http://reasoner.renci.org/ontology>
                   where {
                     ?thing rdfs:label ?label .
                   }
                   order by ?thing ?label
                   """ + f"offset {start} limit {UberGraph.QUERY_BATCH_SIZE}"

            rr = self.triplestore.query_template(
                inputs={}, \
                outputs=['thing', 'label'], \
                template_text=text \
                )
            for x in rr:
                y = {}
                try:
                    y['iri'] = Text.opt_to_curie(x['thing'])
                except ValueError as verr:
                    logging.warning(f"WARNING: Unable to translate {x['thing']} to a CURIE; it will be used as-is: {verr}")
                    y['iri'] = x['thing']
                y['label'] = x['label']
                results.append(y)

        return results


    def get_all_descriptions(self):
        # Since this is a very large query, we do this in chunks.
        query_count = """
                      prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>

                      select (count (distinct *) as ?count)
                      from <http://reasoner.renci.org/ontology>
                      where {
                        ?thing rdfs:label ?label .
                      }
                      """
        rr = self.triplestore.query_template(
            inputs={},
            outputs=['count'],
            template_text=query_count
        )
        if len(rr) == 0:
            raise RuntimeError("get_all_descriptions() count failed: no counts returned")
        if len(rr) > 1:
            raise RuntimeError("get_all_descriptions() count failed: too many counts returned")

        total_count = int(rr[0]['count'])

        results = []
        for start in range(0, total_count, UberGraph.QUERY_BATCH_SIZE):
            # end = start + UberGraph.QUERY_BATCH_SIZE if UberGraph.QUERY_BATCH_SIZE < total_count else UberGraph.QUERY_BATCH_SIZE
            print(f"Querying get_all_descriptions() offset {start} limit {UberGraph.QUERY_BATCH_SIZE} (total count: {total_count})")

            text = """
                   prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                   select distinct ?thing ?description
                   from <http://reasoner.renci.org/ontology>
                   where {
                     ?thing <""" + UberGraph.RDF_DESCRIPTION_PROPERTY + """> ?description .
                   }
                   order by ?thing ?description
                   """ + f"offset {start} limit {UberGraph.QUERY_BATCH_SIZE}"

            rr = self.triplestore.query_template(
                inputs={}, \
                outputs=['thing', 'description'], \
                template_text=text \
                )
            for x in rr:
                y = {}
                try:
                    y['iri'] = Text.opt_to_curie(x['thing'])
                except ValueError as verr:
                    print(f"WARNING: Unable to translate {x['thing']} to a CURIE; it will be used as-is: {verr}")
                    y['iri'] = x['thing']
                y['description'] = x['description']
                results.append(y)

        return results


    def get_all_synonyms(self):
        # Since this is a very large query, we do this in chunks.
        query_count = """
                      prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                      prefix owl: <http://www.w3.org/2002/07/owl#>
                      prefix oboInOwl: <http://www.geneontology.org/formats/oboInOwl#>
                      
                      SELECT (COUNT(DISTINCT ?cls) AS ?count)
                      from <http://reasoner.renci.org/ontology>
                      WHERE
                      {
                        ?cls a owl:Class
                        # FILTER (!isBlank(?cls))
                      }
                      """
        rr = self.triplestore.query_template(
            inputs={},
            outputs=['count'],
            template_text=query_count
        )
        if len(rr) == 0:
            raise RuntimeError("get_all_synonyms() count failed: no counts returned")
        if len(rr) > 1:
            raise RuntimeError("get_all_synonyms() count failed: too many counts returned")

        total_count = int(rr[0]['count'])

        results = []
        for start in range(0, total_count, UberGraph.QUERY_BATCH_SIZE):
            print(f"Querying get_all_synonyms() offset {start} limit {UberGraph.QUERY_BATCH_SIZE} (total count: {total_count})")

            text = """
                    prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                    prefix owl: <http://www.w3.org/2002/07/owl#>
                    prefix oboInOwl: <http://www.geneontology.org/formats/oboInOwl#>
                    SELECT ?cls ?pred ?val
                    from <http://reasoner.renci.org/ontology>
                    WHERE
                    {
                      {
                        SELECT DISTINCT ?cls
                        WHERE {
                          ?cls a owl:Class .
                          # FILTER (!isBlank(?cls))
                        }
                        ORDER BY ?cls
                        """ \
                        + f"OFFSET {start} LIMIT {UberGraph.QUERY_BATCH_SIZE}" \
                        + """
                      }
                      VALUES ?pred {
                        oboInOwl:hasRelatedSynonym
                        oboInOwl:hasNarrowSynonym
                        oboInOwl:hasBroadSynonym
                        oboInOwl:hasExactSynonym
                      }
                      ?cls ?pred ?val
                    }
                    """
            rr = self.triplestore.query_template(
                inputs={}, \
                outputs=['cls', 'pred', 'val'], \
                template_text=text \
                )
            for x in rr:
                try:
                    cls_curie = Text.opt_to_curie(x['cls'])
                except ValueError as verr:
                    print(f"Unable to convert {x['cls']} to a CURIE; it will be used as-is: {verr}")
                    cls_curie = x['cls']
                y = ( cls_curie, x['pred'], x['val'])
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
            graph <http://reasoner.renci.org/redundant> {
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
            try:
                y['descendent'] = Text.opt_to_curie(x['descendent'])
            except ValueError as verr:
                print(f"Descendent {x['descendent']} could not be converted to a CURIE, will be used as-is: {verr}")
                y['descendent'] = x['descendent']
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
            graph <http://reasoner.renci.org/redundant> {
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
            try:
                y['descendent'] = Text.opt_to_curie(x['descendent'])
            except ValueError as verr:
                print(f"Descendent {x['descendent']} could not be converted to a CURIE, will be used as-is: {verr}")
                y['descendent'] = x['descendent']
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
          graph <http://reasoner.renci.org/redundant> {
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
            # Sometimes we're getting back just strings that aren't curies, skip those (but complain)
            try:
                dcurie = Text.opt_to_curie(row['descendent'])
                results[ dcurie ].add( (Text.opt_to_curie(row['xref']) ))
            except ValueError as verr:
                print(f'Bad XREF from {row["descendent"]} to {row["xref"]}: {verr}')
                continue

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
            graph <http://reasoner.renci.org/redundant> {{
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
            try:
                desc = Text.opt_to_curie(row['descendent'])
            except ValueError as verr:
                print(f"Descendant {row['descendent']} could not be converted to a CURIE, will be used as-is: {verr}")
                desc = row['descendent']

            if row['match'] is None:
                results[desc] += []
            else:
                # Sometimes, if there are no exact_matches, we'll get some kind of blank node id
                # like 't19830198'. Want to filter those out.
                try:
                    results[ desc ].append(Text.opt_to_curie(row['match']))
                except ValueError as verr:
                    print(f'Row {row} could not be converted to a CURIE: {verr}')
                    continue

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
            graph <http://reasoner.renci.org/redundant> {{
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
            try:
                desc = Text.opt_to_curie(row['descendent'])
            except ValueError as verr:
                print(f"Descendant {row['descendent']} could not be converted to a CURIE, will be used as-is: {verr}")
                desc = row['descendent']

            if row['match'] is None:
                results[desc] += []
            else:
                try:
                    results[ desc].append( (Text.opt_to_curie(row['match']) ))
                except ValueError as verr:
                    # Sometimes, if there are no exact_matches, we'll get some kind of blank node id
                    # like 't19830198'. Want to filter those out.
                    print(f"Value {row['match']} in row {row} could not be converted to a CURIE: {verr}")
                    continue

        return results

    def write_normalized_information_content(self, filename):
        """
        Download the normalized information content and write it to the specified filename.

        :param filename: The filename to write the normalized information content to -- we write them as `IRI\tNIC`.
        :return: The number of normalized information content entries downloaded.
        """
        count_query = "SELECT (COUNT(*) AS ?count) WHERE { ?iri <http://reasoner.renci.org/vocab/normalizedInformationContent> ?nic }"
        count_result = self.triplestore.query(count_query, ['count'])
        total_count = int(count_result[0]['count'])

        assert total_count > 0

        write_count = 0
        with open(filename, "w") as ftsv:
            for start in range(0, total_count, UberGraph.QUERY_BATCH_SIZE):
                print(f"Querying write_normalized_information_content() offset {start} limit {UberGraph.QUERY_BATCH_SIZE} (total count: {total_count})")

                query = "SELECT ?iri ?nic WHERE " \
                        "{ ?iri <http://reasoner.renci.org/vocab/normalizedInformationContent> ?nic }" \
                        f"ORDER BY ASC(?iri) OFFSET {start} LIMIT {UberGraph.QUERY_BATCH_SIZE}"
                results = self.triplestore.query(query, ['iri', 'nic'])

                for row in results:
                    ftsv.write(f"{row['iri']}\t{row['nic']}\n")
                    write_count += 1

        print(f"Wrote {write_count} information content values into {filename}.")
        return write_count

def build_sets(iri, concordfiles, set_type, ignore_list = [], other_prefixes={}, hop_ontologies=False ):
    """Given an IRI create a list of sets.  Each set is a set of equivalent LabeledIDs, and there
    is a set for each subclass of the input iri.  Write these lists to concord files, indexed by the prefix"""
    prefix = Text.get_prefix_or_none(iri)
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
            subclass_prefix = Text.get_prefix_or_none(k)
            if subclass_prefix != prefix:
                continue
        v = set([ norm(x,other_prefixes) for x in v ])
        for x in v:
            if Text.get_prefix_or_none(x) not in ignore_list:
                p = Text.get_prefix_or_none(k)
                if p in concordfiles:
                    concordfiles[p].write(f'{k}\t{types2relations[set_type]}\t{x}\n')


if __name__ == '__main__':
    ug = UberGraph()
    ug.get_all_labels()
