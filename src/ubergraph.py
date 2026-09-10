from collections import defaultdict
from time import sleep

from tqdm import tqdm

from src.babel_utils import norm
from src.triplestore import TripleStore
from src.util import Text, get_logger

SLEEP_BETWEEN_UBERGRAPH_QUERIES = 5  # seconds


# Hierarchy predicates for the get_subclasses_* query family (see the note in
# UberGraph.get_subclasses_of). subClassOf suits is_a ontologies such as UBERON, GO and
# CL; part_of suits partonomy ontologies whose structure is meronymic rather than
# taxonomic, such as EMAPA. Values are full IRIs so they drop straight into a query.
#
# These are interpolated into the SPARQL text verbatim, not bound as query parameters
# (query_template() is a string.Template.safe_substitute), so a hierarchy_predicate argument must
# only ever be one of these constants -- anything else is arbitrary SPARQL injected into the query
# body. Never pass a value derived from source data, a config file, or any other caller-supplied
# string. This is checked at every interpolation site, not merely documented; see
# _assert_known_hierarchy_predicate().
HIERARCHY_SUBCLASS_OF = "<http://www.w3.org/2000/01/rdf-schema#subClassOf>"
HIERARCHY_PART_OF = "<http://purl.obolibrary.org/obo/BFO_0000050>"

HIERARCHY_PREDICATES = frozenset({HIERARCHY_SUBCLASS_OF, HIERARCHY_PART_OF})


def _assert_known_hierarchy_predicate(hierarchy_predicate):
    """Reject any hierarchy predicate that is not one of the module's constants.

    The value reaches the SPARQL text through verbatim substitution, so an unvetted string is an
    injection vector rather than merely a wrong query. Raising here keeps that guarantee at the
    point of use: adding a new traversal predicate means adding a constant, which is a reviewable
    change, rather than passing a literal from a caller.
    """
    if hierarchy_predicate not in HIERARCHY_PREDICATES:
        raise ValueError(
            f"hierarchy_predicate must be one of the HIERARCHY_* constants in src/ubergraph.py, "
            f"got {hierarchy_predicate!r}. It is interpolated into the SPARQL query verbatim, so "
            f"an arbitrary string would be injected into the query body."
        )


class UberGraph:
    # Some of these get_subclass_and_whatever things can/should be merged...

    # UberGraph stored descriptions with the RDF property IAO:0000115 ("definition")
    RDF_DESCRIPTION_PROPERTY = "http://purl.obolibrary.org/obo/IAO_0000115"

    # When the query needs to be queried in batches -- such as, for example, get_all_labels() -- this
    # constant controls how large each batch should be.
    QUERY_BATCH_SIZE = 200_000

    def __init__(self, sparql_url="https://ubergraph.apps.renci.org/sparql"):
        """
        Set up an UberGraph querier.

        TODO: it would be great to read this from the config, but that would require a whole bunch of changes.

        :param sparql_url: The SPARQL endpoint to use.
        """
        self.sparql_url = sparql_url
        self.triplestore = TripleStore(self.sparql_url)
        self.logger = get_logger(str(self))

    def __str__(self):
        return f"UberGraph({self.sparql_url})"

    @staticmethod
    def is_blank_node(curie):
        """
        Test if the given CURIE is a blank node in UberGraph.

        :param curie: A CURIE to check. Must be a string.
        :return: True if this looks like a blank node, false otherwise.
        """

        if not isinstance(curie, str):
            raise ValueError(f"UberGraph.is_blank_node(curie={curie}): curie must be a string.")

        # For Ubergraph, blank nodes are in the form 't27502167'. If we try to convert that into a CURIE, we can ignore it.
        if curie[0] == "t" and curie[1:].isdigit():
            return True
        return False

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
        rr = self.triplestore.query_template(inputs={}, outputs=["count"], template_text=query_count)
        if len(rr) == 0:
            raise RuntimeError("get_all_labels() count failed: no counts returned")
        if len(rr) > 1:
            raise RuntimeError("get_all_labels() count failed: too many counts returned")

        total_count = int(rr[0]["count"])

        results = []
        for start in tqdm(
            range(0, total_count, UberGraph.QUERY_BATCH_SIZE), desc=f"{self}.get_all_labels()", unit="batch"
        ):
            sleep(SLEEP_BETWEEN_UBERGRAPH_QUERIES)

            # end = start + UberGraph.QUERY_BATCH_SIZE if UberGraph.QUERY_BATCH_SIZE < total_count else UberGraph.QUERY_BATCH_SIZE
            self.logger.debug(
                f"Querying get_all_labels() offset {start} limit {UberGraph.QUERY_BATCH_SIZE} (total count: {total_count})"
            )

            text = (
                """
                   prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                   select distinct ?thing ?label
                   from <http://reasoner.renci.org/ontology>
                   where {
                     ?thing rdfs:label ?label .
                   }
                   order by ?thing ?label
                   """
                + f"offset {start} limit {UberGraph.QUERY_BATCH_SIZE}"
            )

            rr = self.triplestore.query_template(inputs={}, outputs=["thing", "label"], template_text=text)
            for x in rr:
                y = {}
                try:
                    y["iri"] = Text.opt_to_curie(x["thing"])
                except ValueError as verr:
                    if not UberGraph.is_blank_node(x["thing"]):
                        self.logger.warning(
                            f"Unable to translate {x['thing']} to a CURIE; it will be used as-is: {verr}"
                        )
                    y["iri"] = x["thing"]
                y["label"] = x["label"]
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
        rr = self.triplestore.query_template(inputs={}, outputs=["count"], template_text=query_count)
        if len(rr) == 0:
            raise RuntimeError("get_all_descriptions() count failed: no counts returned")
        if len(rr) > 1:
            raise RuntimeError("get_all_descriptions() count failed: too many counts returned")

        total_count = int(rr[0]["count"])

        results = []
        for start in tqdm(
            range(0, total_count, UberGraph.QUERY_BATCH_SIZE), desc=f"{self}.get_all_descriptions()", unit="batch"
        ):
            sleep(SLEEP_BETWEEN_UBERGRAPH_QUERIES)

            # end = start + UberGraph.QUERY_BATCH_SIZE if UberGraph.QUERY_BATCH_SIZE < total_count else UberGraph.QUERY_BATCH_SIZE
            self.logger.debug(
                f"Querying get_all_descriptions() offset {start} limit {UberGraph.QUERY_BATCH_SIZE} (total count: {total_count})"
            )

            text = (
                """
                   prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                   select distinct ?thing ?description
                   from <http://reasoner.renci.org/ontology>
                   where {
                     ?thing <"""
                + UberGraph.RDF_DESCRIPTION_PROPERTY
                + """> ?description .
                   }
                   order by ?thing ?description
                   """
                + f"offset {start} limit {UberGraph.QUERY_BATCH_SIZE}"
            )

            rr = self.triplestore.query_template(inputs={}, outputs=["thing", "description"], template_text=text)
            for x in rr:
                y = {}
                try:
                    y["iri"] = Text.opt_to_curie(x["thing"])
                except ValueError as verr:
                    if not UberGraph.is_blank_node(x["thing"]):
                        self.logger.warning(
                            f"Unable to translate {x['thing']} to a CURIE; it will be used as-is: {verr}"
                        )
                    y["iri"] = x["thing"]
                y["description"] = x["description"]
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
        rr = self.triplestore.query_template(inputs={}, outputs=["count"], template_text=query_count)
        if len(rr) == 0:
            raise RuntimeError("get_all_synonyms() count failed: no counts returned")
        if len(rr) > 1:
            raise RuntimeError("get_all_synonyms() count failed: too many counts returned")

        total_count = int(rr[0]["count"])

        results = []
        for start in tqdm(range(0, total_count, UberGraph.QUERY_BATCH_SIZE), desc=f"{self}.get_all_synonyms()"):
            sleep(SLEEP_BETWEEN_UBERGRAPH_QUERIES)
            self.logger.debug(
                f"Querying get_all_synonyms() offset {start} limit {UberGraph.QUERY_BATCH_SIZE} (total count: {total_count})"
            )

            text = (
                """
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
                        """
                + f"OFFSET {start} LIMIT {UberGraph.QUERY_BATCH_SIZE}"
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
            )
            rr = self.triplestore.query_template(inputs={}, outputs=["cls", "pred", "val"], template_text=text)
            for x in rr:
                try:
                    cls_curie = Text.opt_to_curie(x["cls"])
                except ValueError as verr:
                    self.logger.warning(f"Unable to convert {x['cls']} to a CURIE; it will be used as-is: {verr}")
                    cls_curie = x["cls"]
                y = (cls_curie, x["pred"], x["val"])
                results.append(y)

        return results

    def get_subclasses_of(self, iri, hierarchy_predicate=HIERARCHY_SUBCLASS_OF):
        """Return everything below `iri` in a hierarchy, each with its label.

        Despite the name, the traversal predicate is configurable. It defaults to
        rdfs:subClassOf — the is_a hierarchy of ontologies like UBERON, GO and CL — but
        can be set to HIERARCHY_PART_OF for partonomy ontologies such as EMAPA, whose
        anatomy terms are linked by part_of rather than subClassOf.

        A strictly accurate name would be get_descendants_of(), but the get_subclasses_*
        family is called from many places and for nearly every caller subClassOf *is*
        the hierarchy; renaming would be churn that makes the common case read less
        naturally for the sake of the rare one. Keeping the name and surfacing the
        exception through an explicit hierarchy_predicate argument leaves the common
        call sites unchanged and makes the non-default ones self-documenting.
        """
        _assert_known_hierarchy_predicate(hierarchy_predicate)
        text = """
        prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        prefix UBERON: <http://purl.obolibrary.org/obo/UBERON_>
        prefix CL: <http://purl.obolibrary.org/obo/CL_>
        prefix EMAPA: <http://purl.obolibrary.org/obo/EMAPA_>
        prefix GO: <http://purl.obolibrary.org/obo/GO_>
        prefix CHEBI: <http://purl.obolibrary.org/obo/CHEBI_>
        prefix MONDO: <http://purl.obolibrary.org/obo/MONDO_>
        prefix HP: <http://purl.obolibrary.org/obo/HP_>
        prefix MP: <http://purl.obolibrary.org/obo/MP_>
        prefix NCIT: <http://purl.obolibrary.org/obo/NCIT_>
        prefix PR: <http://purl.obolibrary.org/obo/PR_>
        prefix EFO: <http://www.ebi.ac.uk/efo/EFO_>
        select distinct ?descendent ?descendentLabel
        from <http://reasoner.renci.org/ontology>
        where {
            graph <http://reasoner.renci.org/redundant> {
                ?descendent $hierarchy_predicate $sourcedefclass .
            }
            OPTIONAL {
                ?descendent rdfs:label ?descendentLabel .
            }
        }
        """
        rr = self.triplestore.query_template(
            inputs={"sourcedefclass": iri, "hierarchy_predicate": hierarchy_predicate},
            outputs=["descendent", "descendentLabel"],
            template_text=text,
        )
        results = []
        for x in rr:
            y = {}
            try:
                y["descendent"] = Text.opt_to_curie(x["descendent"])
            except ValueError as verr:
                self.logger.warning(
                    f"Descendent {x['descendent']} could not be converted to a CURIE, will be used as-is: {verr}"
                )
                y["descendent"] = x["descendent"]
            y["descendentLabel"] = x["descendentLabel"]
            results.append(y)
        return results

    def get_direct_superclasses_of(self, curies):
        """Return the direct (asserted) superclasses of each CURIE in ``curies``, in one query.

        The counterpart of get_subclasses_of(), and deliberately *direct-only*: it reads the
        non-redundant graph, where UberGraph keeps the asserted rdfs:subClassOf edges, rather than
        the closure in the redundant graph. Written for the DrugBank food-and-extract audit CSVs
        (issue #828), where a reviewer needs to see what sits immediately above an entry's NCIt class
        in order to propose the next nonfood_ncit_roots entry; the full ancestor closure is
        too long to read.

        :param curies: An iterable of CURIEs to look up.
        :return: A dict of {CURIE -> [(superclass CURIE, superclass label), ...]}.
        """
        if not curies:
            # An empty VALUES block is legal SPARQL but the round trip is pointless, and a caller that
            # filtered every CURIE out wants an empty answer, not a query.
            return {}
        values = " ".join(Text.curie_to_obo(curie) for curie in curies)
        text = """
        prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        select distinct ?child ?parent ?parentLabel
        from <http://reasoner.renci.org/ontology>
        where {
            values ?child { $values }
            graph <http://reasoner.renci.org/nonredundant> {
                ?child rdfs:subClassOf ?parent .
            }
            OPTIONAL {
                ?parent rdfs:label ?parentLabel .
            }
        }
        """
        superclasses = defaultdict(list)
        for row in self.triplestore.query_template(
            inputs={"values": values},
            outputs=["child", "parent", "parentLabel"],
            template_text=text,
            post=True,  # The VALUES clause makes this query too long for a GET URI.
        ):
            if self.is_blank_node(str(row["parent"])):
                continue
            superclasses[Text.opt_to_curie(row["child"])].append((Text.opt_to_curie(row["parent"]), row["parentLabel"]))
        return superclasses

    def get_subclasses_and_smiles(self, iri):
        text = """
        prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        prefix UBERON: <http://purl.obolibrary.org/obo/UBERON_>
        prefix CL: <http://purl.obolibrary.org/obo/CL_>
        prefix EMAPA: <http://purl.obolibrary.org/obo/EMAPA_>
        prefix GO: <http://purl.obolibrary.org/obo/GO_>
        prefix CHEBI: <http://purl.obolibrary.org/obo/CHEBI_>
        prefix CHEBIP: <http://purl.obolibrary.org/obo/chebi/>
        prefix MONDO: <http://purl.obolibrary.org/obo/MONDO_>
        prefix HP: <http://purl.obolibrary.org/obo/HP_>
        prefix MP: <http://purl.obolibrary.org/obo/MP_>
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
            inputs={"sourcedefclass": iri}, outputs=["descendent", "descendentSmiles"], template_text=text
        )
        results = []
        for x in rr:
            y = {}
            try:
                y["descendent"] = Text.opt_to_curie(x["descendent"])
            except ValueError as verr:
                self.logger.warning(
                    f"Descendent {x['descendent']} could not be converted to a CURIE, will be used as-is: {verr}"
                )
                y["descendent"] = x["descendent"]
            if x["descendentSmiles"] is not None:
                y["SMILES"] = x["descendentSmiles"]
            results.append(y)
        return results

    def get_roles(self, iri):
        """Return the ChEBI roles asserted for every descendant of `iri` that has one.

        Queries the *non-redundant* graph, so each term carries only its directly asserted roles:
        aspirin returns 13 there against 45 in the redundant graph, which is the same information
        plus every ancestor of each role. Storing the redundant closure would multiply the row count
        several-fold to say nothing new -- a consumer that wants ancestors can walk the role
        hierarchy itself.

        Terms with no role are not returned.

        :param iri: The root whose descendants to collect roles for, as a CURIE (e.g. "CHEBI:24431").
        :return: A list of (term CURIE, role CURIE) pairs.
        """
        text = """
        prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        prefix RO: <http://purl.obolibrary.org/obo/RO_>
        prefix CHEBI: <http://purl.obolibrary.org/obo/CHEBI_>
        select distinct ?term ?role
        from <http://reasoner.renci.org/ontology>
        where {
            graph <http://reasoner.renci.org/redundant> {
                ?term rdfs:subClassOf $sourcedefclass .
            }
            graph <http://reasoner.renci.org/nonredundant> {
                ?term RO:0000087 ?role .
            }
        }
        """
        rr = self.triplestore.query_template(
            inputs={"sourcedefclass": iri}, outputs=["term", "role"], template_text=text
        )
        results = []
        for x in rr:
            try:
                term = Text.opt_to_curie(x["term"])
                role = Text.opt_to_curie(x["role"])
            except ValueError as verr:
                self.logger.warning(f"Skipping role assertion {x} that could not be converted to CURIEs: {verr}")
                continue
            results.append((term, role))
        return results

    def get_subclasses_and_xrefs(self, iri, hierarchy_predicate=HIERARCHY_SUBCLASS_OF):
        """Return every term below `iri` in a hierarchy that has an xref, with its xrefs.
        Terms with no xref are not returned.

        As with get_subclasses_of(), the traversal predicate is configurable and the
        "subclasses" in the name is the common case rather than a constraint: pass
        HIERARCHY_PART_OF for partonomy ontologies such as EMAPA. See get_subclasses_of()
        for why the get_subclasses_* family keeps this name rather than being renamed to
        get_descendants_*."""
        _assert_known_hierarchy_predicate(hierarchy_predicate)
        text = """
        prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        prefix UBERON: <http://purl.obolibrary.org/obo/UBERON_>
        prefix CL: <http://purl.obolibrary.org/obo/CL_>
        prefix EMAPA: <http://purl.obolibrary.org/obo/EMAPA_>
        prefix GO: <http://purl.obolibrary.org/obo/GO_>
        prefix CHEBI: <http://purl.obolibrary.org/obo/CHEBI_>
        prefix MONDO: <http://purl.obolibrary.org/obo/MONDO_>
        prefix HP: <http://purl.obolibrary.org/obo/HP_>
        prefix MP: <http://purl.obolibrary.org/obo/MP_>
        prefix NCIT: <http://purl.obolibrary.org/obo/NCIT_>
        prefix PR: <http://purl.obolibrary.org/obo/PR_>
        select distinct ?descendent ?xref
        from <http://reasoner.renci.org/nonredundant>
        from <http://reasoner.renci.org/ontology>
        where {
          graph <http://reasoner.renci.org/redundant> {
                ?descendent $hierarchy_predicate $sourcedefclass .
          }
          ?descendent <http://www.geneontology.org/formats/oboInOwl#hasDbXref> ?xref .
        }
        """
        resultmap = self.triplestore.query_template(
            inputs={"sourcedefclass": iri, "hierarchy_predicate": hierarchy_predicate},
            outputs=["descendent", "xref"],
            template_text=text,
        )
        results = defaultdict(set)
        for row in resultmap:
            # Sometimes we're getting back just strings that aren't curies, skip those (but complain)
            try:
                dcurie = Text.opt_to_curie(row["descendent"])
                results[dcurie].add(Text.opt_to_curie(row["xref"]))
            except ValueError as verr:
                self.logger.warning(f"Bad XREF from {row['descendent']} to {row['xref']}: {verr}")
                continue

        return results

    def get_subclasses_and_exacts(self, iri):
        def text(predicate):
            return f"""
                prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                prefix UBERON: <http://purl.obolibrary.org/obo/UBERON_>
                prefix CL: <http://purl.obolibrary.org/obo/CL_>
                prefix EMAPA: <http://purl.obolibrary.org/obo/EMAPA_>
                prefix GO: <http://purl.obolibrary.org/obo/GO_>
                prefix CHEBI: <http://purl.obolibrary.org/obo/CHEBI_>
                prefix MONDO: <http://purl.obolibrary.org/obo/MONDO_>
                prefix HP: <http://purl.obolibrary.org/obo/HP_>
                prefix MP: <http://purl.obolibrary.org/obo/MP_>
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
            template_text=text("EXACT_MATCH:"), inputs={"identifier": iri}, outputs=["descendent", "match"]
        )
        resultmap += self.triplestore.query_template(
            template_text=text("M_EXACT_MATCH:"), inputs={"identifier": iri}, outputs=["descendent", "match"]
        )
        resultmap += self.triplestore.query_template(
            template_text=text("EQUIVALENT_CLASS:"), inputs={"identifier": iri}, outputs=["descendent", "match"]
        )
        results = defaultdict(list)
        for row in resultmap:
            try:
                desc = Text.opt_to_curie(row["descendent"])
            except ValueError as verr:
                self.logger.warning(
                    f"Descendant {row['descendent']} could not be converted to a CURIE, will be used as-is: {verr}"
                )
                desc = row["descendent"]

            if row["match"] is None:
                results[desc] += []
            else:
                # Sometimes, if there are no exact_matches, we'll get some kind of blank node id
                # like 't19830198'. Want to filter those out.
                try:
                    results[desc].append(Text.opt_to_curie(row["match"]))
                except ValueError as verr:
                    self.logger.warning(f"Row {row} could not be converted to a CURIE: {verr}")
                    continue

        return results

    def get_subclasses_and_close(self, iri):
        def text(predicate):
            return f"""
                prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                prefix UBERON: <http://purl.obolibrary.org/obo/UBERON_>
                prefix CL: <http://purl.obolibrary.org/obo/CL_>
                prefix EMAPA: <http://purl.obolibrary.org/obo/EMAPA_>
                prefix GO: <http://purl.obolibrary.org/obo/GO_>
                prefix CHEBI: <http://purl.obolibrary.org/obo/CHEBI_>
                prefix MONDO: <http://purl.obolibrary.org/obo/MONDO_>
                prefix HP: <http://purl.obolibrary.org/obo/HP_>
                prefix MP: <http://purl.obolibrary.org/obo/MP_>
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
            template_text=text("CLOSE_MATCH:"), inputs={"identifier": iri}, outputs=["descendent", "match"]
        )
        resultmap += self.triplestore.query_template(
            template_text=text("M_CLOSE_MATCH:"), inputs={"identifier": iri}, outputs=["descendent", "match"]
        )
        results = defaultdict(list)
        for row in resultmap:
            try:
                desc = Text.opt_to_curie(row["descendent"])
            except ValueError as verr:
                self.logger.warning(
                    f"Descendant {row['descendent']} could not be converted to a CURIE, will be used as-is: {verr}"
                )
                desc = row["descendent"]

            if row["match"] is None:
                results[desc] += []
            else:
                try:
                    results[desc].append(Text.opt_to_curie(row["match"]))
                except ValueError as verr:
                    # Sometimes, if there are no exact_matches, we'll get some kind of blank node id
                    # like 't19830198'. Want to filter those out.
                    self.logger.warning(f"Value {row['match']} in row {row} could not be converted to a CURIE: {verr}")
                    continue

        return results

    def write_normalized_information_content(self, filename):
        """
        Download the normalized information content and write it to the specified filename.

        :param filename: The filename to write the normalized information content to -- we write them as `IRI\tNIC`.
        :return: The number of normalized information content entries downloaded.
        """
        count_query = "SELECT (COUNT(*) AS ?count) WHERE { ?iri <http://reasoner.renci.org/vocab/normalizedInformationContent> ?nic }"
        count_result = self.triplestore.query(count_query, ["count"])
        total_count = int(count_result[0]["count"])

        assert total_count > 0

        write_count = 0
        with open(filename, "w") as ftsv:
            for start in tqdm(
                range(0, total_count, UberGraph.QUERY_BATCH_SIZE),
                desc=f"{self}.write_normalized_information_content({filename})",
                unit="batch",
            ):
                self.logger.debug(
                    f"Querying write_normalized_information_content() offset {start} limit {UberGraph.QUERY_BATCH_SIZE} (total count: {total_count})"
                )

                query = (
                    "SELECT ?iri ?nic WHERE "
                    "{ ?iri <http://reasoner.renci.org/vocab/normalizedInformationContent> ?nic }"
                    f"ORDER BY ASC(?iri) OFFSET {start} LIMIT {UberGraph.QUERY_BATCH_SIZE}"
                )
                results = self.triplestore.query(query, ["iri", "nic"])

                for row in results:
                    ftsv.write(f"{row['iri']}\t{row['nic']}\n")
                    write_count += 1

        self.logger.info(f"Wrote {write_count} information content values into {filename}.")
        return write_count


def _assert_upper_case_prefixes(argument_name, prefixes):
    """Reject a prefix filter that can never match, before we spend a query finding out.

    `ignore_list` and `allowed_prefixes` are compared against `Text.get_prefix_or_none()`, which
    upper-cases, so an entry that isn't already upper-case silently never matches. That failure is
    invisible for `ignore_list` (it fails open: the junk it was supposed to block is written), and
    it is easy to write, because 13 of the canonical prefix constants in `src/prefixes.py` are not
    upper-case (`orphanet`, `UniProtKB`, `NCBIGene`, ...). Fail loudly at the call instead.

    See https://github.com/NCATSTranslator/Babel/issues/907 for the general fix.
    """
    not_upper = sorted(p for p in prefixes if p != p.upper())
    if not_upper:
        raise ValueError(
            f"build_sets({argument_name}=...) entries are matched against Text.get_prefix_or_none(), "
            f"which upper-cases its result, so these entries can never match: {not_upper}. "
            f'Use the upper-case spelling (e.g. "ORPHANET", not prefixes.ORPHANET which is "orphanet").'
        )


def build_sets(
    iri,
    concordfiles,
    set_type,
    ignore_list=[],
    other_prefixes={},
    hop_ontologies=False,
    allowed_prefixes=None,
    hierarchy_predicate=HIERARCHY_SUBCLASS_OF,
):
    """Given an IRI create a list of sets.  Each set is a set of equivalent LabeledIDs, and there
    is a set for each descendent of the input iri.  Write these lists to concord files, indexed by the prefix

    `ignore_list` blocks the named target prefixes (fail-open: anything unlisted is written).
    `allowed_prefixes`, if not None, is the complement (fail-closed): only targets whose prefix is
    listed are written, so a namespace the source newly starts emitting is rejected until someone
    reviews it. Both are matched against `Text.get_prefix_or_none()`, which upper-cases, so entries
    must be upper-case (e.g. "HTTP", not "http") -- this is checked, not merely documented.

    `hierarchy_predicate` selects how descendents of `iri` are found; it currently applies only to
    set_type="xref" (pass HIERARCHY_PART_OF for partonomy ontologies like EMAPA)."""
    _assert_upper_case_prefixes("ignore_list", ignore_list)
    _assert_known_hierarchy_predicate(hierarchy_predicate)
    if allowed_prefixes is not None:
        _assert_upper_case_prefixes("allowed_prefixes", allowed_prefixes)
    prefix = Text.get_prefix_or_none(iri)
    types2relations = {"xref": "xref", "exact": "oio:exactMatch", "close": "oio:closeMatch"}
    if set_type not in types2relations:
        return
    if hierarchy_predicate != HIERARCHY_SUBCLASS_OF and set_type != "xref":
        raise ValueError(
            f"hierarchy_predicate={hierarchy_predicate!r} is only supported for "
            f"set_type='xref'; set_type={set_type!r} hardcodes rdfs:subClassOf. "
            "Extend get_subclasses_and_exacts() / get_subclasses_and_close() "
            "before using a custom hierarchy predicate with those set types."
        )
    uber = UberGraph()
    if set_type == "xref":
        uberres = uber.get_subclasses_and_xrefs(iri, hierarchy_predicate=hierarchy_predicate)
    elif set_type == "exact":
        uberres = uber.get_subclasses_and_exacts(iri)
    elif set_type == "close":
        uberres = uber.get_subclasses_and_close(iri)
    # Sort both levels so the concord is byte-identical across runs. Neither source of ordering
    # is stable otherwise: the SPARQL queries carry no ORDER BY (so `uberres` insertion order
    # follows arbitrary endpoint row order) and `v` is a set of strings, whose iteration order
    # varies per process under hash randomization. That matters because glom's `unique_prefixes`
    # keeps whichever CURIE of a restricted prefix it sees *first* -- so unsorted output makes
    # clique membership differ between builds of identical code and data. See
    # docs/sources/EMAPA/mappings.md for the case that exposed this (122 UBERON terms xref more
    # than one EMAPA term, and the loser is dropped entirely when it has no ids-file row).
    for k, v in sorted(uberres.items(), key=lambda kv: kv[0]):
        if not hop_ontologies:
            subclass_prefix = Text.get_prefix_or_none(k)
            if subclass_prefix != prefix:
                continue
        v = set([norm(x, other_prefixes) for x in v])
        for x in sorted(v):
            target_prefix = Text.get_prefix_or_none(x)
            if target_prefix in ignore_list:
                continue
            if allowed_prefixes is not None and target_prefix not in allowed_prefixes:
                continue
            p = Text.get_prefix_or_none(k)
            if p in concordfiles:
                concordfiles[p].write(f"{k}\t{types2relations[set_type]}\t{x}\n")


if __name__ == "__main__":
    ug = UberGraph()
    ug.get_all_labels()
