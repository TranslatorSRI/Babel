import logging
import re

from src.prefixes import CLO
from src.categories import CELL_LINE
from src.babel_utils import pull_via_urllib
from src.util import Text, LoggingUtil
import pyoxigraph

logger = LoggingUtil.init_logging(__name__, level=logging.WARNING)

def pull_clo():
    _=pull_via_urllib('http://purl.obolibrary.org/obo/','clo.owl', subpath='CLO', decompress=False)

class CLOgraph:
    """Load the file for querying"""
    def __init__(self,ifname):
        from datetime import datetime as dt
        print('loading CLO')
        start = dt.now()
        self.m= pyoxigraph.MemoryStore()
        with open(ifname,'rb') as inf:
            self.m.load(inf,'application/rdf+xml',base_iri='http://example.org/')
        end = dt.now()
        print('loading complete')
        print(f'took {end-start}')

    def pull_CLO_labels_and_synonyms(self,lname,sname):
        with open(lname, 'w') as labelfile, open(sname,'w') as synfile:
            for labeltype in ['skos:prefLabel','skos:altLabel','rdfs:label']:
                s=f"""   PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
                        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

                        SELECT DISTINCT ?x ?label
                        WHERE {{ ?x {labeltype} ?label }}
                """
                qres = self.m.query(s)
                for row in list(qres):
                    iterm = str(row['x'])
                    label = str(row['label'])
                    if label.startswith('"'):
                        # If the label ends with '"@[language code]", edit that out.
                        pattern = re.compile(r"^\"(.*)\"@\w+$")
                        if pattern.match(label):
                            label = re.sub(pattern, r"\1", label)
                        else:
                            label = label[1:-1]
                    cloid = iterm[:-1].split('/')[-1]
                    if not cloid.startswith("CLO_"):
                        continue
                    clo_id = cloid.split("_")[-1]
                    synfile.write(f'{CLO}:{clo_id}\t{labeltype}\t{label}\n')
                    if not labeltype == 'skos:altLabel':
                        labelfile.write(f'{CLO}:{clo_id}\t{label}\n')

    def pull_CLO_ids(self,roots,idfname):
        with open(idfname, 'w') as idfile:
            for root,rtype in roots:
                s=f""" PREFIX CLO: <http://purl.obolibrary.org/obo/CLO_>
                       PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

                       SELECT DISTINCT ?x
                       WHERE {{ ?x rdfs:subClassOf* {root} }}
                """
                qres = self.m.query(s)
                for row in list(qres):
                    iterm = str(row['x'])
                    cloid = iterm[:-1].split('/')[-1]
                    if cloid.startswith("CLO_"):
                        clo_id = cloid.split("_")[-1]
                        idfile.write(f'{CLO}:{clo_id}\t{rtype}\n')

    def get_exacts(self, iri, outfile):
        query = f"""
         prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
         prefix UBERON: <http://purl.obolibrary.org/obo/UBERON_>
         prefix CL: <http://purl.obolibrary.org/obo/CL_>
         prefix GO: <http://purl.obolibrary.org/obo/GO_>
         prefix CHEBI: <http://purl.obolibrary.org/obo/CHEBI_>
         prefix MONDO: <http://purl.obolibrary.org/obo/MONDO_>
         prefix MONDOH: <http://purl.obolibrary.org/obo/mondo#>
         prefix HP: <http://purl.obolibrary.org/obo/HP_>
         prefix EFO: <http://www.ebi.ac.uk/efo/EFO_>
         prefix NCIT: <http://purl.obolibrary.org/obo/NCIT_>
         prefix SKOS: <http://www.w3.org/2004/02/skos/core#>
         prefix icd11.foundation: <http://id.who.int/icd/entity/>
         SELECT DISTINCT ?match
         WHERE {{
             {{ {iri} SKOS:exactMatch ?match. }}
             UNION
             {{ {iri} MONDOH:exactMatch ?match. }}
         }}
         """
        qres = self.m.query(query)
        nwrite = 0
        for row in list(qres):
            other = str(row["match"])
            try:
                otherid = Text.opt_to_curie(other[1:-1])
            except ValueError as verr:
                print(f"Could not translate {other[1:-1]} into a CURIE, will be used as-is: {verr}")
                otherid = other[1:-1]

            if otherid.upper().startswith(ORPHANET.upper()):
                logger.warning(f"Skipping Orphanet xref '{other[1:-1]}' in EFOgraph.get_xrefs({iri})")
                continue
                # raise RuntimeError(
                #     f"Unexpected ORPHANET in EFOgraph.get_xrefs({iri}): '{other_without_brackets}'"
                # )
            outfile.write(f"{iri}\tskos:exactMatch\t{otherid}\n")
            nwrite += 1
        return nwrite
    def get_xrefs(self, iri, outfile):
        query = f"""
         prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>
         prefix EFO: <http://www.ebi.ac.uk/efo/EFO_>
         prefix oboInOwl: <http://www.geneontology.org/formats/oboInOwl#>
         SELECT DISTINCT ?match
         WHERE {{
             {{ {iri} oboInOwl:hasDbXref ?match. }}
         }}
         """
        qres = self.m.query(query)
        for row in list(qres):
            other = str(row["match"])
            other_without_brackets = other[1:-1]
            try:
                other_id = Text.opt_to_curie(other_without_brackets)
            except ValueError as verr:
                logger.warning(
                    f"Could not translate '{other_without_brackets}' into a CURIE in " +
                    f"EFOgraph.get_xrefs({iri}), skipping: {verr}"
                )
                continue
            if other_id.upper().startswith(ORPHANET.upper()):
                logger.warning(f"Skipping Orphanet xref '{other_without_brackets}' in EFOgraph.get_xrefs({iri})")
                continue
                # raise RuntimeError(
                #     f"Unexpected ORPHANET in EFOgraph.get_xrefs({iri}): '{other_without_brackets}'"
                # )
            #EFO occasionally has xrefs that are just strings, not IRIs or CURIEs
            if ":" in other_id and not other_id.startswith(":"):
                outfile.write(f"{iri}\toboInOwl:hasDbXref\t{other_id}\n")
            else:
                logging.warning(
                    f"Skipping xref '{other_without_brackets}' in EFOgraph.get_xrefs({iri}): " +
                    "not a valid CURIE"
                )


def make_labels(infile,labelfile,synfile):
    m = CLOgraph(infile)
    m.pull_CLO_labels_and_synonyms(labelfile,synfile)

def write_clo_ids(idfname, odfname):
    m = CLOgraph(idfname)
    roots = [("CLO:0000001", CELL_LINE)]
    m.pull_CLO_ids(roots,odfname)

#def make_concords(idfilename, outfilename):
#    """Given a list of identifiers, find out all of the equivalent identifiers from the owl"""
#    m = EFOgraph()
#    with open(idfilename,"r") as inf, open(outfilename,"w") as concfile:
#        for line in inf:
#            efo_id = line.split('\t')[0]
#            nexacts = m.get_exacts(efo_id,concfile)
#            if nexacts == 0:
#                m.get_xrefs(efo_id,concfile)
