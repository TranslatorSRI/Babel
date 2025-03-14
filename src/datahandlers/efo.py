import logging
import re

from src.prefixes import EFO,ORPHANET
from src.babel_utils import pull_via_urllib
from src.babel_utils import make_local_name
from src.util import Text, LoggingUtil
import pyoxigraph

logger = LoggingUtil.init_logging(__name__, level=logging.WARNING)

def pull_efo():
    _=pull_via_urllib('http://www.ebi.ac.uk/efo/','efo.owl', subpath='EFO', decompress=False)

class EFOgraph:
    """Load the mesh rdf file for querying"""
    def __init__(self):
        """There is a problem with enzyme.rdf.  As pulled from expasy, it includes this:

        <owl:Ontology rdf:about="">
        <owl:imports rdf:resource="http://purl.uniprot.org/core/"/>
        </owl:Ontology>

        That about='' really makes pyoxigraph annoyed. So we have to give it a base_iri on load, then its ok"""
        ifname = make_local_name('efo.owl', subpath='EFO')
        from datetime import datetime as dt
        print('loading EFO')
        start = dt.now()
        self.m= pyoxigraph.MemoryStore()
        with open(ifname,'rb') as inf:
            self.m.load(inf,'application/rdf+xml',base_iri='http://example.org/')
        end = dt.now()
        print('loading complete')
        print(f'took {end-start}')

    def pull_EFO_labels_and_synonyms(self,lname,sname):
        with open(lname, 'w') as labelfile, open(sname,'w') as synfile:
            #for labeltype in ['skos:prefLabel','skos:altLabel','rdfs:label']:
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
                    efoid = iterm[:-1].split('/')[-1]
                    if not efoid.startswith("EFO_"):
                        continue
                    efo_id = efoid.split("_")[-1]
                    synfile.write(f'{EFO}:{efo_id}\t{labeltype}\t{label}\n')
                    if not labeltype == 'skos:altLabel':
                        labelfile.write(f'{EFO}:{efo_id}\t{label}\n')

    def pull_EFO_ids(self,roots,idfname):
        with open(idfname, 'w') as idfile:
            for root,rtype in roots:
                s=f""" PREFIX EFO: <http://www.ebi.ac.uk/efo/EFO_>
                       PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

                       SELECT DISTINCT ?x
                       WHERE {{ ?x rdfs:subClassOf* {root} }}
                """
                qres = self.m.query(s)
                for row in list(qres):
                    iterm = str(row['x'])
                    efoid = iterm[:-1].split('/')[-1]
                    if efoid.startswith("EFO_"):
                        efo_id = efoid.split("_")[-1]
                        idfile.write(f'{EFO}:{efo_id}\t{rtype}\n')

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


def make_labels(labelfile,synfile):
    m = EFOgraph()
    m.pull_EFO_labels_and_synonyms(labelfile,synfile)

def make_ids(roots,idfname):
    m = EFOgraph()
    m.pull_EFO_ids(roots,idfname)

def make_concords(idfilename, outfilename):
    """Given a list of identifiers, find out all of the equivalent identifiers from the owl"""
    m = EFOgraph()
    with open(idfilename,"r") as inf, open(outfilename,"w") as concfile:
        for line in inf:
            efo_id = line.split('\t')[0]
            nexacts = m.get_exacts(efo_id,concfile)
            if nexacts == 0:
                m.get_xrefs(efo_id,concfile)
