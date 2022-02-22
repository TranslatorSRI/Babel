from src.prefixes import EC
from src.categories import MOLECULAR_ACTIVITY
from src.babel_utils import pull_via_urllib
from src.babel_utils import make_local_name, pull_via_ftp
import pyoxigraph
from collections import defaultdict


def pull_ec():
    outputfile=pull_via_urllib('https://ftp.expasy.org/databases/enzyme/','enzyme.rdf', subpath='EC', decompress=False)

class ECgraph:
    """Load the mesh rdf file for querying"""
    def __init__(self):
        """There is a problem with enzyme.rdf.  As pulled from expasy, it includes this:

        <owl:Ontology rdf:about="">
        <owl:imports rdf:resource="http://purl.uniprot.org/core/"/>
        </owl:Ontology>

        That about='' really makes pyoxigraph annoyed. So we have to give it a base_iri on load, then its ok"""
        ifname = make_local_name('enzyme.rdf', subpath='EC')
        from datetime import datetime as dt
        print('loading EC')
        start = dt.now()
        self.m= pyoxigraph.SledStore('/tmp/ec.sled')
        with open(ifname,'rb') as inf:
            self.m.load(inf,'application/rdf+xml',base_iri='http://nihilism.com/')
        end = dt.now()
        print('loading complete')
        print(f'took {end-start}')
    def pull_EC_labels_and_synonyms(self,lname,sname):
        with open(lname, 'w') as labelfile, open(sname,'w') as synfile:
            #for labeltype in ['skos:prefLabel','skos:altLabel','rdfs:label']:
            for labeltype in ['skos:prefLabel','skos:altLabel','rdfs:label']:
                s=f"""   PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
                        PREFIX ec: <http://purl.uniprot.org/enzyme/>
                        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

                        SELECT DISTINCT ?x ?label
                        WHERE {{ ?x {labeltype} ?label }}
                """
                qres = self.m.query(s)
                for row in list(qres):
                    iterm = str(row['x'])
                    label = str(row['label'])
                    ecid = iterm[:-1].split('/')[-1]
                    synfile.write(f'{EC}:{ecid}\t{labeltype}\t{label}\n')
                    if not labeltype == 'skos:altLabel':
                        labelfile.write(f'{EC}:{ecid}\t{label}\n')
    def pull_EC_ids(self,idfname):
        with open(idfname, 'w') as idfile:
            s="""  PREFIX ec: <http://purl.uniprot.org/enzyme/>
                   PREFIX uc: <http://purl.uniprot.org/core/>
                   PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

                   SELECT DISTINCT ?x
                   WHERE { ?x rdf:type uc:Enzyme }
            """
            qres = self.m.query(s)
            for row in list(qres):
                iterm = str(row['x'])
                ecid = iterm[:-1].split('/')[-1]
                #idfile.write(f'{EC}:{ecid}\t{rtype}\n')
                idfile.write(f'{EC}:{ecid}\t{MOLECULAR_ACTIVITY}\n')

def make_labels(labelfile,synfile):
    m = ECgraph()
    m.pull_EC_labels_and_synonyms(labelfile,synfile)

def make_ids(idfname):
    m = ECgraph()
    m.pull_EC_ids(idfname)
