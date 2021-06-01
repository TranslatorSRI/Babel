from src.prefixes import RHEA
from src.babel_utils import pull_via_urllib
from src.babel_utils import make_local_name, pull_via_ftp
import pyoxigraph
from collections import defaultdict


def pull_rhea():
    outputfile=pull_via_urllib('https://ftp.expasy.org/databases/rhea/rdf/','rhea.rdf.gz', subpath='RHEA', decompress=True)

class Rhea:
    """Load the mesh rdf file for querying"""
    def __init__(self):
        ifname = make_local_name('rhea.rdf', subpath='RHEA')
        from datetime import datetime as dt
        print('loading rhea')
        start = dt.now()
        self.m= pyoxigraph.MemoryStore()
        with open(ifname,'rb') as inf:
            self.m.load(inf,'application/rdf+xml')
        end = dt.now()
        print('loading complete')
        print(f'took {end-start}')
    def pull_rhea_labels(self,ofname):
        s="""   PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                PREFIX rh: <http://rdf.rhea-db.org/>

                SELECT DISTINCT ?x ?acc ?label
                WHERE { ?x rdfs:label ?label .
                        ?x rh:accession ?acc .}
        """
        qres = self.m.query(s)
        with open(ofname, 'w', encoding='utf8') as outf:
            for row in list(qres):
                iterm = str(row['acc'])
                label = str(row['label'])
                rheaid = iterm[:-1].split('/')[-1]
                #label = ilabel.strip().split('"')[1]
                outf.write(f'{RHEA}:{rheaid}\t{label}\n')

def make_labels(labelfile):
    m = Rhea()
    m.pull_rhea_labels(labelfile)
