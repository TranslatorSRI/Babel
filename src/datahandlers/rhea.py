from src.prefixes import RHEA,EC
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
        self.m= pyoxigraph.SledStore('/tmp/rhea.sled')
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
                iterm = str(row['acc'])[1:-1] #strip "
                label = str(row['label'])[1:-1]
                #The rhea ids in the rdf use the currently approved prefix, but just to be sure...
                rheaid = iterm.split(':')[-1]
                outf.write(f'{RHEA}:{rheaid}\t{label}\n')
    def pull_rhea_ec_concs(self,ofname):
        s="""   PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                PREFIX rh: <http://rdf.rhea-db.org/>

                SELECT DISTINCT ?x ?acc ?ec
                WHERE { ?x rh:ec ?ec .
                        ?x rh:accession ?acc .}
        """
        qres = self.m.query(s)
        with open(ofname, 'w', encoding='utf8') as outf:
            for row in list(qres):
                #<http://purl.uniprot.org/enzyme/1.14.15.16>
                ec = f'{EC}:{str(row["ec"]).split("/")[-1][:-1]}'
                #The rhea ids in the rdf use the currently approved prefix, but just to be sure...
                iterm = str(row['acc'])[1:-1] #strip "
                rheaid = iterm.split(':')[-1]
                outf.write(f'{RHEA}:{rheaid}\toio:equivalent\t{ec}\n')


#Ids are handled by just getting everything from the labels
def make_labels(labelfile):
    m = Rhea()
    m.pull_rhea_labels(labelfile)

def make_concord(concfile):
    m = Rhea()
    m.pull_rhea_ec_concs(concfile)