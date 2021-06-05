from src.prefixes import CHEMBLCOMPOUND
from src.babel_utils import pull_via_ftp, make_local_name
import ftplib
import pyoxigraph

def pull_chembl(outfname):
    fname = get_latest_chembl_name()
    if not fname is None:
        # fname should be like chembl_28.0_molecule.ttl.gz
        #Pull via ftp is going to add the download_dir, so this is a hack until pull_via_ftp is nicer.
        oname = 'CHEMBL/'+outfname.split('/')[-1]
        pull_via_ftp('ftp.ebi.ac.uk', '/pub/databases/chembl/ChEMBL-RDF/latest/', fname, decompress_data=True, outfilename=oname)


def get_latest_chembl_name() -> str:
    # get a handle to the ftp directory
    ftp = ftplib.FTP("ftp.ebi.ac.uk")

    # login
    ftp.login()

    # move to the target directory
    ftp.cwd('/pub/databases/chembl/ChEMBL-RDF/latest')

    # get the directory listing
    files: list = ftp.nlst()

    # close the ftp connection
    ftp.quit()

    # parse the list to determine the latest version of the files
    for f in files:
        if f.endswith('_molecule.ttl.gz'):
            return f
    return None


class ChemblRDF:
    """Load the mesh rdf file for querying"""
    def __init__(self,ifname):
        from datetime import datetime as dt
        print('loading mesh.nt')
        start = dt.now()
        self.m= pyoxigraph.MemoryStore()
        with open(ifname,'rb') as inf:
            self.m.load(inf,'application/turtle')
        end = dt.now()
        print('loading complete')
        print(f'took {end-start}')
    def pull_labels(self,ofname):
        s="""   PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
                PREFIX meshv: <http://id.nlm.nih.gov/mesh/vocab#>
                PREFIX mesh: <http://id.nlm.nih.gov/mesh/>

                SELECT DISTINCT ?term ?label
                WHERE { ?term rdfs:label ?label }
                ORDER BY ?term
        """
        qres = self.m.query(s)
        with open(ofname, 'w', encoding='utf8') as outf:
            for row in list(qres):
                iterm = str(row['term'])
                ilabel = str(row['label'])
                meshid = iterm[:-1].split('/')[-1]
                label = ilabel.strip().split('"')[1]
                outf.write(f'MESH:{meshid}\t{label}\n')

def pull_chembl_labels(infile,outfile):
    m = ChemblRDF(infile)
    m.pull_labels(outfile)

