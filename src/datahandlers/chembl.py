from src.prefixes import CHEMBLCOMPOUND
from src.babel_utils import pull_via_ftp, make_local_name
import ftplib
import pyoxigraph

def pull_chembl(moleculefilename):
    fname = get_latest_chembl_name()
    if not fname is None:
        # fname should be like chembl_28.0_molecule.ttl.gz
        #Pull via ftp is going to add the download_dir, so this is a hack until pull_via_ftp is nicer.
        mparts = moleculefilename.split('/')
        dname = mparts[-2]
        oname = '/'.join(mparts[-2:])
        pull_via_ftp('ftp.ebi.ac.uk', '/pub/databases/chembl/ChEMBL-RDF/latest/', fname, decompress_data=True, outfilename=oname)
        pull_via_ftp('ftp.ebi.ac.uk', '/pub/databases/chembl/ChEMBL-RDF/latest/', 'cco.ttl.gz', decompress_data=True, outfilename=f'{dname}/cco.ttl')
        #oname = 'CHEMBLCOMPOUND/'+moleculefilename.split('/')[-1]
        #pull_via_ftp('ftp.ebi.ac.uk', '/pub/databases/chembl/ChEMBL-RDF/latest/', fname, decompress_data=True, outfilename=oname)
        #pull_via_ftp('ftp.ebi.ac.uk', '/pub/databases/chembl/ChEMBL-RDF/latest/', 'cco.ttl.gz', decompress_data=True, outfilename='CHEMBL/cco.ttl')


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
    """Load the rdf file for querying"""
    # Note that we need both the molecule file, and the cco file.  The latter contains the class hierarchy
    def __init__(self,ifname,ccofile):
        from datetime import datetime as dt
        print('loading chembl')
        start = dt.now()
        self.m= pyoxigraph.MemoryStore()
        with open(ccofile,'rb') as inf:
            self.m.load(inf,'application/turtle')
        with open(ifname,'rb') as inf:
            self.m.load(inf,'application/turtle')
        end = dt.now()
        print('loading complete')
        print(f'took {end-start}')
    def pull_labels(self,ofname):
        s="""PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
             PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
             PREFIX cco: <http://rdf.ebi.ac.uk/terms/chembl#>
             SELECT ?molecule ?label
             WHERE {
                ?molecule a ?type .
                ?type rdfs:subClassOf* cco:Substance .
                ?molecule rdfs:label ?label .
            }
        """
        qres = self.m.query(s)
        with open(ofname, 'w', encoding='utf8') as outf:
            for row in list(qres):
                iterm = str(row['molecule'])
                ilabel = str(row['label'])
                chemblid = iterm[:-1].split('/')[-1]
                label = ilabel[1:-1]

                # Sometimes the CHEMBL label is identical to the chemblid. We don't want those (https://github.com/TranslatorSRI/Babel/issues/430).
                if label == chemblid:
                    label = ''

                outf.write(f'{CHEMBLCOMPOUND}:{chemblid}\t{label}\n')

    def pull_smiles(self,ofname):
        s="""PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
             PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
             PREFIX cco: <http://rdf.ebi.ac.uk/terms/chembl#>
             PREFIX cheminf: <http://semanticscience.org/resource/>
             SELECT ?molecule ?smiles
             WHERE {
                ?molecule cheminf:SIO_000008 ?smile_entity .
                ?smile_entity a cheminf:CHEMINF_000018 ;
                              cheminf:SIO_000300 ?smiles .
            }
        """
        qres = self.m.query(s)
        with open(ofname, 'w', encoding='utf8') as outf:
            for row in list(qres):
                iterm = str(row['molecule'])
                ilabel = str(row['smiles'])
                chemblid = iterm[:-1].split('/')[-1]
                label = ilabel[1:-1]
                outf.write(f'{CHEMBLCOMPOUND}:{chemblid}\t{label}\n')


def pull_chembl_labels_and_smiles(infile,ccofile,labelfile,smifile):
    m = ChemblRDF(infile,ccofile)
    m.pull_labels(labelfile)
    m.pull_smiles(smifile)

