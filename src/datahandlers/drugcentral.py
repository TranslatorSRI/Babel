from src.prefixes import DRUGCENTRAL
from src.babel_utils import pull_via_urllib

def pull_drugcentral():
    dname = pull_via_urllib('https://unmtid-shinyapps.net/download/','structures.smiles.tsv',decompress=False,subpath='DRUGCENTRAL')

def make_labels_and_synonyms(inputfile,labelfile,synfile):
    pass
