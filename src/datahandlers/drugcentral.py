from src.prefixes import DRUGCENTRAL
from src.babel_utils import pull_via_urllib

def pull_drugcentral():
    dname = pull_via_urllib('https://unmtid-shinyapps.net/download/20200516','structures.smiles.tsv',decompress=False,subpath='DRUGCENTRAL')

def make_labels(infile,outfile):
    with open(infile,'r') as inf, open(outfile,'w') as outf:
        h = inf.readline()
        for line in inf:
            x = line.strip().split('\t')
            outf.write(f'{DRUGCENTRAL}:{x[3]}\t{x[4]}\n')

