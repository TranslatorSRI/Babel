from src.prefixes import PUBCHEMCOMPOUND
from src.babel_utils import make_local_name, pull_via_ftp
import gzip

def pull_pubchem():
    files = ['CID-MeSH','CID-Synonym-filtered.gz','CID-Title.gz']
    pull(files)

def pull_pubchem_structures():
    files = ['CID-InChI-Key.gz','CID-SMILES.gz']
    pull(files)

def pull(files):
    for f in files:
        outfile=f'{PUBCHEMCOMPOUND}/{f}'
        pull_via_ftp('ftp.ncbi.nlm.nih.gov', '/pubchem/Compound/Extras', f, outfilename=outfile)

def make_labels_or_synonyms(infile,outfile):
    with gzip.open(infile, 'r') as inf, open(outfile,'w') as outf:
        for l in inf:
            line = l.decode('latin1')
            x = line.strip().split('\t')
            outf.write(f'{PUBCHEMCOMPOUND}:{x[0]}\t{x[1]}\n')

