from src.prefixes import GTOPDB
from src.babel_utils import pull_via_urllib

def pull_gtopdb_ligands():
    pull_via_urllib('https://www.guidetopharmacology.org/DATA/','ligands.tsv',decompress=False,subpath='GTOPDB')

def make_labels_and_synonyms(inputfile,labelfile,synfile):
    with open(inputfile,'r') as inf, open(labelfile,'w') as lf, open(synfile,'w') as sf:
        h = inf.readline()
        for line in inf:
            parts = line.strip().split('\t')
            frontline = line.split('"')[0][:-1] #remove the last (quoted) column
            x = frontline.strip().split(',')
            ident = f'{SMPDB}:{x[0]}'
            name = ','.join(x[2:]) #get the rest of the splits and put them back together.
            if len(name) > 0:
                outf.write(f'{ident}\t{name}\n')
