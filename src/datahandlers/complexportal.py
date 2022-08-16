from src.babel_utils import pull_via_urllib, make_local_name
from src.prefixes import COMPLEXPORTAL

def pull_complexportal():
    pull_via_urllib('http://ftp.ebi.ac.uk/pub/databases/intact/complex/current/complextab/',f'559292.tsv', decompress=False, subpath=COMPLEXPORTAL)

def make_labels_and_synonyms(infile, labelfile, synfile):
    usedsyns = set()
    with open(infile, 'r') as inf, open(labelfile, 'w') as outl, open(synfile, 'w') as outsyn:
        next(inf) # skip header
        for line in inf:
            sline = line.split("\t")
            id = sline[0]
            label = sline[1] # recommended name
            outl.write(f'ComplexPortal:{id}\t{label}\n')
            synonyms_str = sline[2] # aliases
            if not synonyms_str == "-":
                synonyms = synonyms_str.split('|')
                for syn in synonyms:
                    if not syn in usedsyns:
                        outsyn.write(f'ComplexPortal:{id}\t{syn}\n')
                        usedsyns.add(syn)