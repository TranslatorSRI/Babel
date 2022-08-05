from src.babel_utils import pull_via_urllib, make_local_name
from src.prefixes import COMPLEXPORTAL

def pull_complexportal():
    pull_via_urllib('http://ftp.ebi.ac.uk/pub/databases/intact/complex/current/complextab/',f'559292.tsv', decompress=False, subpath=COMPLEXPORTAL)

def make_labels_and_synonyms(infile, labelfile, synfile):
    infile = open(infile, 'r')
    lines = infile.readlines()[1:]
    usedsyns = set()

    with open(labelfile, 'w') as outl, open(synfile, 'w') as outsyn:
        for line in lines:
            sline = line.split("\t")
            id = sline[0]
            label = sline[1] # recommended name
            outl.write(f'Complex ac:{id}\t{label}\n')
            synonyms_str = sline[2] # aliases
            if not synonyms_str == "-":
                synonyms = synonyms_str.split('|')
                for syn in synonyms:
                    if not syn in usedsyns:
                        outsyn.write(f'Complex ac:{id}\t{syn}\n')
                        usedsyns.add(syn)

# test
# path = "/Users/shalkishrivastava/renci/creativity-hub/yeast-kop/Babel/babel_downloads/"
# infile = path + "559292.tsv"
# labels = path + "labels.tsv"
# synonyms = path + "synonyms.tsv"
# make_labels_and_synonyms(infile, labels, synonyms)