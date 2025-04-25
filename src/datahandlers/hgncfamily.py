from src.babel_utils import pull_via_urllib
from src.prefixes import HGNCFAMILY

def pull_hgncfamily():
    # As per https://www.genenames.org/download/gene-groups/#!/#tocAnchor-1-2
    pull_via_urllib('https://storage.googleapis.com/public-download-files/hgnc/csv/csv/genefamily_db_tables/',
                    'family.csv',
                    decompress=False,
                    subpath=HGNCFAMILY)

def pull_labels(infile,outfile):
    with open(infile,'r') as inf:
        data = inf.read().strip()
    lines = data.split('\n')
    with open(outfile,'w') as outf:
        #skip header
        for line in lines[1:]:
            parts = line.split(',')
            if len(parts) < 10:
                continue
            i = f"{HGNCFAMILY}:{parts[0][1:-1]}"
            l = parts[2][1:-1]
            outf.write(f'{i}\t{l}\n')

