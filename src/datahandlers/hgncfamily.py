from src.babel_utils import make_local_name, pull_via_ftp
from src.prefixes import HGNCFAMILY

def pull_hgncfamily():
    outfile=f'{HGNCFAMILY}/family.csv'
    pull_via_ftp('ftp.ebi.ac.uk', '/pub/databases/genenames/new/csv/genefamily_db_tables','family.csv', outfilename=outfile)

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

