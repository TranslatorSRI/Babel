from src.prefixes import PANTHERPATHWAY
from src.babel_utils import pull_via_urllib

def pull_panther_pathways():
    pull_via_urllib('http://data.pantherdb.org/ftp/pathway/current_release/', 'SequenceAssociationPathway3.6.6.txt', decompress=False, subpath='PANTHER.PATHWAY')

def make_pathway_labels(infile,outfile):
    with open(infile,'r') as inf:
        data = inf.read()
    lines = data.split('\n')
    labels = {}
    wrote=set()
    with open(outfile,'w') as outf:
        for line in lines:
            x = line.strip().split('\t')
            if len(x) < 2:
                print(x)
                continue
            pw_id = f'{PANTHERPATHWAY}:{x[0]}'
            name = x[1]
            outline=f'{pw_id}\t{name}\n'
            if outline in wrote:
                continue
            outf.write(outline)
            wrote.add(outline)

