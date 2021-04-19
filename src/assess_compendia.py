import os
from os import path
import jsonlines
from collections import defaultdict

def assess_completeness(input_dir,compendia,reportfile):
    """Given a directory containing id files, make sure that every id in those files ended up in one of the compendia"""
    id_files = os.listdir(input_dir)
    all_identifiers = set()
    for idf in id_files:
        with open(path.join(input_dir,idf),'r') as inf:
            for line in inf:
                x = line.strip().split('\t')[0]
                all_identifiers.add(x)
    for comp in compendia:
        print(comp)
        with jsonlines.open(comp, 'r') as inf:
            for j in inf:
                ids = [x['identifier'] for x in  j['equivalent_identifiers'] ]
                for identifier in ids:
                    all_identifiers.discard(identifier)
    with open(reportfile,'w') as outf:
        l = list(all_identifiers)
        l.sort()
        print(f'Missing identifiers: {len(l)}\n')
        outf.write(f'Missing identifiers: {len(l)}\n')
        for missing_id in l:
            outf.write(f'{missing_id}\n')

def assess(compendium,reportfile):
    nclusters =  0
    clustersizes = defaultdict(int)
    with jsonlines.open(compendium,'r') as inf:
        for j in inf:
            nclusters += 1
            clustersizes[ len(j['equivalent_identifiers']) ] += 1
    with open(reportfile,'w') as outf:
        outf.write(f'{nclusters} clusters\n')
        sizes = list(clustersizes.keys())
        sizes.sort()
        outf.write(f'Max cluster size: {max(sizes)}\n')
        for s in sizes:
            outf.write(f'{s}\t{clustersizes[s]}\n')