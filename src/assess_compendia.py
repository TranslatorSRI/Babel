import os
from os import path
import jsonlines
from collections import defaultdict
from src.util import Text

#TODO: Assess whether we are bringing in any new identifiers (we shouldn't, unless we just don't have id's for a thing)
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
                ids = [x['i'] for x in  j['identifiers'] ]
                for identifier in ids:
                    all_identifiers.discard(identifier)
    with open(reportfile,'w') as outf:
        l = list(all_identifiers)
        l.sort()
        print(f'Missing identifiers: {len(l)}\n')
        outf.write(f'Missing identifiers: {len(l)}\n')
        for missing_id in l:
            outf.write(f'{missing_id}\n')

def makecountset(j):
    eids = [Text.get_prefix_or_none(x['i']) for x in j['identifiers']]
    pcounts = defaultdict(int)
    for p in eids:
        pcounts[p] += 1
    return frozenset( [(k,v)  for k,v in pcounts.items()] )


def assess(compendium,reportfile):
    nclusters =  0
    clustersizes = defaultdict(int)
    clustertypes = defaultdict(int)
    with jsonlines.open(compendium,'r') as inf:
        for j in inf:
            nclusters += 1
            clustersizes[ len(j['identifiers']) ] += 1
            countset = makecountset(j)
            clustertypes[countset] += 1
    with open(reportfile,'w') as outf:
        outf.write(f'{nclusters} clusters\n')
        sizes = list(clustersizes.keys())
        sizes.sort()
        outf.write(f'Max cluster size: {max(sizes)}\n')
        outf.write(f'\nCluster Size Distribution\n')
        for s in sizes:
            outf.write(f'{s}\t{clustersizes[s]}\n')
        ct = [ (v,k) for k,v in clustertypes.items() ]
        ct.sort()
        outf.write('\nCluster Type Distribution\n')
        for v,k in ct:
            outf.write(f'{k}\t{v}\n')
