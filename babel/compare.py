import json
from collections import defaultdict
import sys

def load(fname):
    res = {}
    allids = set()
    with open(fname, 'r') as jf:
        for line in jf:
            entity = json.loads(line)
            identifier = entity['id']['identifier']
            eqids = frozenset([ e['identifier'] for e in entity['equivalent_identifiers']])
            allids.update(eqids)
            res[identifier] = eqids
    return res,allids

def count_prefixes(fname,idset):
    counts = defaultdict(int)
    for i in idset:
        pref = i.split(':')[0]
        counts[pref]+=1
    print(f"Prefixes for {fname}")
    ps = list(counts.keys())
    ps.sort()
    for p in ps:
        print(f'{p} {counts[p]}')

def compare(fname1,fname2):
    sets_1, ids_1 = load(fname1)
    sets_2, ids_2 = load(fname2)
    print(f"{fname1} contains {len(sets_1)} groups from {len(ids_1)} identifiers")
    print(f"{fname2} contains {len(sets_2)} groups from {len(ids_2)} identifiers")
    keys_2 = set(sets_2.keys())
    keys_1 = set(sets_1.keys())
    inboth = keys_2.intersection(keys_1)
    print(f'canonical identifiers:   {fname1}: {len(keys_1)},   {fname2}: {len(keys_2)}, shared: {len(inboth)}')
    count_prefixes(fname1,ids_1)
    count_prefixes(fname2,ids_2)
    #Did we lose any identifiers?
    print(f'There are {len(ids_1.difference(ids_2))} identifiers in {fname1} and not in {fname2}')
    with open('gone_missing','w') as outf:
        for ident in keys_1.difference(keys_2):
            outf.write(f'{ident}\n')
    print(f'There are {len(ids_2.difference(ids_1))} identifiers in {fname2} and not in {fname1}')
    #exit()

    #lost_key = 0
    #changed_value = 0
    #for key in old:
    #    if key not in new:
    #        lost_key+=1
    #        print(key)
    #    elif new[key] != old[key]:
    #        changed_value +=1
    #        #print(key)
    #        #print(' ',old[key])
    #        #print(' ',new[key])
    #print(lost_key)
    #print(changed_value)
#
if __name__ == '__main__':
    compare(sys.argv[1],sys.argv[2])
