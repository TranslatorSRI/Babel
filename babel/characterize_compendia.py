import os
from json import loads
from collections import defaultdict

def get_compendia():
    dname = os.path.join(os.path.dirname(__file__), 'compendia')
    somefiles = os.listdir(dname)
    compendia = [f'{dname}/{x}'for x in somefiles if x.endswith('.txt')]
    return compendia

def characterize_one_compendium(fname,threshold = 300):
    used_ids = set()
    counts = defaultdict(int)
    with open(fname,'r') as inf:
        for line in inf:
            entity = loads(line)
            ident = entity['id']['identifier']
            if ident in used_ids:
                print('overused identifier')
                print(ident)
                exit()
            used_ids.add(ident)
            eids = entity['equivalent_identifiers']
            counts[len(eids)] += 1
            if len(eids) > threshold:
                for eid in eids:
                    print(eid)
#                exit()
    skeys = list(counts.keys())
    skeys.sort()
    for k in skeys:
        print(k,counts[k])

def go():
    characterize_one_compendium('compendia/phenotypes.txt')
    return
    comps = get_compendia()
    for c in comps:
        characterize_one_compendium(c)
        break

if __name__ == '__main__':
    go()
