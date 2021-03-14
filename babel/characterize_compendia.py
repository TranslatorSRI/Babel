import os
from json import loads
from collections import defaultdict

def get_compendia():
    dname = os.path.join(os.path.dirname(__file__), 'compendia')
    somefiles = os.listdir(dname)
    compendia = [f'{dname}/{x}'for x in somefiles if x.endswith('.txt')]
    return compendia

def characterize_one_compendium(fname,threshold = 80):
    used_ids = set()
    all_ids = set()
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
            all_ids.update([e['identifier'] for e in eids])
            if len(eids) == threshold:
                for eid in eids:
                    print(eid)
#                exit()
    skeys = list(counts.keys())
    skeys.sort()
#    for k in skeys:
#        print(k,counts[k])
    print(fname)
    print(len(used_ids),'rows in the compendium')
    print(skeys[-5:])
    print(len(all_ids), ' total identifiers')

def go():
    characterize_one_compendium('compendia/disease.txt')
    characterize_one_compendium('compendia/cull_s_term/disease.txt')
    return
    comps = get_compendia()
    for c in comps:
        characterize_one_compendium(c)
        break

if __name__ == '__main__':
    go()
