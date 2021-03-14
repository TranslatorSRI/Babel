import json

def load(fname):
    res = {}
    with open(fname, 'r') as jf:
        for line in jf:
            entity = json.loads(line)
            identifier = entity['id']['identifier']
            eqids = frozenset([ e['identifier'] for e in entity['equivalent_identifiers']])
            res[identifier] = eqids
    return res


def compare(fname):
    new = load(f'compendia/{fname}')
    old = load(f'compendia/older/{fname}')
    lost_key = 0
    changed_value = 0
    for key in old:
        if key not in new:
            lost_key+=1
            print(key)
        elif new[key] != old[key]:
            changed_value +=1
            #print(key)
            #print(' ',old[key])
            #print(' ',new[key])
    print(lost_key)
    print(changed_value)

if __name__ == '__main__':
    compare('phenotypes.txt')
