from collections import defaultdict
from ast import literal_eval

def read_bad_hp_mappings(fn):
    drops = defaultdict(set)
    with open(fn,'r') as infile:
        for line in infile:
            if line.startswith('-'):
                continue
            x = line.strip().split('\t')
            hps = x[0]
            commaindex = hps.index(',')
            curie = hps[1:commaindex]
            name = hps[commaindex+1:-1]
            badset = literal_eval(x[1])
            drops[curie].update(badset)
    return drops

def write_bad_hpos(drops,fname):
    with open(fname,'w') as outf:
        for k,vs in drops.items():
            for v in vs:
                outf.write(f'{k} {v}\n')

def go():
    x=read_bad_hp_mappings('hpo_errors.txt')
    write_bad_hpos(x,'badHPx.txt')

if __name__ == '__main__':
    go()
