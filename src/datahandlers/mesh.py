from src.ubergraph import UberGraph
from src.babel_utils import make_local_name, pull_via_ftp
from collections import defaultdict
import os, gzip
from json import loads,dumps

def pull_mesh():
    pull_via_ftp('ftp.nlm.nih.gov', '/online/mesh/rdf', 'mesh.nt.gz', decompress_data=True, outfilename='MESH/mesh.nt')

def pull_mesh_labels():
    ifname = make_local_name('mesh.nt', subpath='MESH')
    ofname = make_local_name('labels', subpath='MESH')
    badlines = 0
    with open(ofname, 'w') as outf, open(ifname,'r') as data:
        for line in data:
            if line.startswith('#'):
                continue
            triple = line[:-1].strip().split('\t')
            try:
                s,v,o = triple
                if v == '<http://www.w3.org/2000/01/rdf-schema#label>':
                    meshid = s[:-1].split('/')[-1]
                    label = o.strip().split('"')[1]
                    outf.write(f'MESH:{meshid}\t{label}\n')
            except ValueError:
                badlines += 1
    print(f'{badlines} lines were bad')
