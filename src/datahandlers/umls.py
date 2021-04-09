from src.ubergraph import UberGraph
from src.babel_utils import make_local_name, pull_via_ftp
from collections import defaultdict
import os, gzip
from json import loads,dumps

def read_umls_priority():
    mrp = os.path.join('input_data', 'umls_precedence.txt')
    pris = []
    with open(mrp,'r') as inf:
        h =inf.readline()
        for line in  inf:
            x = line.strip().split()
            if x[2] == 'No':
                pris.append( (x[0],x[1],'N'))
            elif x[2] == 'Yes':
                pris.append( (x[0],x[1],'Y'))
            else:
                pass
    prid = { x:i for i,x in enumerate(pris) }
    return prid

def pull_umls():
    """Run through MRCONSO.RRF creating label and synonym files for UMLS and SNOMEDCT"""
    mrcon = os.path.join('input_data', 'MRCONSO.RRF')
    rows = defaultdict(list)
    priority = read_umls_priority()
    snomed_label_name = make_local_name('labels', subpath='SNOMEDCT')
    snomed_syn_name = make_local_name('synonyms', subpath='SNOMEDCT')
    with open(mrcon, 'r') as inf, open(snomed_label_name,'w') as snolabels, open(snomed_syn_name,'w') as snosyns:
        for line in inf:
            x = line.strip().split('|')
            cui = x[0]
            lang = x[1]
            #Only keep english terms
            if lang != 'ENG':
                continue
            #only keep unsuppressed rows
            suppress = x[16]
            if suppress == 'O' or suppress == 'E':
                continue
            source = x[11]
            termtype = x[12]
            term = x[14]
            #While we're here, if this thing is snomed, lets get it
            if source == 'SNOMEDCT_US':
                snomed_id = f'SNOMEDCT:{x[15]}'
                if termtype == 'PT':
                    snolabels.write(f'{snomed_id}\t{term}\n')
                snosyns.write(f'{snomed_id}\thttp://www.geneontology.org/formats/oboInOwl#hasExactSynonym\t{term}\n')
            #UMLS is a collection of sources. They pick one of the names from these sources for a concept,
            # and that's based on a priority that they define. Here we get the priority for terms so we
            # can get the right one for the label
            pkey = (source,termtype,suppress)
            try:
                pri= priority[pkey]
            except:
                #print(pkey)
                pri = 1000000
            rows[cui].append( (pri,term,line) )
    lname = make_local_name('labels', subpath='UMLS')
    sname = make_local_name('synonyms', subpath='UMLS')
    with open(lname,'w') as labels, open(sname,'w') as synonyms:
        for cui,crows in rows.items():
            crows.sort()
            labels.write(f'UMLS:{cui}\t{crows[0][1]}\n')
            syns = set( [crow[1] for crow in crows])
            for s in syns:
                synonyms.write(f'UMLS:{cui}\thttp://www.geneontology.org/formats/oboInOwl#hasExactSynonym\t{s}\n')

