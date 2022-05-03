from src.babel_utils import make_local_name, pull_via_ftp
from src.prefixes import UMLS
from collections import defaultdict
import os

def write_umls_ids(category_map,umls_output,blacklist=set()):
    categories = set(category_map.keys())
    mrsty = os.path.join('input_data', 'private', 'MRSTY.RRF')
    umls_keepers = set()
    with open(mrsty,'r') as inf, open(umls_output,'w') as outf:
        for line in inf:
            x = line.strip().split('|')
            cat = x[2]
            if cat in categories:
                if not x[0] in blacklist:
                    outf.write(f'{UMLS}:{x[0]}\t{category_map[cat]}\n')


#I've made this more complicated than it ought to be for 2 reasons:
# One is to keep from having to pass through the umls file more than once, but that's a bad reason
# The second is because I want to use the UMLS as a source for some terminologies (SNOMED) even if there's another
#  way.  I'm going to modify this to do one thing at a time, and if it takes a little longer, then so be it.
def build_sets(umls_input, umls_output , other_prefixes, bad_mappings=defaultdict(set), acceptable_identifiers={}):
    """Given a list of umls identifiers we want to generate all the concordances
    between UMLS and that other entity"""
    umls_ids = set()
    with open(umls_input) as inf:
        for line in inf:
            u = line.strip().split('\t')[0].split(':')[1]
            umls_ids.add(u)
    lookfor = set(other_prefixes.keys())
    mrconso = os.path.join('input_data', 'private', 'MRCONSO.RRF')
    pairs = set()
    #test_cui = 'C0026827'
    with open(mrconso,'r') as inf, open(umls_output,'w') as concordfile:
        for line in inf:
            x = line.strip().split('|')
            cui = x[0]
            if cui not in umls_ids:
                continue
            lang = x[1]
            #Only keep english terms
            if lang != 'ENG':
                continue
            #only keep unsuppressed rows
            suppress = x[16]
            if suppress == 'O' or suppress == 'E':
                continue
            #only keep sources we're looking for
            source = x[11]
            if source not in lookfor:
                continue
            #For some dippy reason, in the id column they say "HGNC:76"
            pref = other_prefixes[source]
            if ':' in x[13]:
                other_id = f'{pref}:{x[13].split(":")[-1]}'
            else:
                other_id = f'{pref}:{x[13]}'
            #I don't know why this is in here, but it is not an identifier equivalent to anything
            if other_id == 'NCIT:TCGA':
                continue
            tup = (f'{UMLS}:{cui}',other_id)
            #Don't include bad mappings or bad ids
            if tup[1] in bad_mappings[tup[0]]:
                continue
            if (pref in acceptable_identifiers) and (not tup[1] in acceptable_identifiers[pref]):
                continue
            if tup not in pairs:
                concordfile.write(f'{tup[0]}\teq\t{tup[1]}\n')
                pairs.add(tup)

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
    mrcon = os.path.join('input_data', 'private', 'MRCONSO.RRF')
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
            labels.write(f'{UMLS}:{cui}\t{crows[0][1]}\n')
            syns = set( [crow[1] for crow in crows])
            for s in syns:
                synonyms.write(f'{UMLS}:{cui}\thttp://www.geneontology.org/formats/oboInOwl#hasExactSynonym\t{s}\n')

