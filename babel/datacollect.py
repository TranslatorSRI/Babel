from babel.ubergraph import UberGraph
from babel.babel_utils import make_local_name, pull_via_ftp
from collections import defaultdict
import os, gzip
from json import loads,dumps

def pull_uber_labels():
    uber = UberGraph()
    labels = uber.get_all_labels()
    ldict = defaultdict(set)
    for unit in labels:
        iri = unit['iri']
        p = iri.split(':')[0]
        ldict[p].add( ( unit['iri'], unit['label'] ) )
    for p in ldict:
        if p not in ['http','ro'] and not p.startswith('t') and not '#' in p:
            fname = make_local_name('labels',subpath=p)
            with open(fname,'w') as outf:
                for unit in ldict[p]:
                    outf.write(f'{unit[0]}\t{unit[1]}\n')

def pull_uber_synonyms():
    uber = UberGraph()
    labels = uber.get_all_synonyms()
    print(len(labels))
    ldict = defaultdict(set)
    for unit in labels:
        iri = unit[0]
        p = iri.split(':')[0]
        ldict[p].add(  unit )
    for p in ldict:
        print(p)
        if p not in ['http','ro'] and not p.startswith('t') and not '#' in p:
            fname = make_local_name('synonyms',subpath=p)
            with open(fname,'w') as outf:
                for unit in ldict[p]:
                    outf.write(f'{unit[0]}\t{unit[1]}\t{unit[2]}\n')

def pull_ubers():
    pull_uber_labels()
    pull_uber_synonyms()

def pull_mesh_labels():
    data = pull_via_ftp('ftp.nlm.nih.gov', '/online/mesh/rdf', 'mesh.nt.gz', decompress_data=True)
    fname = make_local_name('labels', subpath='MESH')
    badlines = 0
    with open(fname, 'w') as outf:
        for line in data.split('\n'):
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

def read_umls_priority():
    mrp = os.path.join(os.path.dirname(__file__), 'input_data', 'umls_precedence.txt')
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
    mrcon = os.path.join(os.path.dirname(__file__), 'input_data', 'MRCONSO.RRF')
    rows = defaultdict(list)
    priority = read_umls_priority()
    with open(mrcon, 'r') as inf:
        for line in inf:
            x = line.strip().split('|')
            cui = x[0]
            lang = x[1]
            if lang != 'ENG':
                continue
            suppress = x[16]
            if suppress == 'O' or suppress == 'E':
                continue
            source = x[11]
            termtype = x[12]
            term = x[14]
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


def pull_pubchem_labels():
    print('LABEL PUBCHEM')
    f_name =  'CID-Title.gz'
    cname = pull_via_ftp('ftp.ncbi.nlm.nih.gov','/pubchem/Compound/Extras/', f_name, outfilename=f_name)
    fname = make_local_name('labels', subpath='PUBCHEM.COMPOUND')
    with open(fname, 'w') as outf, gzip.open(cname,mode='rt',encoding='latin-1') as inf:
        for line in inf:
            x = line.strip().split('\t')
            outf.write(f'PUBCHEM.COMPOUND:{x[0]}\t{x[1]}\n')

def pull_pubchem_synonyms():
    f_name = 'CID-Synonym-filtered.gz'
    sname = pull_via_ftp('ftp.ncbi.nlm.nih.gov', '/pubchem/Compound/Extras/', f_name, outfilename=f_name)
    fname = make_local_name('synonyms', subpath='PUBCHEM.COMPOUND')
    with open(fname, 'w') as outf, gzip.open(sname,mode='rt',encoding='latin-1') as inf:
        for line in inf:
            x = line.strip().split('\t')
            if x[1].startswith('CHEBI'):
                continue
            if x[1].startswith('SCHEMBL'):
                continue
            outf.write(f'PUBCHEM.COMPOUND:{x[0]}\thttp://www.geneontology.org/formats/oboInOwl#hasRelatedSynonym\t{x[1]}\n')

def pull_pubchem():
    pull_pubchem_labels()
    pull_pubchem_synonyms()

def pull_hgnc():
    data = pull_via_ftp('ftp.ebi.ac.uk', '/pub/databases/genenames/new/json', 'hgnc_complete_set.json')
    hgnc_json = loads(data)
    lname = make_local_name('labels', subpath='HGNC')
    sname = make_local_name('synonyms', subpath='HGNC')
    with open(lname,'w') as lfile, open(sname,'w') as sfile:
        for gene in hgnc_json['response']['docs']:
            hgnc_id =gene['hgnc_id']
            symbol = gene['symbol']
            lfile.write(f'{hgnc_id}\t{symbol}\n')
            name = gene['name']
            sfile.write(f'{hgnc_id}\thttp://www.geneontology.org/formats/oboInOwl#hasExactSynonym\t{name}\n')
            if 'alias_symbol' in gene:
                print('alias symbol')
                alias_symbols = gene['alias_symbol']
                for asym in alias_symbols:
                    sfile.write(f'{hgnc_id}\thttp://www.geneontology.org/formats/oboInOwl#hasRelatedSynonym\t{asym}\n')
            if 'alias_name' in gene:
                print('alias name')
                alias_names = gene['alias_name']
                for asym in alias_names:
                    sfile.write(f'{hgnc_id}\thttp://www.geneontology.org/formats/oboInOwl#hasRelatedSynonym\t{asym}\n')


if __name__ == '__main__':
    #pull_ubers()
    #pull_mesh_labels()
    #pull_umls()
    #pull_pubchem()
    pull_hgnc()

