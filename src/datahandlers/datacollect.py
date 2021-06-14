from src.ubergraph import UberGraph
from src.babel_utils import make_local_name, pull_via_ftp
from collections import defaultdict
import os, gzip
from json import loads,dumps

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
                alias_symbols = gene['alias_symbol']
                for asym in alias_symbols:
                    sfile.write(f'{hgnc_id}\thttp://www.geneontology.org/formats/oboInOwl#hasRelatedSynonym\t{asym}\n')
            if 'alias_name' in gene:
                alias_names = gene['alias_name']
                for asym in alias_names:
                    sfile.write(f'{hgnc_id}\thttp://www.geneontology.org/formats/oboInOwl#hasRelatedSynonym\t{asym}\n')


def pull_prot(which,refresh):
    #swissname = pull_via_ftplib('ftp.uniprot.org','/pub/databases/uniprot/current_release/knowledgebase/complete/',f'uniprot_{which}.fasta.gz',decompress_data=True,outfilename=f'uniprot_{which}.fasta')
    if refresh:
        swissname = pull_via_urllib('ftp://ftp.uniprot.org/pub/databases/uniprot/current_release/knowledgebase/complete/',f'uniprot_{which}.fasta.gz')
    else:
        swissname = make_local_name(f'uniprot_{which}.fasta')
    swissprot_labels = {}
    nlines = 0
    maxn = 1000
    with open(swissname,'r') as inf:
        for line in inf:
            nlines += 1
            if line.startswith('>'):
                #example fasta line:
                #>sp|Q6GZX4|001R_FRG3G Putative transcription factor 001R OS=Frog virus 3 (isolate Goorha) OX=654924 GN=FV3-001R PE=4 SV=1
                x = line.split('|')
                uniprotid = f'UniProtKB:{x[1]}'
                name = x[2].split(' OS=')[0]
                swissprot_labels[uniprotid] = f'{name} ({which})'
            #if nlines > maxn:
            #    break
    print('numlines',nlines)
    print('nl',len(swissprot_labels))
    swissies = [ (k,) for k in swissprot_labels.keys() ]
    print('s',len(swissies))
    return swissies, swissprot_labels

def pull_prots(refresh_swiss=False,refresh_trembl=False):
    swiss,labels = pull_prot('sprot',refresh_swiss)
    fname = make_local_name('labels', subpath='UNIPROTKB')
    with open(fname,'w') as synonyms:
        for k,v in labels.items():
            synonyms.write(f'{k}\t{v}\n')
        tremb,tlabels = pull_prot('trembl',refresh_trembl)
        for k,v in tlabels.items():
            synonyms.write(f'{k}\t{v}\n')

if __name__ == '__main__':
    #pull_ubers()
    #pull_mesh_labels()
    #pull_umls()
    #pull_pubchem()
    #pull_hgnc()
    pull_prots()

