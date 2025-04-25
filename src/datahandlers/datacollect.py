from src.prefixes import HGNC, UNIPROTKB
from src.properties import PrefixPropertyStore
from src.babel_utils import make_local_name, pull_via_ftp, pull_via_urllib
from json import loads

def pull_hgnc():
    data = pull_via_ftp('ftp.ebi.ac.uk', '/pub/databases/genenames/new/json', 'hgnc_complete_set.json')
    hgnc_json = loads(data)
    lname = make_local_name('labels', subpath=HGNC)
    sname = make_local_name('synonyms', subpath=HGNC)
    with PrefixPropertyStore(prefix=HGNC, autocommit=False) as pps:
        for gene in hgnc_json['response']['docs']:
            hgnc_id =gene['hgnc_id']
            symbol = gene['symbol']
            pps.insert_values(curie=hgnc_id, prop='label', values=[symbol], source="datacollect.py:pull_hgnc()")

            name = gene['name']
            pps.insert_values(curie=hgnc_id, prop='hasExactSynonym', values=[name], source="datacollect.py:pull_hgnc()")
            if 'alias_symbol' in gene:
                alias_symbols = gene['alias_symbol']
                for asym in alias_symbols:
                    pps.insert_values(curie=hgnc_id, prop='hasRelatedSynonym', values=[asym], source="datacollect.py:pull_hgnc()")
            if 'alias_name' in gene:
                alias_names = gene['alias_name']
                for asym in alias_names:
                    pps.insert_values(curie=hgnc_id, prop='hasRelatedSynonym', values=[asym], source="datacollect.py:pull_hgnc()")

        with open(lname,'w') as lfile, open(sname,'w') as sfile:
            pps.to_tsv(lfile, include_properties=False)
            pps.to_tsv(sfile, include_properties=True)


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
                uniprotid = f'{UNIPROTKB}:{x[1]}'
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
    fname = make_local_name('labels', subpath=UNIPROTKB)
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

