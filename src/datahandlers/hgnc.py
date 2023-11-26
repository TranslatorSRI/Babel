from src.babel_utils import make_local_name, pull_via_urllib
import json

def pull_hgnc():
    # On 2023nov26, I would get an error trying to download this file using FTP on Python (although
    # weirdly enough, I could download the file without any problem using macOS Finder). So I changed
    # it to use HTTP instead.
    pull_via_urllib(
        'https://ftp.ebi.ac.uk/pub/databases/genenames/new/json/',
        'hgnc_complete_set.json',
        decompress=False,
        subpath="HGNC")

def pull_hgnc_labels_and_synonyms(infile):
    with open(infile,'r') as data:
        hgnc_json = json.load(data)
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