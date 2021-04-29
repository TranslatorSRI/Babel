from src.babel_utils import pull_via_urllib
import json

mods = ['WB', 'FB', 'ZFIN', 'MGI', 'RGD', 'SGD']

def pull_mods():
    for mod in mods:
        pull_via_urllib('https://fms.alliancegenome.org/download/',f'GENE-DESCRIPTION-JSON_{mod}.json.gz',subpath=mod)

def write_labels(dd):
    for mod in mods:
        with open(f'{dd}/{mod}/GENE-DESCRIPTION-JSON_{mod}.json','r') as inf:
            j = json.load(inf)
        with open(f'{dd}/{mod}/labels','w') as outf:
            for gene in j['data']:
                outf.write(f'{gene["gene_id"]}\t{gene["gene_name"]}\n')