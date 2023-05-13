from zipfile import ZipFile
from os import path,listdir,rename
from src.prefixes import HMDB
from src.babel_utils import pull_via_urllib
import xmltodict

def pull_hmdb():
    dname = pull_via_urllib('https://hmdb.ca/system/downloads/current/','hmdb_metabolites.zip',decompress=False,subpath='HMDB')
    ddir = path.dirname(dname)
    with ZipFile(dname, 'r') as zipObj:
        zipObj.extractall(ddir)

def handle_metabolite(metabolite,lfile,synfile,smifile):
    hmdbident=f'{HMDB}:{metabolite["accession"]}'
    label = metabolite['name']
    lfile.write(f'{hmdbident}\t{label}\n')
    syns = metabolite['synonyms']
    if (syns is not None) and ('synonym' in syns):

        # In some cases, syns['synonym'] may be a single string.
        # If so, we turn it into a single-element list.
        synonyms_list = syns['synonym']
        if not isinstance(synonyms_list, list):
            synonyms_list = [synonyms_list]

        for sname in synonyms_list:
            synfile.write(f'{hmdbident}\toio:exact\t{sname}\n')
    if 'smiles' in metabolite:
        smifile.write(f'{hmdbident}\t{metabolite["smiles"]}\n')

def make_labels_and_synonyms_and_smiles(inputfile,labelfile,synfile,smifile):
    with open(inputfile,'r') as inf:
        xml = inf.read()
    parsed = xmltodict.parse(xml)
    metabolites = parsed['hmdb']['metabolite']
    with open(labelfile,'w') as lfile, open(synfile,'w') as sfile, open(smifile,'w') as smiles:
        for metabolite in metabolites:
            handle_metabolite(metabolite,lfile,sfile,smiles)
