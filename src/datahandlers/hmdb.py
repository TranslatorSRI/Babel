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

def handle_metabolite(metabolite,lfile,sfile):
    print(metabolite.keys())
    hmdbident=f'{HMDB}:{metabolite["accession"]}'
    label = metabolite['name']
    lfile.write(f'{hmdbident}\t{label}\n')
    print(metabolite['synonyms'], len(metabolite['synonyms']))
    syns = metabolite['synonyms']
    if 'synonym' in syns:
        print( syns['synonym'] )
        for sname in syns['synonym']:
            sfile.write(f'{hmdbident}\oio:exact\t{sname}\n')
    print('end')

def make_labels_and_synonyms(inputfile,labelfile,synfile):
    with open(inputfile,'r') as inf:
        xml = inf.read()
    parsed = xmltodict.parse(xml)
    metabolites = parsed['hmdb']['metabolite']
    with open(labelfile,'w') as lfile, open(synfile,'w') as sfile:
        for metabolite in metabolites:
            handle_metabolite(metabolite,lfile,sfile)
