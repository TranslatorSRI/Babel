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

class writer:
    def __init__(self,lfile,sfile):
        self.lfile = lfile
        self.sfile = sfile
    def handle_metabolite(self,_,metabolite):
        print(metabolite.keys())
        hmdbident=f'{HMDB}:{metabolite["accession"]}'
        label = metabolite['name']
        self.lfile.write(f'{hmdbident}\t{label}\n')
        print(metabolite['synonyms'], len(metabolite['synonyms']))
        for synel in metabolite['synonyms']:
            sname = synel['synonym']
            self.sfile.write(f'{hmdbident}\oio:exact\t{sname}\n')

def make_labels_and_synonyms(inputfile,labelfile,synfile):
    with open(inputfile,'r') as inf:
        xml = inf.read()
    with open(labelfile,'w') as lf, open(synfile,'w') as sf:
        w = writer(lf,sf)
        xmltodict.parse(xml, item_depth=2, item_callback=w.handle_metabolite)
