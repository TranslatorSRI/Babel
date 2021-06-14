from src.prefixes import OIO, ORPHANET
from src.babel_utils import pull_via_urllib
import json
from zipfile import ZipFile
#ugh XML
import xml.etree.ElementTree as ET

def pull_orphanet():
    pull_via_urllib('http://www.orphadata.org/data/RD-CODE/Packs/','Orphanet_Nomenclature_Pack_EN.zip', subpath='Orphanet', decompress=False)

def pull_orphanet_labels_and_synonyms(infile,labelfile,synonymfile):
    #Everything in DOID is a disease.
    with ZipFile(infile, 'r') as myzip:
        bytes = myzip.read('Orphanet_Nomenclature_Pack_EN/ORPHAnomenclature_en.xml')
        print(len(bytes))
    root=ET.fromstring(bytes)
    with open(labelfile,'w') as labels, open(synonymfile,'w') as syns:
        for dl in root:
            if dl.tag == 'DisorderList':
                for disorder in dl:
                    oc = disorder.find('OrphaCode').text
                    curie = f'{ORPHANET}:{oc}'
                    name = disorder.find('Name').text
                    labels.write(f'{curie}\t{name}\n')
                    syns.write(f'{curie}\t{OIO}:hasExactSynonym\t{name}\n')
                    allsyns = disorder.find('SynonymList')
                    if allsyns is None:
                        continue
                    for syn in allsyns:
                        syns.write(f'{curie}\t{OIO}:hasExactSynonym\t{syn.text}\n')
