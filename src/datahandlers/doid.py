from src.prefixes import DOID, OIO
from src.babel_utils import pull_via_urllib
import json

def pull_doid():
    pull_via_urllib('https://raw.githubusercontent.com/DiseaseOntology/HumanDiseaseOntology/main/src/ontology/','doid.json', subpath='DOID', decompress=False)

def pull_doid_labels_and_synonyms(infile,labelfile,synonymfile):
    #Everything in DOID is a disease.
    with open(infile,'r') as inf:
        j = json.load(inf)
    with open(labelfile,'w') as labels, open(synonymfile,'w') as syns:
        for entry in j['graphs'][0]['nodes']:
            if ('meta' in entry) and ('deprecated' in entry['meta']) and (entry['meta']['deprecated'] == True):
                continue
            doid_id = entry['id']
            if not doid_id.startswith('http://purl.obolibrary.org/obo/DOID_'):
                continue
            doid_curie = f'{DOID}:{doid_id.split("_")[-1]}'
            if 'lbl' in entry:
                label = entry['lbl']
                labels.write(f'{doid_curie}\t{label}\n')
                syns.write(f'{doid_curie}\t{OIO}:hasExactSynonym\t{label}\n')
            if ('meta' in entry)  and ('synonyms' in entry['meta']):
                for s in entry['meta']['synonyms']:
                    syns.write(f'{doid_curie}\t{OIO}:hasExactSynonym\t{s["val"]}\n')
