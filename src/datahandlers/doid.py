from src.prefixes import DOID
from src.babel_utils import pull_via_urllib
import json

def pull_doid():
    pull_via_urllib('https://raw.githubusercontent.com/DiseaseOntology/HumanDiseaseOntology/main/src/ontology/','doid.json', subpath='DOID', decompress=False)

def get_doid_labels_and_synonyms(infile,labelfile,synonymfile):
    #Everything in DOID is a disease.
    with open(infile,'r') as inf:
        j = json.load(inf)
    with open(labelfile,'w') as labels, open(synonymfile,'w') as syns:
        for entry in j['graphs'][0]['nodes']:
            if 'deprecated' in entry['meta'] and entry['meta']['deprecated'] == True:
                continue
            doid_id = entry['id']
            doid_curie = f'{DOID}:{doid_id.split("_")}'
            label = entry['lbl']
            labels.write(f'{doid_id}\t{label}\n')
            syns.write(f'{doid_id}\thttp://www.geneontology.org/formats/oboInOwl#hasExactSynonym\t{label}\n')
