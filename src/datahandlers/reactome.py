from src.prefixes import REACT
from src.categories import PATHWAY, BIOLOGICAL_PROCESS_OR_ACTIVITY, MOLECULAR_ACTIVITY
import requests,json

#Reactome doesn't have a great download, but it does have a decent service that lets you get the files you could have
# downloaded.   In reactome, there are "events" which have subclasses of "pathway" and "reaction like event".
# You can pull down all the events, but it's per-species.  So first you need to get the species.
def pull_reactome(outfile):
    #Get the "main" reactome species
    doc = requests.get('https://reactome.org/ContentService/data/species/main').json()
    species=[(d['taxId'],d['displayName']) for d in doc ]
    elements = []
    for taxid,speciesname in species:
        elements += requests.get(f'https://reactome.org/ContentService/data/eventsHierarchy/{taxid}').json()
    with open(outfile,'w') as outf:
        json.dump(elements,outf)


def make_labels(infile,labelfile):
    with open(infile,'r') as inf:
        elements = json.load(inf)
    with open(labelfile,'w') as labels:
        for element in elements:
            parse_element_for_labels(element,labels)

def parse_element_for_labels(e,lfile):
    oid = e['stId']
    name = e['name']
    species = e['species']
    lfile.write(f'{REACT}:{oid}\t{name} ({species})\n')
    if 'children' in e:
        for child in e['children']:
            parse_element_for_labels(child,lfile)

def write_ids(infile,idfile):
    with open(infile, 'r') as inf:
        elements = json.load(inf)
    with open(idfile, 'w') as outf:
        for element in elements:
            parse_element_for_ids(element, outf)

def parse_element_for_ids(e,lfile):
    oid = e['stId']
    rtype = e['type']
    btypes = {'Pathway':PATHWAY, 'TopLevelPathway':PATHWAY, 'BlackBoxEvent':MOLECULAR_ACTIVITY,
              'Depolymerisation': MOLECULAR_ACTIVITY, 'FailedReaction': MOLECULAR_ACTIVITY,
              'Polymerisation': MOLECULAR_ACTIVITY, 'Reaction': MOLECULAR_ACTIVITY,
              'CellLineagePath': PATHWAY, 'CellDevelopmentStep': BIOLOGICAL_PROCESS_OR_ACTIVITY}
    lfile.write(f'{REACT}:{oid}\t{btypes[rtype]}\n')
    if 'children' in e:
        for child in e['children']:
            parse_element_for_ids(child,lfile)
