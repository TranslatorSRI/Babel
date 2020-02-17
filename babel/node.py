import requests
from src.util import Text
from src.LabeledID import LabeledID
from collections import defaultdict

class NodeFactory:
    def __init__(self):
        #self.url_base = 'http://arrival.edc.renci.org:32511/bl'
        self.url_base = 'https://bl-lookup-sri.renci.org/bl'
        self.ancestor_map = {}
        self.prefix_map = {}
        self.ignored_prefixes = set()

    def get_ancestors(self,input_type):
        if input_type in self.ancestor_map:
            return self.ancestor_map[input_type]
        url = f'{self.url_base}/{input_type}/ancestors/'
        response = requests.get(url)
        ancs = response.json()
        self.ancestor_map[input_type] = ancs
        return ancs

    def get_prefixes(self,input_type):
        if input_type in self.prefix_map:
            return self.prefix_map[input_type]
        url = f'{self.url_base}/{input_type}'
        response = requests.get(url)
        j = response.json()
        prefs = j['id_prefixes']
        self.prefix_map[input_type] = prefs
        return prefs

    def make_json_id(self,input):
        if isinstance(input,LabeledID):
            if input.label is not None and input.label != '':
                return {'identifier': input.identifier, 'label': input.label}
            return {'identifier': input.identifier}
        return {'identifier': input}

    def clean_list(self,input_identifiers):
        #Sometimes we end up with something like [(HP:123,'name'),HP:123,UMLS:3445] Clean up
        cleanup = defaultdict(list)
        for x in list(input_identifiers):
            if isinstance(x,LabeledID):
                cleanup[x.identifier].append(x)
            else:
                cleanup[x].append(x)
        cleaned = []
        for v in cleanup.values():
            if len(v) == 1:
                cleaned.append(v[0])
            else:
                #Originally, we were just trying to get the LabeledID.  But sometimes we get more than one, so len(v)
                # can be more than two.
                wrote = False
                for vi in v:
                    if isinstance(vi,LabeledID):
                        cleaned.append(vi)
                        wrote = True
                        break
                if not wrote:
                    print(input_identifiers)
                    exit()
        return cleaned

    def create_node(self,input_identifiers,node_type):
        #This is where we will normalize, i.e. choose the best id, and add types in accord with BL.
        #we should also include provenance and version information for the node set build.
        ancestors = self.get_ancestors(node_type)
        ancestors.reverse()
        prefixes = self.get_prefixes(node_type)
        cleaned = self.clean_list(input_identifiers)
        try:
            idmap = defaultdict(list)
            for i in list(cleaned):
                idmap[Text.get_curie(i).upper()].append(i)
        except AttributeError:
            print('something very bad')
            print(input_identifiers)
            for i in list(input_identifiers):
                print(i)
                print(Text.get_curie(i))
                print(Text.get_curie(i).upper())
            exit()
        identifiers = []
        accepted_ids = set()
        #Converting identifiers from LabeledID to dicts
        for p in prefixes:
            pupper = p.upper()
            if pupper in idmap:
                for v in idmap[pupper]:
                    newid = Text.recurie(v,p)
                    identifiers.append(self.make_json_id(newid))
                    accepted_ids.add(v)
        #Warn if we have prefixes that we're ignoring
        for k,vals in idmap.items():
            for v in vals:
                if v not in accepted_ids and (k,node_type) not in self.ignored_prefixes:
                    print(f'Ignoring prefix {k} for type {node_type}, identifier {v}')
                    self.ignored_prefixes.add( (k,node_type) )
        if len(identifiers) == 0:
            return None
        best_id = identifiers[0]['identifier']
        # identifiers is in preferred order, so choose the first non-empty label to be the node label
        labels = list(filter(lambda x:len(x) > 0, [ l['label'] for l in identifiers if 'label' in l ]))
        label = None
        if len(labels) > 0:
            label = labels[0]

        node = {
            'id': {'identifier':best_id,},
            'equivalent_identifiers': identifiers,
            'type': [node_type] + ancestors
        }
        if label is not None:
            node['id']['label'] =  label
        return node