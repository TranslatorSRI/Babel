import requests
from src.util import Text
from src.LabeledID import LabeledID

class NodeFactory:
    def __init__(self):
        self.url_base = 'http://robokop.renci.org:8144/bl'
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

    def create_node(self,input_identifiers,node_type):
        #This is where we will normalize, i.e. choose the best id, and add types in accord with BL.
        #we should also include provenance and version information for the node set build.
        ancestors = self.get_ancestors(node_type)
        prefixes = self.get_prefixes(node_type)
        idmap = { Text.get_curie(i).upper(): i for i in list(input_identifiers) }
        identifiers = []
        accepted_ids = set()
        for p in prefixes:
            pupper = p.upper()
            if pupper in idmap:
                identifiers.append(Text.recurie(idmap[pupper],p))
                accepted_ids.add(idmap[pupper])
        #Warn if we have prefixes that we're ignoring
        for k,v in idmap.items():
            if v not in accepted_ids and (k,node_type) not in self.ignored_prefixes:
                print(f'Ignoring prefix {k} for type {node_type}, identifier {v}')
                self.ignored_prefixes.add( (k,node_type) )
        best_id = identifiers[0]
        if isinstance(best_id, LabeledID):
            best_id = best_id.identifier
        label = None
        # identifiers is in preferred order, so choose the first non-empty label to be the node label
        labels = list(filter(lambda x:len(x) > 0, [ l.label for l in identifiers if isinstance(l,LabeledID) ]))
        if len(labels) > 0:
            label = labels[0]
        node = {
            'id': best_id,
            'equivalent_identifiers': identifiers,
            'type': [node_type] + self.get_ancestors(node_type)
        }
        if label is not None:
            node['label'] = label
        return node