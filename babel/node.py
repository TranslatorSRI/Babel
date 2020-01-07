import requests
from src.util import Text

class NodeFactory:
    def __init__(self):
        self.url_base = 'http://robokop.renci.org:8144/blm'
        self.ancestor_map = {}
        self.prefix_map = {}

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
        url = f'{self.url_base}/{input_type}/prefixes/'
        response = requests.get(url)
        prefs = response.json()
        self.prefix_map[input_type] = prefs
        return prefs

    def create_node(self,input_identifiers,node_type):
        #This is where we will normalize, i.e. choose the best id, and add types in accord with BL.
        #we should also include provenance and version information for the node set build.
        ancestors = self.get_ancestors(node_type)
        prefixes = self.get_prefixes(node_type)
        idmap = { Text.get_curie(i): i for i in list(input_identifiers) }
        identifiers = []
        for p in prefixes:
            if p in idmap:
                identifiers.append(idmap[p])
        node = {
            'id': identifiers[0],
            'equivalent_identifiers': identifiers,
            'type': [node_type]
        }
        return node