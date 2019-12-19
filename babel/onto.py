import json
import requests
from src.util import LoggingUtil
from src.LabeledID import LabeledID


logger = LoggingUtil.init_logging(__name__)

class Onto():
    """UberGraph contains many obo ontologies that we can ask questions of.  Onto wraps that blazegraph
    instance in some simple APIs to make it easy to ask basic questions of the ontologies.  If more complicated
    queries are required UberGraph can be directly queried with sparql"""
    def __init__(self):
        self.url='https://onto.renci.org'

    def get(self,url):
        obj=None
        rv = requests.get(url)
        if rv.status_code == 200:
            obj = rv.json()
        return obj

    def get_ids(self,ontology_name):
        u=f"{self.url}/id_list/{ontology_name.upper()}"
        obj = self.get(u)
        return obj

    def is_a(self,identifier,candidate_ancestor):
        obj = self.get(f"{self.url}/is_a/{identifier}/{candidate_ancestor}")
        if obj is None:
            return False
        return obj is not None and 'is_a' in obj and obj['is_a']

    def get_label(self,identifier):
        """ Get the label for an identifier. """
        obj = self.get(f"{self.url}/label/{identifier}")
        return obj['label'] if obj and 'label' in obj else None

    def search(self,name,is_regex=False, full=False):
        """ Search ontologies for a term. """
        gurl=f"{self.url}/search/{name}?regex={'true' if is_regex else 'false'}"
        print(gurl)
        obj = self.get(gurl)
        results = []
        if full:
            results = obj['values'] if 'values' in obj else []
        else:
            results = [ v['id'] for v in obj['values'] ] if obj and 'values' in obj else []
        return results

    def get_xrefs(self,identifier, filter=None):
        """ Get external references. Optionally filter results. """
        obj = self.get(f"{self.url}/xrefs/{identifier}")
        result = []
        if 'xrefs' in obj:
            for xref in obj['xrefs']:
                if filter:
                    for f in filter:
                        if 'id' in xref:
                            if xref['id'].startswith(f):
                                result.append (xref['id'])
                else:
                    result.append (xref)
        return result

    def get_exact_matches(self,identifier):
        """ Get exact matches.  Seems to be mostly a MONDO thing """
        obj = self.get(f"{self.url}/exactMatch/{identifier}")
        result = []
        try:
            if 'exact matches' in obj:
                result.extend(obj['exact matches'])
        except Exception as e:
            print(identifier)
            exit()
        return result

    def get_synonyms(self,identifier,curie_pattern=None):
        return self.get(f"{self.url}/synonyms/{identifier}")

    def lookup(self,identifier):
        obj = self.get(f"{self.url}/lookup/{identifier}")
        if obj == None : logger.warning(f'Error: {self.url}/lookup/{identifier} returned {None} ')
        return [ ref["id"] for ref in obj['refs'] ] if obj and 'refs' in obj else []

    def get_anscestors(self, identifier):
        return self.get(f"{self.url}/superterms/{identifier}")
    
    def get_parents(self, identifier):
        return self.get(f"{self.url}/parents/{identifier}")['parents']

    def get_children(self, identifier):
        return self.get(f"{self.url}/children/{identifier}")

