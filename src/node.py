import requests
from src.util import Text
from src.LabeledID import LabeledID
from collections import defaultdict
import os
from bmt import Toolkit

class SynonymFactory():
    def __init__(self,syndir):
        self.synonym_dir = syndir
        self.synonyms = {}

    def load_synonyms(self,prefix):
        print(f'Loading {prefix}')
        lbs = defaultdict(set)
        labelfname = os.path.join(self.synonym_dir, prefix, 'labels')
        if os.path.exists(labelfname):
            with open(labelfname, 'r') as inf:
                for line in inf:
                    x = line.strip().split('\t')
                    lbs[x[0]].add( ('http://www.geneontology.org/formats/oboInOwl#hasExactSynonym',x[1]) )
        synfname = os.path.join(self.synonym_dir, prefix, 'synonyms')
        if os.path.exists(synfname):
            with open(synfname, 'r') as inf:
                for line in inf:
                    x = line.strip().split('\t')
                    if len(x) < 3:
                        continue
                    lbs[x[0]].add( (x[1], x[2]) )
        self.synonyms[prefix] = lbs
        print(f'Loaded')

    def get_synonyms(self,node):
        node_synonyms = set()
        for ident in node['equivalent_identifiers']:
            thisid = ident['identifier']
            pref = Text.get_curie(thisid)
            if not pref in self.synonyms:
                self.load_synonyms(pref)
            node_synonyms.update( self.synonyms[pref][thisid] )
        return node_synonyms

class NodeFactory:
    def __init__(self,label_dir):
        #self.url_base = 'http://arrival.edc.renci.org:32511/bl'
        self.url_base = 'https://bl-lookup-sri.renci.org/bl'
        self.toolkit = Toolkit('https://raw.githubusercontent.com/biolink/biolink-model/1.6.1/biolink-model.yaml')
        self.ancestor_map = {}
        self.prefix_map = {}
        self.ignored_prefixes = set()
        self.extra_labels = {}
        self.label_dir = label_dir

    def get_ancestors(self,input_type):
        if input_type in self.ancestor_map:
            return self.ancestor_map[input_type]
        a = self.toolkit.get_ancestors(input_type)
        ancs = [ self.toolkit.get_element(ai)['class_uri'] for ai in a ]
        if input_type not in ancs:
            ancs = [input_type] + ancs
        self.ancestor_map[input_type] = ancs
        return ancs

    def get_prefixes(self,input_type):
        if input_type in self.prefix_map:
            return self.prefix_map[input_type]
        url = f'{self.url_base}/{input_type}'
        response = requests.get(url)
        try:
            j = response.json()
            prefs = j['id_prefixes']
        except:
            #this is a mega hack to deal with the taxon change
            prefs = ['NCBITaxon','MESH']
        #The pref are in a particular order, but apparently it can have dups (ugh)
        newprefs = ['']
        for pref in prefs:
            if not pref  == newprefs[-1]:
                newprefs.append(pref)
        prefs = newprefs[1:]
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

    def load_extra_labels(self,prefix):
        labelfname = os.path.join(self.label_dir,prefix,'labels')
        lbs = {}
        if os.path.exists(labelfname):
            with open(labelfname,'r') as inf:
                for line in inf:
                    x = line.strip().split('\t')
                    lbs[x[0]] = x[1]
        self.extra_labels[prefix] = lbs

    def apply_labels(self, input_identifiers, labels):
        #Originally we needed to clean up the identifer lists, because there would be both labeledids and
        # string ids and we had to reconcile them.
        # But now, we only allow regular ids in the list, and now we need to turn some of them into labeled ids for output
        labeled_list = []
        for iid in input_identifiers:
            if isinstance(iid,LabeledID):
                print('LabeledID dont belong here, pass in labels seperately',iid)
                exit()
            if iid in labels:
                labeled_list.append( LabeledID(identifier=iid, label = labels[iid]))
            else:
                prefix = Text.get_prefix(iid)
                if prefix not in self.extra_labels:
                    self.load_extra_labels(prefix)
                if iid in self.extra_labels[prefix]:
                    labeled_list.append( LabeledID(identifier=iid, label = self.extra_labels[prefix][iid]))
                else:
                    labeled_list.append(iid)
        return labeled_list

    def create_node(self,input_identifiers,node_type,labels={}):
        #This is where we will normalize, i.e. choose the best id, and add types in accord with BL.
        #we should also include provenance and version information for the node set build.
        ancestors = self.get_ancestors(node_type)
        #ancestors.reverse()
        prefixes = self.get_prefixes(node_type)
        if len(input_identifiers) == 0:
            return None
        if len(input_identifiers) > 1000:
            print('this seems like a lot')
            print(len(input_identifiers))
        cleaned = self.apply_labels(input_identifiers,labels)
        try:
            idmap = defaultdict(list)
            for i in list(cleaned):
                idmap[Text.get_curie(i).upper()].append(i)
        except AttributeError:
            print('something very bad')
            print(input_identifiers)
            print(len(input_identifiers))
            for i in list(input_identifiers):
                print(i)
                print(type(i))
                print(Text.get_curie(i))
                print(Text.get_curie(i).upper())
            exit()
        identifiers = []
        accepted_ids = set()
        #Converting identifiers from LabeledID to dicts
        #In order to be consistent from run to run, we need to worry about the
        # case where e.g. there are 2 UMLS id's and UMLS is the preferred pref.
        # We're going to choose the canonical ID here just by sorting the N .
        for p in prefixes:
            pupper = p.upper()
            if pupper in idmap:
                newids = []
                for v in idmap[pupper]:
                    newid = Text.recurie(v,p)
                    jid = self.make_json_id(newid)
                    newids.append( (jid['identifier'],jid) )
                    accepted_ids.add(v)
                newids.sort()
                identifiers += [ nid[1] for nid in newids ]
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
            'type': ancestors
        }
        if label is not None:
            node['id']['label'] =  label
        return node
