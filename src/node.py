import requests
from src.util import Text
from src.LabeledID import LabeledID
from collections import defaultdict
import os
from bmt import Toolkit
from src.prefixes import PUBCHEMCOMPOUND

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
        for ident in node['identifiers']:
            thisid = ident['identifier']
            pref = Text.get_curie(thisid)
            if not pref in self.synonyms:
                self.load_synonyms(pref)
            node_synonyms.update( self.synonyms[pref][thisid] )
        return node_synonyms

class InformationContentFactory:
    def __init__(self,ic_file):
        self.ic = {}
        with open(ic_file, 'r') as inf:
            for line in inf:
                x = line.strip().split('\t')
                node_id = Text.obo_to_curie(x[0][:-1]) # -1 takes off the >
                ic = x[2]
                self.ic[node_id] = ic
            print(f"Loaded {len(self.ic)} InformationContent values")

    def get_ic(self, node):
        ICs = []
        for ident in node['identifiers']:
            thisid = ident['identifier']
            if thisid in self.ic:
               ICs.append(self.ic[thisid])
        if len(ICs) == 0:
            return None
        return min(ICs)


class NodeFactory:
    def __init__(self,label_dir,biolink_version):
        self.toolkit = Toolkit(f'https://raw.githubusercontent.com/biolink/biolink-model/v{biolink_version}/biolink-model.yaml')
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
        print(input_type)
        j = self.toolkit.get_element(input_type)
        prefs = j['id_prefixes']
        if input_type == 'biolink:Protein':
            prefs=['UniProtKB','PR','ENSEMBL','FB','UMLS']
        elif len(prefs) == 0:
            print('no prefixes for', input_type, 'Using small molecules')
            prefs = self.get_prefixes("biolink:SmallMolecule")
        elif input_type == 'biolink:Polypeptide':
            prefs = list(set(prefs + self.get_prefixes('biolink:SmallMolecule')))
        elif input_type == 'biolink:ChemicalEntity':
            #This just has to be here for now
            prefs = list(set(prefs + self.get_prefixes('biolink:SmallMolecule')))
        #The pref are in a particular order, but apparently it can have dups (ugh)
        # The particular dups are gone now, but the code remains in case they come back...
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

    def create_node(self,input_identifiers,node_type,labels={},extra_prefixes=[]):
        #This is where we will normalize, i.e. choose the best id, and add types in accord with BL.
        #we should also include provenance and version information for the node set build.
        #ancestors = self.get_ancestors(node_type)
        #ancestors.reverse()
        prefixes = self.get_prefixes(node_type) + extra_prefixes
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
        # Except for PUBCHEMs.  They get their own special mess.
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
                if pupper == PUBCHEMCOMPOUND.upper() and len(newids) > 1:
                    newids = pubchemsort(newids,cleaned)
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
            'identifiers': identifiers,
            'type': node_type
        }
        #if label is not None:
        #    node['id']['label'] =  label
        return node

def pubchemsort(pc_ids, labeled_ids):
    """Figure out the correct ordering of pubchem identifiers.
       pc_ids is a list of tuples of (identifier,json) where json = {"identifier":id, "label":x}
       but may not have a label.
       It is just for the pubchems
       labeled_ids is a list of the other ids.  The entries can be a bare string for stuff w/o a label
       or a labeled ID for stuff with a label."""
    # For most types / prefixes we're just sorting the allowed id's.  This gives us a consistent ID from run to run
    # But there's a special case: The biolink-preferred identifier for chemicals is PUBCHEM.COMPOUND.
    # Out merging is based on INCHIKEYS.  However, it happens all the time that more than one PC has the same  inchikey
    # (because of the way they discard hydrogens).
    # This leads to some nastiness e.g. with water.  There are 2 pubchems with the same inchikey.  One is
    # H2O (water) and one is H.OH (hydron;hydroxide).  Just a lexical sorting of the identifiers puts the crap one first.
    # Observations: 1. there are many other identifiers e.g. mesh chebi etc that have the same label (water).
    # 2. almost always the shortest name is best
    # 2a. With the exception of titles that are CID somthing or are SMILES...
    # So here we're going to try a couple things: first we're going to see if we can match other labels.
    # Failing that,  we'll take the shortest non CID name.  Hard to recognize smiles but we can see if that turns
    # into a problem or not.
    label_counts = defaultdict(int)
    pclabels = {}
    for lid in labeled_ids:
        try:
            if lid.identifier.startswith(PUBCHEMCOMPOUND):
                pclabels[lid.label.upper()] = lid.identifier
            else:
                label_counts[lid.label.upper()] += 1
        except:
            pass
    matches = [ (label_counts[pclabel],pcident) for pclabel,pcident in pclabels.items() ]
    matches.sort()
    if len(matches) == 0:
        best = (0,'')
    else:
        best = matches[-1]
    #There are two cases here: we matched something (best[0] > 0) or we didn't (best[0] == 0)
    if best[0] > 0:
        best_pubchem_id = best[1]
    else:
        try:
            #now we are going to pick the shortest pubchem label that isn't CID something
            lens = [ (len(pclabel), pcident) for pclabel,pcident in pclabels.items() if not pclabel.startswith('CID') ]
            lens.sort()
            if len(lens) > 0:
                best_pubchem_id = lens[0][1]
            else:
                just_ids = list(pclabels.values())
                just_ids.sort()
                best_pubchem_id = just_ids[0]
        except:
            #Gross, there just aren't any labels
            best_pubchem_id = sorted(pc_ids)[0][0]
    for pcelement in pc_ids:
        pcid,_ = pcelement
        if pcid == best_pubchem_id:
            best_pubchem = pcelement
    pc_ids.remove(best_pubchem)
    return [best_pubchem] + pc_ids
