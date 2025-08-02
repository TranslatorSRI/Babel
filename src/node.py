import json
import logging
import os
from collections import defaultdict
from urllib.parse import urlparse

import curies

from src.util import Text, get_config, get_biolink_model_toolkit, get_biolink_prefix_map
from src.LabeledID import LabeledID
from src.prefixes import PUBCHEMCOMPOUND


class SynonymFactory:
    """
    A class used to load and retrieve synonyms for given node identifiers

    Attributes:
        synonym_dir (str): The directory where the synonym files are located
        synonyms (dict): A dictionary to store the loaded synonyms

    Methods:
        __init__(syndir)
            Initializes the SynonymFactory with the specified directory

        load_synonyms(prefix)
            Loads the synonyms for a given prefix from the corresponding files

        get_synonyms(node)
            Retrieves the synonyms for a given node from the loaded synonyms

    """

    def __init__(self,syndir):
        self.synonym_dir = syndir
        self.synonyms = {}
        self.common_synonyms = None
        print(f"Created SynonymFactory for directory {syndir}")

    def load_synonyms(self,prefix):
        lbs = defaultdict(set)
        labelfname = os.path.join(self.synonym_dir, prefix, 'labels')
        print(f'Loading synonyms for {prefix} from {labelfname}')
        count_labels = 0
        count_synonyms = 0
        if os.path.exists(labelfname):
            with open(labelfname, 'r') as inf:
                for line in inf:
                    x = line.strip().split('\t')
                    lbs[x[0]].add( ('http://www.geneontology.org/formats/oboInOwl#hasExactSynonym',x[1]) )
                    count_labels += 1
        synfname = os.path.join(self.synonym_dir, prefix, 'synonyms')
        if os.path.exists(synfname):
            with open(synfname, 'r') as inf:
                for line in inf:
                    x = line.strip().split('\t')
                    if len(x) < 3:
                        continue
                    lbs[x[0]].add( (x[1], x[2]) )
                    count_synonyms += 1
        self.synonyms[prefix] = lbs
        print(f'Loaded {count_labels} labels and {count_synonyms} synonyms for {prefix} from {labelfname}')

    def get_synonyms(self,node):
        config = get_config()
        if self.common_synonyms is None:
            # Load the common synonyms.
            self.common_synonyms = defaultdict(set)

            for common_synonyms_file in config['common']['synonyms']:
                common_synonyms_path = os.path.join(config['download_directory'], 'common', common_synonyms_file)
                count_common_file_synonyms = 0
                with open(common_synonyms_path, 'r') as synonymsf:
                    # Note that these files may contain ANY prefix -- we should only fallback to this if we have no other
                    # option.
                    for line in synonymsf:
                        row = json.loads(line)
                        self.common_synonyms[row['curie']].add((row['predicate'], row['synonym']))
                        count_common_file_synonyms += 1
                logging.info(f"Loaded {count_common_file_synonyms} common synonyms from {common_synonyms_path}")

        node_synonyms = set()
        for ident in node['identifiers']:
            thisid = ident['identifier']
            pref = Text.get_prefix(thisid)
            if pref not in self.synonyms:
                self.load_synonyms(pref)
            node_synonyms.update( self.synonyms[pref][thisid] )
            node_synonyms.update( self.common_synonyms.get(thisid, set()) )
        return node_synonyms


class DescriptionFactory:
    """
    Class to handle loading and retrieving descriptions for nodes.
    """

    def __init__(self,rootdir):
        self.root_dir = rootdir
        self.descriptions = {}
        self.common_descriptions = None
        print(f"Created DescriptionFactory for directory {rootdir}")

    def load_descriptions(self,prefix):
        print(f'Loading descriptions for {prefix}')
        descs = defaultdict(set)
        descfname = os.path.join(self.root_dir, prefix, 'descriptions')
        desc_count = 0
        if os.path.exists(descfname):
            with open(descfname, 'r') as inf:
                for line in inf:
                    x = line.strip().split('\t')
                    descs[x[0]].add("\t".join(x[1:]))
                    desc_count += 1
        self.descriptions[prefix] = descs
        print(f'Loaded {desc_count} descriptions for {prefix}')

    def get_descriptions(self,node):
        config = get_config()
        if self.common_descriptions is None:
            # Load the common synonyms.
            self.common_descriptions = defaultdict(list)

            for common_descriptions_file in config['common']['descriptions']:
                common_descriptions_path = os.path.join(config['download_directory'], 'common', common_descriptions_file)
                count_common_file_descriptions = 0
                with open(common_descriptions_path, 'r') as descriptionsf:
                    # Note that these files may contain ANY CURIE -- we should only fallback to this if we have no other
                    # option.
                    for line in descriptionsf:
                        row = json.loads(line)
                        self.common_descriptions[row['curie']].extend(row['descriptions'])
                        count_common_file_descriptions += 1
                logging.info(f"Loaded {count_common_file_descriptions} common descriptions from {common_descriptions_path}")


        node_descriptions = defaultdict(set)
        for ident in node['identifiers']:
            thisid = ident['identifier']
            pref = thisid.split(':', 1)[0]
            if not pref in self.descriptions:
                self.load_descriptions(pref)
            node_descriptions[thisid].update( self.descriptions[pref][thisid] )
            node_descriptions[thisid].update( self.common_descriptions.get(thisid, {}) )
        return node_descriptions


class TaxonFactory:
    """ A factory for loading taxa for CURIEs where available.
    """

    def __init__(self,rootdir):
        self.root_dir = rootdir
        self.taxa = {}

    def load_taxa(self, prefix):
        print(f'Loading taxa for {prefix}')
        taxa_per_prefix = defaultdict(set)
        taxafilename = os.path.join(self.root_dir, prefix, 'taxa')
        taxon_count = 0
        if os.path.exists(taxafilename):
            with open(taxafilename, 'r') as inf:
                for line in inf:
                    x = line.strip().split('\t')
                    taxa_per_prefix[x[0]].add("\t".join(x[1:]))
                    taxon_count += 1
        self.taxa[prefix] = taxa_per_prefix
        print(f'Loaded {taxon_count} taxon-CURIE mappings for {prefix}')

    def get_taxa(self, node):
        node_taxa = defaultdict(set)
        for ident in node['identifiers']:
            thisid = ident['identifier']
            pref = thisid.split(':', 1)[0]
            if not pref in self.taxa:
                self.load_taxa(pref)
            node_taxa[thisid].update(self.taxa[pref][thisid])
        return node_taxa


class InformationContentFactory:
    """

    InformationContentFactory

    A class for creating and using information content objects.

    Attributes:
        ic (dict): A dictionary containing information content values for different nodes.

    Methods:
        __init__(ic_file)
            Initializes an InformationContentFactory object by loading information content values from a file.
            The information content values are stored in the 'ic' attribute of the object.

            Parameters:
                ic_file (str): The path to the file containing the information content values.

        get_ic(node)
            Returns the minimum information content value for a given node.

            Parameters:
                node (dict): The node for which to retrieve the information content value.

            Returns:
                float or None: The minimum information content value for the node,
                               or None if no information content value is found.
    """

    def __init__(self, ic_file):
        config = get_config()
        self.ic = {}

        unmapped_urls = []
        biolink_prefix_map = get_biolink_prefix_map()
        ubergraph_iri_stem_to_prefix_map = curies.Converter.from_reverse_prefix_map(config['ubergraph_iri_stem_to_prefix_map'])

        count_by_prefix = defaultdict(int)
        with open(ic_file, 'r') as inf:
            for line in inf:
                x = line.strip().split('\t')
                # We talk in CURIEs, but the infores download is in URLs. We can use the Biolink
                # prefix map to convert between them.
                node_id = biolink_prefix_map.compress(x[0])
                if node_id is None:
                    # Try the ubergraph_iri_stem_to_prefix_map
                    node_id = ubergraph_iri_stem_to_prefix_map.compress(x[0])

                # If None, log this URL as unmapped.
                if node_id is None:
                    unmapped_urls.append(x[0])

                ic = x[1]
                self.ic[node_id] = float(ic)

                # Track IC values by prefix.
                if isinstance(node_id, str):
                    prefix = node_id.split(':')[0]
                else:
                    # Probably None, but we'll collect everything.
                    prefix = str(node_id)
                count_by_prefix[prefix] += 1

        # Sort the dictionary items by value in descending order
        sorted_by_prefix = sorted(count_by_prefix.items(), key=lambda item: item[1], reverse=True)

        print(f"Loaded {len(self.ic)} InformationContent values from {len(count_by_prefix.keys())} prefixes:")
        # Now you can print the sorted items
        for key, value in sorted_by_prefix:
            print(f'- {key}: {value}')

        # We see a number of URLs being mapped to None (250,871 at present). Let's optionally raise an error if that
        # happens.
        if len(unmapped_urls) > 0:
            # Group unmapped URLs by netloc
            unmapped_urls_by_netloc = defaultdict(list)
            for url in unmapped_urls:
                netloc = urlparse(url).netloc
                unmapped_urls_by_netloc[netloc].append(url)

            # Print them in reverse count order.
            print(f"Found {len(unmapped_urls)} unmapped URLs:")
            netlocs_by_count = sorted(unmapped_urls_by_netloc.items(), key=lambda item: len(item[1]), reverse=True)
            for netloc, urls in netlocs_by_count:
                print(f" - {netloc} [{len(urls)}]")
                for url in sorted(urls):
                    print(f"   - {url}")

            assert None not in sorted_by_prefix, ("Found invalid CURIEs in information content values, probably "
                                                  "because they couldn't be mapped from URLs to CURIEs.")


    def get_ic(self, node):
        ICs = []
        for ident in node['identifiers']:
            thisid = ident['identifier']
            if thisid in self.ic:
                # IC values are numeric values between 0 and 100.
                # Make sure this is a float for min() purposes.
                ICs.append(float(self.ic[thisid]))
        if len(ICs) == 0:
            return None
        return min(ICs)


class NodeFactory:
    def __init__(self,label_dir,biolink_version):
        self.toolkit = get_biolink_model_toolkit(biolink_version)
        self.ancestor_map = {}
        self.prefix_map = {}
        self.ignored_prefixes = set()
        self.extra_labels = {}
        self.label_dir = label_dir
        self.common_labels = None

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
        # biolink doesnt yet include UMLS as a valid prefix for biological process. There is a PR here:
        # https://github.com/biolink/biolink-model/pull/1541
        # once that's merged and makes its way to BMT, we can remove the following hack:
        ### HACK ###
        if input_type == 'biolink:BiologicalProcess':
            prefs.append('UMLS')
        ### END HACK ###
        if len(prefs) == 0:
            raise RuntimeError(f'No Biolink prefixes for {input_type}')
        # The pref are in a particular order, but apparently they can have dups (ugh)
        # We de-duplicate those here.
        prefixes_deduplicated = list()
        for pref in prefs:
            # Don't add a prefix that we've already added.
            if pref in prefixes_deduplicated:
                continue

            prefixes_deduplicated.append(pref)

        self.prefix_map[input_type] = prefixes_deduplicated
        return prefixes_deduplicated


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
        if self.label_dir is None:
            print (f"WARNING: no label_dir specified in load_extra_labels({self}, {prefix}), can't load extra labels for {prefix}. Skipping.")
            return
        if prefix is None:
            print (f"WARNING: no prefix specified in load_extra_labels({self}, {prefix}), can't load extra labels. Skipping.")
            return
        labelfname = os.path.join(self.label_dir,prefix,'labels')
        lbs = {}
        if os.path.exists(labelfname):
            with open(labelfname,'r') as inf:
                for line in inf:
                    x = line.strip().split('\t')
                    lbs[x[0]] = x[1]
        self.extra_labels[prefix] = lbs

    def apply_labels(self, input_identifiers, labels):
        # Before we work on the labels (or try to load any extra labels), let's load up the common labels.
        config = get_config()
        if self.common_labels is None:
            # Load the common labels.
            self.common_labels = {}

            for common_labels_file in config['common']['labels']:
                common_labels_path = os.path.join(config['download_directory'], 'common', common_labels_file)
                count_common_file_labels = 0
                with open(common_labels_path, 'r') as labelf:
                    # Note that these files may contain ANY prefix -- we should only fallback to this if we have no other
                    # option.
                    for line in labelf:
                        x = line.strip().split('\t')
                        curie = x[0]
                        new_label = x[1]
                        if curie in self.common_labels:
                            # We have multiple labels! For simplicity's sake, let's choose the longest one.
                            if len(new_label) <= len(self.common_labels[curie]):
                                continue
                        self.common_labels[x[0]] = x[1]
                        count_common_file_labels += 1
                logging.info(f"Loaded {count_common_file_labels} common labels from {common_labels_path}")

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
                try:
                    prefix = Text.get_prefix(iid)
                except ValueError as e:
                    print(f"ERROR: Unable to apply_labels({self}, {input_identifiers}, {labels}): could not obtain prefix for identifier {iid}")
                    raise e
                if prefix not in self.extra_labels:
                    self.load_extra_labels(prefix)
                if iid in self.extra_labels[prefix]:
                    labeled_list.append( LabeledID(identifier=iid, label = self.extra_labels[prefix][iid]))
                elif iid in self.common_labels:
                    # We only fall back to common labels if the prefix label doesn't have anything.
                    labeled_list.append( LabeledID(identifier=iid, label = self.common_labels[iid]))
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
            print(f'this seems like a lot of input_identifiers in node.create_node() [{len(input_identifiers)}]: {input_identifiers}')
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
                try:
                    newids.sort()
                except TypeError as e:
                    print(newids)
                    raise e
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
