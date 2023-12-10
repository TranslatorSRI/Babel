import logging
import subprocess
from ftplib import FTP
from io import BytesIO
import gzip
from datetime import datetime as dt
from datetime import timedelta
import time
import requests
import os
import urllib
import jsonlines
from src.node import NodeFactory, SynonymFactory, DescriptionFactory, InformationContentFactory, get_config
from src.util import Text
from src.LabeledID import LabeledID
from collections import defaultdict
import sqlite3
from typing import List, Tuple

def make_local_name(fname,subpath=None):
    config = get_config()
    if subpath is None:
        return os.path.join(config['download_directory'],fname)
    odir = os.path.join(config['download_directory'],subpath)
    try:
        os.makedirs(odir)
    except:
        pass
    return os.path.join(odir,fname)


class StateDB():
    def __init__(self,fname):
        self.dbname = make_local_name(fname)
        new = True
        if os.path.exists(self.dbname):
            new = False
        self.connection = sqlite3.connect(self.dbname)
        if new:
            self.initialize_db()
    def initialize_db(self):
        curr = self.connection.cursor()
        curr.execute('CREATE TABLE cache (key text, value text)')
        self.connection.commit()
    def get(self,key):
        curr = self.connection.cursor()
        curr.execute('SELECT value FROM cache WHERE key=?', (key,))
        result = curr.fetchone()
        if result is not None:
            return result[0]
        return None
    def put(self,key,value):
        curr=self.connection.cursor()
        curr.execute(f"INSERT INTO cache VALUES (?,?)",(key,value))
        self.connection.commit()

#The signature here should be modified to be like pull via urlllib
def pull_via_ftp(ftpsite, ftpdir, ftpfile, decompress_data=False, outfilename=None):
    """Retrieve data via ftp.
    Setting decompress=True will ungzip the data
    If outfilename is None (default) then the data will be returned.
    Otherwise it will be written to the downloads directory."""
    ftp = FTP(ftpsite)
    ftp.login()
    ftp.cwd(ftpdir)
    print('   getting data')
    config = get_config()
    if outfilename is None:
        with BytesIO() as data:
            ftp.retrbinary(f'RETR {ftpfile}', data.write)
            ftp.quit()
            binary = data.getvalue()
            if decompress_data:
                return gzip.decompress(binary).decode()
            else:
                return binary.decode()
    ofilename = os.path.join(config['download_directory'],outfilename)
    odir = os.path.abspath(os.path.dirname(ofilename))
    if not os.path.exists(odir):
        os.makedirs(odir)
    print(f'  writing data to {ofilename}')
    print(f'{ftpsite}/{ftpdir}/{ftpfile}')
    if not decompress_data:
        with open(ofilename,'wb') as ofile:
            ftp.retrbinary(f'RETR {ftpfile}', ofile.write)
            ftp.quit()
    else:
        with BytesIO() as data:
            ftp.retrbinary(f'RETR {ftpfile}', data.write)
            ftp.quit()
            value = gzip.decompress(data.getvalue()).decode()
        with open(ofilename,'w') as ofile:
            ofile.write(value)
    return ofilename

def dump_dict(outdict,outfname):
    config = get_config()
    oname = os.path.join(os.path.dirname(__file__),config['download_directory'], outfname)
    with open(oname,'w') as outf:
        for k,v in outdict.items():
            outf.write(f'{k}\t{v}\n')

def dump_dicts(dicts,fname):
    config = get_config()
    oname = os.path.join(os.path.dirname(__file__),config['download_directory'], fname)
    with open(oname,'w') as outf:
        for k in dicts:
            outf.write(f'{k}\t{dicts[k]}\n')

def dump_sets(sets,fname):
    config = get_config()
    oname = os.path.join(os.path.dirname(__file__),config['download_directory'], fname)
    print('dumping: ',oname)
    with open(oname,'w') as outf:
        for s in sets:
            outf.write(f'{s}\n')

class ThrottledRequester:
    """Make sure that the time from the last call to the current call is greater than or equal to
    a configurable delta.   Wait before making request to ensure this. Used to make sure eutils
    doesn't get angry.  Returns the json, as well as a flag whether this call waited or not."""
    def __init__(self,delta_ms):
        self.last_time = None
        self.delta = timedelta(milliseconds = delta_ms)
    def get(self,url):
        now = dt.now()
        throttled=False
        if self.last_time is not None:
            cdelta = now - self.last_time
            if cdelta < self.delta:
                waittime = self.delta - cdelta
                time.sleep(waittime.microseconds / 1e6)
                throttled = True
        self.last_time = dt.now()
        response = requests.get(url)
        return response, throttled
    def get_json(self,url):
        """Add retries to the throttling, return json"""
        ntries = 0
        maxtries = 100
        while ntries < maxtries:
            try:
                response,_ = self.get(url)
                result = response.json()
                return result
            except Exception as e:
                ntries += 1



def pull_via_urllib(url: str, in_file_name: str, decompress = True, subpath=None):
    """
    Retrieve files via urllib, optionally decompresses it, and writes it locally into downloads
    url: str - the url with the correct version attached
    in_file_name: str - the name of the target file to work
    returns: str - the output file name
    """
    #Everything goes in downloads
    download_dir = get_config()['download_directory']
    working_dir = download_dir

    # get the (local) download file name, derived from the input file name
    if subpath is None:
        dl_file_name = os.path.join(download_dir,in_file_name)
    else:
        dl_file_name = os.path.join(download_dir,subpath,in_file_name)

    # Add support for redirects
    opener = urllib.request.build_opener(urllib.request.HTTPRedirectHandler())

    # get a handle to the ftp file
    print(url+in_file_name)
    handle = opener.open(url + in_file_name)

    # create the compressed file
    with open(dl_file_name, 'wb') as compressed_file:
        # while there is data
        while True:
            # read a block of data
            data = handle.read(1024)

            # fif nothing read about
            if len(data) == 0:
                break

            # write out the data to the output file
            compressed_file.write(data)

    if decompress:
        out_file_name = dl_file_name[:-3]

        # create the output text file
        with open(out_file_name, 'w') as output_file:
            # open the compressed file
            with gzip.open(dl_file_name, 'rt') as compressed_file:
                for line in compressed_file:
                    # write the data to the output file
                    output_file.write(line)

        #remove the compressed file
        os.remove(dl_file_name)
    else:
        out_file_name = dl_file_name

    # return the filename to the caller
    return out_file_name


def pull_via_wget(
        url_prefix: str,
        in_file_name: str,
        decompress=True,
        subpath:str=None,
        continue_incomplete:bool=True,
        retries:int=10):
    """
    Download a file using wget. We call wget from the command line, and use command line options to
    request continuing incomplete downloads.

    :param url_prefix: The URL prefix to download.
    :param in_file_name: The filename to download -- this will be concatenated to the URL prefix. This should include
        the compression extension (e.g. `.gz`); we will remove that extension during decompression.
    :param decompress: Whether this is a Gzip file that should be decompressed after download.
    :param subpath: The subdirectory of `babel_download` where this file should be stored.
    :param continue_incomplete: Should wget continue an incomplete download?
    :param retries: The number of retries to attempt.
    """

    # Prepare download URL and location
    download_dir = get_config()['download_directory']
    url = url_prefix + in_file_name
    if subpath:
        dl_file_name = os.path.join(download_dir, subpath, in_file_name)
    else:
        dl_file_name = os.path.join(download_dir, in_file_name)

    # Prepare wget options.
    wget_command_line = [
        'wget',
        '--progress=bar:force:noscroll',
    ]
    if continue_incomplete:
        wget_command_line.append('--continue')
    if retries > 0:
        wget_command_line.append(f'--tries={retries}')

    # Add URL and output file.
    wget_command_line.append(url)
    wget_command_line.extend(['-O', dl_file_name])

    # Execute wget.
    logging.info(f"Downloading {dl_file_name} using wget: {wget_command_line}")
    process = subprocess.run(wget_command_line)
    if process.returncode != 0:
        raise RuntimeError(f"Could not execute wget {wget_command_line}: {process.stderr}")

    # Decompress the downloaded file if needed.
    uncompressed_filename = None
    if decompress:
        if dl_file_name.endswith('.gz'):
            uncompressed_filename = dl_file_name[:-3]
            process = subprocess.run(['gunzip', dl_file_name])
            if process.returncode != 0:
                raise RuntimeError(f"Could not execute gunzip ['gunzip', {dl_file_name}]: {process.stderr}")
        else:
            raise RuntimeError(f"Don't know how to decompress {in_file_name}")

    if os.path.isfile(uncompressed_filename):
        file_size = os.path.getsize(uncompressed_filename)
        if file_size > 0:
            logging.info(f"Downloaded {uncompressed_filename} from {url}, file size {file_size} bytes.")
    else:
        raise RuntimeError(f'Expected uncompressed file {uncompressed_filename} does not exist.')



def sort_identifiers_with_boosted_prefixes(identifiers, prefixes):
    """
    Given a list of identifiers (with `identifier` and `label` keys), sort them using
    the following rules:
    - Any identifier that has a prefix in prefixes is sorted based on its order in prefixes.
    - Any identifier that does not have a prefix in prefixes is left in place.

    :param identifiers: A list of identifiers to sort. This is a list of dictionaries
        containing `identifier` and `label` keys, and possible others that we ignore.
    :param prefixes: A list of prefixes, in the order in which they should be boosted.
        We assume that CURIEs match these prefixes if they are in the form `{prefix}:...`.
    :return: The list of identifiers sorted as described above.
    """

    # Thanks to JetBrains AI.
    return sorted(
        identifiers,
        key=lambda identifier: prefixes.index(identifier.split(':', 1)[0]) if identifier.split(':', 1)[0] in prefixes else len(prefixes)
    )


def write_compendium(synonym_list,ofname,node_type,labels={},extra_prefixes=[],icrdf_filename=None):
    """
    :param synonym_list:
    :param ofname:
    :param node_type:
    :param labels:
    :param extra_prefixes: We default to only allowing the prefixes allowed for a particular type in Biolink.
        If you want to allow additional prefixes, list them here.
    :param icrdf_filename: (REQUIRED) The file to read the information content from (icRDF.tsv). Although this is a
        named parameter to make it easier to specify this when calling write_compendium(), it is REQUIRED, and
        write_compendium() will throw a RuntimeError if it is not specified. This is to ensure that it has been
        properly specified as a prerequisite in a Snakemake file, so that write_compendium() is not run until after
        icRDF.tsv has been generated.
    :return:
    """
    config = get_config()
    cdir = config['output_directory']
    biolink_version = config['biolink_version']
    node_factory = NodeFactory(make_local_name(''),biolink_version)
    synonym_factory = SynonymFactory(make_local_name(''))

    # Load the preferred_name_boost_prefixes -- this tells us which prefixes to boost when
    # coming up with a preferred label for a particular Biolink class.
    preferred_name_boost_prefixes = config['preferred_name_boost_prefixes']

    # Create an InformationContentFactory based on the specified icRDF.tsv file. Default to the one in the download
    # directory.
    if not icrdf_filename:
        raise RuntimeError("No icrdf_filename parameter provided to write_compendium() -- this is required!")
    ic_factory = InformationContentFactory(icrdf_filename)

    description_factory = DescriptionFactory(make_local_name(''))
    node_test = node_factory.create_node(input_identifiers=[],node_type=node_type,labels={},extra_prefixes = extra_prefixes)
    with jsonlines.open(os.path.join(cdir,'compendia',ofname),'w') as outf, jsonlines.open(os.path.join(cdir,'synonyms',ofname),'w') as sfile:
        for slist in synonym_list:
            node = node_factory.create_node(input_identifiers=slist, node_type=node_type,labels = labels, extra_prefixes = extra_prefixes)
            if node is not None:
                nw = {"type": node['type']}
                ic = ic_factory.get_ic(node)
                if ic is not None:
                    nw['ic'] = ic

                descs = description_factory.get_descriptions(node)
                nw['identifiers'] = []
                for nids in node['identifiers']:
                    id_info = {'i': nids['identifier']}

                    if 'label' in nids:
                        id_info['l'] = nids['label']

                    if id_info['i'] in descs:
                        # Sort from the shortest description to the longest.
                        id_info['d'] = list(sorted(descs[id_info['i']], key=lambda x: len(x)))

                    nw['identifiers'].append(id_info)

                outf.write( nw )

                # get_synonyms() returns tuples in the form ('http://www.geneontology.org/formats/oboInOwl#hasExactSynonym', 'Caudal articular process of eighteenth thoracic vertebra')
                # But we're only interested in the synonyms themselves, so we can skip the relationship for now.
                curie = node["identifiers"][0]["identifier"]
                synonyms = [result[1] for result in synonym_factory.get_synonyms(node)]
                # Why are we running the synonym list through set() again? Because get_synonyms returns unique pairs of (relation, synonym).
                # So multiple identical synonyms may be returned as long they have a different relation. But since we don't care about the
                # relation, we should get rid of any duplicated synonyms here.
                synonyms_list = sorted(set(synonyms), key=lambda x: len(x))
                try:
                    types = node_factory.get_ancestors(node["type"])
                    document = {"curie": curie,
                                "names": synonyms_list,
                                "types": [t[8:] for t in types]} # remove biolink:

                    # To pick a preferred label for this clique, we need to do three things:
                    # 1. We sort all labels in the preferred-name order. By default, this should be
                    #    the preferred CURIE order, but if this clique is in one of the Biolink classes in
                    #    preferred_name_boost_prefixes, we boost those prefixes in that order to the top of the list.
                    # 2. We filter out any suspicious labels.
                    #    (If this simple filter doesn't work, and if prefixes are inconsistent, we can build upon the
                    #    algorithm proposed by Jeff at
                    #    https://github.com/NCATSTranslator/Feedback/issues/259#issuecomment-1605140850)
                    # 3. We choose the first label that isn't blank. If no labels remain, we generate a warning.

                    # Step 1.1. Sort labels in boosted prefix order if possible.
                    possible_labels = []
                    for typ in types:
                        if typ in preferred_name_boost_prefixes:
                            # This is the most specific matching type, so we use this.
                            possible_labels = map(lambda identifier: identifier.get('label', ''),
                                sort_identifiers_with_boosted_prefixes(
                                    node["identifiers"],
                                    preferred_name_boost_prefixes["typ"]
                                ))
                            break

                    # Step 1.2. If we didn't have a preferred_name_boost_prefixes, just use the identifiers in their
                    # Biolink prefix order.
                    if not possible_labels:
                        possible_labels = map(lambda identifier: identifier.get('label', ''), node["identifiers"])

                    # Step 2. Filter out any suspicious labels.
                    filtered_possible_labels = [l for l in possible_labels if
                                                l and                               # Ignore blank or empty names.
                                                not l.startswith('CHEMBL')          # Some CHEMBL names are just the identifier again.
                                                ]

                    # Step 3. Pick the first label that isn't blank.
                    if filtered_possible_labels:
                        document["preferred_name"] = filtered_possible_labels[0]
                    else:
                        logging.warning(f"No preferred name for {node}")

                    # We previously used the shortest length of a name as a proxy for how good a match it is, i.e. given
                    # two concepts that both have the word "acetaminophen" in them, we assume that the shorter one is the
                    # more interesting one for users. I'm not sure if there's a better way to do that -- for instance,
                    # could we consider the information content values? -- but in the interests of getting something
                    # working quickly, this code restores that previous method.

                    # Since synonyms_list is sorted,
                    if len(synonyms_list) == 0:
                        logging.debug(f"Synonym list for {node} is empty: no valid name. Skipping.")
                        continue
                    else:
                        document["shortest_name_length"] = len(synonyms_list[0])

                    # We want to see if we can use the CURIE suffix to sort concepts with similar identifiers.
                    # We want to sort this numerically, so we only do this if the CURIE suffix is numerical.
                    curie_parts = curie.split(':', 1)
                    if len(curie_parts) > 0:
                        # Try to cast the CURIE suffix to an integer. If we get a ValueError, don't worry about it.
                        try:
                            document["curie_suffix"] = int(curie_parts[1])
                        except ValueError:
                            pass

                    sfile.write( document )
                except Exception as ex:
                    print(f"Exception thrown while write_compendium() was generating {ofname}: {ex}")
                    print(node["type"])
                    print(node_factory.get_ancestors(node["type"]))
                    exit()

def glom(conc_set, newgroups, unique_prefixes=['INCHIKEY'],pref='HP',close={}):
    """We want to construct sets containing equivalent identifiers.
    conc_set is a dictionary where the values are these equivalent identifier sets and
    the keys are all of the elements in the set.   For each element in a set, there is a key
    in the dictionary that points to the set.
    newgroups is an iterable that of new equivalence groups (expressed as sets,tuples,or lists)
    with which we want to update conc_set."""
    n = 0
    bad = 0
    shit_prefixes=set(['KEGG','PUBCHEM'])
    test_id = 'xUBERON:0002262'
    debugit = False
    excised = set()
    for xgroup in newgroups:
        if isinstance(xgroup,frozenset):
            group = set(xgroup)
        else:
            group = xgroup
        #As of now, xgroup should never be more than two things
        if len(xgroup) > 2:
            print(xgroup)
            print('nope nope nope')
            raise ValueError
        n+=1
        if debugit:
            print("new group",group)
        if test_id in group:
            print('higroup',group)
        #Find all the equivalence sets that already correspond to any of the identifiers in the new set.
        existing_sets_w_x = [ (conc_set[x],x) for x in group if x in conc_set ]
        #All of these sets are now going to be combined through the equivalence of our new set.
        existing_sets = [ es[0] for es in existing_sets_w_x ]
        x =  [ es[1] for es in existing_sets_w_x ]
        newset=set().union(*existing_sets)
        if debugit:
            print("merges:",existing_sets)
        #put all the new stuff in it.  Do it element-wise, cause we don't know the type of the new group
        for element in group:
            newset.add(element)
        if test_id in newset:
            print('hiset',newset)
            print('input_set',group)
            print('esets')
            for eset in existing_sets:
                print(' ',eset,group.intersection(eset))
        for check_element in newset:
            prefix = check_element.split(':')[0]
            if prefix in shit_prefixes:
                print(prefix)
                print(check_element)
                raise Exception('garbage')
        if debugit:
            print("final set",newset)
        #make sure we didn't combine anything we want to keep separate
        setok = True
        if test_id in group:
            print('setok?',setok)
        for up in unique_prefixes:
            if test_id in group:
                print('up?',up)
            idents = [e if type(e)==str else e.identifier for e in newset]
            if len(set([e for e in idents if (e.split(':')[0] ==up)])) > 1:
                bad += 1
                setok = False
                wrote = set()
                for s in existing_sets:
                    fs = frozenset(s)
                    wrote.add(fs)
                for gel in group:
                    if Text.get_curie(gel) == pref:
                        killer = gel
                #for preset in wrote:
                #    print(f'{killer}\t{set(group).intersection(preset)}\t{preset}\n')
                #print('------------')
        NPC = sum(1 for s in newset if s.startswith("PUBCHEM.COMPOUND:"))
        if (("PUBCHEM.COMPOUND:3100" in newset) and (NPC > 3)):
            if debugit:
                l = sorted(list(newset))
                print("bad")
                for li in l:
                    print(li)
                exit()
        if (not setok):
            #Our new group created a new set that merged stuff we didn't want to merge.
            #Previously we did a lot of fooling around at this point.  But now we're just going to say, I have a
            # pairwise concordance.  That can at most link two groups.  just don't link them. In other words,
            # we are simply ignoring this concordance.
            continue
            #Let's figure out the culprit(s) and excise them
            #counts = defaultdict(int)
            #for x in group:
            #    counts[x] += 1
            ##THe way existing sets was created, means that the same set can be in there twice, and we don't want to
            # count things that way
            #unique_existing_sets = []
            #for ex in existing_sets:
            #    u = True
            #    for q in unique_existing_sets:
            #        if ex == q:
            #            u = False
            #    if u:
            #        unique_existing_sets.append(ex)
            #for es in unique_existing_sets:
            #    for y in es:
            #        counts[y] += 1
            #bads = [ x for x,y in counts.items() if y > 1 ]
            #now we know which identifiers are causing trouble.
            #We don't want to completely throw them out, but we can't allow them to gum things up.
            #So, we need to first remove them from all the sets, then we need to put them in their own set
            #It might be good to track this somehow?
            #excised.update(bads)
            #for b in bads:
            #    if b in group:
            #        group.remove(b)
            #    for exset in existing_sets:
            #        if b in exset:
            #            exset.remove(b)
            #    conc_set[b] = set([b])
            #for x in group:
            #    conc_set[x] = group
            #continue
        #Now check the 'close' dictionary to see if we've accidentally gotten to a close match becoming an exact match
        setok = True
        for cpref, closedict in close.items():
            idents = set([e if type(e) == str else e.identifier for e in newset])
            prefidents = [e for e in idents if e.startswith(cpref)]
            for pident in prefidents:
                for cd in closedict[pident]:
                    if cd in newset:
                        setok = False
            if len(prefidents) == 0:
                continue
        if not setok:
            continue
        #Now make all the elements point to this new set:
        for element in newset:
            conc_set[element] = newset

def get_prefixes(idlist):
    """ Return a dictionary of identifiers from idlist with their prefix as the key.

    :param idlist: A list of identifiers. Should NOT contain any LabeledIDs.
    """
    prefs = defaultdict(list)
    for ident in idlist:
        if isinstance(ident,LabeledID):
            print('nonono')
            exit()
            prefs.add(Text.get_curie(ident.identifier))
        else:
            prefs[Text.get_curie(ident)].append(ident)
    return prefs


def clean_sets(result_dict):
    """The keys for this are unique and unmergable: Don't merge GO!
    But there are values that are showing up in multiple GOs (could be
    MetaCycs or RHEAs or Reactomes).  It's just how GO is mapping.  Now,
    the right answer here is probably to kboom this whole mess.  But 
    for prototype, we're just going to filter out garbage merge values).
    Note that this isn't limited to GO. Even MONDO include some #exactMatch
    to the same MESH from two different MONDO ids"""
    cmap = defaultdict(int)
    for v in result_dict.values():
        for x in v:
            cmap[x] += 1
    bad_values = [ k for k,v in cmap.items() if v > 1 ]
    for bv in bad_values:
        if bv.startswith('Meta'):
            print(bv)
    for k,v in result_dict.items():
        newv = [ vi for vi in v if vi not in bad_values ]
        result_dict[k] = newv
    return result_dict

def filter_out_non_unique_ids(old_list):
    """
    filters out elements that exist accross rows
    eg input [{'z', 'x', 'y'}, {'z', 'n', 'm'}]
    output [{'x', 'y'}, {'m', 'n'}]
    """
    idcounts = defaultdict(int)
#    mondomap = defaultdict(list)
    for terms in old_list:
        for term in terms:
            idcounts[term] += 1
#            mondomap[term].append(terms)
    bad_ids = set( [k for k,v in idcounts.items() if v > 1])
#    for b in bad_ids:
#        mm = mondomap[b]
#        mondos = []
#        for ms in mm:
#            for x in ms:
#                if Text.get_curie(x) == 'MONDO':
#                    mondos.append(x)
#        print(b, mondos)
    new_list = list(map(
        lambda term_list : \
        set(
            filter(
                lambda term: term not in bad_ids,
                term_list
            )), old_list))
    return new_list


def read_identifier_file(infile):
    """Identifier files are mostly just lists of identifiers that constitutes all the id's from a given
    source that should be included in a normalization run.   There is an optional second column that contains
    a hint to the normalizer about the proper biolink type for this entity."""
    types = {}
    identifiers = list()
    with open(infile,'r') as inf:
        for line in inf:
            x = line.strip().split('\t')
            identifiers.append((x[0],))
            if len(x) > 1:
                types[x[0]] = x[1]
    return identifiers,types


def remove_overused_xrefs(pairlist: List[Tuple], bothways:bool = False):
    """Given a list of tuples (id1, id2) meaning id1-[xref]->id2, remove any id2 that are associated with more
    than one id1.  The idea is that if e.g. id1 is made up of UBERONS and 2 of those have an xref to say a UMLS
    then it doesn't mean that all of those should be identified.  We don't really know what it means, so remove it."""
    xref_counts_v = defaultdict(int)
    xref_counts_k = defaultdict(int)
    for k, v in pairlist:
        xref_counts_v[v] += 1
        xref_counts_k[k] += 1
    improved_pairs = []
    for k,v in pairlist:
        if xref_counts_v[v] < 2:
            if bothways:
                if xref_counts_k[k] < 2:
                    improved_pairs.append( (k,v) )
            else:
                improved_pairs.append((k, v))
    return improved_pairs

def norm(x,op):
    #Get curie returns the uppercase
    pref = Text.get_curie(x)
    if pref in op:
        return Text.recurie(x,op[pref])
    return x