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
from babel.node import NodeFactory
from src.util import Text
from src.LabeledID import LabeledID
from json import load
from collections import defaultdict
import sqlite3

def make_local_name(fname):
    config = get_config()
    return os.path.join(os.path.dirname(os.path.abspath(__file__)),config['download_directory'],fname)

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
    ofilename = os.path.join(os.path.dirname(os.path.abspath(__file__)),config['download_directory'],outfilename)
    print(f'  writing data to {ofilename}')
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



def pull_via_urllib(url: str, in_file_name: str, decompress = True):
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
    dl_file_name = os.path.join(os.path.dirname(os.path.abspath(__file__)),download_dir,in_file_name)

    # get a handle to the ftp file
    handle = urllib.request.urlopen(url + in_file_name)

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
    else:
        out_file_name = dl_file_name

    # return the filename to the caller
    return out_file_name

def write_compendium(synonym_list,ofname,node_type,labels={}):
    cdir = os.path.dirname(os.path.abspath(__file__))
    node_factory = NodeFactory()
    with jsonlines.open(os.path.join(cdir,'compendia',ofname),'w') as outf:
        for slist in synonym_list:
            node = node_factory.create_node(input_identifiers=slist, node_type=node_type,labels = labels)
            if node is not None:
                outf.write( node )

def glom(conc_set, newgroups, unique_prefixes=['INCHIKEY'],pref='HP',close={}):
    """We want to construct sets containing equivalent identifiers.
    conc_set is a dictionary where the values are these equivalent identifier sets and
    the keys are all of the elements in the set.   For each element in a set, there is a key
    in the dictionary that points to the set.
    newgroups is an iterable that of new equivalence groups (expressed as sets,tuples,or lists)
    with which we want to update conc_set."""
    n = 0
    bad = 0
    for group in newgroups:
        n+=1
        #Find all the equivalence sets that already correspond to any of the identifiers in the new set.
        existing_sets_w_x = [ (conc_set[x],x) for x in group if x in conc_set ]
        #All of these sets are now going to be combined through the equivalence of our new set.
        existing_sets = [ es[0] for es in existing_sets_w_x ]
        x =  [ es[1] for es in existing_sets_w_x ]
        newset=set().union(*existing_sets)
        #put all the new stuff in it.  Do it element-wise, cause we don't know the type of the new group
        for element in group:
            newset.add(element)
        #make sure we didn't combine anything we want to keep separate
        setok = True
        for up in unique_prefixes:
            idents = [e if type(e)==str else e.identifier for e in newset]
            if len(set([e for e in idents if e.startswith(up)])) > 1:
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
        if not setok:
            continue
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
    #if bad > 0:
    #    print(f'Found {bad} mixups')
        #exit()

def get_prefixes(idlist):
    prefs = set()
    for ident in idlist:
        if isinstance(ident,LabeledID):
            prefs.add(Text.get_curie(ident.identifier))
        else:
            prefs.add(Text.get_curie(ident))
    return prefs

def get_config():
    cname = os.path.join(os.path.dirname(__file__),'..', 'config.json')
    with open(cname,'r') as json_file:
        data = load(json_file)
    return data

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
