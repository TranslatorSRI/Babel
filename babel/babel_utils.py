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
from babel.node import create_node

def pull_via_ftp(ftpsite, ftpdir, ftpfile, decompress_data=False, outfilename=None):
    """Retrieve data via ftp.
    Setting decompress=True will ungzip the data
    If outfilename is None (default) then the data will be returned.
    Otherwise it will be written to the downloads directory."""
    ftp = FTP(ftpsite)
    ftp.login()
    ftp.cwd(ftpdir)
    with BytesIO() as data:
        ftp.retrbinary(f'RETR {ftpfile}', data.write)
        binary = data.getvalue()
    ftp.quit()
    if decompress_data:
        value = gzip.decompress(binary).decode()
    else:
        value = binary.decode()
    if outfilename is None:
        return value
    ofilename = os.path.join(os.path.dirname(os.path.abspath(__file__)),'downloads',outfilename)
    with open(ofilename,'w') as ofile:
        ofile.write(value)
    return ofilename

def dump_dict(outdict,outfname):
    oname = os.path.join(os.path.dirname(__file__),'downloads', outfname)
    with open(oname,'w') as outf:
        for k,v in outdict.items():
            outf.write(f'{k}\t{v}\n')

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
        return requests.get(url).json(), throttled


def pull_via_urllib(url: str, in_file_name: str, decompress = True):
    """
    Retrieve files via urllib, optionally decompresses it, and writes it locally into downloads
    url: str - the url with the correct version attached
    in_file_name: str - the name of the target file to work
    returns: str - the output file name
    """
    #Everything goes in downloads
    working_dir = 'downloads'
    # get the (local) download file name, derived from the input file name
    dl_file_name = os.path.join(os.path.dirname(os.path.abspath(__file__)),'downloads',in_file_name)

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

def write_compendium(synonym_list,ofname,node_type):
    cdir = os.path.dirname(os.path.abspath(__file__))
    with jsonlines.open(os.path.join(cdir,'compendia',ofname),'w') as outf:
        for slist in synonym_list:
            outf.write( create_node(identifiers=slist, node_type=node_type) )

def glom(conc_set, newgroups, unique_prefixes=['INCHI']):
    """We want to construct sets containing equivalent identifiers.
    conc_set is a dictionary where the values are these equivalent identifier sets and
    the keys are all of the elements in the set.   For each element in a set, there is a key
    in the dictionary that points to the set.
    newgroups is an iterable that of new equivalence groups (expressed as sets,tuples,or lists)
    with which we want to update conc_set."""
    for group in newgroups:
        #Find all the equivalence sets that already correspond to any of the identifiers in the new set.
        existing_sets = [ conc_set[x] for x in group if x in conc_set ]
        #All of these sets are now going to be combined through the equivalence of our new set.
        newset=set().union(*existing_sets)
        #put all the new stuff in it.  Do it element-wise, cause we don't know the type of the new group
        for element in group:
            newset.add(element)
        #make sure we didn't combine anything we want to keep separate
        setok = True
        for up in unique_prefixes:
            idents = [e if type(e)==str else e.identifier for e in newset]
            if len([1 for e in idents if e.startswith(up)]) > 1:
                setok = False
                break
        if not setok:
            continue
        #Now make all the elements point to this new set:
        for element in newset:
            conc_set[element] = newset

