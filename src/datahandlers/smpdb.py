from zipfile import ZipFile
from os import path
from src.prefixes import SMPDB
from src.babel_utils import pull_via_urllib

def pull_smpdb():
    dname = pull_via_urllib('http://smpdb.ca/downloads/','smpdb_pathways.csv.zip',decompress=False,subpath='SMPDB')
    ddir = path.dirname(dname)
    with ZipFile(dname, 'r') as zipObj:
        zipObj.extractall(ddir)

def make_labels(inputfile,labelfile):
    """Get the SMPDB file.  It's not good - there are \n and commas, and commas are also the delimiter. I mean, what?"""
    with open(inputfile,'r') as inf, open(labelfile,'w') as outf:
        h = inf.readline()
        for line in inf:
            if ',' not in line:
                continue
            if not line.startswith('SMP'):
                continue
            #print(line)
            #my god what a stupid file.  It's a csv, but there are commas in the data.  Not only that
            # but the last column is quoted, but the next to the last column (which also has commas) is not.
            frontline = line.split('"')[0][:-1] #remove the last (quoted) column
            x = frontline.strip().split(',')
            ident = f'{SMPDB}:{x[0]}'
            name = ','.join(x[2:]) #get the rest of the splits and put them back together.
            if len(name) > 0:
                outf.write(f'{ident}\t{name}\n')
