import logging

#from src.LabeledID import LabeledID
from src.util import LoggingUtil
from babel.babel_utils import write_compendium, pull_via_urllib, get_config
from zipfile import ZipFile

#logger = LoggingUtil.init_logging(__name__, level=logging.ERROR)

def pull_smpdb():
    """Get the SMPDB file.  It's not good - there are \n and commas, and commas are also the delimiter. I mean, what?"""
    dname = pull_via_urllib('http://smpdb.ca/downloads/','smpdb_pathways.csv.zip',decompress=False)
    ddir = get_config()['download_directory']
    with ZipFile(dname, 'r') as zipObj:
        zipObj.extractall(ddir)
    infname = f'{ddir}/smpdb_pathways.csv'
    smpdbs = []
    labels = {}
    with open(infname,'r') as inf:
        h = inf.readline()
        for line in inf:
            if ',' not in line:
                continue
            if not line.startswith('SMP'):
                continue
            #print(line)
            x = line.strip().split(',')
            ident = f'SMPDB:{x[0]}'
            name = x[2]
            smpdbs.append( (ident,) )
            labels[ident] = name
    return smpdbs,labels


def load_pathways():
    """
    Right now, we're just pulling SMPDB, but that's not very satisfying
    """
    smpdb,labels = pull_smpdb()
    write_compendium(smpdb,'pathways.txt','pathway', labels=labels)


if __name__ == '__main__':
    load_pathways()
