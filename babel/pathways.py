import logging

#from src.LabeledID import LabeledID
from src.util import LoggingUtil
from babel.babel_utils import write_compendium, pull_via_urllib, get_config, pull_via_ftp,glom
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

def pull_panther():
    data = pull_via_ftp('ftp.pantherdb.org',
                        '/pathway/current_release/',
                        'SequenceAssociationPathway3.6.5.txt')
    lines = data.split('\n')
    labels = {}
    for line in lines:
        x = line.strip().split('\t')
        if len(x) < 2:
            print(x)
            continue
        pw_id = f'PANTHER.PATHWAY:{x[0]}'
        name = x[1]
        labels[pw_id] = name
    pw_identifiers = [ (i,) for i in labels]
    return pw_identifiers,labels

def load_pathways():
    """
    Right now, we're just pulling SMPDB, but that's not very satisfying
    """
    smpdb,labels = pull_smpdb()
    panth,labels_2 = pull_panther()
    print(len(panth))
    print(len(labels_2))
    labels.update(labels_2)
    #No need to glom atm
    identifiers = smpdb + panth
    write_compendium(identifiers,'pathways.txt','biolink:Pathway', labels=labels)


if __name__ == '__main__':
    load_pathways()
