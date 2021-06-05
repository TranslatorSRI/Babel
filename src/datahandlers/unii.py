from zipfile import ZipFile
from os import path,listdir,rename
from src.prefixes import UNII
from src.babel_utils import pull_via_urllib

def pull_unii():
    dname = pull_via_urllib('https://fdasis.nlm.nih.gov/srs/download/srs/','UNIIs.zip',decompress=False,subpath='UNII')
    ddir = path.dirname(dname)
    with ZipFile(dname, 'r') as zipObj:
        zipObj.extractall(ddir)
    #this zip file unzips into a readme and a file named something like "UNII_Names_<date>.txt" and we need to rename it for make
    files = listdir(ddir)
    for filename in files:
        if filename.startswith('UNII_Names'):
            original = path.join(ddir,filename)
            final = path.join(ddir,'Latest_UNII_Names.txt')
            rename(original,final)

def make_labels_and_synonyms(inputfile,labelfile,synfile):
    idcol = 2
    labelcol = 3
    syncol = 0
    wrotelabels = set()
    wrotesyns = set()
    with open(inputfile,'r') as inf, open(labelfile,'w') as lf, open(synfile,'w') as sf:
        h = inf.readline()
        for line in inf:
            parts = line.strip().split('\t')
            ident = f'{UNII}:{parts[idcol]}'
            label = parts[labelcol]
            synonym = parts[syncol]
            lstring = f'{ident}\t{label}\n'
            sstring = f'{ident}\t{synonym}\n'
            if lstring not in wrotelabels:
                lf.write(lstring)
                wrotelabels.add(lstring)
            if sstring not in wrotesyns:
                sf.write(sstring)
