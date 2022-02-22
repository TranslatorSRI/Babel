from zipfile import ZipFile
from os import path,listdir,rename
from src.prefixes import UNII
from src.babel_utils import pull_via_urllib
import shutil

def pull_unii(download_dir):
    for (pullfile,originalprefix,finalname) in [('UNIIs.zip','UNII_Names','Latest_UNII_Names.txt'),
                                                ('UNII_Data.zip','UNII_Records','Latest_UNII_Records.txt')]:
        # This should be downloadable from the web, but since this service moved to the FDA [1], there do not appear
        # to be direct downloadable. Instead, the user will need to download these files manually from
        # https://precision.fda.gov/uniisearch/archive and store them in `input_data/private/UNII`.
        # [1] https://www.nlm.nih.gov/pubs/techbull/nd21/nd21_fda_srs.html
        # dname = pull_via_urllib('https://fdasis.nlm.nih.gov/srs/download/srs/',pullfile,decompress=False,subpath='UNII')
        # ddir = path.dirname(dname)
        ddir = path.join('input_data', 'private', 'UNII)
        dname = path.join(ddir, pullfile)
        with ZipFile(dname, 'r') as zipObj:
            zipObj.extractall(ddir)
        #this zip file unzips into a readme and a file named something like "UNII_Names_<date>.txt" and we need to rename it for make
        files = listdir(ddir)
        for filename in files:
            if filename.startswith(originalprefix):
                original = path.join(ddir,filename)
                final = path.join(ddir,finalname)
                rename(original,final)

                # Also copy these files to the download directory.
                shutil.copyfile(final, path.join(download_dir, final))


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
