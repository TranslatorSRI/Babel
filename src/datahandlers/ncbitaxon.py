from src.babel_utils import pull_via_ftp
import gzip
import tarfile

def pull_ncbitaxon():
    pull_via_ftp('ftp.ncbi.nih.gov','/pub/taxonomy','taxdump.tar.gz',decompress_data=True,outfilename=f'NCBITAXON/taxdump.tar')

def make_labels(infile,labelfile):
    taxtar = tarfile.open(infile,'r')
    f = taxtar.extractfile('names.dmp')
    l = f.readlines()
    results = {}
    with open(labelfile,'w') as outf:
        for line in l:
            sline = line.decode('utf-8').strip().split('|')
            parts = [x.strip() for x in sline]
            #The labelfile should be everything, not just what we want for a specific purpose.
            #if 'scientific name' == parts[3]:
            outf.write(f'NCBITaxon:{parts[0]}\t{parts[1]}\n')

