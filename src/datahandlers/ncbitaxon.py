from src.babel_utils import pull_via_ftp
import gzip
import tarfile

def pull_ncbitaxon():
    pull_via_ftp('ftp.ncbi.nih.gov','/pub/taxonomy','taxdump.tar.gz',decompress_data=True,outfilename=f'NCBITAXON/taxdump.tar')

def make_labels_and_synonyms(infile,labelfile,synfile):
    taxtar = tarfile.open(infile,'r')
    f = taxtar.extractfile('names.dmp')
    l = f.readlines()
    usedsyns= set()
    with open(labelfile,'w') as outf, open(synfile,'w') as outsyn:
        for line in l:
            sline = line.decode('utf-8').strip().split('|')
            parts = [x.strip() for x in sline]
            if 'scientific name' == parts[3]:
                outf.write(f'NCBITaxon:{parts[0]}\t{parts[1]}\n')
            elif 'synonym' == parts[3]:
                if parts[1] in usedsyns:
                    continue
                outsyn.write(f'NCBITaxon:{parts[0]}\toio:exactSynonym\t{parts[1]}\n')
                usedsyns.add(parts[1])

