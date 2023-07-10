from src.babel_utils import pull_via_ftp
from src.prefixes import NCBITAXON
import tarfile

def pull_ncbitaxon():
    pull_via_ftp('ftp.ncbi.nlm.nih.gov','/pub/taxonomy','taxdump.tar.gz',decompress_data=True,outfilename=f'{NCBITAXON}/taxdump.tar')

def make_labels_and_synonyms(infile,labelfile,synfile):
    taxtar = tarfile.open(infile,'r')
    f = taxtar.extractfile('names.dmp')
    l = f.readlines()
    usedsyns= set()
    with open(labelfile,'w') as outf, open(synfile,'w') as outsyn:
        for line in l:
            sline = line.decode('utf-8').strip().split('|')
            parts = [x.strip() for x in sline]

            name_class = parts[3]
            # name_class can be one of the following values (counts from May 1 release of NCBITaxon,
            # possibly -- from https://github.com/TranslatorSRI/NameResolution/issues/71#issuecomment-1618909473):
            #      25 	genbank acronym
            #     230 	blast name
            #     667 	in-part
            #    2086 	acronym
            #   14641 	common name
            #   30328 	genbank common name
            #   56575 	equivalent name
            #   75081 	includes
            #  220185 	type material
            #  245827 	synonym
            #  670412 	authority
            # 2503930 	scientific name

            if 'scientific name' == parts[3]:
                outf.write(f'NCBITaxon:{parts[0]}\t{parts[1]}\n')
            elif 'synonym' == parts[3]:
                if parts[1] in usedsyns:
                    continue
                outsyn.write(f'NCBITaxon:{parts[0]}\toio:exactSynonym\t{parts[1]}\n')
                usedsyns.add(parts[1])

