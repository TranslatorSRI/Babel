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
    with open(labelfile,'w') as labelf, open(synfile,'w') as outsyn:
        for line in l:
            sline = line.decode('utf-8').strip().split('|')
            parts = [x.strip() for x in sline]

            name_class = parts[3]
            # name_class can be one of the following values (counts from May 1, 2023 release of NCBITaxon,
            # possibly -- from https://github.com/TranslatorSRI/NameResolution/issues/71#issuecomment-1618909473):
            #      25 	genbank acronym             <no examples in Mar 13, 2025>
            #     230 	blast name                  "false scorpions"
            #     667 	in-part                     "Nucleopolyhedrovirus"
            #    2086 	acronym                     "GBV-A"
            #   14641 	common name                 "big tick-trefoil"
            #   30328 	genbank common name         "Musschenbroek's Sulawesi Maxomys"
            #   56575 	equivalent name             "Lactobacillus crispatus strain 125-2-CHN"
            #   75081 	includes                    "Symbiobacterium sp. KY38"
            #  220185 	type material               "BR<BEL>:collector:C.F.P.Martius:709"
            #  245827 	synonym                     "Caridina meridionalis sensu Wang, Liang & Li (2008)"
            #  670412 	authority                   "Lavandula bipinnata Kuntze, 1891"
            # 2503930 	scientific name             "Knoxia platycarpa"

            match name_class:
                # Labels: we use the scientific name and common name.
                case 'scientific name' | 'common name' | 'genbank common name':
                    labelf.write(f'{NCBITAXON}:{parts[0]}\t{parts[1]}\n')
                # Synonyms: we use taxonomic synonyms and equivalent names.
                # The blast name also seems to be useful, so let's add that as well.
                case 'synonym' | 'equivalent name' | 'blast name':
                    # We previously uniquified the synonyms, but I don't think that's useful, because we can't really
                    # control which one gets the first synonym (I guess it's the smallest identifier)
                    outsyn.write(f'{NCBITAXON}:{parts[0]}\toio:exactSynonym\t{parts[1]}\n')

