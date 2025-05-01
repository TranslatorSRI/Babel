import logging
from collections import defaultdict

from src.babel_utils import pull_via_ftp
from src.prefixes import NCBITAXON
import tarfile

def pull_ncbitaxon():
    pull_via_ftp('ftp.ncbi.nlm.nih.gov','/pub/taxonomy','taxdump.tar.gz',decompress_data=True,outfilename=f'{NCBITAXON}/taxdump.tar')

def make_labels_and_synonyms(infile,labelfile,synfile):
    """
    Generate labels and synonyms for NCBITaxon IDs.

    We have two goals here:
    1.  To come up with a good label for every taxon. Ideally, this should be in the form "[scientific name] ([common name])",
        with GenBank common names preferred over other common names. If a taxon has multiple scientific names or common names,
        we should pick the FIRST one to appear in this file. A single best label for every taxon should be written to the labels
        file.
    2.  EVERY useful synonym should be added to the synonyms file.

    :param infile: taxdump.tar, the file containing the individual data dumps for NCBI Taxonomy.
    :param labelfile: The output file to write the labels to.
    :param synfile: The output file to write the synonyms to.
    """
    taxtar = tarfile.open(infile,'r')
    f = taxtar.extractfile('names.dmp')
    l = f.readlines()

    # It would be nice to put together the scientific name and common name(s) for a particular taxon, so
    # we put that into a dictionary and write them out separately later.
    names_by_txid = {}

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

            ncbi_txid = f"{NCBITAXON}:{parts[0]}"
            name = parts[1]

            # Write down the txid.
            if ncbi_txid not in names_by_txid:
                names_by_txid[ncbi_txid] = {}

            # Flag to write this entry as a synonym. Essentially we want to add everything we select
            # below as a synonym (including scientific names, equivalent names and so on), but things
            # we don't select should NOT be added as a synonym (e.g. acronym). So we set it to be True
            # here, but then change it to False if it isn't one of the name classes we're interested in.
            flag_write_as_synonym = True

            match name_class:
                # Labels: we use the scientific name and common name, which we put together afterwards.
                case 'scientific name':
                    if 'scientific name' not in names_by_txid[ncbi_txid]:
                        # We only write down the first one.
                        names_by_txid[ncbi_txid]['scientific name'] = name
                    else:
                        logging.warning(f"Found additional scientific name for {ncbi_txid}: '{name}' (previously '{names_by_txid[ncbi_txid]['scientific name']}'), ignoring.")

                case 'common name':
                    if 'common name' not in names_by_txid[ncbi_txid]:
                        # We only write down the first one as the "official" common name.
                        names_by_txid[ncbi_txid]['common name'] = name
                    else:
                        logging.warning(f"Found additional common name for {ncbi_txid}: '{name}' (previously '{names_by_txid[ncbi_txid]['common name']}'), ignoring.")

                case 'genbank common name':
                    if 'genbank common name' not in names_by_txid[ncbi_txid]:
                        # We only write down the first one as the "official" common name.
                        names_by_txid[ncbi_txid]['genbank common name'] = name
                    else:
                        logging.warning(f"Found additional GenBank common name for {ncbi_txid}: '{name}' (previously '{names_by_txid[ncbi_txid]['genbank common name']}'), ignoring.")

                # Synonyms: we use taxonomic synonyms and equivalent names, which are directly added as a synonym.
                case 'synonym' | 'equivalent name':
                    # We previously uniquified the synonyms, but I don't think that's useful, because we can't really
                    # control which one gets the first synonym, so let's just add them all.
                    pass

                # For every other name class, we DON'T want to write this as a synonym.
                case _:
                    flag_write_as_synonym = False

            # If this name should be written out as a synonym, then do so.
            if flag_write_as_synonym:
                outsyn.write(f'{ncbi_txid}\toio:exactSynonym\t{name}\n')

        # Now that we've read the full taxdump file, let's write out all the labels.
        for txid in names_by_txid:
            scientific_name = names_by_txid[txid].get('scientific name', '')
            common_name = names_by_txid[txid].get('common name', '')
            genbank_common_name = names_by_txid[txid].get('genbank common name', '')

            # We prefer the GenBank common name, if there is one.
            if scientific_name and genbank_common_name:
                labelf.write(f'{txid}\t{scientific_name} ({genbank_common_name})\n')
            # Otherwise, the first common name is fine.
            elif scientific_name and common_name:
                labelf.write(f'{txid}\t{scientific_name} ({common_name})\n')
            # Fall back to using whichever name is available.
            elif scientific_name:
                labelf.write(f'{txid}\t{scientific_name}\n')
            elif genbank_common_name:
                labelf.write(f'{txid}\t{genbank_common_name}\n')
            elif common_name:
                labelf.write(f'{txid}\t{common_name}\n')
            else:
                logging.warning(f"No scientific or common name found for {txid}, skipping.")