from datetime import datetime
import json

from snakemake.logging import Logger

from src.prefixes import UMLS


def write_leftover_umls(compendia, mrconso, umls_compendium, report, done):
    """
    Search for "leftover" UMLS concepts, i.e. those that are defined and valid in MRCONSO but are not
    mapped to a concept in Babel.

    As described in https://github.com/TranslatorSRI/NodeNormalization/issues/119#issuecomment-1154751451

    :param mrconso:
    :param umls_compendium:
    :param compendia:
    :param report:
    :param done:
    :return:
    """

    logging = Logger()
    logging.info(f"write_leftover_umls({compendia}, {mrconso}, {umls_compendium}, {report}, {done})")

    # For now, we have many more UMLS entities in MRCONSO than in the compendia, so
    # we'll make an in-memory list of those first. Once that flips, this should be
    # switched to the other way around (or perhaps written into an in-memory database
    # of some sort).
    referenced_umls = set()

    with open(umls_compendium, 'w') as compendiumf:
        for compendium in compendia:
            logging.info(f"Starting compendium: {compendium}")
            umls_ids = set()

            with open(compendium, 'r') as f:
                for row in f:
                    cluster = json.loads(row)
                    for id in cluster['identifiers']:
                        if id['i'].startswith(UMLS + ':'):
                            umls_ids.add(id['i'])

            logging.info(f"Completed compendium {compendium} with {len(umls_ids)} UMLS IDs")
            referenced_umls.update(umls_ids)

        logging.info(f"Completed all compendia with {len(referenced_umls)} UMLS IDs")
        print(referenced_umls)

        # Create a compendium that consists solely of all MRCONSO entries that haven't been referenced.
        # Code aparted from datahandlers.umls.build_sets()
        with open(mrconso, 'r') as inf:
            for line in inf:
                x = line.strip().split('|')
                cui = x[0]
                umls_id = f"{UMLS}:{cui}"
                if umls_id in referenced_umls:
                    logging.debug(f"UMLS ID {umls_id} is in referenced_umls, skipping")
                    continue
                lang = x[1]

                # TODO: note that this technically skips terms that don't have any English labels.
                #Only keep english terms
                if lang != 'ENG':
                    continue

                #only keep unsuppressed rows
                suppress = x[16]
                if suppress == 'O' or suppress == 'E':
                    continue
                #only keep sources we're looking for
                source = x[11]
                # if source not in lookfor:
                #     continue
                tty = x[12]

                # The STR value should be the label.
                str = x[14]

                # TODO: map UMLS type to Biolink type.
                biolink_type = 'biolink:NamedThing'

                # Write this UMLS term to UMLS.txt as a single-identifier term.
                cluster = {
                    'type': biolink_type,
                    'identifiers': [{
                        'i': umls_id,
                        'l': str
                    }]
                }
                compendiumf.write(json.dumps(cluster) + "\n")
                # logging.info(f"Writing {cluster} to {outf}")

                # if (source == 'MSH') and (tty not in acceptable_mesh_tty):
                #     continue
                # #For some dippy reason, in the id column they say "HGNC:76"
                # pref = other_prefixes[source]
                # if ':' in x[13]:
                #     other_id = f'{pref}:{x[13].split(":")[-1]}'
                # else:
                #     other_id = f'{pref}:{x[13]}'
                # #I don't know why this is in here, but it is not an identifier equivalent to anything
                # if other_id == 'NCIT:TCGA':
                #     continue
                # tup = (f'{UMLS}:{cui}',other_id)
                # #Don't include bad mappings or bad ids
                # if tup[1] in bad_mappings[tup[0]]:
                #     continue
                # if (pref in acceptable_identifiers) and (not tup[1] in acceptable_identifiers[pref]):
                #     continue
                # if tup not in pairs:
                #     concordfile.write(f'{tup[0]}\teq\t{tup[1]}\n')
                #     pairs.add(tup)

    # TODO: write out reports on unreferenced UMLS.
    with open(report, 'w') as outf:
        outf.write("TODO report goes here")

    # TODO: write out `done` file.
    with open(done, 'w') as outf:
        outf.write(f"done\n{datetime.now()}")