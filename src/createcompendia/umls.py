from datetime import datetime
import json

from snakemake.logging import Logger
from bmt import Toolkit

from src.prefixes import UMLS


def write_leftover_umls(compendia, mrconso, mrsty, umls_compendium, report, done):
    """
    Search for "leftover" UMLS concepts, i.e. those that are defined and valid in MRCONSO but are not
    mapped to a concept in Babel.

    As described in https://github.com/TranslatorSRI/NodeNormalization/issues/119#issuecomment-1154751451

    :param compendia: A list of compendia to collect.
    :param mrconso: MRCONSO.RRF file path
    :param mrsty: MRSTY.RRF file path
    :param umls_compendium: The UMLS compendium file to write out.
    :param report: The report file to write out.
    :param done: The done file to write out.
    :return: Nothing.
    """

    logging = Logger()
    logging.info(f"write_leftover_umls({compendia}, {mrconso}, {mrsty}, {umls_compendium}, {report}, {done})")

    # For now, we have many more UMLS entities in MRCONSO than in the compendia, so
    # we'll make an in-memory list of those first. Once that flips, this should be
    # switched to the other way around (or perhaps written into an in-memory database
    # of some sort).
    referenced_umls = set()

    # If we were interested in keeping all UMLS labels, we would create an identifier file as described in
    # babel_utils.read_identifier_file() and then glom them with babel_utils.glom(). However, for this initial
    # run, it's probably okay to just pick the first label for each code.
    umls_ids_already_included = set()

    with open(umls_compendium, 'w') as compendiumf, open(report, 'w') as reportf:
        # Write something to the compendium file so that Snakemake knows we've started.
        compendiumf.write("\n")
        compendiumf.flush()

        # This defaults to the version of the Biolink model that is included with this BMT.
        biolink_toolkit = Toolkit()

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
        reportf.write(f"Completed all compendia with {len(referenced_umls)} UMLS IDs")
        # print(referenced_umls)

        # Load all the semantic types.
        types_by_id = dict()
        types_by_tui = dict()
        with open(mrsty, 'r') as inf:
            for line in inf:
                x = line.strip().split('|')
                umls_id = f"{UMLS}:{x[0]}"
                tui = x[1]
                # stn = x[2]
                sty = x[3]

                if umls_id not in types_by_id:
                    types_by_id[umls_id] = dict()
                if tui not in types_by_id[umls_id]:
                    types_by_id[umls_id][tui] = set()
                types_by_id[umls_id][tui].add(sty)

                if tui not in types_by_tui:
                    types_by_tui[tui] = set()
                types_by_tui[tui].add(sty)

        logging.info(f"Completed loading {len(types_by_id.keys())} UMLS IDs from MRSTY.RRF.")
        reportf.write(f"Completed loading {len(types_by_id.keys())} UMLS IDs from MRSTY.RRF.")

        with open('babel_outputs/reports/umls-types.tsv', 'w') as outf:
            for tui in sorted(types_by_tui.keys()):
                for sty in sorted(list(types_by_tui[tui])):
                    outf.write(f"{tui}\t{sty}\n")

        # Create a compendium that consists solely of all MRCONSO entries that haven't been referenced.
        # Code adapted from datahandlers.umls.build_sets()
        count_no_umls_type = 0
        count_multiple_umls_type = 0
        with open(mrconso, 'r') as inf:
            for line in inf:
                x = line.strip().split('|')
                cui = x[0]
                umls_id = f"{UMLS}:{cui}"
                if umls_id in referenced_umls:
                    logging.debug(f"UMLS ID {umls_id} is in referenced_umls, skipping.")
                    continue
                if umls_id in umls_ids_already_included:
                    logging.debug(f"UMLS ID {umls_id} has already been included, skipping.")
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
                # tty = x[12]

                # The STR value should be the label.
                str = x[14]

                # Lookup type.
                def umls_type_to_biolink_type(umls_tui):
                    biolink_type = biolink_toolkit.get_element_by_mapping(f'STY:{umls_tui}', most_specific=True, formatted=True, mixin=True)
                    if biolink_type is None:
                        logging.debug(f"No Biolink type found for UMLS TUI {umls_tui}")
                    return biolink_type

                umls_type_results = types_by_id.get(umls_id, {'biolink:NamedThing': {'Named thing'}})
                biolink_types = list(map(umls_type_to_biolink_type, umls_type_results.keys()))
                if len(biolink_types) == 0:
                    logging.debug(f"No UMLS type found for {umls_id}: {umls_type_results} -> {biolink_types}, skipping")
                    reportf.write(f"NO_UMLS_TYPE [{umls_id}]: {umls_type_results} -> {biolink_types}")
                    count_no_umls_type += 1
                    continue
                if len(biolink_types) > 1:
                    logging.debug(f"Multiple UMLS types not yet supported for {umls_id}: {umls_type_results} -> {biolink_types}, skipping")
                    biolink_types_as_str = map(lambda t: "(None)" if t is None else t, biolink_types)
                    reportf.write(f"MULTIPLE_UMLS_TYPES [{umls_id}]: {'|'.join(biolink_types_as_str)}")
                    count_multiple_umls_type += 1
                    continue

                # Write this UMLS term to UMLS.txt as a single-identifier term.
                cluster = {
                    'type': biolink_types,
                    'identifiers': [{
                        'i': umls_id,
                        'l': str
                    }]
                }
                compendiumf.write(json.dumps(cluster) + "\n")
                umls_ids_already_included.add(umls_id)
                logging.debug(f"Writing {cluster} to {compendiumf}")

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

        reportf.write(f"Wrote out {len(umls_ids_already_included)} UMLS IDs into the leftover UMLS compendium.")
        logging.info(f"Wrote out {len(umls_ids_already_included)} UMLS IDs into the leftover UMLS compendium.")

        logging.info(f"Found {count_no_umls_type} UMLS IDs without UMLS types and {count_multiple_umls_type} UMLS IDs with multiple UMLS types.")
        reportf.write(f"Found {count_no_umls_type} UMLS IDs without UMLS types and {count_multiple_umls_type} UMLS IDs with multiple UMLS types.")

    # Write out `done` file.
    with open(done, 'w') as outf:
        outf.write(f"done\n{datetime.now()}")

    logging.info("Complete")