# Once we generate the compendium files, we need to convert them into the
# Knowledge Graph Exchange (KGX, https://github.com/biolink/kgx) format.
# This file provides code for doing that, based on the code from
# https://github.com/TranslatorSRI/NodeNormalization/blob/68096b2f16e6c2eedb699178ace71cea98dc794f/node_normalizer/loader.py#L70-L208
import gzip
import hashlib
import json
import os
from itertools import combinations

import logging
from src.util import LoggingUtil

# Default logger for this file.
logger = LoggingUtil.init_logging(__name__, level=logging.INFO)


def convert_compendium_to_kgx(compendium_filename, kgx_nodes_filename, kgx_edges_filename):
    """
    Convert a compendium file to KGX (https://github.com/biolink/kgx) format.

    Based on the code in https://github.com/TranslatorSRI/NodeNormalization/blob/68096b2f16e6c2eedb699178ace71cea98dc794f/node_normalizer/loader.py#L70-L208

    :param compendium_filename: The compendium file to convert.
    :param kgx_nodes_gz_filename: The KGX nodes gzipped file to write out.
    :param kgx_edges_gz_filename: The KGX edges gzipped file to write out.
    """

    logger.info(f"convert_compendium_to_kgx({compendium_filename}, {kgx_nodes_filename}, {kgx_edges_filename})")

    # Set up data structures.
    nodes: list = []
    edges: list = []
    pass_nodes: list = []

    count_lines = 0
    count_nodes = 0
    count_edges = 0

    # Used to count batches of 10000 lines to process together.
    batch_size = 10000
    line_counter = 0

    # Make the output directories if they don't exist.
    os.makedirs(os.path.dirname(kgx_nodes_filename), exist_ok=True)
    os.makedirs(os.path.dirname(kgx_edges_filename), exist_ok=True)

    # Open the compendium file for reading.
    with open(compendium_filename, "r", encoding="utf-8") as compendium:
        # Open the nodes and edges files for writing.
        with \
            gzip.open(kgx_nodes_filename, "wt", encoding="utf-8") as node_file, \
            gzip.open(kgx_edges_filename, "wt", encoding="utf-8") as edge_file:

            # set the flag for suppressing the first ",\n" in the written data
            first = True

            # At this point we should validate the compendium file, but the report
            # has already run, so hopefully it's already validated?

            # for each line in the file
            for line in compendium:
                # increment the record counter
                line_counter += 1

                # clear storage for this pass
                pass_nodes.clear()

                # load the line into memory
                instance: dict = json.loads(line)

                # all ids (even the root one) are in the equivalent identifiers
                if len(instance["identifiers"]) > 0:
                    # loop through each identifier and create a node
                    for equiv_id in instance["identifiers"]:
                        # check to see if there is a label. if there is use it
                        if "l" in equiv_id:
                            name = equiv_id["l"]
                        else:
                            name = ""

                        # add the node to the ones in this pass
                        pass_nodes.append(
                            {
                                "id": equiv_id["i"],
                                "name": name,
                                "category": instance["type"],
                                "equivalent_identifiers": list(x["i"] for x in instance["identifiers"]),
                            }
                        )

                    # get the combinations of the nodes in this pass
                    combos = combinations(pass_nodes, 2)

                    # for all the node combinations create an edge between them
                    for c in combos:
                        # create a unique id
                        record_id: str = c[0]["id"] + c[1]["id"] + f"{compendium_filename}"

                        # save the edge
                        edges.append(
                            {
                                "id": f'{hashlib.md5(record_id.encode("utf-8")).hexdigest()}',
                                "subject": c[0]["id"],
                                "predicate": "biolink:same_as",
                                "object": c[1]["id"],
                            }
                        )

                # save the nodes in this pass to the big list
                nodes.extend(pass_nodes)

                # did we reach the write threshold
                if line_counter == batch_size:
                    # first time in doesn't get a leading comma
                    if first:
                        prefix = ""
                    else:
                        prefix = "\n"

                    # reset the first record flag
                    first = False

                    # get all the nodes in a string and write them out
                    nodes_to_write = prefix + "\n".join([json.dumps(node) for node in nodes])
                    node_file.write(nodes_to_write)
                    count_nodes += len(nodes)

                    # are there any edges to output
                    if len(edges) > 0:
                        # get all the edges in a string and write them out
                        edges_to_write = prefix + "\n".join([json.dumps(edge) for edge in edges])
                        edge_file.write(edges_to_write)
                        count_edges += len(edges)

                    # reset for the next group
                    nodes.clear()
                    edges.clear()

                    # Count total lines
                    count_lines += line_counter
                    logger.info(f"Processed {count_lines} lines from {compendium_filename}")

                    # reset the line counter for the next group
                    line_counter = 0

            # pick up any remainders in the file
            if len(nodes) > 0:
                nodes_to_write = "\n" + "\n".join([json.dumps(node) for node in nodes])
                node_file.write(nodes_to_write)
                count_nodes += len(nodes)

            if len(edges) > 0:
                edges_to_write = "\n" + "\n".join([json.dumps(edge) for edge in edges])
                edge_file.write(edges_to_write)
                count_edges += len(edges)

            # Count total lines
            count_lines += line_counter
            logger.info(f"Processed a total of {count_lines} lines from {compendium_filename}")

    logger.info(f"Converted {compendium_filename} to KGX: " +
                f"wrote {count_nodes} nodes to {kgx_nodes_filename} and " +
                f"wrote {count_edges} edges to {kgx_edges_filename}.")