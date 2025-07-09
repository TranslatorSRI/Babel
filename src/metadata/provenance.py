import os.path
from collections import defaultdict
from datetime import datetime

import yaml

def write_download_metadata(filename, *, name, url='', description='', sources=None, counts=None):
    write_metadata(filename, 'download', name, url=url, description=description, sources=sources, counts=None)

def write_concord_metadata(filename, *, name, concord_filename, url='', description='', sources=None, counts=None):
    # Concord files should all be in the format:
    #   <curie>\t<predicate>\t<curie>
    # From this, we extract three counts:
    #   'count_concords': Number of lines in this file.
    #   'count_distinct_curies': Number of distinct CURIEs.
    #   'predicates': A dictionary of counts per predicate.
    #   'prefix_counts': A dictionary of prefix pairs along with the predicate
    count_concords = 0
    distinct_curies = set()
    predicate_counts = defaultdict(int)
    curie_prefix_counts = defaultdict(int)
    with open(concord_filename, 'r') as concordf:
        for line in concordf:
            row = line.split('\t')
            if len(row) != 3:
                raise ValueError(f"Concord file {concord_filename} has a line with {len(row)} columns, not 3: {line}")
            curie1 = row[0]
            predicate = row[1]
            curie2 = row[2]

            count_concords += 1
            predicate_counts[predicate] += 1
            distinct_curies.add(curie1)
            distinct_curies.add(curie2)

            prefixes = [curie1.split(':')[0], curie2.split(':')[0]]
            sorted_prefixes = sorted(prefixes)
            curie_prefix_counts[f"{predicate}({sorted_prefixes[0]}, {sorted_prefixes[1]})"] += 1

    if counts is None:
        counts = {}

    if 'concords' in counts:
        raise ValueError(f"Cannot add counts to concord metadata for {name} because it already has counts: {counts}")

    counts['concords'] = {
        'count_concords': count_concords,
        'count_distinct_curies': len(distinct_curies),
        'predicates': dict(predicate_counts),
        'prefix_counts': dict(curie_prefix_counts),
    }

    write_metadata(filename, 'concord', name, url=url, description=description, sources=sources, counts=counts)

def write_combined_metadata(filename, typ, name, *, sources=None, url='', description='', counts=None, combined_from_filenames=None):
    combined_from = {}
    if combined_from_filenames is not None:
        for metadata_yaml in combined_from_filenames:
            with open(metadata_yaml, 'r') as metaf:
                metadata_block = yaml.safe_load(metaf)
                if metadata_block is None or metadata_block == {}:
                    raise ValueError("Metadata file {metadata_yaml} is empty.")

                if 'name' not in metadata_block:
                    raise ValueError(f"Metadata file {metadata_yaml} is missing a 'name' field: {metadata_block}")

                metadata_name = metadata_block['name']

                if type(metadata_name) is not str:
                    raise ValueError(f"Metadata file {metadata_yaml} has a 'name' field that is not a string: {metadata_block}")

                if metadata_name in combined_from:
                    # If it's not already a list, then make it into a list.
                    if type(combined_from[metadata_name]) is not list:
                        combined_from[metadata_name] = [combined_from[metadata_name]]
                    combined_from[metadata_name].append(metadata_block)
                else:
                    combined_from[metadata_name] = metadata_block

    write_metadata(
        filename,
        typ=typ,
        name=name,
        sources=sources,
        url=url,
        description=description,
        counts=counts,
        combined_from=combined_from
    )

def write_metadata(filename, typ, name, *, sources=None, url='', description='', counts=None, combined_from=None):
    if type(typ) is not str:
        raise ValueError(f"Metadata entry type must be a string, not {type(typ)}: '{typ}'")
    if type(name) is not str:
        raise ValueError(f"Metadata entry name must be a string, not {type(name)}: '{name}'")
    if sources is None:
        sources = []
    if counts is None:
        counts = []
    if combined_from is None:
        combined_from = []

    metadata_dir = os.path.dirname(filename)
    os.makedirs(metadata_dir, exist_ok=True)
    with open(filename, 'w') as fout:
        yaml.dump({
            'created_at': datetime.now().isoformat(),
            'type': typ,
            'name': name,
            'url': url,
            'description': description,
            'sources': sources,
            'counts': counts,
            'combined_from': combined_from,
        }, fout)
