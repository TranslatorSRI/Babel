import os.path
from datetime import datetime

import yaml

def write_download_metadata(filename, name, *, url='', description='', sources=None, counts=None):
    write_metadata(filename, 'download', name, url=url, description=description, sources=sources, counts=None)

def write_concord_metadata(filename, name, *, url='', description='', sources=None, counts=None):
    write_metadata(filename, 'concord', name, url=url, description=description, sources=sources, counts=None)

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
