from datetime import datetime

import yaml

def write_download_metadata(filename, name, url='', description='', sources=None, counts=None):
    write_metadata(filename, 'download', name, url=url, description=description, sources=sources, counts=None)

def write_concord_metadata(filename, name, url='', description='', sources=None, counts=None):
    write_metadata(filename, 'concord', name, url=url, description=description, sources=sources, counts=None)

def write_metadata(filename, typ, name, sources=None, url='', description='', counts=None):
    if type(name) != str:
        raise ValueError(f"Metadata entry name must be a string, not {type(name)}: '{name}'")
    if sources is None:
        sources = []
    if counts is None:
        counts = []
    with open(filename, 'w') as fout:
        yaml.dump({
            'created_at': datetime.now().isoformat(),
            'type': typ,
            'name': name,
            'url': url,
            'description': description,
            'sources': sources,
            'counts': counts,
        }, fout)
