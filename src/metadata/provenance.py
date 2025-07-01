from datetime import datetime

import yaml

def write_download_metadata(filename, name, url='', description='', sources=None):
    write_metadata(filename, 'download', name, url=url, description=description, sources=sources)

def write_concord_metadata(filename, name, url='', description='', sources=None):
    write_metadata(filename, 'concord', name, url=url, description=description, sources=sources)

def write_metadata(filename, typ, name, sources=None, url='', description=''):
    if sources is None:
        sources = []
    with open(filename, 'w') as fout:
        yaml.dump({
            'created_at': datetime.now().isoformat(),
            'type': typ,
            'name': name,
            'url': url,
            'description': description,
            'sources': sources,
        }, fout)
