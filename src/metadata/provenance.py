from datetime import datetime

import yaml


def write_concord_metadata(filename, name, description='', sources=[]):
    write_metadata(filename, 'concord', name, description, sources)

def write_metadata(filename, typ, name, description='', sources=[]):
    with open(filename, 'w') as fout:
        yaml.dump({
            'created_at': datetime.now().isoformat(),
            'type': typ,
            'name': name,
            'description': description,
            'sources': sources,
        })
