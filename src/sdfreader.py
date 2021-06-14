def read_sdf(infile,interesting_keys):
    """Given an sdf file name and a set of keys that we'd like to extract, return a dictionary going
    chebiid -> {properties} where the properties are chosen from the interesting keys"""
    with open(infile,'r') as inf:
        chebisdf = inf.read()
    lines = chebisdf.split('\n')
    chunk = []
    chebi_props = {}
    for line in lines:
        if '$$$$' in line:
            chebi_id, chebi_dict = chebi_sdf_entry_to_dict(chunk, interesting_keys=interesting_keys)
            chebi_props[chebi_id] = chebi_dict
            chunk = []
        else:
            if line != '\n':
                line = line.strip('\n')
                chunk += [line]
    return chebi_props

def chebi_sdf_entry_to_dict(sdf_chunk, interesting_keys = {}):
    """
    Converts each SDF entry to a dictionary
    """
    final_dict = {}
    current_key = 'mol_file'
    chebi_id = ''
    for line in sdf_chunk:
        if len(line):
            if '>' == line[0]:
                current_key = line.replace('>','').replace('<','').strip().replace(' ', '').lower()
                current_key = 'formula' if current_key == 'formulae' else current_key
                if current_key in interesting_keys:
                    final_dict[interesting_keys[current_key]] = ''
                continue
            if current_key == 'chebiid':
                chebi_id = line
            if current_key in interesting_keys:
                final_dict[interesting_keys[current_key]] += line
    return (chebi_id, final_dict)