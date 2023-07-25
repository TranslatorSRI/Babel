import json
# Starting with a conflation file, and a set of compendia, create a new compendium merging conflated cliques.

def get_conflation_ids(conffilename):
    """ Given the name of a conflation file, where each line looks like:
    ['RXCUI:1092396', 'RXCUI:849078', 'PUBCHEM.COMPOUND:446284', 'RXCUI:1874904', 'RXCUI:1292744', 'RXCUI:830735', 'RXCUI:880179', 'UMLS:C1370123', 'RXCUI:968674', 'RXCUI:1535149', 'RXCUI:884745', 'RXCUI:1092388', 'RXCUI:1870972', 'RXCUI:1192495', 'RXCUI:451805', 'RXCUI:1092394', 'RXCUI:2391745', 'RXCUI:830716', 'RXCUI:831573', 'RXCUI:1356795', 'RXCUI:1014098']
    return a set of all the ids in the file.
    """
    ids = set()
    with open(conffilename,'r') as inf:
        for line in inf:
            ids.update(line.strip().split('\t'))
    return ids

def get_compendia_names(cdir,compendia, ids):
    """Given a list of compendium filenames, as well as a set of ids, open each compendium file.
    Each row in the compendium file looks like this:
    {"type": "biolink:SmallMolecule", "ic": "100", "identifiers": [{"i": "PUBCHEM.COMPOUND:3386", "l": "Fluoxetine", "d": []}, {"i": "CHEMBL.COMPOUND:CHEMBL41", "l": "FLUOXETINE", "d": []}, {"i": "UNII:01K63SUP8D", "l": "FLUOXETINE", "d": []}, {"i": "CHEBI:5118", "l": "fluoxetine", "d": ["A racemate comprising equimolar amounts of (R)- and (S)-fluoxetine. A selective serotonin reuptake inhibitor (SSRI), it is used (generally as the hydrochloride salt) for the treatment of depression (and the depressive phase of bipolar disorder), bullimia nervosa, and obsessive-compulsive disorder."]}, {"i": "CHEBI:86990", "l": "N-methyl-3-phenyl-3-[4-(trifluoromethyl)phenoxy]propan-1-amine", "d": ["An aromatic ether consisting of 4-trifluoromethylphenol in which the hydrogen of the phenolic hydroxy group is replaced by a 3-(methylamino)-1-phenylpropyl group."]}, {"i": "DRUGBANK:DB00472", "d": []}, {"i": "MESH:D005473", "l": "Fluoxetine", "d": []}, {"i": "CAS:54910-89-3", "d": []}, {"i": "CAS:57226-07-0", "d": []}, {"i": "DrugCentral:1209", "l": "fluoxetine", "d": []}, {"i": "GTOPDB:203", "l": "fluoxetine", "d": []}, {"i": "HMDB:HMDB0014615", "l": "Fluoxetine", "d": []}, {"i": "INCHIKEY:RTHCYVBBDHJXIQ-UHFFFAOYSA-N", "d": []}, {"i": "UMLS:C0016365", "l": "Fluoxetine", "d": []}, {"i": "RXCUI:4493", "d": []}]}
    Each element in the identifiers list contains "i", an identifier, "l", a label, and "d", a list of descriptions.
    Not every element will have a label or description.
    If the identifier is in the set of ids, add the first available label to a dictionary between the identifier and the label.
    """
    id2name = {}
    for compendium in compendia:
        with open(f"{cdir}/{compendium}",'r') as inf:
            for line in inf:
                row = json.loads(line)
                clique_leader = row['identifiers'][0]['i']
                if clique_leader in ids:
                    #Get the first available label
                    for identifier in row['identifiers']:
                        if 'l' in identifier:
                            id2name[identifier['i']] = identifier['l']
                            break
    return id2name

def label_cliques(conflation_fname,id2name):
    """Given a conflation file, where each row looks like
    ['RXCUI:1092396', 'RXCUI:849078', 'PUBCHEM.COMPOUND:446284', 'RXCUI:1874904', 'RXCUI:1292744', 'RXCUI:830735', 'RXCUI:880179', 'UMLS:C1370123', 'RXCUI:968674', 'RXCUI:1535149', 'RXCUI:884745', 'RXCUI:1092388', 'RXCUI:1870972', 'RXCUI:1192495', 'RXCUI:451805', 'RXCUI:1092394', 'RXCUI:2391745', 'RXCUI:830716', 'RXCUI:831573', 'RXCUI:1356795', 'RXCUI:1014098']
    and a dictionary between identifiers and labels, label the cliques.
    Write a new file "labeled.txt" where each row looks like:
    [{"i": "RXCUI:1092396", "l": "Acetinophem"}, {"i": "RXCUI:849078", "l": "100 mg Tylenol"}, ...]
    """
    print(len(id2name))
    with open('labeled.txt','w') as outf, open(conflation_fname,'r') as conflation:
        for line in conflation:
            clique = []
            for identifier in line:
                if identifier in id2name:
                    clique.append({'i':identifier,'l':id2name[identifier]})
                else:
                    print(identifier)
                    exit()
            outf.write(json.dumps(clique)+'\n')

def main():
    conflation_fname = '/scratch/bizon/babel_outputs/conflation/DrugChemical.txt'
    compendia = ["ChemicalEntity.txt","ComplexMolecularMixture.txt","MolecularMixture.txt","SmallMolecule.txt","ChemicalMixture.txt","Drug.txt","Polypeptide.txt"]
    ids = get_conflation_ids(conflation_fname)
    id2name = get_compendia_names('/scratch/bizon/babel_outputs/compendia',compendia,ids)
    label_cliques(conflation_fname,id2name)