import pandas as pd

peps = pd.read_csv('peptides.tsv','\t')
peps.fillna('',inplace=True)
print(len(peps),'Total peptides')
hpeps = peps[ peps['Species'].str.contains('Human') ]
print(len(hpeps),'Human peptides')

#if the peptide has a smiles, we should be ok
smipeps = hpeps[ hpeps['SMILES'] != "" ]
print(len(smipeps))
#if the peptide has a inchi, we should be ok
ikpeps = hpeps[ hpeps['InChIKey'] != "" ]
print(len(ikpeps))

#those two seem to show the same number of compounds, so let's remove them
mpeps = hpeps[ hpeps['SMILES'] == "" ]
print(len(mpeps), 'human peptides w/o smiles')

ligands_with_1AA = set(mpeps[ mpeps['Single letter amino acid sequence'] != '' ]['Ligand id'])
ligands_with_3AA = set(mpeps[ mpeps['Three letter amino acid sequence'] != '' ]['Ligand id'])

print(len(ligands_with_1AA), 'have an AA sequence(single letter)')
print(len(ligands_with_3AA), 'have an AA sequence(3 letter)')
print("FOR THE LOVE OF GOD WHY WOULD THOSE BE DIFFERENT!???!??!?!?")

print("Every 3 letter AA also has a 1 letter AA: ",ligands_with_3AA.issubset(ligands_with_1AA))
#Since the above says true, we can just go with the 1 letter AA

#What's left that has nothing?
lostpeps = mpeps[ mpeps['Single letter amino acid sequence'] == '' ]
print(len(lostpeps), ' garbage peptides')
print(lostpeps['Name'])

#Now what are these things?
#Activan A, AB, B: Dunno.  There isn't any activan in CHEBI or KEGG
#Agrin: don't see it in CHEBI/KEGG?  Gene in KEGG, but not chemical
#Complement C5: Same as Agrin 
#CRCLF1 heterodimer: Nothing
#fibrinogen: C00393, DB09222
#fibronectin: DB15150 KEGG:C00516 CHEBI:5058
#FLICE-like inhibitory protein: no idea what this is...
#FSH: (follicle stimulating hromone; follitropin): DB00066 , KEGG:C18184, CHEBI:81569
#hCG: Chorionic Gonadotrophin, D06457, C18185   
#hepatocyte growth factor. No chemicals ids
#huntingtin: no chemical ids
#IL-12: no chemical ids
#IL-17A/IL-17F: no chemical ids
#IL-23: no chem ids
#IL-27: no chem ids
#inhibin A: no chem ids
#inhibin B: no chem ids
#INSL3: CHEBI:80336, KEGG:C16124
#INSL5: CHEBI:80363, KEGG:C16178
#insulin: DB00030, CHEBI:5931, C00723
#LH: CHEBI:81568, C18183, DB14741
#low-density lipoprotein receptor-related protein: no chem ids
#lymphotoxin &beta;<sub>2</sub>&alpha;<sub>1</sub> heterotrimer: no chem ids
#matrix metalloporeinase 1: NCID
#matrix metalloporeinase 13: NCID
#matrix metalloporeinase 2: NCID
#microtubule associated protein tau: NCID
#M&uuml;llerian inhibiting substance: NCID
#PDGF AA: NCID
#PDGF AB: NCID
#PDGF BB: NCID
#protein C: DB11312
#relaxin: DB05794
#relaxin-1: CHEBI:80333, C16121
#relaxin-3: CHEBI:80335, C16123
#thrombin: C00752, PubChem:4014, CHEBI:9574, DB11300
#TSH: CHEBI:81567, C18182
#VEGFE: NCID
#von Willebrand factor: DB13133
