import logging
import os
from collections import defaultdict
from os.path import dirname

import jsonlines
import requests
import ast
import gzip

from src.properties import Property, HAS_ALTERNATIVE_ID
from src.metadata.provenance import write_concord_metadata, write_combined_metadata
from src.ubergraph import UberGraph
from src.prefixes import MESH, CHEBI, UNII, DRUGBANK, INCHIKEY, PUBCHEMCOMPOUND,GTOPDB, KEGGCOMPOUND, DRUGCENTRAL, CHEMBLCOMPOUND, UMLS, RXCUI
from src.categories import MOLECULAR_MIXTURE, SMALL_MOLECULE, CHEMICAL_ENTITY, POLYPEPTIDE, COMPLEX_MOLECULAR_MIXTURE, CHEMICAL_MIXTURE, DRUG
from src.sdfreader import read_sdf

from src.datahandlers.unichem import data_sources as unichem_data_sources
from src.babel_utils import write_compendium, glom, get_prefixes, read_identifier_file, remove_overused_xrefs

import src.datahandlers.mesh as mesh
import src.datahandlers.umls as umls
from src.util import get_memory_usage_summary, Text, get_logger

logger = get_logger(__name__)

def get_type_from_smiles(smiles):
    if '.' in smiles:
        return MOLECULAR_MIXTURE
    else:
        return SMALL_MOLECULE

def write_umls_ids(mrsty, outfile):
    groups = ['A1.4.1.1.1.1', #antibiotic
              'A1.4.1.1.3.2', # Hormone
              'A1.4.1.1.3.4',# Vitamin
              'A1.4.1.1.3.5',# Immunologic Factor
              'A1.4.1.1.4',# Indicator, Reagent, or Diagnostic Aid
              'A1.4.1.2',# Chemical Viewed Structurally
              'A1.4.1.2.1',# Organic Chemical
              'A1.4.1.2.1.5',# Nucleic Acid, Nucleoside, or Nucleotide
              'A1.4.1.2.2',# Inorganic Chemical
              'A1.4.1.2.3',# Element, Ion, or Isotope
              'A1.3.3' #Clinical Drug
             ]
    #Leaving out these ones:
    exclude_umls_sty_trees = {
        'A1.4.1.1.3.6',     # Receptor
        'A1.4.1.1.3.3',     # Enzyme
        'A1.4.1.2.1.7',     # Amino Acid, Peptide, or Protein
    }
    umlsmap = {a:CHEMICAL_ENTITY for a in groups}
    umlsmap["A1.3.3"] = DRUG
    umls.write_umls_ids(mrsty, umlsmap, outfile, blocklist_umls_semantic_type_tree=exclude_umls_sty_trees)

def write_rxnorm_ids(infile, outfile):
    groups = ['A1.4.1.1.1.1', #antibiotic
              'A1.4.1.1.3.2', # Hormone
              'A1.4.1.1.3.3',# Enzyme
              'A1.4.1.1.3.4',# Vitamin
              'A1.4.1.1.3.5',# Immunologic Factor
              'A1.4.1.1.4',# Indicator, Reagent, or Diagnostic Aid
              'A1.4.1.2',# Chemical Viewed Structurally
              'A1.4.1.2.1',# Organic Chemical
              'A1.4.1.2.1.5',# Nucleic Acid, Nucleoside, or Nucleotide
              'A1.4.1.2.2',# Inorganic Chemical
              'A1.4.1.2.3', # Element, Ion, or Isotope
              "A1.4", # Substance
              "A1.3.3",  # Clinical Drug
              "A1.4.1.1.1", #Pharmacologic Substance
             ]
    #Leaving out these ones:
    filter_types=['A1.4.1.1.3.6', # Receptor
                  'A1.4.1.2.1.7'] #Amino Acid, Peptide, or Protein
    umlsmap = {a:CHEMICAL_ENTITY for a in groups}
    umlsmap ["A1.3.3"] = DRUG
    umlsmap ["A1.4.1.1.1"] = DRUG
    umls.write_rxnorm_ids(umlsmap, filter_types, infile, outfile, prefix=RXCUI, styfile="RXNSTY.RRF")

def build_chemical_umls_relationships(mrconso, idfile,outfile, metadata_yaml):
    umls.build_sets(mrconso, idfile, outfile, {'MSH': MESH,  'DRUGBANK': DRUGBANK, 'RXNORM': RXCUI }, provenance_metadata_yaml=metadata_yaml)

def build_chemical_rxnorm_relationships(conso, idfile, outfile, metadata_yaml):
    umls.build_sets(conso, idfile, outfile, {'MSH': MESH,  'DRUGBANK': DRUGBANK}, cui_prefix=RXCUI, provenance_metadata_yaml=metadata_yaml)

def write_pubchem_ids(labelfile,smilesfile,outfile):
    #Trying to be memory efficient here.  We could just ingest the whole smilesfile which would make this code easier
    # but since they're already sorted, let's give it a shot
    with open(labelfile,'r') as inlabels, gzip.open(smilesfile, 'rt', encoding='utf-8') as insmiles, open(outfile,'w') as outf:
        sn = -1
        flag_file_ended = False
        for labelline in inlabels:
            x = labelline.split('\t')[0]
            pn = int(x.split(':')[-1])
            while not flag_file_ended and sn < pn:
                line = insmiles.readline()
                if line == '':
                    # Get this: a blank line in readline() means that we've reached the end-of-file.
                    # (A '\n' would indicate that we've just read a blank line.)
                    flag_file_ended = True
                    break
                smiline = line.strip().split('\t')
                if len(smiline) != 2:
                    raise RuntimeError(f"Could not parse line from {smilesfile}: '{line}'")
                sn = int(smiline[0])

            if sn == pn:
                #We have a smiles for this id
                stype = get_type_from_smiles(smiline[1])
                outf.write(f'{x}\t{stype}\n')
            else:
                #sn > pn, we went past it.  No smiles for that
                print('no smiles:',x,pn,sn)
                outf.write(f'{x}\t{CHEMICAL_ENTITY}\n')


def write_mesh_ids(outfile):
    #Get the D tree,
    # D01	Inorganic Chemicals
    # D02	Organic Chemicals
    # D03	Heterocyclic Compounds
    # D04	Polycyclic Compounds
    # D05	Macromolecular Substances  NO
    # D06	Hormones, Hormone Substitutes, and Hormone Antagonists
    # D08	Enzymes and Coenzymes  NO, include with ... Activities?
    # D09	Carbohydrates
    # D10	Lipids
    # D12	Amino Acids, Peptides, and Proteins
    # D12.125 AA yes
    # D12.644 Peptides yes
    # D12.776 proteins  NO
    # D13	Nucleic Acids, Nucleotides, and Nucleosides
    # D20	Complex Mixtures
    # D23	Biological Factors
    # D25	Biomedical and Dental Materials
    # D26	Pharmaceutical Preparations
    # D27	Chemical Actions and Uses NO
    meshmap = { f'D{str(i).zfill(2)}': CHEMICAL_ENTITY for i in range(1, 27)}
    meshmap['D05'] = 'EXCLUDE'
    meshmap['D08'] = 'EXCLUDE'
    meshmap['D12.776'] = 'EXCLUDE'
    meshmap['D12.125'] = POLYPEPTIDE
    meshmap['D12.644'] = POLYPEPTIDE
    meshmap['D13'] = POLYPEPTIDE
    meshmap['D20'] = COMPLEX_MOLECULAR_MIXTURE
    #Also add anything from SCR_Chemical, if it doesn't have a tree map
    mesh.write_ids(meshmap, outfile, order=['EXCLUDE', POLYPEPTIDE, COMPLEX_MOLECULAR_MIXTURE, CHEMICAL_ENTITY], extra_vocab={'SCR_Chemical':CHEMICAL_ENTITY})

#def write_obo_ids(irisandtypes,outfile,exclude=[]):
#    order = [CHEMICAL_SUBSTANCE]
#    obo.write_obo_ids(irisandtypes, outfile, order, exclude=[])

def write_chebi_ids(outfile):
    #We're not using obo.write_obo_ids here because we need to 1) grab smiles as well and 2) figure out the types
    chemical_entity_id = f'{CHEBI}:24431'
    racimate_id = f'{CHEBI}:60911'
    mixture_id = f'{CHEBI}:60004'
    peptide_id = f'{CHEBI}:16670'
    uber = UberGraph()
    uberres_chems = uber.get_subclasses_and_smiles(chemical_entity_id)
    uberres_racimates = set([x['descendent'] for x in uber.get_subclasses_of(racimate_id)]) #no smiles for this one
    uberres_mixtures = set([x['descendent'] for x in uber.get_subclasses_of(mixture_id)]) #no smiles for this one
    uberres_peptides = set([x['descendent'] for x in uber.get_subclasses_of(peptide_id)]) #no smiles for this one
    with open(outfile, 'w') as idfile:
        for k in uberres_chems:
            desc = k["descendent"]
            if not desc.startswith('CHEBI'):
                continue
            if desc in uberres_racimates:
                ctype = MOLECULAR_MIXTURE
            elif desc in uberres_peptides:
                ctype = POLYPEPTIDE
            elif desc in uberres_mixtures:
                ctype = CHEMICAL_MIXTURE
            elif 'SMILES' in k:
                #Is it a mixture?
                ctype = get_type_from_smiles(k['SMILES'])
            else:
                #What is it?
                ctype = CHEMICAL_ENTITY
            idfile.write(f'{k["descendent"]}\t{ctype}\n')

def write_unii_ids(infile,outfile):
    """UNII contains a bunch of junk like leaves.   We are going to try to clean it a bit to get things
    that are actually chemicals.  In biolink 2.0 we cn revisit exactly what happens here."""
    with open(infile,'r', encoding='windows-1252') as inf, open(outfile,'w') as outf:
        h = inf.readline().strip().split('\t')
        bad_cols = ['NCBI','PLANTS','GRIN','MPNS']
        bad_colnos = [ h.index(bc) for bc in bad_cols ]
        for line in inf:
            x = line.strip().split('\t')

            flag_skip = False
            for bcn in bad_colnos:
                if len(x[bcn]) > 0:
                    #This is a plant or an eye of newt or something
                    flag_skip = True
                    break

            if not flag_skip:
                outf.write(f'{UNII}:{x[0]}\t{CHEMICAL_ENTITY}\n')

def write_drugbank_ids(infile,outfile):
    """We don't have a good drugbank source, so we're going to dig through unichem and get out drugbank ids."""
    #doublecheck so that we know we're getting the right value
    drugbank_id = '2'
    assert unichem_data_sources[drugbank_id] == DRUGBANK
    written = set()
    with open(infile,'r') as inf, open(outfile,'w') as outf:
        header_line = inf.readline()
        assert header_line == "UCI\tSRC_ID\tSRC_COMPOUND_ID\tASSIGNMENT\n", f"Incorrect header line in {infile}: {header_line}"
        for line in inf:
            x = line.rstrip().split('\t')
            if x[1] == drugbank_id:
                if x[2] in written:
                    continue
                dbid = f'{DRUGBANK}:{x[2]}'
                outf.write(f'{dbid}\t{CHEMICAL_ENTITY}\n')
                written.add(x[2])

def write_chemical_ids_from_labels_and_smiles(labelfile,smifile,outfile):
    smiles = {}
    with open(smifile,'r') as inf:
        for line in inf:
            x = line.strip().split('\t')
            smiles[x[0]] = x[1]
    with open(labelfile,'r') as inf, open(outfile,'w') as outf:
        for line in inf:
            hmdbid = line.split('\t')[0]
            if hmdbid in smiles:
                ctype = get_type_from_smiles(smiles[hmdbid])
            else:
                ctype = CHEMICAL_ENTITY
            outf.write(f'{hmdbid}\t{ctype}\n')


def parse_smifile(infile,outfile,smicol,idcol,pref,stripquotes=False):
    idcol_index = None
    smicol_index = None
    with open(infile,'r') as inf, open(outfile,'w') as outf:
        for line in inf:
            if line.startswith('"# GtoPdb Version'):
                # Version line! Skip.
                continue
            if line.startswith('"Ligand ID"'):
                # Header line! Check, then skip.
                header = line.strip().split('\t')
                # print("header: ", header)
                assert header == [
                    '"Ligand ID"', '"Name"', '"Species"', '"Type"', '"Approved"', '"Withdrawn"',
                    '"Labelled"', '"Radioactive"', '"PubChem SID"', '"PubChem CID"', '"UniProt ID"',
                    '"Ensembl ID"', '"ChEMBL ID"', '"Ligand Subunit IDs"', '"Ligand Subunit Name"',
                    '"Ligand Subunit UniProt IDs"', '"Ligand Subunit Ensembl IDs"', '"IUPAC name"',
                    '"INN"', '"Synonyms"', '"SMILES"', '"InChIKey"', '"InChI"', '"GtoImmuPdb"',
                    '"GtoMPdb"', '"Antibacterial"'], f"SMIFile {infile} has a modified header, please update."
                try:
                    idcol_index = header.index(idcol)
                except ValueError as e:
                    logging.error(f"Could not find ID column '{idcol}' in header {header} for {infile}")
                    exit(1)
                try:
                    smicol_index = header.index(smicol)
                except ValueError as e:
                    logging.error(f"Could not find SMILES column '{smicol}' in header {header} for {infile}")
                    exit(1)
                continue
            x = line.split('\t')
            if stripquotes:
                x = [ xi[1:-1] for xi in x ]
            if idcol_index is None:
                logging.error(f"Could not find ID column '{idcol}' in {infile}")
                exit(1)
            if smicol_index is None:
                logging.error(f"Could not find SMILES column '{smicol}' in {infile}")
                exit(1)
            smi = x[smicol_index]
            dcid = f'{pref}:{x[idcol_index]}'
            ctype = get_type_from_smiles(smi)
            outf.write(f'{dcid}\t{ctype}\n')

def write_drugcentral_ids(infile,outfile):
    smicol = 1
    idcol = 0
    with open(infile,'r') as inf, open(outfile,'w') as outf:
        for line in inf:
            x = line.strip().split('\t')
            if x[smicol] == 'None':
                outf.write(f'{x[idcol]}\t{CHEMICAL_ENTITY}\n')
            else:
                outf.write(f'{x[idcol]}\t{get_type_from_smiles(x[smicol])}\n')


def write_gtopdb_ids(infile,outfile):
    parse_smifile(infile,outfile,'"SMILES"','"Ligand ID"',GTOPDB,stripquotes=True)

def write_unichem_concords(structfile,reffile,outdir):
    inchikeys = read_inchikeys(structfile)
    concfiles = {}
    for num,name in unichem_data_sources.items():
        concname = f'{outdir}/UNICHEM_{name}'
        print(concname)
        concfiles[num] = open(concname,'w')
    with open(reffile,'rt') as inf:
        header_line = inf.readline()
        assert header_line == "UCI\tSRC_ID\tSRC_COMPOUND_ID\tASSIGNMENT\n", f"Incorrect header line in {reffile}: {header_line}"
        for line in inf:
            x = line.rstrip().split('\t')
            outf = concfiles[x[1]]
            assert x[3] == '1'  # Only '1' (current) assignments should be in this file
                                # (see https://chembl.gitbook.io/unichem/definitions/what-is-an-assignment).
            outf.write(f'{unichem_data_sources[x[1]]}:{x[2]}\toio:equivalent\t{inchikeys[x[0]]}\n')
    for outf in concfiles.values():
        outf.close()

def read_inchikeys(struct_file):
    #struct header [0'uci', 1'standardinchi', 2'standardinchikey'],
    inchikeys = {}
    with gzip.open(struct_file, 'rt') as inf:
        header_line = inf.readline()
        assert header_line == "UCI\tSTANDARDINCHI\tSTANDARDINCHIKEY\n", f"Unexpected header line in {struct_file}: {header_line}"
        for sline in inf:
            line = sline.rstrip().split('\t')
            if len(line) == 0:
                continue
            uci = line[0]
            inchikeys[uci] = f'{INCHIKEY}:{line[2]}'
    return inchikeys

def combine_unichem(concordances,output):
    PREFIXES_TO_REMOVE_OVERUSED_XREFS = [UNII, KEGGCOMPOUND, DRUGCENTRAL]

    dicts = {}
    for infile in concordances:
        print(infile)
        print('loading',infile)
        pairs = []

        # We will want to only remove overused xrefs for specific prefixes.
        # UniChem files should only have a single prefix in the first column,
        # but out of paranoia we'll double-check that.
        prefixes_in_file = set()

        with open(infile,'r') as inf:
            for line in inf:
                x = line.strip().split('\t')
                pairs.append( ([x[0], x[2]]) )
                # Get the prefix from the first row to determine if we need to remove overused xrefs
                prefixes_in_file.add(Text.get_prefix(x[0]))

        # Was there more than one prefix in the first column?
        if len(prefixes_in_file) != 1:
            raise RuntimeError(f"More than one prefix found in {infile}: {prefixes_in_file}. All UNICHEM files should have only one prefix.")
        prefix_to_check = prefixes_in_file.pop()

        # Only remove overused xrefs for specific prefixes
        newpairs = pairs
        if prefix_to_check in PREFIXES_TO_REMOVE_OVERUSED_XREFS:
            newpairs = remove_overused_xrefs(pairs)
        setpairs = [ set(x) for x in newpairs ]
        glom(dicts, setpairs, unique_prefixes=[INCHIKEY])
    chem_sets = set([frozenset(x) for x in dicts.values()])
    with jsonlines.open(output, mode='w') as writer:
        for chemset in chem_sets:
            writer.write(list(chemset))

def read_partial_unichem(unichem_partial):
    chem_sets = {}
    with jsonlines.open(unichem_partial) as reader:
        for chemlist in reader:
            chemset = set(chemlist)
            for element in chemset:
                chem_sets[element] = chemset
    return chem_sets

def is_cas(thing):
    #The last digit in a CAS is a checksum. We could use, but are not atm.
    x = thing.split('-')
    if len(x) != 3:
        return False
    if len(x[-1]) != 1:
        return False
    for xi in x:
        if not xi.isnumeric():
            return False
    return True

def make_pubchem_cas_concord(pubchemsynonyms, outfile, metadata_yaml):
    with open(pubchemsynonyms,'r') as inf, open(outfile,'w') as outf:
        for line in inf:
            x = line.strip().split('\t')
            if is_cas(x[1]):
                outf.write(f'{x[0]}\txref\tCAS:{x[1]}\n')

    write_concord_metadata(
        metadata_yaml,
        name='make_pubchem_cas_concord()',
        description='make_pubchem_cas_concord() creates xrefs from PUBCHEM identifiers in the PubChem synonyms file ' +
                    f'({pubchemsynonyms}) to Chemical Abstracts Service (CAS) identifiers.',
        concord_filename=outfile,
    )

def make_pubchem_mesh_concord(pubcheminput,meshlabels,outfile, metadata_yaml):
    mesh_label_to_id={}
    #Meshlabels has all kinds of stuff. e.g. these are both in there:
    #MESH:D014867    Water
    #MESH:M0022883   Water
    #but we only want the ones that are MESH:D... or MESH:C....
    with open(meshlabels,'r') as inf:
        for line in inf:
            x = line.strip().split('\t')
            if x[0].split(':')[-1][0] in ['C','D']:
                mesh_label_to_id[x[1]] = x[0]
    #The pubchem - mesh pairs are supposed to be ordered in this file such that the
    # first mapping is the 'best' i.e. the one most frequently reported.
    # We will only use the first one
    used_pubchem = set()
    with open(pubcheminput, 'r') as inf, open(outfile,'w') as outf:
        for line in inf:
            x = line.strip().split('\t') # x[0] = puchemid (no prefix), x[1] = mesh label
            if x[0] in used_pubchem:
                continue
            try:
                mesh_id = mesh_label_to_id[x[1]]
            except:
                print(f'no mesh for label {x[1]}')
                continue
            outf.write(f'{PUBCHEMCOMPOUND}:{x[0]}\txref\t{mesh_id}\n')
            used_pubchem.add(x[0])

    write_concord_metadata(
        metadata_yaml,
        name='make_pubchem_mesh_concord()',
        description=f'make_pubchem_mesh_concord() loads MeSH labels from {meshlabels}, then creates xrefs from PubChem ' +
                   f'identifiers in the PubChem input file ({pubcheminput}) to those MeSH identifiers using the labels as keys.',
        concord_filename=outfile,
    )

def build_drugcentral_relations(infile,outfile, metadata_yaml):
    prefixmap = { 'CHEBI': CHEBI,
                  'ChEMBL_ID': CHEMBLCOMPOUND,
                  'DRUGBANK_ID': DRUGBANK,
                  'IUPHAR_LIGAND_ID': GTOPDB,
                  'MESH_DESCRIPTOR_UI': MESH,
                  'PUBCHEM_CID': PUBCHEMCOMPOUND,
                  'UMLSCUI': UMLS,
                  'UNII': UNII}
    external_id_col = 1
    external_ns_col = 2
    drugcentral_id_col = 3
    with open(infile,'r') as inf, open(outfile,'w') as outf:
        for line in inf:
            parts = line.strip().split('\t')
            #print(parts)
            if len(parts) < 4:
                continue
            external_ns = parts[external_ns_col]
            if external_ns not in prefixmap:
                continue
            #print('ok')
            outf.write(f'{DRUGCENTRAL}:{parts[drugcentral_id_col]}\txref\t{prefixmap[external_ns]}:{parts[external_id_col]}\n')

    write_concord_metadata(
        metadata_yaml,
        name='build_drugcentral_relations()',
        description=f'Build xrefs from DrugCentral ({infile}) to {DRUGCENTRAL} using the prefix map {prefixmap}.',
        concord_filename=outfile,
    )

def make_gtopdb_relations(infile,outfile, metadata_yaml):
    with open(infile,'r') as inf, open(outfile,'w') as outf:
        h = inf.readline()
        # We might have a header/version line. If so, skip to the next line.
        if h.startswith('"# GtoPdb Version'):
            h = inf.readline()
        h = h.strip().split('\t')
        gid_index = h.index('"Ligand ID"')
        inchi_index = h.index('"InChIKey"')
        for line in inf:
            x = line.strip().split('\t')
            if x[inchi_index] == '""':
                continue
            gid = f'{GTOPDB}:{x[gid_index][1:-1]}'
            inchi = f'{INCHIKEY}:{x[inchi_index][1:-1]}'
            outf.write(f'{gid}\txref\t{inchi}\n')

    write_concord_metadata(
        metadata_yaml,
        name='make_gtopdb_relations()',
        description=f'Transform Ligand ID/InChIKey mappings from {infile} into a concord.',
        concord_filename=outfile,
    )

def make_chebi_relations(sdf,dbx,outfile,propfile_gz,metadata_yaml):
    """CHEBI contains relations both about chemicals with and without inchikeys.  You might think that because
    everything is based on unichem, we could avoid the with structures part, but history has shown that we lose
    links in that case, so we will use both the structured and unstructured chemical entries."""
    #THE SDF and XREF stuff are handled in the same function because knowing what we found in the SDF impacts
    # what we want to get out of the xrefs. But the function is quite unwieldy
    #READ SDF
    ikeys = { x:x for x in ['chebiname', 'chebiid', 'secondarychebiid','inchikey','smiles', 'keggcompounddatabaselinks', 'pubchemdatabaselinks'] }
    chebi_sdf_dat = read_sdf(sdf,ikeys)
    #CHEBIs in the sdf by definition have structure (the sdf is a structure file)
    structured_chebi = set(chebi_sdf_dat.keys())
    #READ xrefs
    with open(dbx,'r') as inf:
        dbxdata = inf.read()
    kk = 'keggcompounddatabaselinks'
    pk = 'pubchemdatabaselinks'
    secondary_chebi_id = 'secondarychebiid'

    # What if we don't have a propfile directory?
    os.makedirs(dirname(propfile_gz), exist_ok=True)

    with open(outfile,'w') as outf, gzip.open(propfile_gz, 'wt') as propf:
        #Write SDF structured things
        for cid,props in chebi_sdf_dat.items():
            if secondary_chebi_id in props:
                secondary_ids = props[secondary_chebi_id]
                for secondary_id in secondary_ids:
                    propf.write(Property(
                        curie = cid,
                        predicate = HAS_ALTERNATIVE_ID,
                        value = secondary_id,
                        source = f'Listed as a CHEBI secondary ID in the ChEBI SDF file ({sdf})'
                    ).to_json_line())
            if kk in props:
                # This is apparently a list now sometimes?
                kegg_ids = props[kk]
                if not isinstance(kegg_ids, list):
                    kegg_ids = [kegg_ids]
                for kegg_id in kegg_ids:
                    outf.write(f'{cid}\txref\t{KEGGCOMPOUND}:{kegg_id}\n')
            if pk in props:
                #Apparently there's a lot of structure here?
                database_links = props[pk]
                # To simulate previous behavior, I'll concatenate previous values together.
                v = "".join(database_links)
                parts = v.split('SID: ')
                for p in parts:
                    if 'CID' in p:
                        mapped = True
                        x = p.split('CID: ')[1]
                        outf.write(f'{cid}\txref\t{PUBCHEMCOMPOUND}:{x}\n')
        #DO THE xref stuff
        lines = dbxdata.split('\n')
        for line in lines[1:]:
            x = line.strip().split('\t')
            if len(x) < 4:
                continue
            cid = f'{CHEBI}:{x[1]}'
            if cid in structured_chebi:
                continue
            if x[3] == 'KEGG COMPOUND accession':
                outf.write(f'{cid}\txref\t{KEGGCOMPOUND}:{x[4]}\n')
            if x[3] == 'Pubchem accession':
                outf.write(f'{cid}\txref\t{PUBCHEMCOMPOUND}:{x[4]}\n')

    write_concord_metadata(
        metadata_yaml,
        name='make_chebi_relations()',
        description=f'make_chebi_relations() creates xrefs from the ChEBI database ({sdf}) to {PUBCHEMCOMPOUND} and {KEGGCOMPOUND}.',
        concord_filename=outfile,
    )


def get_mesh_relationships(mesh_id_file,cas_out, unii_out, cas_metadata, unii_metadata):
    meshes = set()
    with open(mesh_id_file,'r') as inf:
        for line in inf:
            x = line.split('\t')
            meshes.add(x[0])
    regis = mesh.pull_mesh_registry()
    with open(cas_out,'w') as casout, open(unii_out,'w') as uniiout:
        for meshid,reg in regis:
            if meshid not in meshes:
                continue
            if reg.startswith('EC'):
                continue
            if reg.startswith('txid'):
                #is a taxon
                continue
            if is_cas(reg):
                casout.write(f'{meshid}\txref\tCAS:{reg}\n')
            else:
                #is a unii?
                uniiout.write(f'{meshid}\txref\tUNII:{reg}\n')

    write_concord_metadata(
        cas_metadata,
        name='get_mesh_relationships()',
        sources=[{
            'type': 'MeSH Registry',
            'name': 'MeSH Registry',
        }],
        description=f'get_mesh_relationships() iterates through the MeSH registry, filters it to the MeSH IDs '
                    f'in {mesh_id_file}, then writes out CAS mappings to {cas_out}',
        concord_filename=cas_out,
    )

    write_concord_metadata(
        unii_metadata,
        name='get_mesh_relationships()',
        sources=[{
            'type': 'MeSH Registry',
            'name': 'MeSH Registry',
        }],
        description=f'get_mesh_relationships() iterates through the MeSH registry, filters it to the MeSH IDs '
                    f'in {mesh_id_file}, then writes out non-CAS mappings (i.e. UNII mappings) to {unii_out}',
        concord_filename=unii_out,
    )

def get_wikipedia_relationships(outfile, metadata_yaml):
    url = 'https://query.wikidata.org/sparql?format=json&query=SELECT ?chebi ?mesh WHERE { ?compound wdt:P683 ?chebi . ?compound wdt:P486 ?mesh. }'
    results = requests.get(url).json()
    pairs = [(f'{MESH}:{r["mesh"]["value"]}', f'{CHEBI}:{r["chebi"]["value"]}')
             for r in results['results']['bindings']
             if not r['mesh']['value'].startswith('M')]
    #Wikidata is great, except when it sucks.   One thing it likes to do is to
    # have multiple CHEBIs for a concept, say ignoring stereochemistry or
    # the like.  No good.   It's easy enough to filter these out, but then
    # we wouldn't have the mesh associated with anything. A spot check makes it seem like
    # cases of this type usually also have a UNII.  So we can perhaps remove ugly pairs without
    # a problem. We leave them in at this point, and they will get filtered out on reading
    with open(outfile,'w') as outf:
        m2c = defaultdict(list)
        for m,c in pairs:
            outf.write(f'{m}\txref\t{c}\n')

    write_concord_metadata(
        metadata_yaml,
        name="get_wikipedia_relationships()",
        sources=[{
            'type': 'Wikidata',
            'name': 'Wikidata SPARQL query',
        }],
        description='Wikidata SPARQL query to find Wikidata entities with both CHEBI and MESH IDs, and build a concordance between them.',
        concord_filename=outfile,
    )

def build_untyped_compendia(concordances, identifiers, unichem_partial, untyped_concord, type_file, metadata_yaml, input_metadata_yamls):
    """:concordances: a list of files from which to read relationships
       :identifiers: a list of files from which to read identifiers and optional categories"""
    dicts = read_partial_unichem(unichem_partial)
    types = {}
    for ifile in identifiers:
        print(ifile)
        new_identifiers,new_types = read_identifier_file(ifile)
        glom(dicts, new_identifiers, unique_prefixes=[INCHIKEY])
        types.update(new_types)
    for infile in concordances:
        print(infile)
        print('loading',infile)
        pairs = []
        with open(infile,'r') as inf:
            for line in inf:
                x = line.strip().split('\t')
                pairs.append( ([x[0], x[2]]) )
        p = False
        if DRUGCENTRAL in [ n.split(':')[0] for n in pairs[0] ]:
            p = True
            i = 'DrugCentral:4970'
        if p:
            print('before filtering:')
            for pair in pairs:
                if i in pair:
                    print(pair)
        newpairs = remove_overused_xrefs(pairs)
        setpairs = [ set(x) for x in newpairs ]
        if p:
            print('after filtering:')
            for pair in newpairs:
                if i in pair:
                    print(pair)
        glom(dicts, setpairs, unique_prefixes=[INCHIKEY])
        if p:
            print('after glomming:')
            print(dicts[i])
    with open(type_file,'w') as outf:
        for x,y in types.items():
            outf.write(f'{x}\t{y}\n')
    untyped_sets = set([frozenset(x) for x in dicts.values()])
    with open(untyped_concord, 'w') as outf:
        for s in untyped_sets:
            outf.write(f'{set(s)}\n')

    # Build the metadata file by combining the input metadata_yamls.
    write_combined_metadata(
        filename=metadata_yaml,
        typ='untyped_compendium',
        name='chemicals.build_untyped_compendia()',
        description=f'Generate an untyped compendium from concordances {concordances}, identifiers {identifiers}, " +'
                    f'unichem_partial {unichem_partial}, untyped_concord {untyped_concord}, and type file {type_file}.',
        # sources=None, url='', counts=None,
        combined_from_filenames=input_metadata_yamls,
    )

def build_compendia(type_file, untyped_compendia_file, properties_jsonl_gz_files, metadata_yamls, icrdf_filename):
    types = {}
    with open(type_file,'r') as inf:
        for line in inf:
            x = line.strip().split('\t')
            types[x[0]] = x[1]
    logger.info(f'Loaded {len(types)} types from {type_file}: {get_memory_usage_summary()}')

    untyped_sets = set()
    with open(untyped_compendia_file,'r') as inf:
        for line in inf:
            s = ast.literal_eval(line.strip())
            untyped_sets.add(frozenset(s))
    logger.info(f'Loaded {len(untyped_sets)} untyped sets from {untyped_compendia_file}: {get_memory_usage_summary()}')

    typed_sets = create_typed_sets(untyped_sets, types)
    logger.info(f'Created {len(typed_sets)} typed sets from {len(untyped_sets)} untyped sets: {get_memory_usage_summary()}')

    for biotype, sets in typed_sets.items():
        baretype = biotype.split(':')[-1]
        if biotype == DRUG:
            write_compendium(metadata_yamls, sets, f'{baretype}.txt', biotype, {}, extra_prefixes=[MESH,UNII], icrdf_filename=icrdf_filename, properties_jsonl_gz_files=properties_jsonl_gz_files)
        else:
            write_compendium(metadata_yamls, sets, f'{baretype}.txt', biotype, {}, extra_prefixes=[RXCUI], icrdf_filename=icrdf_filename, properties_jsonl_gz_files=properties_jsonl_gz_files)

def create_typed_sets(eqsets, types):
    """
    Given a set of sets of equivalent identifiers, we want to type each one into
    being a subclass of ChemicalEntity.

    :param eqsets: A list of lists of identifiers (should NOT be a list of LabeledIDs, but a list of strings).
    :param types: A dictionary of known types for each identifier. (Some identifiers don't have known types.)
    """
    order = [DRUG, MOLECULAR_MIXTURE, SMALL_MOLECULE, POLYPEPTIDE, COMPLEX_MOLECULAR_MIXTURE, CHEMICAL_MIXTURE, CHEMICAL_ENTITY]
    typed_sets = defaultdict(set)
    # logging.warning(f"create_typed_sets: eqsets={eqsets}, types=...")
    for equivalent_ids in eqsets:
        # logging.warning(f"Processing equivalent_ids={equivalent_ids}.")
        # prefixes = set([ Text.get_curie(x) for x in equivalent_ids])
        prefixes = get_prefixes(equivalent_ids)
        found = False
        for prefix in [PUBCHEMCOMPOUND]:
            if prefix in prefixes and not found:
                #I only want to accept the type if all pubchems agree on it.
                pctypes = set()
                for x in prefixes[prefix]:
                    if x in types:
                        pctypes.add(types[x])
                    else:
                        # logging.warning(f"No type found for {x}, skipping.")
                        pass

                if len(pctypes) == 1:
                    typed_sets[list(pctypes)[0]].add(equivalent_ids)
                    found = True
                elif pctypes == {'biolink:SmallMolecule', 'biolink:MolecularMixture'}:
                    # This is a common case (8,178 cases in 2022oct13) which occurs in cases where the InChI for
                    # e.g. water (SMILES: O) and hydron;hydroxide ([H+].[OH-]) are identical, causing them to be
                    # merged. (They may also be merged if we combine two identifiers into a single clique that is
                    # linked to two PubChem entries.)
                    #
                    # The comprehensive solution would be to use SMILES or molecular formula or per-database
                    # type information to split these cliques. Instead, as a temporary solution, we will split
                    # everything we're _sure_ is a biolink:MolecularMixture into a separate clique, and leave all
                    # the other identifiers as a biolink:SmallMolecule.
                    #
                    # First reported in https://github.com/TranslatorSRI/Babel/issues/83
                    molecular_mixture_ids = set()
                    all_other_ids = set()
                    for eq_id in equivalent_ids:
                        if eq_id in types and types[eq_id] == 'biolink:MolecularMixture':
                            molecular_mixture_ids.add(eq_id)
                        else:
                            all_other_ids.add(eq_id)

                    logging.info(
                        f"Found a clique that that contains PUBCHEM types " +
                        "({'biolink:SmallMolecule', 'biolink:MolecularMixture'}). This clique will be split " +
                        f"into a biolink:MolecularMixture ({molecular_mixture_ids}) and " +
                        f"a biolink:SmallMolecule ({all_other_ids})"
                    )
                    typed_sets['biolink:MolecularMixture'].add(frozenset(molecular_mixture_ids))
                    typed_sets['biolink:SmallMolecule'].add(frozenset(all_other_ids))
                    found = True
                else:
                    logging.warning(f"An unexpected number of PUBCHEM types found for {equivalent_ids} ({len(pctypes)}): {pctypes}")
        if not found:
            typecounts = defaultdict(int)
            for eid in equivalent_ids:
                if eid in types:
                    typecounts[types[eid]] += 1
            if len(typecounts) == 0:
                #print('how did we not get any types?')
                #print(equivalent_ids)
                #One thing that happens is that we can have PUBCHEMs that have been deleted, but are still in UNICHEM
                # then the pubchem doesn't get assigned a type, but still ends up in the compendium
                typed_sets[CHEMICAL_ENTITY].add(equivalent_ids)
            elif len(typecounts) == 1:
                t = list(typecounts.keys())[0]
                typed_sets[t].add(equivalent_ids)
            else:
                # First attempt is majority vote, and after that by most specific
                otypes = [(-c, order.index(t), t) for t, c in typecounts.items()]
                otypes.sort()
                t = otypes[0][2]
                typed_sets[t].add(equivalent_ids)
    return typed_sets

