import os
import requests
import re
from collections import defaultdict
from Bio import SwissProt
from babel.babel_utils import pull_via_ftp,get_config

###
# CHEBI
###

def pull_chebi():
    #Get stuff from the SDF.  This will be things with smiles and with or without inchi
    ck = { x:x for x in ['chebiname', 'chebiid', 'secondarychebiid','inchikey','smiles',
              'keggcompounddatabaselinks', 'pubchemdatabaselinks'] }
    chebi_parts = pull_chebi_sdf(ck)
    chebi_with_structure,chebi_pubchem,chebi_kegg,chebi_unmapped = extract_from_chebi_sdf(chebi_parts)
    #We should have anything with a structure handled. But what about stuff that doesn't have one?
    # Check the db_xref
    kegg_chebi, pubchem_chebi, unmapped_chebi = pull_database_xrefs(skips = chebi_with_structure)
    return chebi_pubchem + pubchem_chebi, chebi_kegg + kegg_chebi, chebi_unmapped + unmapped_chebi

def pull_chebi_sdf(interesting_keys):
    chebisdf = pull_via_ftp('ftp.ebi.ac.uk', '/pub/databases/chebi/SDF/', 'ChEBI_complete.sdf.gz',decompress_data=True)
    chebi_props = {}
    lines = chebisdf.split('\n')
    chunk = []
    for line in lines:
        if '$$$$' in line:
            chebi_id,chebi_dict = chebi_sdf_entry_to_dict(chunk, interesting_keys= interesting_keys)
            chebi_props[chebi_id] = chebi_dict
            chunk = []
        else:
            if line != '\n':
                line = line.strip('\n')
                chunk += [line]
    return chebi_props

def pull_database_xrefs(skips=[]):
    chebixrefs = pull_via_ftp('ftp.ebi.ac.uk', '/pub/databases/chebi/Flat_file_tab_delimited/', 'database_accession.tsv')
    lines = chebixrefs.split('\n')
    kegg_chebi = []
    pubchem_chebi = []
    unstructured_chebis = set()
    mapped_chebis = set()
    for line in lines[1:]:
        x = line.strip().split('\t')
        if len(x) < 4:
            continue
        cid = f'CHEBI:{x[1]}'
        if cid in skips:
            continue
        unstructured_chebis.add(cid)
        if x[3] == 'KEGG COMPOUND accession':
            kegg_chebi.append( (cid, f'KEGG.COMPOUND:{x[4]}') )
            mapped_chebis.add(cid)
        if x[3] == 'Pubchem accession':
            pubchem_chebi.append( (cid, f'PUBCHEM:{x[4]}') )
            mapped_chebis.add(cid)
    uc = unstructured_chebis.difference(mapped_chebis)
    unmapped_chebis = [ (x,) for x in uc ]
    return kegg_chebi,pubchem_chebi,unmapped_chebis

def extract_from_chebi_sdf(chebi_parts):
    #Now, we have a choice.  In terms of going chebi to kegg/pubchem we can do it for everything
    # or just for things without inchi.
    #The problem with with doing it for things with inchi is that we're trusting chebi without
    # verifying the KEGG inchi i.e. UniChem is already doing this, and we're not trusting them
    # here.  What to do about conficts (even if we notice them?)
    #The problem with not doing things with inchi is the case where the Chebi has an Inchi but
    # KEGG doesn't.  KEGG doesn't make a download available, which makes this more complicated than
    # it needs to be.  IF we DID have a KEGG download, we could be more careful.
    #As is, let's assume # that the CHEBI/KEGG and CHEBI/PUBCHEM are good and return mappings for everything.
    # NB They aren't good!  There are a couple thousand cases where we have INCHIKEY for both CHEBI and PUBCHEM
    # and they are different even though there is a chebi pubchem xref.  We're going to let glom catch those though.
    chebi_pubchem = []
    chebi_kegg = []
    chebi_unmapped = []
    chebi_with_structure = set()
    for cid,props in chebi_parts.items():
        mapped=False
        chebi_with_structure.add(cid)
        kk = 'keggcompounddatabaselinks'
        if kk in props:
            mapped=True
            chebi_kegg.append( (cid,f'KEGG.COMPOUND:{props[kk]}'))
        pk = 'pubchemdatabaselinks'
        if pk in props:
            v = props[pk]
            parts = v.split('SID: ')
            for p in parts:
                if 'CID' in p:
                    mapped = True
                    x = p.split('CID: ')[1]
                    r = (cid, f'PUBCHEM:{x}')
                    chebi_pubchem.append(r)
        if not mapped:
            chebi_unmapped.append( (cid,) )
    return chebi_with_structure,chebi_pubchem,chebi_kegg, chebi_unmapped

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

###
# Uniprot
###

def pull_uniprot(repull=False):
    if repull:
        xmlname = pull_via_ftp('ftp.uniprot.org','/pub/databases/uniprot/current_release/knowledgebase/taxonomic_divisions/' ,'uniprot_sprot_human.dat.gz',decompress_data=True,outfilename='uniprot_sprot_human.dat')
    else:
        xmlname = os.path.join(os.path.dirname(os.path.abspath(__file__)),get_config()['download_directory'],'uniprot_sprot_human.dat')
    seq_to_idlist = defaultdict(set)
    #I only want the PRO sequences.  One day, I could get the -1 -2 sequences as well if
    # there were a reason.
    with open(xmlname,'r') as unif:
        for record in SwissProt.parse(unif):
            uniprotid = f'UniProtKB:{record.accessions[0]}'
            #xrefs = [ f"{x[0]}:{x[1]}" for x in record.cross_references if x[0].lower() in ['mint','string','nextprot']]
            #xrefs.append( f'PR:{record.accessions[0]}' )
            #xrefs.append( uniprotid )
            feats = [ f for f in record.features if f[4].startswith('PRO_') and isinstance(f[1],int) and isinstance(f[2],int) ]
            fseq = [(record.sequence[f[1]-1:f[2]],f[4]) for f  in feats ]
            #seq_to_idlist[record.sequence].update(xrefs)
            for fs,fn in fseq:
                seq_to_idlist[fs].add(f'{uniprotid}#{fn}')
    return seq_to_idlist

###
#   IUPHAR
###

def pull_iuphar():
    s2iuphar = pull_iuphar_by_structure()
    hand_iuphar = pull_iuphar_by_hand()
    return s2iuphar,hand_iuphar

def pull_iuphar_by_hand():
    """This is a concordance file that was made by hand"""
    fname = os.path.join(os.path.dirname (__file__), 'input_data','iuphar_concord.txt')
    conc = []
    with open(fname,'r') as iupf:
        for line in iupf:
            if line.startswith('#'):
                continue
            y = line.strip().split(',')
            #x = set(y)
            conc.append(tuple(y))
    return conc

def pull_iuphar_by_structure():
    r=requests.get('https://www.guidetopharmacology.org/DATA/peptides.tsv')
    lines = r.text.split('\n')
    seq_to_iuphar = defaultdict(set)
    for line in lines[1:]:
        x = line.strip().split('\t')
        if len(x) < 2:
            continue
        if not 'Human' in x[2]:
            continue
        if len(x[14]) > 2: #it has "" even if nothing else :(
            seq = x[14][1:-1]
            seq3 = x[15][1:-1]
            iuid = f'GTOPDB:{x[0][1:-1]}'
            if 'X' in seq:
                xind = seq.find('X')
                bad = seq3.split('-')[xind]
                if bad == 'pGlu':
                    seq = seq[:xind] + 'Q' + seq[xind+1:]
                elif bad == 'Hyp':
                    seq = seq[:xind] + 'P' + seq[xind+1:]
                else:
                    print(iuid,bad)
            seq_to_iuphar[seq].add(iuid)
    return seq_to_iuphar


###
#   KEGG
###

# KEGG has a set of compounds that have a 'sequence' tag
# according to https://www.genome.jp/kegg/compound/:
#   Peptide entries in KEGG COMPOUND are designated with "Peptide"
#   in the first Entry line (see example here). They are always
#   represented as sequence information using the three-letter
#   amino acid codes, but they may or may not contain the full
#   atomic structure representation. Small bioactive peptides are
#   categorized in the BRITE hierarchy file shown below.
# Following the referenced link leads one to
# https://www.genome.jp/kegg-bin/download_htext?htext=br08005.keg&format=json&filedir=
# Which can be parsed to find the KEGG compounds that have a sequence.
# As for crawling them and pulling the sequence, should we be going through the KEGG client? probably?
def pull_kegg_sequences():
    kegg_sequences = defaultdict(set)
    r=requests.get('https://www.genome.jp/kegg-bin/download_htext?htext=br08005.keg&format=json&filedir=')
    j = r.json()
    identifiers = []
    handle_kegg_list(j['children'],identifiers)
    for i,kid in enumerate(identifiers):
        s = get_sequence(kid)
        kegg_sequences[s].add(f'KEGG.COMPOUND:{kid}')
    return kegg_sequences

def handle_kegg_list(childlist,names):
    for child in childlist:
        if 'children' in child:
            handle_kegg_list(child['children'],names)
        else:
            n = child['name'].split()
            names.append(n[0])

def get_sequence(compound_id):
    onetothree={'A':'Ala' ,'B':'Asx' ,'C':'Cys' ,'D':'Asp' ,'E':'Glu' ,'F':'Phe' ,'G':'Gly' ,
           'H':'His' ,'I':'Ile' ,'K':'Lys' ,'L':'Leu' ,'M':'Met' ,'N':'Asn' ,'P':'Pro' ,
           'Q':'Gln' ,'R':'Arg' ,'S':'Ser' ,'T':'Thr' ,'V':'Val' ,'W':'Trp' ,'X':'X',
           'Y':'Tyr' ,'Z':'Glx' }
    aamap = {v:k for k,v in onetothree.items()}
    #phosphoGlutamate?  This matches for
    aamap['Glp'] = 'Q'
    url = f'http://rest.kegg.jp/get/cpd:{compound_id}'
    raw_results = requests.get(url)#.json()
    results = raw_results.text.split('\n')
    mode = 'looking'
    for line in results:
        if mode == 'looking' and line.startswith('SEQUENCE'):
            x = ' '.join(line.strip().split()[1:])
            mode = 'reading'
        elif mode == 'reading':
            ls = line.strip()
            if ls.startswith('ORGANISM') or ls.startswith('TYPE'):
                break
            x += " " + ls
    #At least one of these things contains a one-letter AA sequence (?!) C16008.  Try to recognize it
    toks = x.split()
    lens = [len(t) for t in toks]
    modelen = max(set(lens), key=lens.count)
    if modelen == 10:
        #single aa code, broken into blocks of 10
        return ''.join(toks)
    elif modelen != 3:
        #probably still a one-AA list, but let's check some cases
        if len(toks) == 1 or len(toks[0]) == 10:
            return ''.join(toks)
        else:
            print("not sure what this is",x)
            raise(x)
    #OK, anything left should be a 3-letter AA string
    #remove parenthetical comments
    regex = "\((.*?)\)"
    xprime = re.sub(regex, '', x)
    #do a cleanup for things like Arg-NH2
    xps = xprime.split()
    c = []
    for a in xprime.split():
        q = a.split('-')
        for qq in q:
            if qq in aamap:
                c.append(qq)
                break
    #Change to 1 letter codes
    s = [ aamap[a] for a in c ]
    return ''.join(s)


if __name__ == '__main__':
    pull_uniprot(repull=True)
