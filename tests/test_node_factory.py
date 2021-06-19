import pytest
import os
from src.node import NodeFactory
from src.LabeledID import LabeledID
import src.prefixes as pref

def test_get_ancestors():
    """ """
    here=os.path.abspath(os.path.dirname(__file__))
    labeldir = os.path.join(here,'testdata')
    fac = NodeFactory(labeldir)
    ancestors = fac.get_ancestors('biolink:ChemicalEntity')
    assert len(ancestors) == 5
    assert 'biolink:ChemicalEntity' in ancestors  #self is in ancestors
    assert 'biolink:PhysicalEssence' in ancestors
    assert 'biolink:NamedThing' in ancestors
    assert 'biolink:Entity' in ancestors
    assert 'biolink:PhysicalEssenceOrOccurrent' in ancestors #mixins are in ancestors

def test_prefixes():
    here=os.path.abspath(os.path.dirname(__file__))
    labeldir = os.path.join(here,'testdata')
    fac = NodeFactory(labeldir)
    prefixes = fac.get_prefixes('biolink:SmallMolecule')
    expected_prefixes = [pref.PUBCHEMCOMPOUND, pref.CHEMBLCOMPOUND, pref.UNII, pref.CHEBI, pref.DRUGBANK, pref.MESH,
                         pref.CAS, pref.DRUGCENTRAL, pref.GTOPDB, pref.HMDB, pref.KEGGCOMPOUND, pref.CHEMBANK, pref.AEOLUS,
                         pref.PUBCHEMSUBSTANCE, pref.SIDERDRUG, pref.INCHI, pref.INCHIKEY, pref.KEGGGLYCAN, pref.KEGGDRUG,
                         pref.KEGGDGROUP, pref.KEGGENVIRON]
    for p,ep in zip(prefixes,expected_prefixes):
        assert p==ep

def test_taxon_prefixes():
    """There was some churn in biolink around organism, so we had it specialcased for a while"""
    here=os.path.abspath(os.path.dirname(__file__))
    labeldir = os.path.join(here,'testdata')
    fac = NodeFactory(labeldir)
    prefixes = fac.get_prefixes('biolink:OrganismTaxon')
    expected_prefixes = [pref.NCBITAXON, pref.MESH]
    for p,ep in zip(prefixes,expected_prefixes):
        assert p==ep

def test_normalization():
    """Basic normalization - do we pick the right identifier?.  Note that the identifiers are made up."""
    here=os.path.abspath(os.path.dirname(__file__))
    labeldir = os.path.join(here,'testdata')
    fac = NodeFactory(labeldir)
    node = fac.create_node(['MESH:D012034','CHEBI:1234'],'biolink:SmallMolecule')
    assert node['id']['identifier'] == 'CHEBI:1234'
    assert len(node['equivalent_identifiers']) == 2
    ids = [x['identifier'] for x in node['equivalent_identifiers']]
    assert 'MESH:D012034' in ids
    assert 'CHEBI:1234' in ids
    assert len(node['type']) == 7
    assert 'biolink:ChemicalEntity' in node['type']
    assert 'biolink:NamedThing' in node['type']

def test_normalization_bad_prefix():
    """When we include the prefix CHEMBL, it does not get added to the list of prefixes (it should be CHEMBL.COMPOUND)"""
    here=os.path.abspath(os.path.dirname(__file__))
    labeldir = os.path.join(here,'testdata')
    fac = NodeFactory(labeldir)
    node = fac.create_node(['MESH:D012034','CHEBI:1234','CHEMBL:CHEMBL123'],'biolink:SmallMolecule')
    assert node['id']['identifier'] == 'CHEBI:1234'
    assert len(node['equivalent_identifiers']) == 2
    ids = [x['identifier'] for x in node['equivalent_identifiers']]
    assert 'MESH:D012034' in ids
    assert 'CHEBI:1234' in ids

def test_normalization_labeled_id():
    """Make sure that the node creator can handle a list that is mixed bare and labeled identifiers"""
    here=os.path.abspath(os.path.dirname(__file__))
    labeldir = os.path.join(here,'testdata')
    fac = NodeFactory(labeldir)
    labels={'CHEBI:1234':'name'}
    node = fac.create_node(['MESH:D012034','CHEBI:1234','CHEMBL:CHEMBL123'],'biolink:SmallMolecule',labels)
    assert node['id']['identifier'] == 'CHEBI:1234'
    assert len(node['equivalent_identifiers']) == 2
    assert 'MESH:D012034' in [x['identifier'] for x in node['equivalent_identifiers']]
    assert node['id']['label'] == 'name'

def test_labeling_2():
    """Check that we will label a node with the first available label, even if it's nto for the best node.
    Here only the last identifier has a label, but we want to apply it"""
    here=os.path.abspath(os.path.dirname(__file__))
    labeldir = os.path.join(here,'testdata')
    fac = NodeFactory(labeldir)
    node = fac.create_node([f'{pref.ENSEMBL}:81239',f'{pref.NCBIGENE}:123', f'{pref.DICTYBASE}:1234'],'biolink:Gene',{f'{pref.DICTYBASE}:1234': 'name'})
    print(node['id'])
    assert node['id']['identifier'] == f'{pref.NCBIGENE}:123'
    assert node['equivalent_identifiers'][0]['identifier'] == node['id']['identifier']
    assert node['equivalent_identifiers'][1]['identifier'] == f'{pref.ENSEMBL}:81239'
    assert node['equivalent_identifiers'][2]['identifier'] == f'{pref.DICTYBASE}:1234'
    assert node['id']['label'] == 'name'


def test_clean_list():
    input = frozenset({'UMLS:C1839767', 'UMLS:C1853383', LabeledID('HP:0010804','Tented upper lip vermilion'), 'UMLS:C1850072','HP:0010804'})
    #input = ['HP:0010804', 'UMLS:C1839767', 'UMLS:C1853383', LabeledID('HP:0010804','Tented upper lip vermilion'), 'UMLS:C1850072']
    here=os.path.abspath(os.path.dirname(__file__))
    labeldir = os.path.join(here,'testdata')
    nf = NodeFactory(labeldir)
    output = nf.clean_list(input)
    assert len(output) == 4
    lidfound = False
    for x in output:
        if isinstance(x,LabeledID):
            lidfound = True
            assert(x.identifier == 'HP:0010804')
    assert lidfound

def test_losing_umls():
    here=os.path.abspath(os.path.dirname(__file__))
    labeldir = os.path.join(here,'testdata')
    fac = NodeFactory(labeldir)
    input = frozenset({'HP:0010804', 'UMLS:C1839767', 'UMLS:C1853383', 'HP:0010804', 'UMLS:C1850072'})
    node = fac.create_node(input,'biolink:PhenotypicFeature',{'HP:0010804':"Tented upper lip vermilion"})
    print (node['id'])
    assert node['id']['identifier'] == 'HP:0010804'
    assert node['id']['label'] == 'Tented upper lip vermilion'
    assert len(node['equivalent_identifiers']) == 4

def test_same_value_different_prefix():
    here=os.path.abspath(os.path.dirname(__file__))
    labeldir = os.path.join(here,'testdata')
    fac = NodeFactory(labeldir)
    input = frozenset({'FB:FBgn0261954', 'ENSEMBL:FBgn0261954', 'NCBIGene:46006'})
    node = fac.create_node(input,'biolink:Gene',{})
    assert len(node['equivalent_identifiers']) == 3
    assert len(set([x['identifier'] for x in node['equivalent_identifiers']])) == 3
