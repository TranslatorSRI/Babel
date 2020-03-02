import pytest
from babel.node import NodeFactory
from src.LabeledID import LabeledID

def test_get_subclasses():
    """ """
    fac = NodeFactory()
    ancestors = fac.get_ancestors('chemical_substance')
    assert len(ancestors) == 3
    assert 'named_thing' in ancestors
    assert 'biological_entity' in ancestors
    assert 'molecular_entity' in ancestors

def test_get_double_subclasses():
    """ """
    fac = NodeFactory()
    _ = fac.get_ancestors('chemical_substance')
    ancestors = fac.get_ancestors('chemical_substance')
    assert len(ancestors) == 3
    assert 'named_thing' in ancestors
    assert 'biological_entity' in ancestors
    assert 'molecular_entity' in ancestors

def test_prefixes():
    fac = NodeFactory()
    prefixes = fac.get_prefixes('chemical_substance')
    expected_prefixes = ['CHEBI', 'CHEMBL.COMPOUND', 'DRUGBANK', 'PUBCHEM', 'MESH', 'HMDB', 'INCHI', 'INCHIKEY', 'UNII', 'KEGG.COMPOUND', 'GTOPDB']
    for p,ep in zip(prefixes,expected_prefixes):
        assert p==ep

def test_normalization():
    """Basic normalization - do we pick the right identifier?.  Note that the identifiers are made up."""
    fac = NodeFactory()
    node = fac.create_node(['MESH:D012034','CHEBI:1234'],'chemical_substance')
    assert node['id']['identifier'] == 'CHEBI:1234'
    assert len(node['equivalent_identifiers']) == 2
    ids = [x['identifier'] for x in node['equivalent_identifiers']]
    assert 'MESH:D012034' in ids
    assert 'CHEBI:1234' in ids
    assert len(node['type']) == 4
    assert 'chemical_substance' in node['type']
    assert 'biological_entity' in node['type']
    assert 'molecular_entity' in node['type']
    assert 'named_thing' in node['type']

def test_normalization_bad_prefix():
    """When we include the prefix CHEMBL, it does not get added to the list of prefixes (it should be CHEMBL.COMPOUND)"""
    fac = NodeFactory()
    node = fac.create_node(['MESH:D012034','CHEBI:1234','CHEMBL:CHEMBL123'],'chemical_substance')
    assert node['id']['identifier'] == 'CHEBI:1234'
    assert len(node['equivalent_identifiers']) == 2
    ids = [x['identifier'] for x in node['equivalent_identifiers']]
    assert 'MESH:D012034' in ids
    assert 'CHEBI:1234' in ids
    assert len(node['type']) == 4
    assert 'chemical_substance' in node['type']
    assert 'biological_entity' in node['type']
    assert 'molecular_entity' in node['type']
    assert 'named_thing' in node['type']

def test_normalization_labeled_id():
    """Make sure that the node creator can handle a list that is mixed bare and labeled identifiers"""
    fac = NodeFactory()
    labels={'CHEBI:1234':'name'}
    node = fac.create_node(['MESH:D012034','CHEBI:1234','CHEMBL:CHEMBL123'],'chemical_substance',labels)
    assert node['id']['identifier'] == 'CHEBI:1234'
    assert len(node['equivalent_identifiers']) == 2
    assert 'MESH:D012034' in [x['identifier'] for x in node['equivalent_identifiers']]
    assert len(node['type']) == 4
    assert 'chemical_substance' in node['type']
    assert 'biological_entity' in node['type']
    assert 'molecular_entity' in node['type']
    assert 'named_thing' in node['type']
    assert node['id']['label'] == 'name'

def test_fix_casing():
    """We want to 1) Recognize and 2) fix  mis-cased CURIE prefixes"""
    fac = NodeFactory()
    node = fac.create_node(['ensembl:81239','NCBIGENE:123', 'dictybase:1234'],'gene', {'dictybase:1234':'name'})
    assert node['id']['identifier'] == 'NCBIGene:123'
    assert node['equivalent_identifiers'][0]['identifier'] == node['id']['identifier']
    assert node['equivalent_identifiers'][1]['identifier'] == 'ENSEMBL:81239'
    assert node['equivalent_identifiers'][2]['identifier'] == 'dictyBase:1234'

def test_labeling_2():
    """Check that we will label a node with the first available label, even if it's nto for the best node.
    Here only the last identifier has a label, but we want to apply it"""
    fac = NodeFactory()
    node = fac.create_node(['ensembl:81239','NCBIGENE:123', 'dictybase:1234'],'gene',{'dictybase:1234': 'name'})
    print(node['id'])
    assert node['id']['identifier'] == 'NCBIGene:123'
    assert node['equivalent_identifiers'][0]['identifier'] == node['id']['identifier']
    assert node['equivalent_identifiers'][1]['identifier'] == 'ENSEMBL:81239'
    assert node['equivalent_identifiers'][2]['identifier'] == 'dictyBase:1234'
    assert node['id']['label'] == 'name'


def test_clean_list():
    input = frozenset({'UMLS:C1839767', 'UMLS:C1853383', LabeledID('HP:0010804','Tented upper lip vermilion'), 'UMLS:C1850072','HP:0010804'})
    #input = ['HP:0010804', 'UMLS:C1839767', 'UMLS:C1853383', LabeledID('HP:0010804','Tented upper lip vermilion'), 'UMLS:C1850072']
    nf = NodeFactory()
    output = nf.clean_list(input)
    assert len(output) == 4
    lidfound = False
    for x in output:
        if isinstance(x,LabeledID):
            lidfound = True
            assert(x.identifier == 'HP:0010804')
    assert lidfound

def test_losing_umls():
    input = frozenset({'HP:0010804', 'UMLS:C1839767', 'UMLS:C1853383', 'HP:0010804', 'UMLS:C1850072'})
    fac = NodeFactory()
    node = fac.create_node(input,'phenotypic_feature',{'HP:0010804':"Tented upper lip vermilion"})
    print (node['id'])
    assert node['id']['identifier'] == 'HP:0010804'
    assert node['id']['label'] == 'Tented upper lip vermilion'
    assert len(node['equivalent_identifiers']) == 4
