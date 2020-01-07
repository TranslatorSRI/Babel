import pytest
from babel.node import NodeFactory

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
