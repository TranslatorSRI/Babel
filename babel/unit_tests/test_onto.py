import pytest
from babel.onto import Onto
from src.util import Text

def test_mondo_ids():
    onto=Onto()
    prefix = 'MONDO'
    mondoidents = onto.get_ids(prefix)
    assert isinstance(mondoidents,list)
    assert len(mondoidents) > 10000
    prefs = set( [ Text.get_curie(x) for x in mondoidents ])
    assert len(prefs) == 1
    assert prefix in prefs

def test_get_label():
    onto = Onto()
    mondo_id = 'MONDO:0004979'
    label = onto.get_label(mondo_id)
    assert label == 'asthma'
