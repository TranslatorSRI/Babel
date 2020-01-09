import pytest
from babel.node import NodeFactory
from src.LabeledID import LabeledID
from babel.babel_utils import filter_out_non_unique_ids

def test_filtering():
    """
    filters out elements that exist accross rows
    eg input [{'z', 'x', 'y'}, {'z', 'n', 'm'}]
    output [{'x', 'y'}, {'m', 'n'}]
    """
    input = [{'z','x','y'},{'z','n','m'}]
    output = filter_out_non_unique_ids(input)
    assert len(output) == 2
    for o in output:
        assert len(o) == 2
        assert 'z' not in o
