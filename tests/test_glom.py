import pytest
from src.babel_utils import glom

"""glom is a tool that looks at list of sets of values and combines them together if they share members"""

def test_uberon():
    uberon=[('UBERON:123',)]
    dict={}
    glom(dict,uberon,unique_prefixes='UBERON')
    uber2 = [set(['UBERON:123','SOME:other'])]
    glom(dict,uber2,unique_prefixes='UBERON')
    print(dict)

def test_simple():
    """Given 3 sets, 2 of which share a member, output 2 sets, with the sharing sets combined"""
    d = {}
    eqs = [('1','2'), ('2','3'), ('4','5')]
    glom(d,eqs)
    assert len(d) == 5
    assert d['1'] == d['2'] == d['3'] == {'1','2','3'}
    assert d['4'] == d['5'] == {'4','5'}

def test_two_calls():
    """Test using glom iteratively. The first call joins the first two sets, then the second call joins
    the next two and the new set."""
    d = {}
    eqs = [('1','2'), ('2','3'), ('4','5'), ('6','7')]
    oeqs = [('5','7')]
    glom(d,eqs)
    glom(d,oeqs)
    assert d['1']==d['2']==d['3']=={'1','2','3'}
    assert d['4']==d['5']==d['6']==d['7']=={'4','5','6','7'}

def test_sets():
    """Test using set() as opposed to {}"""
    d = {}
    eqs = [{'1','2'}, set(['2','3']), set(['4','5']), set(['6','7'])]
    oeqs = [{'5','7'}]
    glom(d,eqs)
    glom(d,oeqs)
    assert d['1']==d['2']==d['3']=={'1','2','3'}
    assert d['4']==d['5']==d['6']==d['7']=={'4','5','6','7'}

def test_bigger_sets():
    """Test when the sets have more than two members"""
    d = {}
    eqs = [{'1','2','3'}, {'4','5','6'} ]
    glom(d,eqs)
    assert d['1']==d['2']==d['3']=={'1','2','3'}
    assert d['4']==d['5']==d['6']=={'4','5','6'}
    eqs = [{'3','4','6','7'} ]
    glom(d,eqs)
    assert d['1']==d['2']==d['3']==d['4']==d['5']==d['6']==d['7']=={'1','2','3','4','5','6','7'}


