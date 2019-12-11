import pytest
from babel.babel_utils import glom

"""glom is a tool that looks at list of sets of values and combines them together if they share members"""

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

def test_load_diseases_and_phenotypes(rosetta):
    """Test grouping MONDO identifiers.  Mondo groups shouldn't be combined"""
    mondo_sets = build_sets(rosetta.core.mondo,['MONDO:0004979','MONDO:0004784','MONDO:0004765'])
    #hpo_sets = build_sets(rosetta.core.hpo,['HP:0002099'])
    dicts = {}
    glom(dicts,mondo_sets)
    print("*",dicts['MONDO:0004979'])
    print("*",dicts['MONDO:0004784'])
    print("*",dicts['MONDO:0004765'])
    assert dicts['MONDO:0004979'] != dicts['MONDO:0004784'] != dicts['MONDO:0004765']


def build_sets(o,mids):
    sets=[]
    for mid in mids:
        xr = o.get_xrefs(mid)
        print( xr )
        dbx = set([x for x in xr if not x.startswith('ICD')])
        print('-----:',dbx)
        dbx.add(mid)
        sets.append(dbx)
    return sets

