import pytest
from src.ubergraph import UberGraph
from src.util import Text

def test_get_subclasses():
    """check that we get both direct and indirect subclasses of a node.
    We're using chemoreceptor cell to test, which has 4 direct children,
    2 of which have children, for 12 total descendents.  The query also
    returns the input in the output, so that's 13 total"""
    ubergraph=UberGraph()
    iri = 'CL:0000206'
    subs = ubergraph.get_subclasses_of(iri)
    assert len(subs)==13
    for sub in subs:
        assert 'descendent' in sub
        assert sub['descendent'].startswith('CL')
        assert 'descendentLabel' in sub

def test_get_subclasses_xref():
    """check that we get both direct and indirect subclasses of a node.
    We're using chemoreceptor cell to test, which has 4 direct children,
    2 of which have children, for 12 total descendents.  The query also
    returns the input in the output, so that's 13 total"""
    ubergraph=UberGraph()
    iri = 'CL:0000206'
    subs = ubergraph.get_subclasses_and_xrefs(iri)
    assert len(subs)==13
    xrefs = subs[ ('CL:0000207','olfactory receptor cell') ]
    assert len(xrefs) == 3

def test_get_subclasses_no_xref():
    """This HP has no subclasses and it has no xrefs. So just itself should be returne
    as an entry, and xrefs should be an empty list"""
    ubergraph=UberGraph()
    iri = 'HP:0020154'
    subs = ubergraph.get_subclasses_and_xrefs(iri)
    assert len(subs)==1
    xrefs = list(subs.values())[0]
    assert isinstance(xrefs,list)
    assert len(xrefs) == 0

def test_get_subclasses_exact():
    """Check out that we can get subclasses, along with labels and the exact matches for them
    Starting with Ciliophora infectious disease which has one subclass"""
    ubergraph=UberGraph()
    iri = 'MONDO:0005704'
    subs = ubergraph.get_subclasses_and_exacts(iri)
    assert len(subs)==2
    for k,v in subs.items():
        print(k)
        print(v)

def test_get_sub_exact_no_exact():
    """If a class doesn't have any exact matches, do we still get it?"""
    ubergraph = UberGraph()
    #this should have 3 subclasses.  One of them (MONDO:0022643) has no exact matches
    iri = 'MONDO:0002355'
    subs = ubergraph.get_subclasses_and_exacts(iri)
    assert len(subs) == 4 #self gets returned too
    k = ('MONDO:0022643' , 'carcinoma of the vocal tract')
    assert k in subs
    assert len(subs[k]) == 0
    print(subs)

