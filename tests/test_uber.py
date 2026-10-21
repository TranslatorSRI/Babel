from contextlib import contextmanager

import pytest

# These tests require a live connection to ubergraph.apps.renci.org.
# They are only run when pytest is invoked with --network or --all
# (see tests/conftest.py for CLI options).
#
# The shared `ubergraph` fixture (tests/conftest.py) handles three cases:
#   - Network unreachable → all tests SKIP with the connection error
#   - Server reachable but returns HTTP error on probe → all tests XFAIL
# Within each test, _server_errors_are_xfail() catches SPARQL exceptions and
# marks that test XFAIL, while assertion failures on valid-but-wrong data are
# normal failures.
pytestmark = [
    pytest.mark.network,
]


@contextmanager
def _server_errors_are_xfail():
    """Wrap a UberGraph method call; xfail the test if the server returns an error."""
    try:
        yield
    except Exception as e:
        pytest.xfail(f"UberGraph query failed (server-side issue): {e}")


def test_get_subclasses(ubergraph):
    """check that we get both direct and indirect subclasses of a node.
    We're using neutrophil (CL:0000775) which has 6 descendants; the query
    also returns the input itself, so 7 total. We previously used chemoreceptor
    cell (CL:0000206) but FBbt added ~300 Drosophila olfactory neurons under it."""
    with _server_errors_are_xfail():
        subs = ubergraph.get_subclasses_of("CL:0000775")
    assert len(subs) == 7
    for sub in subs:
        assert "descendent" in sub
        assert sub["descendent"].startswith("CL")
        assert "descendentLabel" in sub


def test_get_subclasses_xref(ubergraph):
    """This ubergraph function only returns subclasses that have an xref — 6 of the 7.
    CL:0000775 (neutrophil) itself has 5 xrefs (BTO, CALOHA, FMA, MESH, ZFA)."""
    with _server_errors_are_xfail():
        subs = ubergraph.get_subclasses_and_xrefs("CL:0000775")
    assert len(subs) == 6
    xrefs = subs["CL:0000775"]
    assert len(xrefs) == 5


def test_get_subclasses_xref_filtered_to_descendent_prefix(ubergraph):
    """With descendent_prefix, only terms from that ontology should come back. The six CL terms
    under neutrophil (CL:0000775) should all be returned for "CL" and none for "UBERON"."""
    with _server_errors_are_xfail():
        cl_subs = ubergraph.get_subclasses_and_xrefs("CL:0000775", descendent_prefix="CL")
        uberon_subs = ubergraph.get_subclasses_and_xrefs("CL:0000775", descendent_prefix="UBERON")
    assert len(cl_subs) == 6
    assert all(k.startswith("CL:") for k in cl_subs)
    assert len(uberon_subs) == 0


def test_get_subclasses_no_xref(ubergraph):
    """This HP has no subclasses and it has no xrefs. So it returns nothing"""
    with _server_errors_are_xfail():
        subs = ubergraph.get_subclasses_and_xrefs("HP:0020154")
    assert len(subs) == 0


def test_get_subclasses_exact(ubergraph):
    """Check out that we can get subclasses, along with labels and the exact matches for them
    Starting with Ciliophora infectious disease which has one subclass"""
    with _server_errors_are_xfail():
        subs = ubergraph.get_subclasses_and_exacts("MONDO:0005704")
    assert len(subs) == 2
    for k, v in subs.items():
        print(k)
        print(v)


def test_get_sub_exact_no_exact(ubergraph):
    """If a class doesn't have any exact matches, do we still get it?"""
    # this should have 3 subclasses.  One of them (MONDO:0022643) has no exact matches
    with _server_errors_are_xfail():
        subs = ubergraph.get_subclasses_and_exacts("MONDO:0002355")
    assert len(subs) == 4  # self gets returned too
    k = "MONDO:0022643"
    assert k in subs
    assert len(subs[k]) == 0
    print(subs)
