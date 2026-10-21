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


def test_get_roles_returns_chebi_role_assertions(ubergraph):
    """get_roles() should return ChEBI's directly asserted RO:0000087 roles for a branch.

    Asserted against CHEBI:35366 "fatty acid" rather than the whole chemical-entity root so the
    query stays cheap. The specific check is that the *non-redundant* graph is used: aspirin has 13
    directly asserted roles against 45 in the redundant graph, and storing the closure would
    multiply the row count to say nothing new.
    """
    with _server_errors_are_xfail():
        pairs = ubergraph.get_roles("CHEBI:35366")

    assert pairs, "expected ChEBI to assert roles for descendants of CHEBI:35366"
    # Every pair is (term CURIE, role CURIE), both ChEBI, and no term is its own role.
    for term, role in pairs:
        assert term.startswith("CHEBI:"), term
        assert role.startswith("CHEBI:"), role
        assert term != role

    # A role's own ancestors must not be present: that is what the non-redundant graph buys.
    # CHEBI:25212 "metabolite" is asserted on many fatty acids; its ancestor CHEBI:33232
    # "application" is not asserted on any of them.
    roles = {role for _, role in pairs}
    assert "CHEBI:25212" in roles
    assert "CHEBI:33232" not in roles
