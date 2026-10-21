"""Network tests for the PANTHER family download.

The pipeline uses FTP (ftp.pantherdb.org), but FTP is often blocked by HPC firewalls.
PANTHER also exposes the same files over HTTP; these tests check both endpoints.

Run with: uv run pytest --network tests/datahandlers/test_pantherfamily.py
"""

import contextlib
import ftplib
import urllib.error
import urllib.request

import pytest

import src.datahandlers.pantherfamily as pantherfamily
from src.babel_utils import get_user_agent
from src.datahandlers.pantherfamily import FTP_DIR, FTP_FILE, FTP_HOST, HTTP_BASE


def _fetch_ftp_listing():
    """List the PANTHER release directory over FTP.

    Raises (TimeoutError, OSError) when the FTP connection is refused or times out --
    the usual case on hosts where outbound FTP (port 21) is firewalled -- and lets
    ftplib.all_errors propagate when the session fails *after* connecting (wrong
    directory, listing failure, ...), which points at an upstream change rather than
    a firewall.
    """
    ftp = ftplib.FTP(FTP_HOST, timeout=30)
    try:
        ftp.login()
        ftp.cwd(FTP_DIR)
        return ftp.nlst()
    finally:
        with contextlib.suppress(ftplib.all_errors):
            ftp.quit()


def _assert_expected_file_listed(names):
    """The FTP listing must contain the PANTHER human family file.

    A listing that succeeds but misses the expected file is an upstream change (file
    moved, renamed, or dropped), never a network flake -- see issue #1102.
    """
    assert any(FTP_FILE in n for n in names), (
        f"Expected to find '{FTP_FILE}' in FTP listing of {FTP_HOST}{FTP_DIR}; got: {names[:10]}"
    )


@pytest.mark.network
def test_panther_ftp_accessible():
    """PANTHER FTP endpoint should be reachable and list the expected file.

    A refused or timed-out connection is an expected xfail on firewalled hosts (HPC
    nodes), where the pipeline falls back to HTTP instead. A session that fails
    *after* connecting -- or a listing missing the expected file -- fails outright:
    that is the signal that ftp.pantherdb.org changed upstream, which the old
    blanket xfail would have hidden (issue #1102).
    """
    try:
        names = _fetch_ftp_listing()
    except (TimeoutError, OSError) as e:
        pytest.xfail(f"FTP connection to {FTP_HOST} refused or timed out -- likely firewalled: {e}")
    except ftplib.all_errors as e:
        pytest.fail(
            f"Connected to {FTP_HOST}, but the FTP session failed -- possible upstream change at ftp.pantherdb.org: {e}"
        )
    _assert_expected_file_listed(names)


@pytest.mark.network
def test_panther_http_accessible_with_user_agent():
    """PANTHER HTTP endpoint should be reachable with our User-Agent.

    This is the fallback when FTP is blocked (e.g. on HPC). If this test passes
    and test_panther_ftp_accessible xfails, the pull_pantherfamily() function
    falls back to HTTP automatically.
    """
    url = HTTP_BASE + FTP_FILE
    req = urllib.request.Request(url, headers={"User-Agent": get_user_agent()})
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            chunk = resp.read(1024)
        assert len(chunk) > 0, "PANTHER HTTP endpoint returned an empty body"
    except urllib.error.HTTPError as e:
        pytest.fail(f"PANTHER HTTP URL {url} returned HTTP {e.code} with User-Agent '{get_user_agent()}'")
    except (TimeoutError, urllib.error.URLError) as e:
        pytest.xfail(f"PANTHER HTTP endpoint unreachable: {e}")


@pytest.mark.network
def test_pull_pantherfamily_falls_back_to_http_when_ftp_blocked(tmp_path, monkeypatch):
    """pull_pantherfamily() must fall back to HTTP when FTP is blocked (HPC firewall scenario).

    Simulates the HPC environment where port 21 is refused, confirms the HTTP mirror
    is tried and produces a non-empty output file at the expected path.
    """

    def ftp_refused(*args, **kwargs):
        raise OSError("Connection refused (simulating HPC FTP firewall)")

    monkeypatch.setattr("src.datahandlers.pantherfamily.pull_via_ftp", ftp_refused)
    monkeypatch.setattr("src.datahandlers.pantherfamily.get_config", lambda: {"download_directory": str(tmp_path)})
    monkeypatch.setattr("src.datahandlers.pantherfamily.get_user_agent", lambda: "Babel/test")

    try:
        pantherfamily.pull_pantherfamily()
    except (TimeoutError, urllib.error.URLError) as e:
        pytest.xfail(f"HTTP fallback also unreachable (no network?): {e}")

    outfile = tmp_path / "PANTHER.FAMILY" / "family.csv"
    assert outfile.exists(), f"Expected output file {outfile} was not created"
    assert outfile.stat().st_size > 0, f"Output file {outfile} is empty"


# FTP session behavior (offline)
#
# The xfail-vs-fail distinction above cannot be exercised in CI by hitting
# ftp.pantherdb.org, so these tests drive _fetch_ftp_listing() through a stub
# FTP server and call the real test function to pin each branch.


class _StubFTP:
    """ftplib.FTP stand-in with scriptable connection/session behavior.

    connect_error is raised from the constructor (simulating a refused/timed-out
    connection); session_error is raised from login()/cwd()/nlst() (simulating a
    session that fails after connecting). Otherwise nlst() returns `listing`.
    """

    def __init__(self, listing=None, connect_error=None, session_error=None):
        if connect_error is not None:
            raise connect_error
        self._listing = listing if listing is not None else []
        self._session_error = session_error

    def login(self):
        self._raise_session_error()

    def cwd(self, _directory):
        self._raise_session_error()

    def nlst(self):
        self._raise_session_error()
        return self._listing

    def quit(self):
        pass

    def _raise_session_error(self):
        if self._session_error is not None:
            raise self._session_error


def _ftp_refused(*args, **kwargs):
    raise OSError("Connection refused (simulating firewalled FTP)")


def _ftp_session_fails(*args, **kwargs):
    return _StubFTP(session_error=ftplib.error_perm("550 No such file or directory (simulating upstream move)"))


@pytest.mark.unit
def test_panther_ftp_accessible_xfails_when_ftp_refused(monkeypatch):
    """A refused FTP connection must xfail (firewalled host), not fail.

    This pins the deliberate HPC behavior: on a firewalled host the pipeline falls
    back to HTTP instead of erroring.
    """
    monkeypatch.setattr(ftplib, "FTP", _ftp_refused)
    with pytest.raises(pytest.xfail.Exception):
        test_panther_ftp_accessible()


@pytest.mark.unit
def test_panther_ftp_session_failure_after_connect_fails_not_xfails(monkeypatch):
    """A session error after connecting must fail, not xfail.

    The old blanket xfail reported an upstream change (moved directory, dropped
    file) the same way it reports a firewalled host. Failing keeps the signal that
    ftp.pantherdb.org changed (issue #1102).
    """
    monkeypatch.setattr(ftplib, "FTP", _ftp_session_fails)
    with pytest.raises(pytest.fail.Exception, match="possible upstream change"):
        test_panther_ftp_accessible()


@pytest.mark.unit
def test_panther_ftp_listing_missing_file_fails(monkeypatch):
    """A successful listing without the expected file must fail.

    Negative control for _assert_expected_file_listed(): if ftp.pantherdb.org drops
    or renames the family file, the test fails instead of passing silently.
    """
    monkeypatch.setattr(ftplib, "FTP", lambda *args, **kwargs: _StubFTP(listing=["README.txt", "PTHR18.0_human"]))
    with pytest.raises(AssertionError, match="PTHR19.0_human"):
        test_panther_ftp_accessible()


@pytest.mark.unit
def test_panther_ftp_accessible_passes_when_file_listed(monkeypatch):
    """A reachable endpoint listing the expected file passes with no xfail."""
    monkeypatch.setattr(ftplib, "FTP", lambda *args, **kwargs: _StubFTP(listing=["README.txt", FTP_FILE]))
    test_panther_ftp_accessible()  # must not raise
