"""Tests for the HMDB download.

HMDB fronts its download URL with a Cloudflare bot challenge, so the unattended download cannot
succeed and the operator has to place the zip by hand. These tests cover the two halves of that:
that Babel still recognizes the live response as a challenge, and that a hand-placed zip is
actually used on the next run.

Run the network test with: uv run pytest --network tests/datahandlers/test_hmdb.py
"""

import urllib.error
import urllib.request
import zipfile
from unittest.mock import patch

import pytest

from src.babel_utils import get_user_agent, raise_if_cloudflare_challenge
from src.datahandlers.hmdb import HMDB_DOWNLOAD_URL, HMDB_ZIP_FILENAME, pull_hmdb
from src.util import get_config

HMDB_ZIP_URL = HMDB_DOWNLOAD_URL + HMDB_ZIP_FILENAME


# LIVE HMDB DOWNLOAD


@pytest.mark.network
def test_hmdb_download_either_works_or_is_a_recognized_challenge():
    """The live HMDB URL should either serve the file or be recognized as a Cloudflare challenge.

    As of 2026-09-11 it is the latter (HTTP 403, `cf-mitigated: challenge`). This asserts the
    disjunction rather than reachability because both outcomes are fine -- what is not fine is a
    403 Babel fails to classify, which would send the rule into a retry loop with an unhelpful
    error instead of telling the operator to fetch the zip by hand.
    """
    req = urllib.request.Request(HMDB_ZIP_URL, headers={"User-Agent": get_user_agent()})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            assert len(resp.read(1024)) > 0, "HMDB returned an empty body"
    except urllib.error.URLError as e:
        with pytest.raises(RuntimeError, match="Cloudflare bot challenge"):
            raise_if_cloudflare_challenge(HMDB_ZIP_URL, "babel_downloads/HMDB/" + HMDB_ZIP_FILENAME, e)


# MANUALLY PLACED ZIP


@pytest.mark.unit
def test_pull_hmdb_uses_a_manually_placed_zip(tmp_path):
    """A zip already sitting in the download directory should be unpacked without re-downloading.

    This is what makes the "place it here and re-run" instruction in the Cloudflare error true:
    pull_via_urllib() deletes its target before each attempt, so a manual copy would otherwise be
    wiped and the rule would fail the same way forever.
    """
    hmdb_dir = tmp_path / "HMDB"
    hmdb_dir.mkdir()
    with zipfile.ZipFile(hmdb_dir / HMDB_ZIP_FILENAME, "w") as zipobj:
        zipobj.writestr("hmdb_metabolites.xml", "<hmdb/>")

    config = {**get_config(), "download_directory": str(tmp_path)}
    with (
        patch("src.datahandlers.hmdb.get_config", return_value=config),
        patch("src.datahandlers.hmdb.pull_via_urllib") as pull,
    ):
        pull_hmdb()

    pull.assert_not_called()
    assert (hmdb_dir / "hmdb_metabolites.xml").read_text() == "<hmdb/>"


@pytest.mark.unit
def test_pull_hmdb_downloads_when_no_zip_is_present(tmp_path):
    """With nothing in place, the normal download should still be attempted.

    The reuse branch above is only safe if its condition is the right way round; a test that
    covered the pre-placed case alone would pass just as happily on an inverted `if`, which would
    never download anything.
    """
    config = {**get_config(), "download_directory": str(tmp_path)}
    with (
        patch("src.datahandlers.hmdb.get_config", return_value=config),
        patch("src.datahandlers.hmdb.pull_via_urllib") as pull,
        patch("src.datahandlers.hmdb.ZipFile"),
    ):
        pull.return_value = str(tmp_path / "HMDB" / HMDB_ZIP_FILENAME)
        pull_hmdb()

    pull.assert_called_once_with(HMDB_DOWNLOAD_URL, HMDB_ZIP_FILENAME, decompress=False, subpath="HMDB")
