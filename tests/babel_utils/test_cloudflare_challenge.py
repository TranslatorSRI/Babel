"""Unit tests for detecting a Cloudflare bot-challenge response during download.

HMDB (and possibly other sources later) now fronts its download URL with a Cloudflare
challenge page instead of a plain 403 -- see docs at
https://developers.cloudflare.com/cloudflare-challenges/challenge-types/challenge-pages/detect-response/.
Retrying or changing the User-Agent doesn't help, so `pull_via_urllib` must recognize this
specific case (via the `cf-mitigated: challenge` response header) and fail immediately with
an actionable message, instead of burning through its retry budget on a URL that will never
succeed automatically.
"""

import email
import io
import urllib.error
from unittest.mock import patch

import pytest

from src.babel_utils import pull_via_urllib, raise_if_cloudflare_challenge
from src.util import get_config


def _http_error(raw_headers: str) -> urllib.error.HTTPError:
    """Build an HTTPError whose .headers is an email.message.Message, exactly as urllib produces.

    Not a plain dict: Message.get() is case-insensitive while dict.get() is not, so a dict
    fixture would pass even if the code only matched one specific header casing.
    """
    return urllib.error.HTTPError(
        url="https://example.com/file.zip",
        code=403,
        msg="Forbidden",
        hdrs=email.message_from_string(raw_headers),
        fp=io.BytesIO(b""),
    )


@pytest.mark.unit
def test_challenge_detected_from_csp_when_cf_mitigated_is_absent():
    """A challenge page should still be recognized from its Cloudflare-widget CSP alone.

    `cf-mitigated` is a label Cloudflare chose and could rename; having to load the widget from
    challenges.cloudflare.com is how the page works. Header copied verbatim from a live
    https://hmdb.ca/system/downloads/current/hmdb_metabolites.zip 403 on 2026-09-11, trimmed to
    the directives that name the widget.
    """
    csp = (
        "content-security-policy: default-src 'none'; script-src 'nonce-sdE6vqamOBuKVQaBr2ZyGU' "
        "'unsafe-eval' https://challenges.cloudflare.com; frame-src 'self' "
        "https://challenges.cloudflare.com blob:"
    )
    with pytest.raises(RuntimeError, match="Cloudflare bot challenge"):
        raise_if_cloudflare_challenge("https://example.com/file.zip", "/downloads/SOURCE/file.zip", _http_error(csp))


@pytest.mark.unit
@pytest.mark.parametrize("header_casing", ["cf-mitigated: challenge", "Cf-Mitigated: challenge"])
def test_cloudflare_challenge_raises_actionable_error(header_casing):
    """A 403 with `cf-mitigated: challenge` (in any casing) should raise a RuntimeError naming the
    URL and local path."""
    error = _http_error(header_casing)

    with pytest.raises(RuntimeError, match="Cloudflare bot challenge"):
        raise_if_cloudflare_challenge("https://example.com/file.zip", "/downloads/SOURCE/file.zip", error)


@pytest.mark.unit
def test_plain_403_is_not_treated_as_a_challenge():
    """A regular 403 (e.g. wrong URL) without the Cloudflare header should not raise -- let it retry as before."""
    error = _http_error("")

    raise_if_cloudflare_challenge("https://example.com/file.zip", "/downloads/SOURCE/file.zip", error)


@pytest.mark.unit
def test_urlerror_without_headers_is_not_treated_as_a_challenge():
    """A plain URLError (DNS failure, connection refused) has no .headers at all; the check must
    fall through to the caller's normal retry rather than raising AttributeError."""
    error = urllib.error.URLError("Name or service not known")

    raise_if_cloudflare_challenge("https://example.com/file.zip", "/downloads/SOURCE/file.zip", error)


# PULL_VIA_URLLIB -- the check has to actually abort the download loop, not just be callable.


def _pull_with_opener_raising(error, tmp_path):
    """Run pull_via_urllib() against an opener that always raises `error`, into tmp_path.

    Returns the mock for babel_utils' time.sleep so a test can assert whether the retry
    backoff was reached.
    """
    # Overlay the real config rather than stubbing one: pull_via_urllib also reaches
    # get_user_agent(), which wants build.branch and babel.github_url.
    config = {**get_config(), "download_directory": str(tmp_path)}
    with (
        patch("src.babel_utils.get_config", return_value=config),
        patch("src.babel_utils.urllib.request.build_opener") as build_opener,
        patch("src.babel_utils.time.sleep") as sleep,
    ):
        build_opener.return_value.open.side_effect = error
        yield_error = None
        try:
            pull_via_urllib("https://example.com/", "file.zip", decompress=False, subpath="SOURCE")
        except BaseException as exc:  # noqa: BLE001 -- the test inspects whatever came out
            yield_error = exc
    return yield_error, sleep


@pytest.mark.unit
def test_pull_via_urllib_aborts_immediately_on_a_challenge(tmp_path):
    """A challenge response should abort with the actionable RuntimeError and without sleeping.

    This is the behavior the whole check exists for: the generic path would instead sleep, retry,
    and eventually fail with "more than N attempts", which tells the operator nothing about needing
    to fetch the file by hand.
    """
    error, sleep = _pull_with_opener_raising(_http_error("cf-mitigated: challenge"), tmp_path)

    assert isinstance(error, RuntimeError)
    assert "Cloudflare bot challenge" in str(error)
    # The local path is what the operator needs in order to place the file by hand.
    assert str(tmp_path / "SOURCE" / "file.zip") in str(error)
    sleep.assert_not_called()


@pytest.mark.unit
def test_pull_via_urllib_still_retries_a_plain_403(tmp_path):
    """A 403 without the Cloudflare header must keep the old behavior -- back off, retry, and fail
    with the attempt-count message -- so the fast path is scoped to challenges alone."""
    error, sleep = _pull_with_opener_raising(_http_error(""), tmp_path)

    assert isinstance(error, RuntimeError)
    assert "attempts" in str(error)
    assert "Cloudflare" not in str(error)
    sleep.assert_called()
