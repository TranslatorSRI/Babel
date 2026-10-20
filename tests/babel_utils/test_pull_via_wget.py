"""Unit tests for pull_via_wget()'s recursive, timestamping download.

This is the mechanism that lets a recursive download reuse files preloaded from a previous run: a
recursive `wget --timestamping` fetch must leave already-present files alone and download only the
ones we don't have yet. (The PubMed XML download that motivated it has been replaced by the pubmed2db
export, which is fetched file by file; see docs/RunningBabel.md, "Preloading PubMed downloads".)

The tests run against a throwaway HTTP server on localhost, so they need no external network —
but they do need the `wget` binary, and are skipped if it isn't installed.
"""

import functools
import os
import shutil
import threading
import time
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch

import pytest

from src.babel_utils import WgetRecursionOptions, pull_via_wget

requires_wget = pytest.mark.skipif(shutil.which("wget") is None, reason="wget is not installed")


@pytest.fixture
def http_server(tmp_path):
    """Serve tmp_path/'remote' over HTTP on localhost; yields (base_url, remote_dir)."""
    remote_dir = tmp_path / "remote"
    (remote_dir / "files").mkdir(parents=True)

    handler = functools.partial(SimpleHTTPRequestHandler, directory=str(remote_dir))
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/", remote_dir
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


# PRELOADING


@requires_wget
@pytest.mark.unit
def test_recursive_wget_downloads_every_file_into_an_empty_directory(http_server, tmp_path):
    """A recursive download into an empty directory should fetch every file the server offers."""
    base_url, remote_dir = http_server
    (remote_dir / "files" / "one.txt").write_text("one")
    (remote_dir / "files" / "two.txt").write_text("two")

    local_dir = tmp_path / "local"
    pull_via_wget(
        base_url,
        "files",
        decompress=False,
        outpath=str(local_dir),
        recurse=WgetRecursionOptions.RECURSE_DIRECTORY_ONLY,
        continue_incomplete=False,
    )

    assert (local_dir / "one.txt").read_text() == "one"
    assert (local_dir / "two.txt").read_text() == "two"


@requires_wget
@pytest.mark.unit
def test_recursive_wget_keeps_preloaded_files_and_downloads_only_the_new_ones(http_server, tmp_path):
    """A file we already have (same size, not older than the server's copy) should be left
    untouched, while a file we don't have yet should be downloaded. This is what lets a new
    Babel run reuse files carried over from a previous run."""
    base_url, remote_dir = http_server
    (remote_dir / "files" / "preloaded.txt").write_text("REMOTE content")
    (remote_dir / "files" / "new.txt").write_text("brand new")

    # Backdate the server's copy so that our local copy is unambiguously the newer one, which is
    # what wget --timestamping compares. Same byte count: wget re-downloads on a size mismatch.
    old = time.time() - 3600
    os.utime(remote_dir / "files" / "preloaded.txt", (old, old))

    local_dir = tmp_path / "local"
    local_dir.mkdir()
    preloaded = local_dir / "preloaded.txt"
    preloaded.write_text("LOCAL content!")
    assert preloaded.stat().st_size == (remote_dir / "files" / "preloaded.txt").stat().st_size

    pull_via_wget(
        base_url,
        "files",
        decompress=False,
        outpath=str(local_dir),
        recurse=WgetRecursionOptions.RECURSE_DIRECTORY_ONLY,
        continue_incomplete=False,
    )

    # The preloaded file was not re-downloaded: its (distinguishable) local content survives.
    assert preloaded.read_text() == "LOCAL content!"
    # The file we didn't have was downloaded.
    assert (local_dir / "new.txt").read_text() == "brand new"


@requires_wget
@pytest.mark.unit
def test_recursive_wget_redownloads_a_stale_preloaded_file_rather_than_resuming_it(http_server, tmp_path):
    """A preloaded file the server has since revised should be re-downloaded in full, so that
    carrying files forward can't pin us to a superseded copy.

    This is the regression test for --continue: wget resumes by appending the bytes it is missing
    to the local file, so with --continue this download produced "old local contentt" — the stale
    content plus the one-byte tail of the new file. pull_via_wget() now refuses --continue in a
    recursive download; this test proves the resulting fetch is a clean, whole-file one."""
    base_url, remote_dir = http_server
    (remote_dir / "files" / "revised.txt").write_text("new remote content")

    local_dir = tmp_path / "local"
    local_dir.mkdir()
    revised = local_dir / "revised.txt"
    revised.write_text("old local content")

    # Preloaded with its original (older) mtime preserved, as `mv`/`cp -p`/`rsync -a` would.
    old = time.time() - 3600
    os.utime(revised, (old, old))

    pull_via_wget(
        base_url,
        "files",
        decompress=False,
        outpath=str(local_dir),
        recurse=WgetRecursionOptions.RECURSE_DIRECTORY_ONLY,
        continue_incomplete=False,
    )

    assert revised.read_text() == "new remote content"


# UNSAFE OPTION COMBINATIONS
#
# These guards fire before wget is invoked, so these tests need neither the server nor the binary.


@pytest.mark.unit
def test_non_recursive_download_omits_timestamping(tmp_path):
    """wget disables -N (and warns) whenever -O is also passed, so passing --timestamping alongside
    -O is a pure no-op that just prints a warning. A non-recursive pull_via_wget() call (which
    always uses -O) should not pass --timestamping even though it defaults to True."""

    def fake_wget(command_line, **kwargs):
        # -O's argument is the output file; touch it so the post-download file-type check passes.
        out_file = command_line[command_line.index("-O") + 1]
        open(out_file, "w").close()
        return type("Result", (), {"returncode": 0, "stderr": ""})()

    with patch("subprocess.run", side_effect=fake_wget) as mock_run:
        pull_via_wget(
            "http://127.0.0.1:1/",
            "file.txt",
            decompress=False,
            outpath=str(tmp_path / "local" / "file.txt"),
        )

    wget_call = mock_run.call_args_list[0].args[0]
    assert "--timestamping" not in wget_call
    assert "-O" in wget_call


@pytest.mark.unit
def test_a_recursive_download_that_asks_to_continue_is_refused(tmp_path):
    """Recursion plus --continue is the combination that corrupts a file changed upstream, so
    pull_via_wget() should refuse it rather than quietly downloading with one of them dropped.
    continue_incomplete defaults to True, so a recursive caller has to say otherwise."""
    with pytest.raises(ValueError, match="cannot combine continue_incomplete=True with recursion"):
        pull_via_wget(
            "http://127.0.0.1:1/",
            "files",
            decompress=False,
            outpath=str(tmp_path / "local"),
            recurse=WgetRecursionOptions.RECURSE_DIRECTORY_ONLY,
        )


@pytest.mark.unit
def test_a_recursive_download_without_timestamping_is_refused(tmp_path):
    """With --continue refused, --timestamping is the only thing keeping wget from saving a second
    copy of every file we already have as `file.1`, so a recursive download requires it."""
    with pytest.raises(ValueError, match="cannot disable timestamping in a recursive download"):
        pull_via_wget(
            "http://127.0.0.1:1/",
            "files",
            decompress=False,
            outpath=str(tmp_path / "local"),
            recurse=WgetRecursionOptions.RECURSE_DIRECTORY_ONLY,
            continue_incomplete=False,
            timestamping=False,
        )
