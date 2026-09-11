"""Unit tests for src/accel.py, the gateway to the optional Rust extension.

src/accel.py decides whether the compiled extension is available and safe to use. Its three
branches each have a consequence that is expensive to discover late:

- a missing extension must fall back to Python, because every snakefile imports `src.*` at
  DAG-parse time and raising would take down all 245 rules for anyone without a Rust toolchain;
- a stale extension must raise, because the extension is installed editable and a `git pull` does
  not rebuild it, so a silent stale binary could run for a 12-hour rule;
- `BABEL_DISABLE_RUST` must force the Python path, which is what makes an A/B measurement possible.

The module resolves availability at import time, so the fallback branches are tested by reloading
it under a patched environment. The staleness check is tested directly against stub modules --
building a genuinely stale extension would mean compiling Rust inside a unit test.
"""

import builtins
import importlib
import types

import pytest

import src.accel


def reload_accel(monkeypatch, *, disable=None, hide_extension=False):
    """Re-import src.accel under a patched environment and return the fresh module."""
    monkeypatch.delenv("BABEL_DISABLE_RUST", raising=False)
    if disable is not None:
        monkeypatch.setenv("BABEL_DISABLE_RUST", disable)

    if hide_extension:
        real_import = builtins.__import__

        def fail_on_accel(name, globals=None, locals=None, fromlist=(), level=0):
            if fromlist and "_accel" in fromlist:
                raise ImportError("no compiled extension in this test")
            return real_import(name, globals, locals, fromlist, level)

        monkeypatch.setattr(builtins, "__import__", fail_on_accel)

    return importlib.reload(src.accel)


@pytest.fixture(autouse=True)
def restore_accel():
    """Leave src.accel in its real state, so reloads here can't leak into other tests."""
    yield
    importlib.reload(src.accel)


def stub_extension(version):
    """A stand-in for the compiled module, carrying just the attribute the guard reads."""
    module = types.ModuleType("src._accel")
    if version is not None:
        module.ABI_VERSION = version
    return module


# FALLING BACK TO PYTHON


@pytest.mark.unit
def test_a_missing_extension_falls_back_instead_of_raising(monkeypatch):
    """Without a compiled extension, importing src.accel must succeed and expose accel = None.

    Raising here would break every Snakemake target for a contributor with no Rust toolchain, not
    just the rules that would have used Rust.
    """
    assert reload_accel(monkeypatch, hide_extension=True).accel is None


@pytest.mark.unit
def test_babel_disable_rust_forces_the_python_path(monkeypatch):
    """BABEL_DISABLE_RUST=1 should select the Python implementations even when the extension is
    built, so the two can be timed against each other in one checkout."""
    assert reload_accel(monkeypatch, disable="1").accel is None


# GUARDING AGAINST A STALE BUILD


@pytest.mark.unit
def test_a_matching_abi_version_is_accepted():
    """The guard returns the module untouched when the versions agree."""
    stub = stub_extension(src.accel._REQUIRED_ABI_VERSION)
    assert src.accel.check_abi_version(stub) is stub


@pytest.mark.unit
@pytest.mark.parametrize(
    "version,description",
    [
        (src.accel._REQUIRED_ABI_VERSION + 1, "extension newer than the checkout"),
        (src.accel._REQUIRED_ABI_VERSION - 1, "extension older than the checkout"),
        (None, "extension predates ABI_VERSION existing at all"),
    ],
)
def test_a_mismatched_abi_version_raises_with_a_rebuild_instruction(version, description):
    """Any mismatch must raise, and the message must say how to fix it.

    The rebuild command is asserted because that is the whole point of the error: the person who
    hits it is mid-run and needs the fix, not a diagnosis.
    """
    assert description  # names the case in the failure output
    with pytest.raises(RuntimeError, match="uv sync --reinstall-package babel-pipeline"):
        src.accel.check_abi_version(stub_extension(version))


@pytest.mark.unit
def test_the_built_extension_matches_the_version_python_expects():
    """The built extension's ABI_VERSION must be the one this checkout expects.

    Every `uv sync` builds the extension (uv bootstraps a Rust toolchain if cargo is missing), so
    its absence means the build silently regressed -- a failure, not a skip, so that regression
    can't hide. Both constants are bumped by hand in the same commit: failing in CI after a Rust
    change means the bump was forgotten; a version mismatch locally means the checkout needs
    `uv sync --reinstall-package babel-pipeline`.
    """
    assert src.accel.accel is not None, "compiled extension not built -- run `uv sync`"
    assert src.accel.accel.ABI_VERSION == src.accel._REQUIRED_ABI_VERSION


@pytest.mark.unit
def test_the_built_extension_exports_every_accelerated_function():
    """Each function src/_accel.pyi declares must actually be exported by the built extension.

    This is the list to extend when a port lands; it is what turns a forgotten `add_function` in
    rust/src/lib.rs into a test failure rather than a fallback to Python that nobody notices.
    """
    assert src.accel.accel is not None, "compiled extension not built -- run `uv sync`"
    for name in ["convert_synonyms_to_sapbert"]:
        assert callable(getattr(src.accel.accel, name, None)), f"src._accel does not export {name}"
