import json
import os

import pytest

from src.node import NodeFactory
from src.util import get_config

# ---------------------------------------------------------------------------
# TSV output assertion helpers (used by both datahandler and pipeline tests)
# ---------------------------------------------------------------------------


def read_tsv(path: str) -> list[list[str]]:
    """Return non-empty lines of a TSV file split into columns."""
    rows = []
    with open(path) as f:
        for line in f:
            stripped = line.rstrip("\n")
            if stripped:
                rows.append(stripped.split("\t"))
    return rows


def assert_large_tsv_file_valid(path: str, num_columns: int, expected_prefix: str) -> None:
    """Stream-validate a TSV too large to hold in memory: every line has num_columns, column 0 is a
    CURIE, and at least one CURIE carries expected_prefix.

    The assert_*_file_valid helpers below all go through read_tsv(), which materializes the whole
    file as a list. That is fine for a handler test over a tmp_path fixture, but NCBIGene's real
    labels/taxa/descriptions files are ~70 million rows (2-3.5 GB each), where the list costs tens of
    GB. Use this instead when the file is a full pipeline output rather than a fixture.
    """
    found_prefix = False
    with open(path) as f:
        for line in f:
            stripped = line.rstrip("\n")
            if not stripped:
                continue
            cols = stripped.split("\t")
            assert len(cols) == num_columns, f"Expected {num_columns} columns, got {len(cols)}: {cols}"
            assert ":" in cols[0], f"First column is not a CURIE: {cols[0]}"
            if not found_prefix and cols[0].startswith(expected_prefix):
                found_prefix = True
    assert found_prefix, f"No {expected_prefix} CURIEs found in {path}"


def assert_labels_file_valid(path: str) -> list[list[str]]:
    """Assert the file is non-empty and every line is PREFIX:ID\\tLabel; return the rows."""
    rows = read_tsv(path)
    assert rows, f"Labels file is empty: {path}"
    for cols in rows:
        assert len(cols) == 2, f"Expected 2 columns, got {len(cols)}: {cols}"
        assert ":" in cols[0], f"First column is not a CURIE: {cols[0]}"
    return rows


def assert_synonyms_file_valid(path: str) -> list[list[str]]:
    """Assert the file is non-empty and every line is PREFIX:ID\\tlabeltype\\tLabel; return the rows."""
    rows = read_tsv(path)
    assert rows, f"Synonyms file is empty: {path}"
    for cols in rows:
        assert len(cols) == 3, f"Expected 3 columns, got {len(cols)}: {cols}"
        assert ":" in cols[0], f"First column is not a CURIE: {cols[0]}"
    return rows


def assert_ids_file_valid(path: str) -> list[list[str]]:
    """Assert the file is non-empty and every line is PREFIX:ID\\tbiolink:Category; return the rows."""
    rows = read_tsv(path)
    assert rows, f"IDs file is empty: {path}"
    for cols in rows:
        assert len(cols) == 2, f"Expected 2 columns, got {len(cols)}: {cols}"
        assert ":" in cols[0], f"First column is not a CURIE: {cols[0]}"
    return rows


def assert_concordance_file_valid(path: str) -> list[list[str]]:
    """Assert the file is non-empty and every line is CURIE\\trelation\\tCURIE; return the rows."""
    rows = read_tsv(path)
    assert rows, f"Concordance file is empty: {path}"
    for cols in rows:
        assert len(cols) == 3, f"Expected 3 columns, got {len(cols)}: {cols}"
        assert ":" in cols[0], f"First column is not a CURIE: {cols[0]}"
        assert ":" in cols[2], f"Third column is not a CURIE: {cols[2]}"
    return rows


def assert_taxa_file_valid(path: str) -> list[list[str]]:
    """Assert the file is non-empty and every line is CURIE\\tNCBITaxon:ID; return the rows."""
    rows = read_tsv(path)
    assert rows, f"Taxa file is empty: {path}"
    for cols in rows:
        assert len(cols) == 2, f"Expected 2 columns, got {len(cols)}: {cols}"
        assert ":" in cols[0], f"First column is not a CURIE: {cols[0]}"
        assert cols[1].startswith("NCBITaxon:"), f"Second column is not an NCBITaxon CURIE: {cols[1]}"
    return rows


def glom_dict_from_cliques(cliques) -> dict:
    """Build a glom-style {curie: set} dict where every CURIE in a clique points to that
    clique's shared set object, mirroring how src.babel_utils.glom stores cliques."""
    out = {}
    for members in cliques:
        s = set(members)
        for c in members:
            out[c] = s
    return out


def assert_descriptions_file_valid(path: str) -> list[list[str]]:
    """Assert the file is non-empty and every line is CURIE\\tdescription; return the rows."""
    rows = []
    with open(path) as f:
        for line in f:
            stripped = line.rstrip("\n")
            if stripped:
                cols = stripped.split("\t", 1)
                assert len(cols) == 2, f"Expected 2 columns in descriptions file, got {len(cols)}: {cols}"
                assert ":" in cols[0], f"First column is not a CURIE: {cols[0]}"
                rows.append(cols)
    assert rows, f"Descriptions file is empty: {path}"
    return rows


CONFLATION_FIXTURE_ROWS = [
    ["NCBIGENE:1", "UniProtKB:A0A000", "UniProtKB:B0B000"],
    ["NCBIGENE:2", "UniProtKB:C0C000"],
    ["NCBIGENE:3"],
]


@pytest.fixture(scope="session")
def geneprotein_conflation_file(tmp_path_factory):
    """Session-scoped NDJSON conflation file with three sample GeneProtein groups."""
    path = tmp_path_factory.mktemp("conflation") / "GeneProtein.txt"
    with open(path, "w", encoding="utf-8") as f:
        for row in CONFLATION_FIXTURE_ROWS:
            f.write(json.dumps(row) + "\n")
    return str(path)


# Biolink Model version used throughout the test suite.  Should match config.yaml.
BIOLINK_VERSION = get_config()["biolink_version"]

# Per-mark timeout overrides (pytest-timeout); unit tests inherit the global timeout = 30.
# A test carrying several of these marks gets the longest of them (see max() in
# pytest_collection_modifyitems), so `slow` is how a pipeline test that really does need an hour
# asks for one -- the plain `pipeline` budget is deliberately tighter than that.
MARK_TIMEOUTS = {
    "network": 600,  # a download can stall on a slow mirror; 10 min before we call it hung.
    "slow": 3600,  # `slow` means genuinely long-running: a full-file scan, an hour to be safe.
    "pipeline": 900,  # most pipeline tests run in well under 15 min once the download is cached.
}


def _system_memory_gib() -> float | None:
    """Best-effort total physical RAM in GiB, or None if it can't be determined.

    Uses POSIX sysconf (available on Linux and macOS); returns None on platforms
    that don't expose it, in which case min_memory_gb-marked tests are NOT skipped.
    """
    try:
        return os.sysconf("SC_PHYS_PAGES") * os.sysconf("SC_PAGE_SIZE") / (1024**3)
    except (ValueError, AttributeError, OSError):
        return None


@pytest.fixture(scope="session")
def node_factory():
    """Session-scoped NodeFactory pointing at tests/data for label lookups.

    common_labels is pre-initialized to an empty dict so that tests don't
    require the babel_downloads/ pipeline output directory to exist.
    """
    here = os.path.abspath(os.path.dirname(__file__))
    labeldir = os.path.join(here, "data")
    fac = NodeFactory(labeldir, BIOLINK_VERSION)
    fac.common_labels = {}
    return fac


@pytest.fixture(scope="session")
def ubergraph():
    """Session-scoped UberGraph instance shared across all ubergraph tests.

    Skips all dependent tests if the server is unreachable (network failure).
    XFails all dependent tests if the server is reachable but returns an error response.
    """
    import urllib.error

    from src.ubergraph import UberGraph

    ug = UberGraph()
    try:
        ug.triplestore.query("SELECT (1 AS ?x) WHERE {}", ["x"])
    except urllib.error.HTTPError as e:
        pytest.xfail(f"UberGraph server returned HTTP {e.code}: {e}")
    except (TimeoutError, urllib.error.URLError, OSError) as e:
        pytest.skip(f"Cannot connect to UberGraph ({ug.sparql_url}): {e}")
    except Exception as e:
        pytest.xfail(f"UberGraph probe query failed: {e}")
    return ug


def pytest_addoption(parser):
    parser.addoption(
        "--network",
        action="store_true",
        default=False,
        help="Run tests that require live internet access",
    )
    parser.addoption(
        "--pipeline",
        action="store_true",
        default=False,
        help="Run tests that invoke Snakemake rules (requires babel_downloads/)",
    )
    parser.addoption(
        "--all",
        action="store_true",
        default=False,
        help="Run all tests (equivalent to --network --pipeline)",
    )
    parser.addoption(
        "--regenerate",
        action="store_true",
        default=False,
        help=(
            "By default, pipeline tests assume that intermediate files "
            "(in babel_outputs/intermediate/) are up-to-date and can be reused. "
            "Use --regenerate to force pipeline processing fixtures to re-run "
            "write_X_ids() even when their output files already exist."
        ),
    )
    parser.addoption(
        "--build-dir",
        default=None,
        help=(
            "A finished build directory (compendia/, reports/, ...) for pipeline tests that read a "
            "build's outputs rather than produce them. Defaults to config.yaml's output_directory; "
            "pass e.g. data/2026jul22 to test the reports copied from a published build."
        ),
    )


def pytest_configure(config):
    # Auto-disable coverage when xdist workers are requested.
    # Markers are registered in pyproject.toml [tool.pytest.ini_options] — not here.
    # Combining pytest-cov with -n causes an OSError("cannot send (already closed?)")
    # from execnet during pytest_sessionfinish.  Setting no_cov here is equivalent to
    # passing --no-cov and takes effect before pytest-cov starts collecting data.
    try:
        n_workers = config.getoption("-n", default=None)
    except (ValueError, AttributeError):
        n_workers = None
    if n_workers is not None and str(n_workers) not in ("0", "no"):
        if hasattr(config.option, "no_cov") and not config.option.no_cov:
            config.option.no_cov = True
            print(  # noqa: T201
                "\nNOTE: coverage disabled automatically because -n was given "
                "(pytest-cov + pytest-xdist causes teardown errors; use --no-cov explicitly to suppress this message)."
            )


def pytest_collection_modifyitems(config, items):
    run_all = config.getoption("--all")
    skip_network = pytest.mark.skip(reason="pass --network (or --all) to run")
    skip_pipeline = pytest.mark.skip(reason="pass --pipeline (or --all) to run (and ensure babel_downloads/ exists)")
    for item in items:
        if "network" in item.keywords and not run_all and not config.getoption("--network"):
            item.add_marker(skip_network)
        if "pipeline" in item.keywords and not run_all and not config.getoption("--pipeline"):
            item.add_marker(skip_pipeline)

    # Skip tests that declare a minimum RAM requirement (@pytest.mark.min_memory_gb(n))
    # when this machine has less than that.  Prevents OOM/swap-thrash on small machines
    # (e.g. the ChEMBL pipeline tests bulk-load a ~17 GB TTL into an in-memory store).
    total_mem_gib = _system_memory_gib()
    if total_mem_gib is not None:
        for item in items:
            marker = item.get_closest_marker("min_memory_gb")
            if marker is None:
                continue
            required = marker.args[0] if marker.args else marker.kwargs.get("n")
            if required is None:
                raise ValueError(
                    f"{item.nodeid}: @pytest.mark.min_memory_gb requires an argument, e.g. min_memory_gb(128)"
                )

            if total_mem_gib < required:
                item.add_marker(
                    pytest.mark.skip(reason=f"needs >= {required} GiB RAM; this machine has {total_mem_gib:.1f} GiB")
                )

    for item in items:
        if item.get_closest_marker("timeout"):
            continue  # explicit override wins
        applicable = [seconds for mark_name, seconds in MARK_TIMEOUTS.items() if item.get_closest_marker(mark_name)]
        if applicable:
            item.add_marker(pytest.mark.timeout(max(applicable)))

    # When running with xdist, group all pipeline tests into one worker so that
    # large session-scoped fixtures (Mesh, UMLS, etc.) are only loaded once.
    # Each xdist worker has its own Python session, so without grouping N workers
    # would each load mesh.nt into RAM simultaneously, exhausting available memory.
    try:
        n_workers = config.getoption("-n", default=None)
    except (ValueError, AttributeError):
        n_workers = None
    if n_workers is not None and str(n_workers) not in ("0", "no"):
        pipeline_items = [item for item in items if item.get_closest_marker("pipeline")]
        if pipeline_items:
            for item in pipeline_items:
                item.add_marker(pytest.mark.xdist_group("pipeline"))
            print(  # noqa: T201
                f"\nNOTE: {len(pipeline_items)} pipeline test(s) grouped to a single xdist "
                "worker to prevent simultaneous mesh.nt loads (OOM risk with -n > 1)."
            )
