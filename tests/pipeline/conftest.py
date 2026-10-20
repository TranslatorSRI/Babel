"""Session-scoped fixtures for pipeline tests.

Processing fixtures write intermediate ID files to the same stable paths that Snakemake
uses: {intermediate_directory}/{snakemake_dir}/ids/{vocab}, where snakemake_dir is the
compendium name unless overridden by compendium_directories in config.yaml (e.g.
diseasephenotype → disease).  Example: babel_outputs/intermediate/anatomy/ids/UMLS.

By default, if a file already exists it is reused without re-running write_X_ids().
Pass --regenerate to force re-processing even when files are present.

Prerequisites vary by vocabulary:
- MESH, OMIM: fixtures auto-download the required file if absent.
- UMLS: requires UMLS_API_KEY when files are not already cached.
- NCIT, GO: require a live UberGraph SPARQL endpoint (no file download).
If a prerequisite is unavailable, all dependent tests are skipped automatically.

All tests in this package are marked `pipeline` and are skipped by default.
Run with:  uv run pytest tests/pipeline/ --pipeline --no-cov -v

## Adding a new vocabulary

1. Add a download/connectivity fixture (e.g. `my_vocab_source`) to this file.
2. Add a processing fixture (e.g. `my_vocab_pipeline_outputs`) that calls all
   write_X_ids() functions for that vocabulary and returns {compendium_name: output_path}.
3. Add one entry to VOCABULARY_REGISTRY: `"MYVOCAB": "my_vocab_pipeline_outputs"`.
That's it — test_vocabulary_partitioning.py picks it up automatically.
"""

import os
import subprocess

import pytest

from src.babel_utils import make_local_name
from src.createcompendia import anatomy, chemicals, diseasephenotype, protein, taxon
from src.datahandlers.mesh import Mesh, pull_mesh
from src.prefixes import EMAPA, MP
from src.ubergraph import build_sets

# ---------------------------------------------------------------------------
# Shared helpers (not fixtures — importable by test files)
# ---------------------------------------------------------------------------


def get_curies_from_ids_file(path: str) -> set[str]:
    """Return the set of CURIEs (first column) from a TSV output file."""
    ids = set()
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                ids.add(line.split("\t")[0])
    return ids


def get_curies_and_types_from_ids_file(path: str) -> dict[str, str | None]:
    """Return {CURIE: biolink_type_or_None} from an intermediate ID file.

    The optional second column is a Biolink type hint written by write_umls_ids() and
    similar functions.  If absent (e.g. MESH), the value is None.
    """
    result = {}
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line:
                parts = line.split("\t")
                result[parts[0]] = parts[1] if len(parts) > 1 else None
    return result


def _snakemake_dir(compendium: str) -> str:
    """Return the Snakemake intermediate directory name for a compendium.

    Falls back to the compendium name itself when not listed in compendium_directories
    (e.g. diseasephenotype → disease, processactivitypathway → process).
    """
    from src.util import (
        get_config,  # deferred: get_config() reads config.yaml at call time so path helpers always reflect the current file
    )

    cfg = get_config()
    return cfg.get("compendium_directories", {}).get(compendium, compendium)


def _intermediate_concord_path(compendium: str, vocab: str) -> str:
    """Stable concord path: {intermediate_directory}/{snakemake_dir}/concords/{vocab}."""
    from src.util import get_config  # deferred: same rationale as _snakemake_dir

    return os.path.join(get_config()["intermediate_directory"], _snakemake_dir(compendium), "concords", vocab)


def _any_concord_xrefs(concords_dir: str, curie1: str, curie2: str) -> bool:
    """Return True if curie1 and curie2 are a direct xref pair in ANY concord file under concords_dir.

    Scans every regular file in concords_dir (skipping .yaml metadata files and subdirectories).
    Concord files are tab-separated <curie1> <relation> <curie2> triples; this function checks
    the first and last columns only (skipping the relation), so column order does not matter.
    Indirect equivalences through multi-hop chains are not detected — this is intentional: it's
    fast for TDD and locates the specific concord file that is the source of a bad link.
    """
    for entry in os.scandir(concords_dir):
        if not entry.is_file() or entry.name.endswith(".yaml"):
            continue
        try:
            with open(entry.path) as f:
                for line in f:
                    parts = line.strip().split("\t")
                    if len(parts) >= 2:
                        curies = {parts[0], parts[-1]}
                        if curie1 in curies and curie2 in curies:
                            return True
        except (OSError, UnicodeDecodeError):
            continue
    return False


def _output_paths(outputs: dict) -> dict[str, str]:
    """Filter a vocab_outputs dict down to just the string-valued output paths.

    Some fixtures return extra non-path data alongside their output paths — for example,
    mesh_pipeline_outputs includes 'excluded_tree_terms' (a set of descriptor CURIEs) for
    use by test_mesh.py.  This helper strips non-string values so callers can
    iterate over compendium output files without special-casing each vocabulary.
    """
    return {name: path for name, path in outputs.items() if isinstance(path, str)}


def _intermediate_id_path(compendium: str, vocab: str) -> str:
    """Return {intermediate_directory}/{snakemake_dir}/ids/{vocab}, matching the Snakemake path.

    Using the same paths as Snakemake means files from a prior pipeline run are reused directly.
    """
    from src.util import get_config  # deferred: same rationale as _snakemake_dir

    return os.path.join(get_config()["intermediate_directory"], _snakemake_dir(compendium), "ids", vocab)


def _maybe_run(outfile: str | list[str], fn, regenerate: bool) -> str:
    """Run fn() to (re)generate outfile unless it exists and regenerate is False.

    fn is a zero-argument callable (typically a lambda) that writes to outfile.
    Creates parent directories as needed.  Always returns outfile (the first, if given several).

    Pass a list when one call produces several outputs together (NCBIGene's single pass over
    gene_info.gz writes labels, synonyms, taxa and descriptions at once): fn() then runs unless
    *every* output is already present, so a half-finished run is not mistaken for a cached one.
    """
    outfiles = [outfile] if isinstance(outfile, str) else outfile
    if regenerate or not all(os.path.exists(f) for f in outfiles):
        for f in outfiles:
            os.makedirs(os.path.dirname(f), exist_ok=True)
        fn()
    else:
        print(f"[pipeline] reusing cached {', '.join(outfiles)} (pass --regenerate to refresh)")  # noqa: T201
    return outfiles[0]


# ---------------------------------------------------------------------------
# Generic download helper
# ---------------------------------------------------------------------------


def _download_or_fail(label: str, pull_fn, expected_path: str) -> str:
    """Download expected_path via pull_fn() if absent; pytest.fail() with details on failure."""
    if not os.path.exists(expected_path):
        try:
            pull_fn()
        except Exception as e:
            pytest.fail(f"Could not download {label}: {e}")
    return expected_path


# ---------------------------------------------------------------------------
# --regenerate fixture
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def regenerate(request):
    """True when --regenerate was passed on the command line.

    Processing fixtures check this to decide whether to re-run write_X_ids()
    even when their output file already exists at the stable intermediate path.
    """
    return request.config.getoption("--regenerate")


@pytest.fixture(scope="session")
def build_dir(request):
    """The finished build directory a test should read: ``--build-dir``, else config.yaml's ``output_directory``.

    For tests over a build's *outputs* (its reports, compendia), as opposed to the processing
    fixtures above, which produce intermediate files. Lets the same test run against a local
    ``babel_outputs/`` or against a build's files copied from stars.renci.org into ``data/``.
    """
    from src.util import get_config  # deferred: same rationale as _snakemake_dir

    return request.config.getoption("--build-dir") or get_config()["output_directory"]


# ---------------------------------------------------------------------------
# Snakemake-backed pipeline output fixture
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def pipeline_output(regenerate):
    """Factory: ensure a Snakemake rule has produced its output file; return the path.

    Reuses existing files without re-running (same caching contract as _maybe_run).
    If the file is absent, runs `uv run snakemake --cores 1 --rerun-incomplete <path>`,
    targeting the output file directly rather than the default `all` target.  Targeting
    the file means Snakemake only resolves the DAG needed to build it; using the default
    target would fail at DAG-build time on unrelated rules whose inputs are absent on a
    from-scratch tree (e.g. generate_mapping_sources_table's metadata YAMLs).
    Fails all dependent tests if Snakemake fails or the output is not produced.

    Usage inside a fixture:
        @pytest.fixture(scope="session")
        def my_fixture(pipeline_output):
            return pipeline_output("my_snakemake_rule", "path/to/output")
    """

    def _get(rule: str, path: str) -> str:
        if os.path.exists(path) and not regenerate:
            return path
        os.makedirs(os.path.dirname(path), exist_ok=True)
        result = subprocess.run(
            ["uv", "run", "snakemake", "--cores", "1", "--rerun-incomplete", path],
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            pytest.fail(
                f"Snakemake failed building '{path}' (rule '{rule}', exit {result.returncode}); "
                f"run `uv run snakemake --cores 1 --rerun-incomplete {path}` to diagnose.\n"
                f"stderr: {result.stderr[:600]}"
            )
        if not os.path.exists(path):
            pytest.fail(
                f"Snakemake rule '{rule}' ran but did not produce expected output: {path}\n"
                f"Re-run with:  uv run snakemake --cores 1 --rerun-incomplete {path}"
            )
        return path

    return _get


# ---------------------------------------------------------------------------
# Chemicals concords directory fixture
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def chemicals_concords_dir(pipeline_output):
    """Ensure at least one chemicals concord file is present; return the concords directory path.

    Uses wikipedia_mesh_chebi as a sentinel: if absent, runs Snakemake's
    get_chemical_wikipedia_relationships rule (no large downloads required, fast).
    Other concord files (UNICHEM, CHEBI, …) are used if already present from a prior full
    pipeline run, but are not generated automatically — UNICHEM in particular requires ~512 GB RAM.
    """
    from src.util import get_config  # deferred: same rationale as _snakemake_dir

    cfg = get_config()
    concords_dir = os.path.join(cfg["intermediate_directory"], _snakemake_dir("chemicals"), "concords")
    sentinel = os.path.join(concords_dir, "wikipedia_mesh_chebi")
    pipeline_output("get_chemical_wikipedia_relationships", sentinel)
    return concords_dir


# ---------------------------------------------------------------------------
# MESH download + processing fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def mesh_nt():
    """Download babel_downloads/MESH/mesh.nt, or skip if unavailable."""
    return _download_or_fail(
        "MESH mesh.nt",
        pull_mesh,
        make_local_name("mesh.nt", subpath="MESH"),
    )


@pytest.fixture(scope="session")
def mesh_pipeline_outputs(mesh_nt, regenerate):
    """Run write_mesh_ids for all five compendia; skip if mesh_nt unavailable.

    Output files are written to babel_outputs/intermediate/{type}/ids/MESH and
    reused on subsequent runs unless --regenerate is passed.
    """

    def p(compendium):
        return _intermediate_id_path(compendium, "MESH")

    _maybe_run(p("chemicals"), lambda: chemicals.write_mesh_ids(p("chemicals")), regenerate)
    _maybe_run(p("protein"), lambda: protein.write_mesh_ids(p("protein")), regenerate)
    _maybe_run(p("anatomy"), lambda: anatomy.write_mesh_ids(p("anatomy")), regenerate)
    _maybe_run(p("diseasephenotype"), lambda: diseasephenotype.write_mesh_ids(p("diseasephenotype")), regenerate)
    _maybe_run(p("taxon"), lambda: taxon.write_mesh_ids(p("taxon")), regenerate)

    return {
        "chemicals": p("chemicals"),
        "protein": p("protein"),
        "anatomy": p("anatomy"),
        "diseasephenotype": p("diseasephenotype"),
        "taxon": p("taxon"),
    }


@pytest.fixture(scope="session")
def excluded_mesh_tree_terms(mesh_nt):
    """Return the set of descriptor CURIEs that must NOT appear in the chemicals output.

    Used only by test_mesh.py.  Kept separate from mesh_pipeline_outputs so
    that the Mesh() load (~2 GB) is only incurred when this fixture is actually
    requested — running vocabulary-partitioning tests alone does not trigger it.

    Only protein subtrees are excluded here; D05.374 (Micelles), D05.750 (Polymers),
    and D05.937 (Smart Materials) are correctly included in chemicals as CHEMICAL_ENTITY.
    """
    m = Mesh()
    terms = set()
    for tree in ["D12.776", "D05.500", "D05.875", "D08.811", "D08.622", "D08.244"]:
        terms.update(m.get_terms_in_tree(tree))
    return terms


# ---------------------------------------------------------------------------
# UMLS download + processing fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def umls_rrf_files():
    """Ensure MRCONSO.RRF and MRSTY.RRF are present; skip if files are absent and download fails."""
    mrconso = make_local_name("MRCONSO.RRF", subpath="UMLS")
    mrsty = make_local_name("MRSTY.RRF", subpath="UMLS")

    if not os.path.exists(mrconso) or not os.path.exists(mrsty):
        # UMLS_API_KEY is only required when the files are not yet cached.
        if not os.environ.get("UMLS_API_KEY"):
            pytest.skip("UMLS_API_KEY not set and UMLS files not cached; cannot download UMLS files")
        try:
            from src.datahandlers import (
                umls as umls_handler,  # deferred: only import the UMLS handler when a download is actually needed; if files are already cached this module never loads
            )
            from src.util import get_config  # deferred: same rationale as _snakemake_dir

            cfg = get_config()
            umls_handler.download_umls(
                cfg["umls_version"],
                cfg["umls"]["subset"],
                cfg["download_directory"] + "/UMLS",
            )
        except Exception as e:
            pytest.skip(f"Could not download UMLS: {e}")

    return {"mrconso": mrconso, "mrsty": mrsty}


@pytest.fixture(scope="session")
def umls_pipeline_outputs(umls_rrf_files, regenerate):
    """Run write_umls_ids for all seven compendia; returns dict of output paths.

    Output files are written to babel_outputs/intermediate/{type}/ids/UMLS and
    reused on subsequent runs unless --regenerate is passed.
    """
    from src.createcompendia import (  # deferred: gene and processactivitypathway are not in the module-level imports to avoid loading every compendium at collection time; they are only needed for UMLS
        gene,
        processactivitypathway,
    )
    from src.util import get_config  # deferred: same rationale as _snakemake_dir

    mrconso = umls_rrf_files["mrconso"]
    mrsty = umls_rrf_files["mrsty"]
    cfg = get_config()
    badumlsfile = os.path.join(cfg["input_directory"], "badumls")

    def p(compendium):
        return _intermediate_id_path(compendium, "UMLS")

    _maybe_run(p("chemicals"), lambda: chemicals.write_umls_ids(mrsty, p("chemicals")), regenerate)
    _maybe_run(p("protein"), lambda: protein.write_umls_ids(mrsty, p("protein")), regenerate)
    _maybe_run(p("anatomy"), lambda: anatomy.write_umls_ids(mrsty, p("anatomy")), regenerate)
    _maybe_run(
        p("diseasephenotype"),
        lambda: diseasephenotype.write_umls_ids(mrsty, p("diseasephenotype"), badumlsfile),
        regenerate,
    )
    _maybe_run(
        p("processactivitypathway"),
        lambda: processactivitypathway.write_umls_ids(mrsty, p("processactivitypathway")),
        regenerate,
    )
    _maybe_run(p("taxon"), lambda: taxon.write_umls_ids(mrsty, p("taxon")), regenerate)
    _maybe_run(p("gene"), lambda: gene.write_umls_ids(mrconso, mrsty, p("gene")), regenerate)

    return {
        name: p(name)
        for name in ["chemicals", "protein", "anatomy", "diseasephenotype", "processactivitypathway", "taxon", "gene"]
    }


# ---------------------------------------------------------------------------
# OMIM download + processing fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def omim_mim2gene():
    """Download babel_downloads/OMIM/mim2gene.txt, or skip if unavailable."""
    from src.datahandlers.omim import (
        pull_omim,  # deferred: omim handler not in module-level imports; only needed for this fixture
    )

    return _download_or_fail(
        "OMIM mim2gene.txt",
        pull_omim,
        make_local_name("mim2gene.txt", subpath="OMIM"),
    )


@pytest.fixture(scope="session")
def omim_pipeline_outputs(omim_mim2gene, regenerate):
    """Run write_omim_ids for disease/phenotype and gene; skip if mim2gene.txt unavailable.

    Output files are written to babel_outputs/intermediate/{type}/ids/OMIM and
    reused on subsequent runs unless --regenerate is passed.
    """
    from src.createcompendia import gene  # deferred: gene not in module-level imports; only needed for OMIM

    infile = omim_mim2gene

    def p(compendium):
        return _intermediate_id_path(compendium, "OMIM")

    _maybe_run(
        p("diseasephenotype"), lambda: diseasephenotype.write_omim_ids(infile, p("diseasephenotype")), regenerate
    )
    _maybe_run(p("gene"), lambda: gene.write_omim_ids(infile, p("gene")), regenerate)

    return {"diseasephenotype": p("diseasephenotype"), "gene": p("gene")}


# ---------------------------------------------------------------------------
# NCBIGene download + processing fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def ncbigene_gene_info():
    """Download babel_downloads/NCBIGene/gene_info.gz, or fail if unavailable.

    This is a large file (>1 GB compressed); dependent tests are marked `slow`.
    """
    from src.datahandlers.ncbigene import pull_ncbigene  # deferred: only import when NCBIGene tests are requested

    return _download_or_fail(
        "NCBIGene gene_info.gz",
        lambda: pull_ncbigene(["gene_info.gz"]),
        make_local_name("gene_info.gz", subpath="NCBIGene"),
    )


@pytest.fixture(scope="session")
def ncbigene_pipeline_outputs(ncbigene_gene_info, regenerate):
    """Run pull_ncbigene_labels_synonyms_and_taxa; returns {labels, synonyms, taxa, descriptions} paths.

    Output files go to babel_downloads/NCBIGene/ and are reused on subsequent runs unless
    --regenerate is passed.  Processing the full gene_info.gz is slow (millions of rows), so
    the four outputs are generated together in one pass.
    """
    from src.datahandlers.ncbigene import pull_ncbigene_labels_synonyms_and_taxa  # deferred

    labels = make_local_name("labels", subpath="NCBIGene")
    synonyms = make_local_name("synonyms", subpath="NCBIGene")
    taxa = make_local_name("taxa", subpath="NCBIGene")
    descriptions = make_local_name("descriptions", subpath="NCBIGene")

    _maybe_run(
        [labels, synonyms, taxa, descriptions],
        lambda: pull_ncbigene_labels_synonyms_and_taxa(ncbigene_gene_info, labels, synonyms, taxa, descriptions),
        regenerate,
    )

    return {"labels": labels, "synonyms": synonyms, "taxa": taxa, "descriptions": descriptions}


# ---------------------------------------------------------------------------
# UberGraph connectivity fixture (shared prerequisite for NCIT and GO)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def ubergraph_connection():
    """Verify the UberGraph SPARQL endpoint is reachable; skip if not.

    NCIT and GO use write_obo_ids() which queries UberGraph live — there is no
    local file to download.  This fixture acts as the prerequisite check that
    both ncit_pipeline_outputs and go_pipeline_outputs depend on.
    """
    try:
        from src.datahandlers.obo import (
            UberGraph,  # deferred: obo module has heavy RDFLib/SPARQL dependencies; only loaded when NCIT/GO tests are actually requested
        )

        ug = UberGraph()
        # Minimal health check: GO:0005575 (cellular component) is a small, stable term.
        result = ug.get_subclasses_of("GO:0005575")
        if not result:
            pytest.skip("UberGraph returned empty result; endpoint may be unavailable")
    except Exception as e:
        pytest.skip(f"UberGraph not accessible: {e}")


# ---------------------------------------------------------------------------
# NCIT processing fixture (uses UberGraph, no file download)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def ncit_pipeline_outputs(ubergraph_connection, regenerate):
    """Run write_ncit_ids for anatomy and disease/phenotype via UberGraph.

    Output files are written to babel_outputs/intermediate/{type}/ids/NCIT and
    reused on subsequent runs unless --regenerate is passed.
    """

    def p(compendium):
        return _intermediate_id_path(compendium, "NCIT")

    _maybe_run(p("anatomy"), lambda: anatomy.write_ncit_ids(p("anatomy")), regenerate)
    _maybe_run(p("diseasephenotype"), lambda: diseasephenotype.write_ncit_ids(p("diseasephenotype")), regenerate)

    return {"anatomy": p("anatomy"), "diseasephenotype": p("diseasephenotype")}


# ---------------------------------------------------------------------------
# GO processing fixture (uses UberGraph, no file download)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def go_pipeline_outputs(ubergraph_connection, regenerate):
    """Run write_go_ids for anatomy and process/activity/pathway via UberGraph.

    Output files are written to babel_outputs/intermediate/{type}/ids/GO and
    reused on subsequent runs unless --regenerate is passed.
    """
    from src.createcompendia import (
        processactivitypathway,  # deferred: processactivitypathway not in module-level imports; only needed for GO
    )

    def p(compendium):
        return _intermediate_id_path(compendium, "GO")

    _maybe_run(p("anatomy"), lambda: anatomy.write_go_ids(p("anatomy")), regenerate)
    _maybe_run(
        p("processactivitypathway"),
        lambda: processactivitypathway.write_go_ids(p("processactivitypathway")),
        regenerate,
    )

    return {"anatomy": p("anatomy"), "processactivitypathway": p("processactivitypathway")}


# ---------------------------------------------------------------------------
# EMAPA / MP processing fixtures (use UberGraph, no file download)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def emapa_pipeline_outputs(ubergraph_connection, regenerate):
    """Run EMAPA anatomy ID extraction and ensure EMAPA concord generation is available."""

    id_path = _intermediate_id_path("anatomy", "EMAPA")
    concord_path = _intermediate_concord_path("anatomy", "EMAPA")

    _maybe_run(id_path, lambda: anatomy.write_emapa_ids(id_path), regenerate)
    _maybe_run(
        concord_path,
        lambda: _write_emapa_concord(concord_path),
        regenerate,
    )

    return {"anatomy": id_path}


def _write_emapa_concord(concord_path: str) -> None:
    """Build the EMAPA concord exactly as build_anatomy_obo_relationships() does.

    Delegating to anatomy.build_emapa_obo_relationships() is what keeps the part_of
    traversal and the ignore_list in step with the real build. Calling build_sets()
    directly here once wrote a subClassOf-only, unfiltered concord to this same stable
    intermediate path, which a later Snakemake run would then treat as up to date.
    """
    os.makedirs(os.path.dirname(concord_path), exist_ok=True)
    with open(concord_path, "w") as emapa_file:
        anatomy.build_emapa_obo_relationships({EMAPA: emapa_file})


@pytest.fixture(scope="session")
def mp_pipeline_outputs(ubergraph_connection, regenerate):
    """Run MP disease/phenotype ID extraction and ensure MP concord generation is available."""

    id_path = _intermediate_id_path("diseasephenotype", "MP")
    concord_path = _intermediate_concord_path("diseasephenotype", "MP")

    _maybe_run(id_path, lambda: diseasephenotype.write_mp_ids(id_path), regenerate)
    _maybe_run(
        concord_path,
        lambda: _write_mp_concord(concord_path),
        regenerate,
    )

    return {"diseasephenotype": id_path}


def _write_mp_concord(concord_path: str) -> None:
    """Build the MP concord exactly as build_disease_obo_relationships() does.

    MP_XREF_ALLOWED_PREFIXES is not optional here. Without it this writes the raw ~663-row xref
    dump (anatomy, processes, citations, Wikipedia URLs) to the same stable intermediate path the
    real build uses, so a later Snakemake run would treat the unfiltered file as up to date and
    glom category-error xrefs as equivalences. It would also leave the allowlist -- the whole point
    of MP's ingest -- untested.
    """
    os.makedirs(os.path.dirname(concord_path), exist_ok=True)
    with open(concord_path, "w") as mp_file:
        build_sets(
            f"{MP}:0000001",
            {MP: mp_file},
            "xref",
            allowed_prefixes=diseasephenotype.MP_XREF_ALLOWED_PREFIXES,
        )


# ---------------------------------------------------------------------------
# Vocabulary registry + parametrized fixture for test_vocabulary_partitioning.py
# ---------------------------------------------------------------------------

# Maps vocabulary name → processing fixture name.
# Each processing fixture returns a {compendium_name: output_path} dict.
# To add a new vocabulary: add its fixtures above, then add one line here.
VOCABULARY_REGISTRY = {
    "MESH": "mesh_pipeline_outputs",
    "UMLS": "umls_pipeline_outputs",
    "OMIM": "omim_pipeline_outputs",
    "NCIT": "ncit_pipeline_outputs",
    "GO": "go_pipeline_outputs",
    "EMAPA": "emapa_pipeline_outputs",
    "MP": "mp_pipeline_outputs",
    "EC": "ec_ids_outputs",
    "CLO": "clo_ids_outputs",
    "EFO": "efo_ids_outputs",
}


# ---------------------------------------------------------------------------
# EC, Rhea, ChEMBL, CLO, EFO download + processing fixtures
#
# EC, CLO, and EFO are registered in VOCABULARY_REGISTRY via slim id-only
# wrapper fixtures (ec_ids_outputs, clo_ids_outputs, efo_ids_outputs) defined
# near the bottom of this file.
#
# Rhea, ChEMBL and NCBIGene are NOT registered: they produce labels/concords/smiles/synonyms but
# no IDs file, so they cannot be exercised by test_vocabulary_partitioning.py
# without adding write_X_ids() functions first.
# See https://github.com/NCATSTranslator/Babel/issues/749
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def ec_rdf_file():
    """Download babel_downloads/EC/enzyme.rdf, or fail if unavailable."""
    from src.datahandlers.ec import pull_ec  # deferred: only import when EC tests are requested

    return _download_or_fail(
        "EC enzyme.rdf",
        pull_ec,
        make_local_name("enzyme.rdf", subpath="EC"),
    )


@pytest.fixture(scope="session")
def ec_pipeline_outputs(ec_rdf_file, regenerate):
    """Run ECgraph label/synonym/ID extraction; returns {labels, synonyms, ids} paths.

    Output files are written to babel_downloads/EC/ (labels, synonyms) and
    babel_outputs/intermediate/process/ids/EC (ids).  Reused on subsequent runs
    unless --regenerate is passed.
    """
    from src.util import get_config  # deferred: same rationale as _snakemake_dir

    cfg = get_config()
    labels = os.path.join(cfg["download_directory"], "EC", "labels")
    synonyms = os.path.join(cfg["download_directory"], "EC", "synonyms")
    ids = _intermediate_id_path("processactivitypathway", "EC")

    from src.datahandlers.ec import ECgraph  # deferred: avoids loading RDF at import time

    needs_labels = regenerate or not os.path.exists(labels) or not os.path.exists(synonyms)
    needs_ids = regenerate or not os.path.exists(ids)
    if needs_labels or needs_ids:
        ecgraph = ECgraph()
        if needs_labels:
            os.makedirs(os.path.dirname(labels), exist_ok=True)
            ecgraph.pull_EC_labels_and_synonyms(labels, synonyms)
        _maybe_run(ids, lambda: ecgraph.pull_EC_ids(ids), regenerate)

    return {"labels": labels, "synonyms": synonyms, "ids": ids}


# ---------------------------------------------------------------------------
# Rhea download + processing fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def rhea_rdf_file():
    """Download babel_downloads/RHEA/rhea.rdf, or fail if unavailable."""
    from src.datahandlers.rhea import pull_rhea  # deferred

    return _download_or_fail(
        "Rhea rhea.rdf",
        pull_rhea,
        make_local_name("rhea.rdf", subpath="RHEA"),
    )


@pytest.fixture(scope="session")
def rhea_pipeline_outputs(rhea_rdf_file, regenerate):
    """Run Rhea label extraction and EC concordance; returns {labels, concords} paths.

    Output files go to babel_downloads/RHEA/labels and
    babel_outputs/intermediate/process/concords/RHEA.  Reused unless --regenerate is passed.
    """
    from src.util import get_config  # deferred

    cfg = get_config()
    labels = os.path.join(cfg["download_directory"], "RHEA", "labels")
    concords = os.path.join(cfg["intermediate_directory"], _snakemake_dir("processactivitypathway"), "concords", "RHEA")
    metadata_yaml = os.path.join(
        cfg["intermediate_directory"], _snakemake_dir("processactivitypathway"), "concords", "metadata-RHEA.yaml"
    )

    from src.datahandlers.rhea import Rhea  # deferred

    needs_labels = regenerate or not os.path.exists(labels)
    needs_concords = regenerate or not os.path.exists(concords)
    if needs_labels or needs_concords:
        rhea = Rhea()
        _maybe_run(labels, lambda: rhea.pull_rhea_labels(labels), regenerate)
        _maybe_run(concords, lambda: rhea.pull_rhea_ec_concs(concords, metadata_yaml), regenerate)

    return {"labels": labels, "concords": concords}


# ---------------------------------------------------------------------------
# ChemBL download + processing fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def chembl_ttl_files():
    """Download babel_downloads/CHEMBL.COMPOUND/chembl_latest_molecule.ttl + cco.ttl, or fail."""
    from src.datahandlers.chembl import pull_chembl  # deferred

    molecule_file = make_local_name("chembl_latest_molecule.ttl", subpath="CHEMBL.COMPOUND")
    cco_file = make_local_name("cco.ttl", subpath="CHEMBL.COMPOUND")
    if not os.path.exists(molecule_file) or not os.path.exists(cco_file):
        try:
            pull_chembl(molecule_file)
        except Exception as e:
            pytest.fail(f"Could not download ChEMBL molecule TTL: {e}")
    if not os.path.exists(cco_file):
        pytest.fail(f"pull_chembl did not produce expected cco.ttl at {cco_file}")
    return {"molecule": molecule_file, "cco": cco_file}


@pytest.fixture(scope="session")
def chembl_pipeline_outputs(chembl_ttl_files, regenerate):
    """Run ChemblRDF label and SMILES extraction; returns {labels, smiles} paths.

    Output files go to babel_downloads/CHEMBL.COMPOUND/labels and .../smiles.
    Reused unless --regenerate is passed.
    """
    from src.util import get_config  # deferred

    cfg = get_config()
    labels = os.path.join(cfg["download_directory"], "CHEMBL.COMPOUND", "labels")
    smiles = os.path.join(cfg["download_directory"], "CHEMBL.COMPOUND", "smiles")
    molecule_file = chembl_ttl_files["molecule"]
    cco_file = chembl_ttl_files["cco"]

    from src.datahandlers.chembl import ChemblRDF  # deferred

    _maybe_run(labels, lambda: ChemblRDF(molecule_file, cco_file).pull_labels(labels), regenerate)
    _maybe_run(smiles, lambda: ChemblRDF(molecule_file, cco_file).pull_smiles(smiles), regenerate)

    return {"labels": labels, "smiles": smiles}


# ---------------------------------------------------------------------------
# CLO download + processing fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def clo_owl_file():
    """Download babel_downloads/CLO/clo.owl, or fail if unavailable."""
    from src.datahandlers.clo import pull_clo  # deferred

    clo_owl = make_local_name("clo.owl", subpath="CLO")
    metadata = make_local_name("metadata.yaml", subpath="CLO")
    if not os.path.exists(clo_owl):
        try:
            pull_clo(metadata)
        except Exception as e:
            pytest.fail(f"Could not download CLO clo.owl: {e}")
    return clo_owl


@pytest.fixture(scope="session")
def clo_pipeline_outputs(clo_owl_file, regenerate):
    """Run CLOgraph label/synonym/ID extraction; returns {labels, synonyms, ids} paths.

    Output files go to babel_downloads/CLO/ (labels, synonyms) and
    babel_outputs/intermediate/cell_line/ids/CLO (ids).  Reused unless --regenerate is passed.
    """
    from src.util import get_config  # deferred

    cfg = get_config()
    labels = os.path.join(cfg["download_directory"], "CLO", "labels")
    synonyms = os.path.join(cfg["download_directory"], "CLO", "synonyms")
    ids = _intermediate_id_path("cell_line", "CLO")

    from src.datahandlers.clo import CLOgraph, write_clo_ids  # deferred

    if regenerate or not os.path.exists(labels) or not os.path.exists(synonyms):
        os.makedirs(os.path.dirname(labels), exist_ok=True)
        CLOgraph(clo_owl_file).pull_CLO_labels_and_synonyms(labels, synonyms)
    _maybe_run(ids, lambda: write_clo_ids(clo_owl_file, ids), regenerate)

    return {"labels": labels, "synonyms": synonyms, "ids": ids}


# ---------------------------------------------------------------------------
# EFO download + processing fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def efo_owl_file():
    """Download babel_downloads/EFO/efo.owl, or fail if unavailable."""
    from src.datahandlers.efo import pull_efo  # deferred

    return _download_or_fail(
        "EFO efo.owl",
        pull_efo,
        make_local_name("efo.owl", subpath="EFO"),
    )


@pytest.fixture(scope="session")
def efo_pipeline_outputs(efo_owl_file, regenerate):
    """Run EFOgraph label/synonym/ID extraction; returns {labels, synonyms, ids} paths.

    Output files go to babel_downloads/EFO/ (labels, synonyms) and
    babel_outputs/intermediate/disease/ids/EFO (ids).  Reused unless --regenerate is passed.
    """
    from src.util import get_config  # deferred

    cfg = get_config()
    labels = os.path.join(cfg["download_directory"], "EFO", "labels")
    synonyms = os.path.join(cfg["download_directory"], "EFO", "synonyms")
    ids = _intermediate_id_path("diseasephenotype", "EFO")

    from src.createcompendia.diseasephenotype import write_efo_ids  # deferred
    from src.datahandlers.efo import EFOgraph  # deferred

    if regenerate or not os.path.exists(labels) or not os.path.exists(synonyms):
        os.makedirs(os.path.dirname(labels), exist_ok=True)
        EFOgraph(efo_owl_file).pull_EFO_labels_and_synonyms(labels, synonyms)
    _maybe_run(ids, lambda: write_efo_ids(efo_owl_file, ids), regenerate)

    return {"labels": labels, "synonyms": synonyms, "ids": ids}


# ---------------------------------------------------------------------------
# Slim ID-only wrappers for handler fixtures registered in VOCABULARY_REGISTRY
#
# EC, CLO, and EFO fixtures return {labels, synonyms, ids} dicts.  The
# vocab_outputs fixture calls _output_paths(), which treats every string value
# as a compendium ID file.  Wrapping here exposes only the ids path so the
# partitioning tests see one compendium per vocabulary, not three.
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def ec_ids_outputs(ec_pipeline_outputs):
    return {"processactivitypathway": ec_pipeline_outputs["ids"]}


@pytest.fixture(scope="session")
def clo_ids_outputs(clo_pipeline_outputs):
    return {"cell_line": clo_pipeline_outputs["ids"]}


@pytest.fixture(scope="session")
def efo_ids_outputs(efo_pipeline_outputs):
    return {"diseasephenotype": efo_pipeline_outputs["ids"]}


@pytest.fixture(scope="session", params=list(VOCABULARY_REGISTRY.keys()))
def vocab_outputs(request):
    """Parametrized fixture yielding (vocab_name, outputs_dict) for each registered vocabulary.

    Uses request.getfixturevalue() so only the fixture for the current vocabulary is
    instantiated — a skip in UMLS does not prevent OMIM or MESH from running.
    """
    fixture_name = VOCABULARY_REGISTRY[request.param]
    outputs = request.getfixturevalue(fixture_name)
    return request.param, outputs
