import gzip
import json
import os

import duckdb
import pytest

from src.exporters.duckdb_exporters import (
    _metadata_subject_filename,
    export_compendia_to_parquet,
    export_conflation_to_parquet,
    export_intermediates_to_parquet,
    log_duckdb_settings_on_error,
)
from src.predicates import CHEMROF_FORMULA, CHEMROF_INCHI
from src.properties import Property
from tests.conftest import CONFLATION_FIXTURE_ROWS

# A minimal two-clique compendium in the same format write_compendium() produces.
_COMPENDIUM_FIXTURE = [
    {
        "type": "biolink:SmallMolecule",
        "ic": None,
        "identifiers": [
            {"i": "CHEBI:15422", "l": "ATP", "d": [], "t": []},
            {"i": "PUBCHEM.COMPOUND:5957", "l": "", "d": [], "t": []},
        ],
        "preferred_name": "ATP",
        "taxa": [],
    },
    {
        "type": "biolink:SmallMolecule",
        "ic": 42.0,
        "identifiers": [
            {"i": "CHEBI:15903", "l": "caffeine", "d": [], "t": []},
        ],
        "preferred_name": "caffeine",
        "taxa": [],
    },
]


@pytest.fixture
def compendium_file(tmp_path):
    path = tmp_path / "Chemical.txt"
    with open(path, "w") as fout:
        for record in _COMPENDIUM_FIXTURE:
            fout.write(json.dumps(record) + "\n")
    return str(path)


@pytest.mark.unit
def test_export_compendia_to_parquet_edge_biolink_type(compendium_file, tmp_path):
    """Edge.parquet must contain biolink_type denormalized from each clique's type field."""
    clique_parquet = str(tmp_path / "Clique.parquet")
    edge_parquet = str(tmp_path / "Edge.parquet")
    duckdb_file = str(tmp_path / "compendium.duckdb")

    export_compendia_to_parquet(compendium_file, clique_parquet, edge_parquet, duckdb_file)

    assert os.path.exists(edge_parquet), "Edge.parquet was not written"

    rows = duckdb.execute(f"SELECT curie, biolink_type FROM read_parquet('{edge_parquet}') ORDER BY curie").fetchall()
    by_curie = {curie: btype for curie, btype in rows}

    # Every edge must carry the owning clique's biolink_type.
    assert by_curie["CHEBI:15422"] == "biolink:SmallMolecule"
    assert by_curie["PUBCHEM.COMPOUND:5957"] == "biolink:SmallMolecule"
    assert by_curie["CHEBI:15903"] == "biolink:SmallMolecule"


@pytest.mark.unit
def test_export_compendia_to_parquet_edge_columns(compendium_file, tmp_path):
    """Edge.parquet must contain the expected columns including the new biolink_type."""
    clique_parquet = str(tmp_path / "Clique.parquet")
    edge_parquet = str(tmp_path / "Edge.parquet")
    duckdb_file = str(tmp_path / "compendium.duckdb")

    export_compendia_to_parquet(compendium_file, clique_parquet, edge_parquet, duckdb_file)

    columns = {col[0] for col in duckdb.execute(f"DESCRIBE SELECT * FROM read_parquet('{edge_parquet}')").fetchall()}
    assert columns == {"clique_leader", "curie", "conflation", "clique_leader_prefix", "curie_prefix", "biolink_type"}


@pytest.mark.unit
def test_log_duckdb_settings_on_error_reraises_and_logs(caplog):
    """On failure the helper should log the operation name and effective settings, then re-raise."""
    con = duckdb.connect()
    con.execute("SET threads=3")
    with caplog.at_level("INFO"):
        with pytest.raises(duckdb.Error):
            with log_duckdb_settings_on_error(con, "my-test-operation"):
                con.execute("SELECT * FROM a_table_that_does_not_exist")

    assert "my-test-operation" in caplog.text
    assert "effective settings" in caplog.text
    # A couple of the diagnostic settings should be reported back.
    assert "memory_limit=" in caplog.text
    assert "threads=3" in caplog.text
    # The on-error path also emits a memory snapshot for the failed operation.
    assert "Memory snapshot (at failure of my-test-operation)" in caplog.text


@pytest.mark.unit
def test_log_duckdb_settings_on_error_passes_through_on_success():
    """When the wrapped block succeeds the helper must not interfere with the result."""
    con = duckdb.connect()
    with log_duckdb_settings_on_error(con, "ok-operation"):
        result = con.execute("SELECT 42").fetchone()
    assert result == (42,)


@pytest.mark.unit
def test_export_conflation_to_parquet(geneprotein_conflation_file, tmp_path):
    duckdb_file = str(tmp_path / "conflation.duckdb")
    parquet_file = str(tmp_path / "Conflation.parquet")

    export_conflation_to_parquet(geneprotein_conflation_file, "GeneProtein", duckdb_file, parquet_file)

    assert os.path.exists(parquet_file)

    rows = duckdb.execute(f"SELECT * FROM read_parquet('{parquet_file}') ORDER BY curie").fetchall()

    # Build expected rows from the fixture definition so the test stays in sync.
    expected = sorted(
        [("GeneProtein", group[0], curie, curie.split(":")[0]) for group in CONFLATION_FIXTURE_ROWS for curie in group],
        key=lambda r: r[2],
    )
    assert rows == expected


@pytest.mark.unit
def test_export_conflation_to_parquet_raises_on_existing_duckdb(geneprotein_conflation_file, tmp_path):
    duckdb_file = str(tmp_path / "conflation.duckdb")
    parquet_file = str(tmp_path / "Conflation.parquet")

    # Create the duckdb file so the guard triggers.
    open(duckdb_file, "w").close()

    with pytest.raises(RuntimeError, match="Will not overwrite"):
        export_conflation_to_parquet(geneprotein_conflation_file, "GeneProtein", duckdb_file, parquet_file)


@pytest.mark.unit
def test_export_conflation_conflation_type_stored(geneprotein_conflation_file, tmp_path):
    duckdb_file = str(tmp_path / "conflation.duckdb")
    parquet_file = str(tmp_path / "Conflation.parquet")

    export_conflation_to_parquet(geneprotein_conflation_file, "DrugChemical", duckdb_file, parquet_file)

    types = duckdb.execute(f"SELECT DISTINCT conflation_type FROM read_parquet('{parquet_file}')").fetchall()
    assert types == [("DrugChemical",)]


@pytest.mark.unit
def test_export_conflation_leader_is_first_curie(geneprotein_conflation_file, tmp_path):
    duckdb_file = str(tmp_path / "conflation.duckdb")
    parquet_file = str(tmp_path / "Conflation.parquet")

    export_conflation_to_parquet(geneprotein_conflation_file, "GeneProtein", duckdb_file, parquet_file)

    leaders = set(duckdb.execute(f"SELECT DISTINCT conflation_leader FROM read_parquet('{parquet_file}')").fetchall())
    expected_leaders = {(group[0],) for group in CONFLATION_FIXTURE_ROWS}
    assert leaders == expected_leaders


@pytest.mark.unit
def test_export_conflation_curie_prefix(geneprotein_conflation_file, tmp_path):
    duckdb_file = str(tmp_path / "conflation.duckdb")
    parquet_file = str(tmp_path / "Conflation.parquet")

    export_conflation_to_parquet(geneprotein_conflation_file, "GeneProtein", duckdb_file, parquet_file)

    mismatched = duckdb.execute(
        f"SELECT curie, curie_prefix FROM read_parquet('{parquet_file}') "
        "WHERE split_part(curie, ':', 1) != curie_prefix"
    ).fetchall()
    assert mismatched == [], f"curie_prefix mismatch: {mismatched}"


def _build_intermediate_tree(intermediate_dir):
    """Create a small intermediate tree with a concords dir and an ids dir for the
    export_intermediates_to_parquet tests. Returns the directory path."""
    concords_dir = intermediate_dir / "datacollect" / "concords"
    ids_dir = intermediate_dir / "datacollect" / "ids"
    concords_dir.mkdir(parents=True)
    ids_dir.mkdir(parents=True)

    # A concord file, including a value with an embedded double quote to exercise quote handling.
    (concords_dir / "Anatomy.txt").write_text('UBERON:0000001\teq\tMESH:D000001\nUBERON:0000002\teq\tFOO:"bar"\n')
    # A metadata sidecar describing the concord file above, plus a bare directory metadata file.
    (concords_dir / "metadata-Anatomy.txt.yaml").write_text("xref(UBERON, MESH): 2\n")
    (concords_dir / "metadata.yaml").write_text("source: datacollect\n")
    # An empty concord file, which should be skipped.
    (concords_dir / "empty.txt").write_text("")

    # A two-column ids file (curie + biolink type) and a one-column ids file (curie only).
    (ids_dir / "CHEBI").write_text("CHEBI:1\tbiolink:SmallMolecule\nCHEBI:2\tbiolink:SmallMolecule\n")
    (ids_dir / "PLAIN").write_text("PLAIN:1\nPLAIN:2\n")

    # A gzipped JSONL property file. The InChI value carries the semicolons a multi-component InChI
    # really uses, so a future change that starts splitting property values on ";" fails here.
    properties_dir = intermediate_dir / "datacollect" / "properties"
    properties_dir.mkdir(parents=True)
    with gzip.open(properties_dir / "structure.jsonl.gz", "wt") as f:
        f.write(
            Property(
                curie="CHEBI:3312",
                predicate=CHEMROF_INCHI,
                value="InChI=1S/2ClH.Ca/h2*1H;/q;;+2/p-2",
                source="test",
            ).to_json_line()
        )
        f.write(Property(curie="CHEBI:3312", predicate=CHEMROF_FORMULA, value="CaCl2", source="test").to_json_line())

    return intermediate_dir


@pytest.mark.unit
def test_export_intermediates_to_parquet(tmp_path):
    intermediate_dir = _build_intermediate_tree(tmp_path / "intermediate")
    duckdb_file = str(tmp_path / "concords.duckdb")
    ids_parquet = str(tmp_path / "Identifier.parquet")
    concords_parquet = str(tmp_path / "Concord.parquet")
    metadata_parquet = str(tmp_path / "Metadata.parquet")
    properties_parquet = str(tmp_path / "Property.parquet")

    export_intermediates_to_parquet(
        str(intermediate_dir), duckdb_file, ids_parquet, concords_parquet, metadata_parquet, properties_parquet
    )

    # Concords: both rows loaded, and the embedded double quote is preserved literally
    # (quote='' on read_csv).
    concords = duckdb.execute(
        f"SELECT subj, pred, obj FROM read_parquet('{concords_parquet}') ORDER BY subj"
    ).fetchall()
    assert concords == [
        ("UBERON:0000001", "eq", "MESH:D000001"),
        ("UBERON:0000002", "eq", 'FOO:"bar"'),
    ]

    # Identifier: two-column file keeps its Biolink type, one-column file gets NULL.
    identifiers = duckdb.execute(
        f"SELECT curie, biolink_type FROM read_parquet('{ids_parquet}') ORDER BY curie"
    ).fetchall()
    assert identifiers == [
        ("CHEBI:1", "biolink:SmallMolecule"),
        ("CHEBI:2", "biolink:SmallMolecule"),
        ("PLAIN:1", None),
        ("PLAIN:2", None),
    ]

    # Property: both rows loaded, and the InChI's semicolons survive intact.
    properties = duckdb.execute(
        f"SELECT curie, predicate, value FROM read_parquet('{properties_parquet}') ORDER BY predicate"
    ).fetchall()
    assert properties == [
        ("CHEBI:3312", CHEMROF_FORMULA, "CaCl2"),
        ("CHEBI:3312", CHEMROF_INCHI, "InChI=1S/2ClH.Ca/h2*1H;/q;;+2/p-2"),
    ]

    # Metadata: the sidecar describing Anatomy.txt resolves its subject filename, and the bare
    # metadata.yaml keeps its own name as the subject.
    metadata = {
        row[0]: row[1:]
        for row in duckdb.execute(
            f"SELECT subject_filename, metadata_json, subject_file_path FROM read_parquet('{metadata_parquet}')"
        ).fetchall()
    }
    concords_dir = intermediate_dir / "datacollect" / "concords"
    # metadata-Anatomy.txt.yaml describes the sibling file Anatomy.txt.
    assert metadata["Anatomy.txt"][0].strip() == "xref(UBERON, MESH): 2"
    assert metadata["Anatomy.txt"][1] == str(concords_dir / "Anatomy.txt")
    # A bare metadata.yaml describes the directory it lives in, so subject_file_path is that dir.
    assert metadata["metadata.yaml"][0].strip() == "source: datacollect"
    assert metadata["metadata.yaml"][1] == str(concords_dir)


@pytest.mark.unit
def test_export_intermediates_to_parquet_filename_is_absolute(tmp_path, monkeypatch):
    """The `filename` column (and the metadata subject paths derived from it) must be absolute
    even when the intermediate directory is passed as a relative path, as the DataFormats docs
    promise -- config passes it in relative to the run directory."""
    _build_intermediate_tree(tmp_path / "intermediate")
    monkeypatch.chdir(tmp_path)

    concords_parquet = str(tmp_path / "Concord.parquet")
    ids_parquet = str(tmp_path / "Identifier.parquet")
    metadata_parquet = str(tmp_path / "Metadata.parquet")
    properties_parquet = str(tmp_path / "Property.parquet")
    export_intermediates_to_parquet(
        "intermediate",  # deliberately relative
        str(tmp_path / "concords.duckdb"),
        ids_parquet,
        concords_parquet,
        metadata_parquet,
        properties_parquet,
    )

    for parquet_file, columns in (
        (concords_parquet, ["filename"]),
        (ids_parquet, ["filename"]),
        (metadata_parquet, ["filename", "subject_file_path"]),
    ):
        for column in columns:
            paths = duckdb.execute(f"SELECT DISTINCT {column} FROM read_parquet('{parquet_file}')").fetchall()
            assert paths, f"{parquet_file} unexpectedly empty"
            for (path,) in paths:
                assert os.path.isabs(path), f"{column} in {parquet_file} is not absolute: {path}"


@pytest.mark.unit
def test_export_intermediates_to_parquet_extensionless_concords_with_sidecar(tmp_path):
    """Regression test for the DrugChemical conflation concords (Babel #754 triage input): they
    live at `intermediate/drugchemical/concords/{RXNORM,UMLS,PUBCHEM_RXNORM}` with no file
    extension and each has a sibling `metadata-<name>.yaml`. Confirm the `**/concords/**/*` glob
    picks up the extension-less data files (loading them into Concord) while the `metadata-*.yaml`
    sidecars are routed to Metadata, not mistaken for concord data."""
    intermediate_dir = tmp_path / "intermediate"
    concords_dir = intermediate_dir / "drugchemical" / "concords"
    concords_dir.mkdir(parents=True)
    # Extension-less concord data files, exactly as build_rxnorm_relationships() et al. write them.
    (concords_dir / "RXNORM").write_text("RXCUI:1\teq\tRXCUI:2\n")
    (concords_dir / "PUBCHEM_RXNORM").write_text("RXCUI:1\tlinked\tPUBCHEM.COMPOUND:3\n")
    # A sidecar describing the RXNORM file above; it must land in Metadata, not Concord.
    (concords_dir / "metadata-RXNORM.yaml").write_text("xref(RXCUI, RXCUI): 1\n")

    concords_parquet = str(tmp_path / "Concord.parquet")
    metadata_parquet = str(tmp_path / "Metadata.parquet")
    properties_parquet = str(tmp_path / "Property.parquet")
    export_intermediates_to_parquet(
        str(intermediate_dir),
        str(tmp_path / "concords.duckdb"),
        str(tmp_path / "Identifier.parquet"),
        concords_parquet,
        metadata_parquet,
        properties_parquet,
    )

    # Both extension-less data files are loaded; the sidecar's contents are not among them.
    concords = duckdb.execute(
        f"SELECT subj, pred, obj FROM read_parquet('{concords_parquet}') ORDER BY subj, obj"
    ).fetchall()
    assert concords == [
        ("RXCUI:1", "linked", "PUBCHEM.COMPOUND:3"),
        ("RXCUI:1", "eq", "RXCUI:2"),
    ]
    # The sidecar is captured as metadata for its subject file (RXNORM), keyed by subject filename.
    metadata = {
        row[0]: row[1]
        for row in duckdb.execute(
            f"SELECT subject_filename, metadata_json FROM read_parquet('{metadata_parquet}')"
        ).fetchall()
    }
    assert metadata["RXNORM"].strip() == "xref(RXCUI, RXCUI): 1"


@pytest.mark.unit
def test_export_intermediates_to_parquet_empty_tree(tmp_path):
    # An intermediate directory with no concords/ or ids/ subdirectories must not raise
    # (regression test: a trailing `del concord_path` used to NameError on an empty glob).
    intermediate_dir = tmp_path / "intermediate"
    intermediate_dir.mkdir()
    duckdb_file = str(tmp_path / "concords.duckdb")
    ids_parquet = str(tmp_path / "Identifier.parquet")
    concords_parquet = str(tmp_path / "Concord.parquet")
    metadata_parquet = str(tmp_path / "Metadata.parquet")
    properties_parquet = str(tmp_path / "Property.parquet")

    export_intermediates_to_parquet(
        str(intermediate_dir), duckdb_file, ids_parquet, concords_parquet, metadata_parquet, properties_parquet
    )

    for parquet_file in (ids_parquet, concords_parquet, metadata_parquet):
        count = duckdb.execute(f"SELECT COUNT(*) FROM read_parquet('{parquet_file}')").fetchone()[0]
        assert count == 0


@pytest.mark.unit
def test_export_intermediates_to_parquet_raises_on_existing_duckdb(tmp_path):
    intermediate_dir = _build_intermediate_tree(tmp_path / "intermediate")
    duckdb_file = str(tmp_path / "concords.duckdb")
    open(duckdb_file, "w").close()

    with pytest.raises(RuntimeError, match="Will not overwrite"):
        export_intermediates_to_parquet(
            str(intermediate_dir),
            duckdb_file,
            str(tmp_path / "Identifier.parquet"),
            str(tmp_path / "Concord.parquet"),
            str(tmp_path / "Metadata.parquet"),
            str(tmp_path / "Property.parquet"),
        )


@pytest.mark.unit
def test_export_intermediates_to_parquet_inconsistent_columns(tmp_path):
    intermediate_dir = tmp_path / "intermediate"
    ids_dir = intermediate_dir / "ids"
    ids_dir.mkdir(parents=True)
    # First line has one column, second line has two: this must be rejected.
    (ids_dir / "BAD").write_text("BAD:1\nBAD:2\tbiolink:SmallMolecule\n")

    with pytest.raises(RuntimeError, match="Inconsistent number of columns"):
        export_intermediates_to_parquet(
            str(intermediate_dir),
            str(tmp_path / "concords.duckdb"),
            str(tmp_path / "Identifier.parquet"),
            str(tmp_path / "Concord.parquet"),
            str(tmp_path / "Metadata.parquet"),
            str(tmp_path / "Property.parquet"),
        )


@pytest.mark.unit
def test_export_intermediates_to_parquet_skips_malformed_concord_line(tmp_path, caplog):
    """A concord line that is not exactly three columns is skipped (not fatal), matching
    write_concord_metadata()'s warn-and-skip behavior; the well-formed lines still load and a
    warning naming the skipped line is emitted."""
    intermediate_dir = tmp_path / "intermediate"
    concords_dir = intermediate_dir / "datacollect" / "concords"
    concords_dir.mkdir(parents=True)
    # Line 2 has two columns and line 4 has four; both must be skipped, lines 1 and 3 kept.
    (concords_dir / "Anatomy.txt").write_text(
        "UBERON:0000001\teq\tMESH:D000001\n"
        "UBERON:0000002\tMESH:D000002\n"
        "UBERON:0000003\teq\tMESH:D000003\n"
        "UBERON:0000004\teq\tMESH:D000004\tEXTRA\n"
    )
    concords_parquet = str(tmp_path / "Concord.parquet")

    with caplog.at_level("WARNING"):
        export_intermediates_to_parquet(
            str(intermediate_dir),
            str(tmp_path / "concords.duckdb"),
            str(tmp_path / "Identifier.parquet"),
            concords_parquet,
            str(tmp_path / "Metadata.parquet"),
            str(tmp_path / "Property.parquet"),
        )

    concords = duckdb.execute(
        f"SELECT subj, pred, obj FROM read_parquet('{concords_parquet}') ORDER BY subj"
    ).fetchall()
    assert concords == [
        ("UBERON:0000001", "eq", "MESH:D000001"),
        ("UBERON:0000003", "eq", "MESH:D000003"),
    ]
    # Both malformed lines are reported, along with the aggregate count.
    assert "Skipping malformed concord line" in caplog.text
    assert "Skipped 2 malformed concord line(s)" in caplog.text


@pytest.mark.unit
def test_export_intermediates_to_parquet_malformed_concord_attributed_to_right_file(tmp_path, caplog):
    """With several concord files, the reject_errors/reject_scans join must attribute a skipped
    line to the file it came from: a clean file loads fully and the warning names only the
    offending file, even though DuckDB accumulates rejects across the whole loop."""
    intermediate_dir = tmp_path / "intermediate"
    concords_dir = intermediate_dir / "datacollect" / "concords"
    concords_dir.mkdir(parents=True)
    # Clean.txt is well-formed; Bad.txt has one two-column line that must be skipped.
    (concords_dir / "Clean.txt").write_text("A:1\teq\tB:1\nA:2\teq\tB:2\n")
    (concords_dir / "Bad.txt").write_text("C:1\teq\tD:1\nC:2\tD:2\n")
    concords_parquet = str(tmp_path / "Concord.parquet")

    with caplog.at_level("WARNING"):
        export_intermediates_to_parquet(
            str(intermediate_dir),
            str(tmp_path / "concords.duckdb"),
            str(tmp_path / "Identifier.parquet"),
            concords_parquet,
            str(tmp_path / "Metadata.parquet"),
            str(tmp_path / "Property.parquet"),
        )

    # All three well-formed rows load; only C:2's malformed line is dropped.
    concords = duckdb.execute(
        f"SELECT subj, pred, obj FROM read_parquet('{concords_parquet}') ORDER BY subj"
    ).fetchall()
    assert concords == [
        ("A:1", "eq", "B:1"),
        ("A:2", "eq", "B:2"),
        ("C:1", "eq", "D:1"),
    ]
    # The warning names Bad.txt (the source of the skipped line) and not Clean.txt.
    assert "Skipped 1 malformed concord line(s)" in caplog.text
    assert str(concords_dir / "Bad.txt") in caplog.text
    assert str(concords_dir / "Clean.txt") not in caplog.text


@pytest.mark.unit
def test_export_intermediates_to_parquet_creates_missing_output_dirs(tmp_path):
    """The Parquet outputs may live in directories that don't exist yet (DuckDB's write_parquet
    won't create parents); the exporter must create them rather than failing."""
    intermediate_dir = _build_intermediate_tree(tmp_path / "intermediate")
    out = tmp_path / "does" / "not" / "exist"  # deliberately absent
    ids_parquet = str(out / "Identifier.parquet")
    concords_parquet = str(out / "Concord.parquet")
    metadata_parquet = str(out / "Metadata.parquet")
    properties_parquet = str(out / "Property.parquet")

    export_intermediates_to_parquet(
        str(intermediate_dir),
        str(tmp_path / "concords.duckdb"),
        ids_parquet,
        concords_parquet,
        metadata_parquet,
        properties_parquet,
    )

    for parquet_file in (ids_parquet, concords_parquet, metadata_parquet):
        assert os.path.exists(parquet_file)


@pytest.mark.unit
@pytest.mark.parametrize(
    "filename,expected",
    [
        ("metadata-Anatomy.txt.yaml", "Anatomy.txt"),
        ("metadata.yaml", "metadata.yaml"),
        ("Anatomy.txt", None),
        ("CHEBI", None),
    ],
)
def test_metadata_subject_filename(filename, expected):
    """A `metadata-<subject>.yaml` sidecar resolves to its subject filename, a bare
    `metadata.yaml` returns its own name (it describes its directory), and a non-metadata
    filename returns None so the caller loads it as a data file."""
    assert _metadata_subject_filename(filename) == expected
