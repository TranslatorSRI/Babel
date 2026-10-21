"""Regression tests for the cross-compendium DuckDB report queries.

These exercise the two-pass duplicate-detection rewrite in
``src/reports/duckdb_reports.py``: a spillable ``COUNT(*)`` pass that finds the
duplicate keys, followed by a ``LIST()`` pass restricted to those keys. The
fixtures are deliberately tiny; the point is to confirm the SQL is valid and that
duplicates (and only duplicates) are reported, not to reproduce the scale that
caused the original out-of-memory failures.
"""

import csv
import json

import duckdb
import pytest

import src.reports.duckdb_reports as duckdb_reports


def _write_parquet(con, path, columns, rows):
    """Write ``rows`` to a Parquet file at ``path`` with the given column names."""
    con.execute(f"CREATE OR REPLACE TABLE t({', '.join(f'{c} VARCHAR' for c in columns)})")
    if rows:
        placeholders = ", ".join(["(" + ", ".join(["?"] * len(columns)) + ")"] * len(rows))
        flat = [value for row in rows for value in row]
        con.execute(f"INSERT INTO t VALUES {placeholders}", flat)
    con.execute(f"COPY t TO '{path}' (FORMAT PARQUET)")


@pytest.fixture
def parquet_root(tmp_path):
    """Build a hive-partitioned Parquet tree (filename=<X>/Clique.parquet, Edge.parquet).

    ``filename`` is supplied by the hive partition directory, so it is intentionally
    absent from the Parquet columns.
    """
    con = duckdb.connect()

    # Clique columns consumed by the report queries (filename comes from the partition dir).
    clique_cols = ["clique_leader", "preferred_name", "biolink_type", "clique_identifier_count"]
    foo_dir = tmp_path / "filename=Foo"
    bar_dir = tmp_path / "filename=Bar"
    foo_dir.mkdir()
    bar_dir.mkdir()

    _write_parquet(
        con,
        str(foo_dir / "Clique.parquet"),
        clique_cols,
        [
            ("A", "Water", "biolink:SmallMolecule", "3"),
            ("B", "Caffeine", "biolink:SmallMolecule", "2"),
        ],
    )
    _write_parquet(
        con,
        str(bar_dir / "Clique.parquet"),
        clique_cols,
        [
            # "A" duplicates the leader in Foo; "C" shares the lowercased name "water".
            ("A", "Water", "biolink:ChemicalEntity", "4"),
            ("C", "water", "biolink:Drug", "1"),
        ],
    )

    # biolink_type is denormalized onto every edge at export time, so the curie report reads it
    # straight off the Edge table instead of joining back to Clique.
    edge_cols = ["curie", "clique_leader", "conflation", "curie_prefix", "clique_leader_prefix", "biolink_type"]
    _write_parquet(
        con,
        str(foo_dir / "Edge.parquet"),
        edge_cols,
        [
            ("CHEBI:1", "A", "None", "CHEBI", "CHEBI", "biolink:SmallMolecule"),
            ("MESH:1", "A", "None", "MESH", "CHEBI", "biolink:SmallMolecule"),
            # conflation != 'None' rows must be ignored by every report.
            ("DRUGBANK:1", "A", "DrugChemical", "DRUGBANK", "CHEBI", "biolink:SmallMolecule"),
        ],
    )
    _write_parquet(
        con,
        str(bar_dir / "Edge.parquet"),
        edge_cols,
        [
            ("CHEBI:1", "A", "None", "CHEBI", "CHEBI", "biolink:ChemicalEntity"),  # duplicate CURIE across Foo/Bar
            ("MESH:1", "C", "None", "MESH", "NCBIGene", "biolink:Drug"),  # duplicate CURIE in a different clique
        ],
    )
    con.close()

    return str(tmp_path) + "/"


def _read_tsv(path):
    with open(path) as f:
        return list(csv.reader(f, delimiter="\t"))


def _read_csv(path):
    with open(path) as f:
        return list(csv.reader(f))


@pytest.mark.unit
def test_check_for_identically_labeled_cliques(parquet_root, tmp_path):
    out = str(tmp_path / "identically_labeled.tsv")
    duckdb_reports.check_for_identically_labeled_cliques(parquet_root, str(tmp_path / "db.duckdb"), out)

    rows = _read_tsv(out)
    header, data = rows[0], rows[1:]
    assert header == ["preferred_name_lc", "clique_leader_count", "clique_leader"]

    # The report now emits one row per (name, clique_leader) pair. "water" (case-folded) is the
    # only duplicated name: it is shared by clique A (in both Foo and Bar) and clique C (Bar), so
    # three Clique rows match -> three output rows, all with clique_leader_count 3. "caffeine" is
    # unique and must not appear.
    assert {r[0] for r in data} == {"water"}
    assert all(r[1] == "3" for r in data)
    assert sorted(r[2] for r in data) == ["A", "A", "C"]


@pytest.mark.unit
def test_check_for_duplicate_curies(parquet_root, tmp_path):
    out = str(tmp_path / "duplicate_curies.tsv")
    duckdb_reports.check_for_duplicate_curies(parquet_root, str(tmp_path / "db.duckdb"), out)

    rows = _read_tsv(out)
    header, data = rows[0], rows[1:]
    by_curie = {r[0]: dict(zip(header, r)) for r in data}

    # Both CHEBI:1 and MESH:1 appear in two cliques; the conflated DRUGBANK:1 row is ignored.
    assert set(by_curie) == {"CHEBI:1", "MESH:1"}
    assert by_curie["CHEBI:1"]["clique_leader_count"] == "2"
    assert by_curie["MESH:1"]["clique_leader_count"] == "2"
    assert "DRUGBANK:1" not in by_curie


@pytest.mark.unit
def test_check_for_duplicate_clique_leaders(parquet_root, tmp_path):
    out = str(tmp_path / "duplicate_clique_leaders.tsv")
    duckdb_reports.check_for_duplicate_clique_leaders(parquet_root, str(tmp_path / "db.duckdb"), out)

    rows = _read_tsv(out)
    header, data = rows[0], rows[1:]
    by_leader = {r[0]: dict(zip(header, r)) for r in data}

    # Only "A" leads a clique in both Foo and Bar.
    assert set(by_leader) == {"A"}
    row = by_leader["A"]
    assert row["clique_leader_count"] == "2"
    # The two-pass rewrite restores these columns; both files' values must be present.
    assert "biolink:ChemicalEntity" in row["biolink_types"]
    assert "biolink:SmallMolecule" in row["biolink_types"]
    assert "Foo" in row["filenames"] and "Bar" in row["filenames"]


# DUPLICATE CLIQUE LEADER CONTROL


def _allowlist(tmp_path, rows):
    path = tmp_path / "known_duplicate_clique_leaders.tsv"
    path.write_text("# comment\nclique_leader\tfilenames\tissue\tnote\n" + "".join(f"{r}\n" for r in rows))
    return str(path)


@pytest.fixture
def leaders_tsv(parquet_root, tmp_path):
    """The duplicate-leader report over the fixture: only "A" leads a clique in both Foo and Bar."""
    out = str(tmp_path / "duplicate_clique_leaders.tsv")
    duckdb_reports.check_for_duplicate_clique_leaders(parquet_root, str(tmp_path / "db.duckdb"), out)
    return out


@pytest.mark.unit
def test_allowlisted_duplicate_leader_passes(leaders_tsv, tmp_path):
    """A duplicate leader listed in the allowlist (in either compendia order) should pass and write the checked file."""
    checked = tmp_path / "checked"
    allowlist = _allowlist(tmp_path, ["A\tBar,Foo\t#0\tfixture"])
    duckdb_reports.assert_no_unexpected_duplicate_clique_leaders(leaders_tsv, allowlist, str(checked))
    assert "1 duplicate clique leaders" in checked.read_text()


@pytest.mark.unit
def test_unexpected_duplicate_leader_fails(leaders_tsv, tmp_path):
    """A duplicate leader not in the allowlist should raise, naming the leader and compendia, and write nothing."""
    checked = tmp_path / "checked"
    with pytest.raises(RuntimeError, match=r"A \[Bar, Foo\]"):
        duckdb_reports.assert_no_unexpected_duplicate_clique_leaders(
            leaders_tsv, _allowlist(tmp_path, []), str(checked)
        )
    assert not checked.exists()


@pytest.mark.unit
def test_same_leader_in_other_compendia_is_not_allowlisted(leaders_tsv, tmp_path):
    """The allowlist is keyed on the compendia pair too: "A" in Foo+Baz does not cover "A" in Foo+Bar."""
    with pytest.raises(RuntimeError):
        duckdb_reports.assert_no_unexpected_duplicate_clique_leaders(
            leaders_tsv, _allowlist(tmp_path, ["A\tFoo,Baz\t#0\tfixture"]), str(tmp_path / "checked")
        )


@pytest.mark.unit
def test_stale_allowlist_rows_are_reported_not_fatal(leaders_tsv, tmp_path):
    """An allowlist row that no longer duplicates should be named in the summary so it gets pruned."""
    checked = tmp_path / "checked"
    allowlist = _allowlist(tmp_path, ["A\tFoo,Bar\t#0\tfixture", "Z\tFoo,Bar\t#0\tfixed"])
    duckdb_reports.assert_no_unexpected_duplicate_clique_leaders(leaders_tsv, allowlist, str(checked))
    assert "1 allowlist entries no longer duplicated (prune them): Z [Bar, Foo]" in checked.read_text()


@pytest.mark.unit
def test_duplicate_leader_in_fewer_compendia_than_allowlisted_passes(leaders_tsv, tmp_path):
    """A duplicate leader whose compendia are a subset of its allowlist row's (a source fix took one of three away)
    should pass, and be named in the summary so the row gets narrowed rather than pruned."""
    checked = tmp_path / "checked"
    allowlist = _allowlist(tmp_path, ["A\tFoo,Bar,Baz\t#0\tfixture"])
    duckdb_reports.assert_no_unexpected_duplicate_clique_leaders(leaders_tsv, allowlist, str(checked))
    summary = checked.read_text()
    assert "0 allowlist entries no longer duplicated" in summary
    assert "1 now in fewer compendia than their allowlist entry (narrow them): A [Bar, Foo]" in summary


@pytest.fixture
def prefix_report(parquet_root, tmp_path):
    """Run generate_prefix_report over the fixture and return the parsed combined report.

    Over the conflation='None' edges: CHEBI:1 is led by A in both Foo (SmallMolecule) and Bar
    (ChemicalEntity); MESH:1 is led by A in Foo and by C in Bar. approx_count_distinct is exact at
    this tiny cardinality, so every count below is exact.
    """
    out = str(tmp_path / "prefix_report.json")
    duckdb_reports.generate_prefix_report(parquet_root, str(tmp_path / "db.duckdb"), out, "2099jan1")
    with open(out) as f:
        return json.load(f)


@pytest.mark.unit
def test_generate_prefix_report_totals(prefix_report):
    """Top-level totals: count_curies is the exact edge count over conflation='None' (4); count_cliques
    is the sum of per-leader-prefix distinct clique counts (A under CHEBI + C under NCBIGene = 2)."""
    assert prefix_report["name"] == "2099jan1"
    assert prefix_report["count_curies"] == 4
    assert prefix_report["count_cliques"] == 2
    # The conflated DRUGBANK:1 edge is excluded, so DRUGBANK never appears.
    assert "DRUGBANK" not in prefix_report["by_curie_prefix"]


@pytest.mark.unit
def test_generate_prefix_report_by_curie_prefix(prefix_report):
    """by_curie_prefix: exact occurrence counts plus the per-filename occurrence breakdown."""
    assert prefix_report["by_curie_prefix"]["CHEBI"] == {
        "curie_count": 2,
        "curie_distinct_count": 1,
        "clique_distinct_count": 1,
        "filenames": {"Foo": 1, "Bar": 1},
    }
    # MESH:1 is led by two different cliques (A, C), so its clique_distinct_count is 2.
    assert prefix_report["by_curie_prefix"]["MESH"] == {
        "curie_count": 2,
        "curie_distinct_count": 1,
        "clique_distinct_count": 2,
        "filenames": {"Foo": 1, "Bar": 1},
    }


@pytest.mark.unit
def test_generate_prefix_report_by_clique(prefix_report):
    """by_clique is keyed by clique-leader prefix and reshapes the (filename, leader, curie) grouping."""
    chebi = prefix_report["by_clique"]["CHEBI"]
    assert chebi["by_file"] == {"Foo": {"CHEBI": 1, "MESH": 1}, "Bar": {"CHEBI": 1}}
    assert chebi["count_curies"] == 3  # Foo CHEBI:1 + Foo MESH:1 + Bar CHEBI:1, all led by A
    assert chebi["count_cliques"] == 1  # only leader A has prefix CHEBI

    ncbigene = prefix_report["by_clique"]["NCBIGene"]
    assert ncbigene["by_file"] == {"Bar": {"MESH": 1}}
    assert ncbigene["count_curies"] == 1
    assert ncbigene["count_cliques"] == 1


@pytest.mark.unit
def test_generate_prefix_report_by_filename(prefix_report):
    """by_filename carries the per-file totals report_tables.generate_cliques_table needs."""
    assert prefix_report["by_filename"]["Foo"] == {
        "curie_count": 2,
        "distinct_curie_count": 2,
    }
    assert prefix_report["by_filename"]["Bar"] == {
        "curie_count": 2,
        "distinct_curie_count": 2,
    }


@pytest.mark.unit
def test_report_tables_consume_prefix_report(parquet_root, tmp_path, monkeypatch):
    """report_tables must build both tables from the combined prefix report without error."""
    from src.reports import report_tables

    report_json = str(tmp_path / "prefix_report.json")
    duckdb_reports.generate_prefix_report(parquet_root, str(tmp_path / "db.duckdb"), report_json, "2099jan1")

    prefix_table = str(tmp_path / "prefix_table.csv")
    report_tables.generate_prefix_table(report_json, prefix_table)
    prefix_rows = _read_csv(prefix_table)
    assert prefix_rows[0] == ["Prefix", "CURIE count", "Approx distinct CURIE count", "Filenames"]
    assert {"CHEBI", "MESH"} <= {r[0] for r in prefix_rows[1:]}

    # generate_cliques_table groups by pipeline; point a pipeline at the fixture's filenames so every
    # referenced filename is present in the data (real pipeline_descriptions name real compendia).
    monkeypatch.setattr(
        report_tables,
        "pipeline_descriptions",
        {"TestPipeline": {"description": "test", "filenames": ["Foo", "Bar"]}},
    )
    cliques_table = str(tmp_path / "cliques_table.csv")
    report_tables.generate_cliques_table(report_json, cliques_table)
    clique_rows = _read_csv(cliques_table)
    header = clique_rows[0]
    # Foo (a SmallMolecule compendium in the fixture) lists CHEBI as a clique-leader prefix.
    foo_rows = [dict(zip(header, r)) for r in clique_rows[1:] if r[header.index("Biolink Types")] == "Foo"]
    assert foo_rows and "CHEBI" in foo_rows[0]["Clique leader prefixes"]
