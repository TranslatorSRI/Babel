"""Tests for ``src/reports/duplicate_curie_routes.py``, the duplicate-CURIE route tracer.

The fixtures are a miniature of the 2026jul22 situation behind Babel issues #308 and #513: a UMLS
CUI typed T116 sits in a UMLS-led Protein clique and, via a DrugCentral xref, in a CHEBI-led
SmallMolecule clique; its MeSH descriptor is claimed by the chemical ids file and reaches Protein
only through the protein UMLS concord. The report rows are shaped exactly as the two build reports
write them (a `Protein.txt[biolink:Protein, leader=..., name=...]; ...` occurrences column, and
DuckDB's list reprs), since the parsers are the part most likely to drift.
"""

import textwrap

import duckdb
import pytest

from src.reports import duplicate_curie_routes as routes

pytestmark = pytest.mark.unit


def _write_parquet(con, path, columns, rows):
    con.execute(f"CREATE OR REPLACE TABLE t({', '.join(f'{c} VARCHAR' for c in columns)})")
    con.executemany(f"INSERT INTO t VALUES ({', '.join('?' * len(columns))})", rows)
    con.execute(f"COPY t TO '{path}' (FORMAT PARQUET)")


@pytest.fixture
def build(tmp_path):
    """A tiny build: the two duplicate reports plus Identifier/Concord Parquet exports."""
    inter = "/runs/x/babel_outputs/intermediate"
    umls_csv = tmp_path / "duplicate-curies.csv"
    umls_csv.write_text(
        textwrap.dedent(
            """\
            umls_curie,umls_label,tui_set,tui_set_labels,num_compendia,num_distinct_cliques,duplicate_scope,occurrences
            UMLS:C0000578,5-hydroxytryptophan,T116|T121,"Amino Acid, Peptide, or Protein|Pharmacologic Substance",2,2,cross-file,"Protein.txt[biolink:Protein, leader=UMLS:C0000578, name=5-hydroxytryptophan]; SmallMolecule.txt[biolink:SmallMolecule, leader=CHEBI:17780, name=Oxitriptan]"
            UMLS:C0016390,Foam Cells,T025,Cell,2,2,cross-file,"Cell.txt[biolink:Cell, leader=CL:0000517, name=macrophage derived foam cell]; PhenotypicFeature.txt[biolink:PhenotypicFeature, leader=HP:0003651, name=Foam cells; with a semicolon]"
            """
        )
    )
    duckdb_tsv = tmp_path / "duplicate_curies.tsv"
    duckdb_tsv.write_text(
        textwrap.dedent(
            """\
            curie\tclique_leaders\tfilenames\tconflations\tclique_leader_count
            MESH:D006916\t['CHEBI:17780', 'UMLS:C0000578']\t[SmallMolecule, Protein]\t[None, None]\t2
            UMLS:C0000578\t['CHEBI:17780', 'UMLS:C0000578']\t[SmallMolecule, Protein]\t[None, None]\t2
            MESH:D003064\t['MESH:D003064', 'UBERON:0008280']\t[ComplexMolecularMixture, AnatomicalEntity]\t[None, None]\t2
            """
        )
    )
    con = duckdb.connect()
    _write_parquet(
        con,
        str(tmp_path / "Identifier.parquet"),
        ["filename", "curie", "biolink_type"],
        [
            (f"{inter}/protein/ids/UMLS", "UMLS:C0000578", "biolink:Protein"),
            (f"{inter}/chemicals/ids/MESH", "MESH:D006916", "biolink:ChemicalEntity"),
            (f"{inter}/chemicals/ids/UMLS", "UMLS:C9999999", "biolink:ChemicalEntity"),
        ],
    )
    _write_parquet(
        con,
        str(tmp_path / "Concord.parquet"),
        ["filename", "subj", "pred", "obj"],
        [
            (f"{inter}/chemicals/concords/DrugCentral", "DrugCentral:4006", "xref", "UMLS:C0000578"),
            (f"{inter}/protein/concords/UMLS", "UMLS:C0000578", "eq", "MESH:D006916"),
            (f"{inter}/protein/concords/NCIT_UMLS", "UMLS:C0000578", "eq", "NCIT:C52181"),
            (f"{inter}/chemicals/concords/UNII", "UNII:XXXX", "xref", "CHEBI:17780"),
        ],
    )
    return {
        "umls_csv": str(umls_csv),
        "duckdb_tsv": str(duckdb_tsv),
        "identifier": str(tmp_path / "Identifier.parquet"),
        "concord": str(tmp_path / "Concord.parquet"),
    }


def test_parse_umls_occurrences_ignores_names():
    """A name containing '; ' or ']' should not break the compendium/leader parse."""
    parsed = routes.parse_umls_occurrences(
        "Cell.txt[biolink:Cell, leader=CL:0000517, name=a; b]; PhenotypicFeature.txt[biolink:PhenotypicFeature, "
        "leader=HP:0003651, name=x]y]"
    )
    assert parsed == {"Cell": "CL:0000517", "PhenotypicFeature": "HP:0003651"}


def test_load_duckdb_duplicates_parses_list_reprs(build):
    rows = routes.load_duckdb_duplicates(build["duckdb_tsv"])
    assert rows[0]["compendia"] == ["SmallMolecule", "Protein"]
    assert rows[0]["leaders"] == ["CHEBI:17780", "UMLS:C0000578"]


def test_analyze_traces_protein_vs_chemical_routes(build):
    """The Protein-vs-chemical CUI should be attributed to protein/ids/UMLS and the DrugCentral concord."""
    analysis = routes.analyze(
        routes.load_umls_duplicates(build["umls_csv"]),
        routes.load_duckdb_duplicates(build["duckdb_tsv"]),
        build["identifier"],
        build["concord"],
        "Protein",
        {"SmallMolecule", "ChemicalEntity"},
    )
    assert analysis["umls_total"] == 2
    assert analysis["umls_pairs"][("Cell", "PhenotypicFeature")] == 1
    assert [row["umls_curie"] for row in analysis["umls_selected"]] == ["UMLS:C0000578"]
    assert analysis["side_a_leaders"] == {"UMLS": 1}
    assert analysis["side_b_leaders"] == {"CHEBI": 1}
    assert analysis["duckdb_by_prefix"] == {"MESH": 1, "UMLS": 1}
    assert [row["curie"] for row in analysis["mesh_selected"]] == ["MESH:D006916"]

    umls = analysis["routes"]["umls"]
    assert umls["ids_files"] == {"protein/ids/UMLS": 1}
    assert umls["concord_files"] == {
        "chemicals/concords/DrugCentral": 1,
        "protein/concords/UMLS": 1,
        "protein/concords/NCIT_UMLS": 1,
    }
    assert umls["touched"]["UMLS:C0000578"] == {
        "chemicals/concords/DrugCentral",
        "protein/concords/UMLS",
        "protein/concords/NCIT_UMLS",
    }
    mesh = analysis["routes"]["mesh"]
    assert mesh["ids_files"] == {"chemicals/ids/MESH": 1}
    assert mesh["concord_files"] == {"protein/concords/UMLS": 1}


def test_render_report_mentions_every_route(build):
    analysis = routes.analyze(
        routes.load_umls_duplicates(build["umls_csv"]),
        routes.load_duckdb_duplicates(build["duckdb_tsv"]),
        build["identifier"],
        build["concord"],
        "Protein",
        {"SmallMolecule", "ChemicalEntity"},
    )
    report = routes.render_report(analysis, "testbuild", build["identifier"], build["concord"])
    assert "# Duplicate CURIE routes: Protein vs ChemicalEntity, SmallMolecule in testbuild" in report
    assert "| `chemicals/concords/DrugCentral` | 1 | 1 |" in report
    assert "| `protein/ids/UMLS` | 1 |" in report
    assert "Touched by a `protein/` concord: 1; by a `chemicals/` concord: 1; by no concord at all: 0." in report
    assert "- `chemicals/concords/DrugCentral`: `DrugCentral:4006` xref `UMLS:C0000578`" in report
    # The Cell/PhenotypicFeature pair is reported in the overview but not traced.
    assert "| Cell + PhenotypicFeature | 1 |" in report
