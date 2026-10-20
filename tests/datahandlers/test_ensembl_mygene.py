# Tests for src/datahandlers/ensembl_mygene.py — the MyGene.info-backed Ensembl pull.
#
# The parsing/normalization core is pure and runs entirely offline against real MyGene
# responses captured verbatim into tests/data/ensembl_mygene/ (each fixture names the gene
# id it was captured from, so it can be re-derived). The only tests that touch the network
# are marked `network` and skipped by default. Sections:
#   NORMALIZATION HELPERS — _as_list / _ensembl_entries / _xref_values
#   ROW EXPANSION         — gene_object_to_rows over the real fixtures
#   TSV RENDERING         — rows_to_tsv header/shape contract
#   PAGING                — iter_mygene_genes scrolling logic (fake http_get)
#   ORCHESTRATION         — pull_ensembl_via_mygene (fake http_get, tmp_path)
#   DOWNSTREAM ROUND-TRIP — the produced TSV feeds the real gene/protein consumers unchanged
#   HTTP RETRY            — _default_http_get transient-failure handling (monkeypatched)
#   LIVE API              — network smoke test against the real MyGene.info service
import itertools
import json
import os

import pytest

from src.datahandlers.ensembl_mygene import (
    BIOMART_COLUMNS,
    _as_list,
    _default_http_get,
    _ensembl_entries,
    _xref_values,
    gene_object_to_rows,
    iter_mygene_genes,
    pull_ensembl_via_mygene,
    rows_to_tsv,
)

FIXTURE_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data", "ensembl_mygene")


def load_fixture(name):
    """Load a captured real MyGene gene object by fixture filename."""
    with open(os.path.join(FIXTURE_DIR, name)) as inf:
        return json.load(inf)


# NORMALIZATION HELPERS


@pytest.mark.unit
@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (None, []),
        ("", []),
        ("ENSG00000123374", ["ENSG00000123374"]),
        (1017, [1017]),
        (["a", "b"], ["a", "b"]),
        (["a", "", None, "b"], ["a", "b"]),  # empties filtered out of lists
        ([], []),
    ],
)
def test_as_list_should_normalize_scalar_list_and_empty(value, expected):
    """A scalar becomes a one-element list, a list is kept (minus empties), and None/'' become []."""
    assert _as_list(value) == expected


@pytest.mark.unit
def test_ensembl_entries_should_scope_proteins_to_their_own_dict_entry():
    """A dict ensembl field yields one (gene, proteins) pair with its protein list intact."""
    entries = list(_ensembl_entries(load_fixture("human_cdk2_ENSG00000123374.json")))
    assert entries == [
        (
            "ENSG00000123374",
            [
                "ENSP00000243067",
                "ENSP00000266970",
                "ENSP00000393605",
                "ENSP00000450983",
                "ENSP00000452138",
                "ENSP00000452514",
                "ENSP00000803871",
            ],
        )
    ]


@pytest.mark.unit
def test_ensembl_entries_should_yield_multiple_entries_when_ensembl_is_a_list():
    """Zebrafish aggregates two Ensembl gene entries; each keeps only its own proteins."""
    entries = list(_ensembl_entries(load_fixture("zebrafish_ENSDARG00000014496.json")))
    assert entries == [
        ("ENSDARG00000014496", ["ENSDARP00000105890", "ENSDARP00000145071", "ENSDARP00000153297"]),
        ("ENSDARG00000110671", ["ENSDARP00000150490"]),
    ]


@pytest.mark.unit
def test_ensembl_entries_should_yield_empty_proteins_for_a_noncoding_gene():
    """An ncRNA entry has no protein key, so its protein list is empty (not missing)."""
    entries = list(_ensembl_entries(load_fixture("fly_FBgn0031778.json")))
    assert entries == [("FBgn0031778", [])]


@pytest.mark.unit
def test_ensembl_entries_should_yield_nothing_without_an_ensembl_field():
    """A gene object lacking any ensembl field produces no entries."""
    assert list(_ensembl_entries({"_id": "x", "symbol": "nope"})) == []


@pytest.mark.unit
def test_xref_values_should_strip_the_mgi_prefix_mygene_adds():
    """MyGene returns MGI already prefixed ('MGI:3704398'); the bare id must be emitted so the
    downstream consumer (which prepends 'MGI:' itself) does not produce 'MGI:MGI:3704398'."""
    mouse = load_fixture("mouse_ENSMUSG00000021148.json")
    assert mouse["MGI"] == "MGI:3704398"  # pin the raw shape this test guards
    assert _xref_values(mouse, "MGI ID") == ["3704398"]


@pytest.mark.unit
def test_xref_values_should_leave_unprefixed_mod_ids_untouched():
    """ZFIN/RGD/FlyBase/WormBase come back bare from MyGene and must pass through unchanged."""
    assert _xref_values(load_fixture("zebrafish_ENSDARG00000014496.json"), "ZFIN ID") == ["ZDB-GENE-040624-12"]
    assert _xref_values(load_fixture("rat_ENSRNOG00000010352.json"), "RGD ID") == ["708518"]
    assert _xref_values(load_fixture("fly_FBgn0031778.json"), "FlyBase ID") == ["FBgn0031778"]
    assert _xref_values(load_fixture("worm_WBGene00047797.json"), "WormBase Gene ID") == ["WBGene00047797"]


@pytest.mark.unit
def test_xref_values_should_be_empty_for_columns_mygene_lacks():
    """SGD has no MyGene field, so its column is always empty (one reason yeast stays on BioMart)."""
    assert _xref_values(load_fixture("human_cdk2_ENSG00000123374.json"), "SGD gene name ID") == []


# ROW EXPANSION


@pytest.mark.unit
def test_gene_object_to_rows_should_emit_one_row_per_protein_for_a_multisoform_gene():
    """CDK2 has 7 Ensembl proteins, so it expands to 7 rows sharing the gene id and NCBI xref."""
    rows = gene_object_to_rows(load_fixture("human_cdk2_ENSG00000123374.json"))
    assert len(rows) == 7
    assert {row["Gene stable ID"] for row in rows} == {"ENSG00000123374"}
    assert {row["Protein stable ID"] for row in rows} == {
        "ENSP00000243067",
        "ENSP00000266970",
        "ENSP00000393605",
        "ENSP00000450983",
        "ENSP00000452138",
        "ENSP00000452514",
        "ENSP00000803871",
    }
    # The NCBI gene xref is gene-level, so every row carries it.
    assert {row["NCBI gene (formerly Entrezgene) ID"] for row in rows} == {"1017"}


@pytest.mark.unit
def test_gene_object_to_rows_should_not_cross_pair_proteins_between_gene_entries():
    """The second zebrafish gene entry must carry only its own single protein, never the first
    entry's three proteins — proteins are scoped per Ensembl entry, not pooled."""
    rows = gene_object_to_rows(load_fixture("zebrafish_ENSDARG00000014496.json"))
    by_gene = {}
    for row in rows:
        by_gene.setdefault(row["Gene stable ID"], set()).add(row["Protein stable ID"])
    assert by_gene == {
        "ENSDARG00000014496": {"ENSDARP00000105890", "ENSDARP00000145071", "ENSDARP00000153297"},
        "ENSDARG00000110671": {"ENSDARP00000150490"},
    }
    # The ZFIN xref applies to both gene entries of this MyGene gene.
    assert {row["ZFIN ID"] for row in rows} == {"ZDB-GENE-040624-12"}


@pytest.mark.unit
def test_gene_object_to_rows_should_emit_a_single_empty_protein_row_for_noncoding_genes():
    """Fly/worm ncRNA genes have no protein, so they yield exactly one row with an empty protein."""
    fly_rows = gene_object_to_rows(load_fixture("fly_FBgn0031778.json"))
    assert fly_rows == [
        {
            "Gene stable ID": "FBgn0031778",
            "Protein stable ID": "",
            "NCBI gene (formerly Entrezgene) ID": "12798573",
            "ZFIN ID": "",
            "SGD gene name ID": "",
            "WormBase Gene ID": "",
            "FlyBase ID": "FBgn0031778",
            "MGI ID": "",
            "RGD ID": "",
        }
    ]


@pytest.mark.unit
def test_gene_object_to_rows_should_produce_no_rows_without_an_ensembl_field():
    """A MyGene object with no ensembl field is dropped (the live query filters these out anyway)."""
    assert gene_object_to_rows({"_id": "x", "entrezgene": "1"}) == []


# TSV RENDERING


@pytest.mark.unit
def test_rows_to_tsv_should_emit_the_exact_biomart_header_the_consumers_index_into():
    """The header line must be BIOMART_COLUMNS verbatim and in order; the downstream readers call
    header.index('Gene stable ID') / header.index('Protein stable ID') and key xrefs by string."""
    tsv = rows_to_tsv(gene_object_to_rows(load_fixture("worm_WBGene00047797.json")))
    lines = tsv.splitlines()
    assert lines[0].split("\t") == BIOMART_COLUMNS
    assert BIOMART_COLUMNS[0] == "Gene stable ID"
    assert "Protein stable ID" in BIOMART_COLUMNS


@pytest.mark.unit
def test_rows_to_tsv_should_write_every_column_on_every_row_and_end_with_a_newline():
    """Each data row has exactly len(BIOMART_COLUMNS) fields, and the text ends with a newline so
    the line-oriented consumers see a complete final row."""
    tsv = rows_to_tsv(gene_object_to_rows(load_fixture("human_cdk2_ENSG00000123374.json")))
    assert tsv.endswith("\n")
    for line in tsv.splitlines():
        assert len(line.split("\t")) == len(BIOMART_COLUMNS)


@pytest.mark.unit
def test_rows_to_tsv_should_render_missing_values_as_empty_cells():
    """A row dict missing a column writes an empty cell rather than KeyError or 'None'."""
    tsv = rows_to_tsv([{"Gene stable ID": "ENSG1", "Protein stable ID": "ENSP1"}])
    data_row = tsv.splitlines()[1].split("\t")
    assert data_row[0] == "ENSG1"
    assert data_row[1] == "ENSP1"
    assert all(cell == "" for cell in data_row[2:])


# PAGING


@pytest.mark.unit
def test_iter_mygene_genes_should_stop_after_a_single_page_without_a_scroll_id():
    """A response with hits but no _scroll_id means the whole result fit on one page."""
    calls = []

    def fake_http_get(url, params):
        calls.append(params)
        return {"hits": [{"_id": "a"}, {"_id": "b"}]}

    ids = [gene["_id"] for gene in iter_mygene_genes(9606, fake_http_get, page_delay=0)]
    assert ids == ["a", "b"]
    assert len(calls) == 1
    assert calls[0]["fetch_all"] == "true"
    assert calls[0]["q"] == "taxid:9606 AND ensembl.gene:*"


@pytest.mark.unit
def test_iter_mygene_genes_should_follow_the_scroll_cursor_until_an_empty_page():
    """A multi-page result is drained by passing each page's _scroll_id back until hits is empty."""
    pages = [
        {"_scroll_id": "s1", "hits": [{"_id": "a"}]},
        {"_scroll_id": "s2", "hits": [{"_id": "b"}]},
        {"_scroll_id": "s3", "hits": []},
    ]
    calls = []

    def fake_http_get(url, params):
        calls.append(params)
        return pages[len(calls) - 1]

    ids = [gene["_id"] for gene in iter_mygene_genes(9606, fake_http_get, page_delay=0)]
    assert ids == ["a", "b"]
    assert calls[1]["scroll_id"] == "s1"
    assert calls[2]["scroll_id"] == "s2"


@pytest.mark.unit
def test_iter_mygene_genes_should_parse_the_real_scrolling_envelope_shape():
    """The committed real scroll page (keys _scroll_id/hits/total) parses and yields its hits."""
    with open(os.path.join(FIXTURE_DIR, "query_scroll_page1.json")) as inf:
        page = json.load(inf)

    def fake_http_get(url, params):
        # First call returns the real captured page; any scroll follow-up ends the iteration.
        return page if "scroll_id" not in params else {"hits": []}

    genes = list(iter_mygene_genes(9606, fake_http_get, page_delay=0))
    assert len(genes) == len(page["hits"])
    assert all("ensembl" in gene for gene in genes)


# ORCHESTRATION


def _fake_http_get_serving(hits_by_taxid):
    """Build a fake http_get that serves one page of hits per taxon (no scroll follow-up)."""

    def fake_http_get(url, params):
        if "scroll_id" in params:
            return {"hits": []}
        taxid = int(params["q"].split("taxid:")[1].split()[0])
        return {"hits": hits_by_taxid.get(taxid, [])}

    return fake_http_get


@pytest.mark.unit
def test_pull_ensembl_via_mygene_should_write_biomart_tsv_and_sentinel_per_taxon(tmp_path):
    """Each configured taxon gets <dataset>/BioMart.tsv plus a JSON sentinel recording counts."""
    taxa = {9606: "hsapiens_gene_ensembl", 10090: "mmusculus_gene_ensembl"}
    fake = _fake_http_get_serving(
        {
            9606: [load_fixture("human_cdk2_ENSG00000123374.json")],
            10090: [load_fixture("mouse_ENSMUSG00000021148.json")],
        }
    )
    sentinel = tmp_path / "BioMartDownloadComplete"
    report = pull_ensembl_via_mygene(str(tmp_path), str(sentinel), taxa=taxa, http_get=fake, page_delay=0)

    assert report["hsapiens_gene_ensembl"]["status"] == "downloaded"
    assert report["hsapiens_gene_ensembl"]["num_genes"] == 1
    assert report["hsapiens_gene_ensembl"]["num_rows"] == 7  # CDK2's 7 proteins
    human_tsv = tmp_path / "hsapiens_gene_ensembl" / "BioMart.tsv"
    assert human_tsv.exists()
    assert (tmp_path / "mmusculus_gene_ensembl" / "BioMart.tsv").exists()
    assert json.loads(sentinel.read_text())["mmusculus_gene_ensembl"]["status"] == "downloaded"


@pytest.mark.unit
def test_pull_ensembl_via_mygene_should_skip_an_already_downloaded_dataset(tmp_path):
    """Resumability: a dataset whose BioMart.tsv already exists is skipped, not overwritten."""
    dataset_dir = tmp_path / "hsapiens_gene_ensembl"
    dataset_dir.mkdir()
    existing = dataset_dir / "BioMart.tsv"
    existing.write_text("SENTINEL-DO-NOT-OVERWRITE\n")

    fake = _fake_http_get_serving({9606: [load_fixture("human_cdk2_ENSG00000123374.json")]})
    report = pull_ensembl_via_mygene(
        str(tmp_path), str(tmp_path / "complete"), taxa={9606: "hsapiens_gene_ensembl"}, http_get=fake, page_delay=0
    )
    assert report["hsapiens_gene_ensembl"]["status"] == "skipped"
    assert existing.read_text() == "SENTINEL-DO-NOT-OVERWRITE\n"


@pytest.mark.unit
def test_pull_ensembl_via_mygene_should_fail_at_the_end_but_keep_other_taxa(tmp_path):
    """A taxon whose http_get raises is recorded as failed and raises RuntimeError only after the
    remaining taxa have still been pulled (mirrors the BioMart pull's continue-on-failure)."""

    def flaky_http_get(url, params):
        if "scroll_id" in params:
            return {"hits": []}
        taxid = int(params["q"].split("taxid:")[1].split()[0])
        if taxid == 10090:
            raise RuntimeError("simulated MyGene outage")
        return {"hits": [load_fixture("human_cdk2_ENSG00000123374.json")]}

    sentinel = tmp_path / "complete"
    with pytest.raises(RuntimeError, match="mmusculus_gene_ensembl"):
        pull_ensembl_via_mygene(
            str(tmp_path),
            str(sentinel),
            taxa={9606: "hsapiens_gene_ensembl", 10090: "mmusculus_gene_ensembl"},
            http_get=flaky_http_get,
            page_delay=0,
        )
    # The sentinel is still written, the healthy taxon succeeded, the sick one is recorded failed.
    report = json.loads(sentinel.read_text())
    assert report["hsapiens_gene_ensembl"]["status"] == "downloaded"
    assert report["mmusculus_gene_ensembl"]["status"] == "failed"
    # No partial output lingers for the failed taxon.
    assert not (tmp_path / "mmusculus_gene_ensembl" / "BioMart.tsv").exists()
    assert not (tmp_path / "mmusculus_gene_ensembl" / "BioMart.tsv.part").exists()


@pytest.mark.unit
def test_pull_ensembl_via_mygene_should_repull_after_a_mid_scroll_failure(tmp_path):
    """REGRESSION (data loss): a failure partway through the scroll must not leave a partial
    BioMart.tsv that a later run mistakes for complete. The first pull serves one page then drops
    the connection on the scroll follow-up; the atomic .part write means no BioMart.tsv is published,
    so a second, healthy pull re-pulls the taxon in full rather than 'skipping' it."""
    outfile = tmp_path / "hsapiens_gene_ensembl" / "BioMart.tsv"
    partfile = tmp_path / "hsapiens_gene_ensembl" / "BioMart.tsv.part"
    page_one = {
        "_scroll_id": "s1",
        "total": 2,
        "hits": [
            load_fixture("human_cdk2_ENSG00000123374.json"),
            load_fixture("human_braf_ENSG00000157764.json"),
        ],
    }

    def dropping_http_get(url, params):
        if "scroll_id" in params:
            raise RuntimeError("simulated mid-scroll network drop")
        return page_one

    # First pull: fails mid-scroll. Nothing complete may be published, and no .part may linger.
    with pytest.raises(RuntimeError, match="hsapiens_gene_ensembl"):
        pull_ensembl_via_mygene(
            str(tmp_path),
            str(tmp_path / "complete"),
            taxa={9606: "hsapiens_gene_ensembl"},
            http_get=dropping_http_get,
            page_delay=0,
        )
    assert not outfile.exists()
    assert not partfile.exists()

    # Second pull (healthy): the taxon is re-pulled in full, not skipped.
    def healthy_http_get(url, params):
        if "scroll_id" in params:
            return {"hits": []}
        return {"total": 2, "hits": page_one["hits"]}

    report = pull_ensembl_via_mygene(
        str(tmp_path),
        str(tmp_path / "complete2"),
        taxa={9606: "hsapiens_gene_ensembl"},
        http_get=healthy_http_get,
        page_delay=0,
    )
    assert report["hsapiens_gene_ensembl"]["status"] == "downloaded"
    assert report["hsapiens_gene_ensembl"]["num_genes"] == 2
    assert outfile.exists()
    body_lines = outfile.read_text().splitlines()
    assert body_lines[0].split("\t") == BIOMART_COLUMNS
    assert len(body_lines) == 1 + report["hsapiens_gene_ensembl"]["num_rows"]


# DOWNSTREAM ROUND-TRIP
#
# The whole point of writing BioMart-shaped TSV is that the existing consumers work unchanged.
# These tests run the REAL gene/protein functions over a MyGene-produced tree — the strongest
# proof of drop-in compatibility (and the test the tests/README "Future Plans" called for).


@pytest.fixture()
def mygene_ensembl_dir(tmp_path):
    """Pull a multi-species fixture set through pull_ensembl_via_mygene into a temp tree."""
    taxa = {
        9606: "hsapiens_gene_ensembl",
        10090: "mmusculus_gene_ensembl",
        7955: "drerio_gene_ensembl",
        7227: "dmelanogaster_gene_ensembl",
    }
    fake = _fake_http_get_serving(
        {
            9606: [load_fixture("human_cdk2_ENSG00000123374.json")],
            10090: [load_fixture("mouse_ENSMUSG00000021148.json")],
            7955: [load_fixture("zebrafish_ENSDARG00000014496.json")],
            7227: [load_fixture("fly_FBgn0031778.json")],
        }
    )
    pull_ensembl_via_mygene(str(tmp_path), str(tmp_path / "complete"), taxa=taxa, http_get=fake, page_delay=0)
    return tmp_path


@pytest.mark.unit
def test_round_trip_write_ensembl_gene_ids_should_read_the_mygene_tsv(mygene_ensembl_dir, tmp_path):
    """write_ensembl_gene_ids walks the MyGene-produced tree and emits every Ensembl gene id once."""
    from src.createcompendia.gene import write_ensembl_gene_ids

    outfile = tmp_path / "gene_ids"
    write_ensembl_gene_ids(str(mygene_ensembl_dir), str(outfile))
    ids = outfile.read_text().split()
    assert sorted(ids) == sorted(
        [
            "ENSEMBL:ENSG00000123374",
            "ENSEMBL:ENSMUSG00000021148",
            "ENSEMBL:ENSDARG00000014496",
            "ENSEMBL:ENSDARG00000110671",  # the second zebrafish gene entry
            "ENSEMBL:FBgn0031778",
        ]
    )


@pytest.mark.unit
def test_round_trip_write_ensembl_protein_ids_should_read_the_mygene_tsv(mygene_ensembl_dir, tmp_path):
    """write_ensembl_protein_ids emits every Ensembl protein id and skips the protein-less fly gene."""
    from src.createcompendia.protein import write_ensembl_protein_ids

    outfile = tmp_path / "protein_ids"
    write_ensembl_protein_ids(str(mygene_ensembl_dir), str(outfile))
    ids = set(outfile.read_text().split())
    # CDK2's 7 proteins + mouse's 2 + zebrafish's 3 + 1; fly contributes none.
    assert "ENSEMBL:ENSP00000243067" in ids
    assert "ENSEMBL:ENSMUSP00000021573" in ids
    assert "ENSEMBL:ENSDARP00000150490" in ids
    assert len(ids) == 7 + 2 + 3 + 1
    assert not any(curie.startswith("ENSEMBL:FBgn") for curie in ids)


@pytest.mark.unit
def test_round_trip_build_gene_ensembl_relationships_pins_current_set_prefix_behavior(mygene_ensembl_dir, tmp_path):
    """build_gene_ensembl_relationships reads the MyGene TSV and pairs each Ensembl gene id with
    the correct bare xref value (MGI prefix stripped, no 'MGI:MGI').

    KNOWN-IMPERFECT BEHAVIOR PINNED HERE (invert, don't delete, if it is ever fixed):
    the function's column_to_prefix maps each header to a *set* ({NCBIGENE}) and writes
    f"{pref}:{value}", so the prefix renders as a Python set repr — "{'NCBIGene'}:1017" rather
    than "NCBIGene:1017". This is a latent bug, but it is DORMANT: the get_gene_ensembl_relationships
    rule writes gene/concords/ENSEMBL, and "ENSEMBL" is NOT in config gene_concords, so gene_compendia
    never consumes this file (the live Ensembl<->NCBIGene linkage comes from NCBIGeneENSEMBL /
    gene2ensembl.gz). If the set-rendering is fixed to use the bare prefix string, invert the
    prefix assertions below from "{'NCBIGene'}" to "NCBIGene" (etc.)."""
    from src.createcompendia.gene import build_gene_ensembl_relationships

    outfile = tmp_path / "concords"
    metadata_yaml = tmp_path / "metadata.yaml"
    build_gene_ensembl_relationships(str(mygene_ensembl_dir), str(outfile), str(metadata_yaml))

    from tests.conftest import assert_concordance_file_valid

    rows = assert_concordance_file_valid(str(outfile))
    edges = {(row[0], row[1], row[2]) for row in rows}

    # The meaningful, source-agnostic guarantee: the right gene id is paired with the right bare
    # xref value. FLYBASE's prefix constant is "FB".
    assert ("ENSEMBL:ENSG00000123374", "eq", "{'NCBIGene'}:1017") in edges
    assert ("ENSEMBL:ENSMUSG00000021148", "eq", "{'MGI'}:3704398") in edges
    assert ("ENSEMBL:ENSDARG00000014496", "eq", "{'ZFIN'}:ZDB-GENE-040624-12") in edges
    assert ("ENSEMBL:FBgn0031778", "eq", "{'FB'}:FBgn0031778") in edges

    # The MyGene-specific trap this pull must avoid: a double-prefixed MGI CURIE.
    assert not any("MGI:MGI" in row[2] for row in rows)
    # Pin the set-repr prefix as the (buggy) current rendering.
    ncbi_prefixes = {row[2].rsplit(":", 1)[0] for row in rows if row[2].endswith(":1017")}
    assert ncbi_prefixes == {"{'NCBIGene'}"}


# HTTP RETRY


class _FakeResponse:
    """Minimal stand-in for requests.Response for exercising _default_http_get offline."""

    def __init__(self, status_code, payload=None, headers=None):
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}

    def raise_for_status(self):
        import requests

        if self.status_code >= 400:
            response = _FakeResponse(self.status_code)
            response.status_code = self.status_code
            error = requests.HTTPError(f"HTTP {self.status_code}")
            error.response = response
            raise error

    def json(self):
        return self._payload


@pytest.mark.unit
def test_default_http_get_should_return_parsed_json_on_success(monkeypatch):
    """A 200 with a JSON body is returned as a dict."""
    import src.datahandlers.ensembl_mygene as mod

    monkeypatch.setattr(mod.requests, "get", lambda url, params, timeout, headers: _FakeResponse(200, {"ok": True}))
    assert _default_http_get("https://mygene.info/v3/query", {}) == {"ok": True}


@pytest.mark.unit
def test_default_http_get_should_honour_retry_after_on_a_429(monkeypatch):
    """A 429 is retried after sleeping for exactly the Retry-After seconds, then the 200 is returned."""
    import src.datahandlers.ensembl_mygene as mod

    sleeps = []
    responses = [_FakeResponse(429, headers={"Retry-After": "7"}), _FakeResponse(200, {"ok": True})]
    monkeypatch.setattr(mod.requests, "get", lambda url, params, timeout, headers: responses.pop(0))
    monkeypatch.setattr(mod.time, "sleep", lambda secs: sleeps.append(secs))
    assert _default_http_get("https://mygene.info/v3/query", {}) == {"ok": True}
    assert sleeps == [7.0]  # the Retry-After value, not the default delay


@pytest.mark.unit
def test_default_http_get_should_ignore_a_non_numeric_retry_after(monkeypatch):
    """An HTTP-date Retry-After (not seconds) falls back to the default delay rather than raising."""
    import src.datahandlers.ensembl_mygene as mod

    sleeps = []
    responses = [
        _FakeResponse(429, headers={"Retry-After": "Wed, 21 Oct 2026 07:28:00 GMT"}),
        _FakeResponse(200, {"ok": True}),
    ]
    monkeypatch.setattr(mod.requests, "get", lambda url, params, timeout, headers: responses.pop(0))
    monkeypatch.setattr(mod.time, "sleep", lambda secs: sleeps.append(secs))
    monkeypatch.setattr(mod, "MYGENE_RETRY_DELAY_SECS", 30)
    assert _default_http_get("https://mygene.info/v3/query", {}) == {"ok": True}
    assert sleeps == [30]


@pytest.mark.unit
def test_default_http_get_should_retry_a_malformed_body_then_succeed(monkeypatch):
    """A 200 whose body is not valid JSON is treated as transient and retried."""
    import src.datahandlers.ensembl_mygene as mod

    class _Malformed(_FakeResponse):
        def json(self):
            raise ValueError("not json")

    responses = [_Malformed(200), _FakeResponse(200, {"ok": True})]
    monkeypatch.setattr(mod.requests, "get", lambda url, params, timeout, headers: responses.pop(0))
    monkeypatch.setattr(mod.time, "sleep", lambda secs: None)
    assert _default_http_get("https://mygene.info/v3/query", {}) == {"ok": True}


@pytest.mark.unit
def test_default_http_get_should_retry_a_connection_error_then_succeed(monkeypatch):
    """A connection error is treated as transient and retried until a response arrives."""
    import requests

    import src.datahandlers.ensembl_mygene as mod

    outcomes = [requests.ConnectionError("boom"), _FakeResponse(200, {"ok": True})]

    def fake_get(url, params, timeout, headers):
        outcome = outcomes.pop(0)
        if isinstance(outcome, Exception):
            raise outcome
        return outcome

    monkeypatch.setattr(mod.requests, "get", fake_get)
    monkeypatch.setattr(mod.time, "sleep", lambda secs: None)
    assert _default_http_get("https://mygene.info/v3/query", {}) == {"ok": True}


@pytest.mark.unit
def test_default_http_get_should_fail_fast_on_a_client_error(monkeypatch):
    """A non-429 4xx is a client error: raise immediately without retrying."""
    import src.datahandlers.ensembl_mygene as mod

    call_count = {"n": 0}

    def fake_get(url, params, timeout, headers):
        call_count["n"] += 1
        return _FakeResponse(404)

    monkeypatch.setattr(mod.requests, "get", fake_get)
    monkeypatch.setattr(mod.time, "sleep", lambda secs: None)
    with pytest.raises(RuntimeError, match="failed for"):
        _default_http_get("https://mygene.info/v3/query", {})
    assert call_count["n"] == 1  # no retries for a 404


@pytest.mark.unit
def test_default_http_get_should_give_up_after_max_retries_on_server_errors(monkeypatch):
    """Persistent 5xx exhausts MYGENE_MAX_RETRIES and raises RuntimeError."""
    import src.datahandlers.ensembl_mygene as mod

    monkeypatch.setattr(mod, "MYGENE_MAX_RETRIES", 3)
    monkeypatch.setattr(mod.requests, "get", lambda url, params, timeout, headers: _FakeResponse(500))
    monkeypatch.setattr(mod.time, "sleep", lambda secs: None)
    with pytest.raises(RuntimeError, match="after 3 attempts"):
        _default_http_get("https://mygene.info/v3/query", {})


# LIVE API


@pytest.mark.network
@pytest.mark.timeout(120)
def test_live_mygene_should_return_parseable_gene_objects():
    """Smoke test against the real MyGene.info service: a real gene object parses into the expected
    Ensembl/NCBI rows. Skipped unless --network is passed."""
    import requests

    response = None
    try:
        response = requests.get(
            "https://mygene.info/v3/gene/ENSG00000123374",
            params={"fields": "ensembl,entrezgene,symbol,taxid"},
            timeout=60,
        )
        response.raise_for_status()
    except requests.RequestException as exc:
        pytest.skip(f"MyGene.info unreachable: {exc}")
    assert response is not None

    rows = gene_object_to_rows(response.json())
    assert {row["Gene stable ID"] for row in rows} == {"ENSG00000123374"}
    assert {row["NCBI gene (formerly Entrezgene) ID"] for row in rows} == {"1017"}
    assert any(row["Protein stable ID"].startswith("ENSP") for row in rows)


@pytest.mark.network
@pytest.mark.timeout(120)
def test_live_mygene_scroll_handshake_should_page_across_scroll_ids():
    """Exercise the real fetch_all -> _scroll_id -> scroll_id wire contract (not just the offline
    fakes): a small page size forces several real scroll pages to yield a handful of worm genes.
    Skipped unless --network is passed."""
    import requests

    try:
        requests.get(
            "https://mygene.info/v3/query", params={"q": "taxid:6239", "size": 1}, timeout=60
        ).raise_for_status()
    except requests.RequestException as exc:
        pytest.skip(f"MyGene.info unreachable: {exc}")

    # page_size=2 + islice(5) => ~3 real pages, exercising the scroll_id follow-up twice. Cheap.
    genes = list(itertools.islice(iter_mygene_genes(6239, _default_http_get, page_size=2, page_delay=0), 5))
    assert len(genes) == 5
    assert all("ensembl" in gene for gene in genes)
