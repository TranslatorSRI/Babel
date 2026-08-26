"""Unit tests for src.datahandlers/gard.py (NCATS GARD rare-disease registry).

The fixture `tests/data/gard_sample.csv` holds two rows copied verbatim from the GARD distribution
CSV (BOM + CRLF, as published): one term with pipe-separated synonyms
([`GARD:0021052`](https://rarediseases.info.nih.gov/?gard_id=0021052)) and one with neither
synonyms nor a URL ([`GARD:0027416`](https://rarediseases.info.nih.gov/?gard_id=0027416)) -- both
are kept, since the presence of a `URL` is not a reliable signal that a row is a real rare disease
(a term may simply lack a GARD page). Re-derive either row from the `gard_download_url` in
`config.yaml`.
"""

import csv
import locale
import warnings
from pathlib import Path

import pytest
import yaml

from src.categories import DISEASE
from src.datahandlers.gard import (
    content_version_id,
    fetch_gard_about_page,
    find_gard_download_links,
    normalize_gard_curie,
    published_filename,
    pull_gard,
    pull_gard_labels_and_synonyms,
)
from src.node import NodeFactory
from src.prefixes import GARD, OIO
from src.util import get_biolink_model_toolkit, get_config
from tests.conftest import assert_labels_file_valid, assert_synonyms_file_valid

FIXTURE = Path(__file__).resolve().parent.parent / "data" / "gard_sample.csv"

# Verbatim rows from the GARD distribution CSV. The distribution zero-pads local ids to seven
# digits; Babel emits the unpadded form (what DOID's xrefs use), so the expected output ids drop
# the padding the fixture carries.
_WITH_SYNS = "GARD:21052"  # published as GARD:0021052; has a URL + pipe-separated synonyms
_WITH_SYNS_NAME = "10q22.3q23.3 microduplication syndrome"
_WITH_SYNS_SYNS = ["dup(10)(q22.3q23.3)", "trisomy 10q22.3q23.3"]
_NO_SYNS = "GARD:27416"  # published as GARD:0027416; no synonyms and no URL -- still kept
_NO_SYNS_NAME = "10p13-p14 deletion syndrome"


@pytest.mark.unit
def test_pull_gard_labels_and_synonyms_writes_label_and_synonyms(tmp_path):
    """The DisplayName is written as a label and as an exact synonym, and each pipe-separated
    synonym becomes its own exact-synonym row. Rows without a URL are kept -- a missing GARD page
    does not mean the term is not a rare disease."""
    labels = str(tmp_path / "labels")
    syns = str(tmp_path / "synonyms")
    pull_gard_labels_and_synonyms(str(FIXTURE), labels, syns)

    label_rows = assert_labels_file_valid(labels)
    syn_rows = assert_synonyms_file_valid(syns)

    # Both terms get a label row (GARD:<id>\t<name>), including the no-synonym, no-URL term.
    label_map = {r[0]: r[1] for r in label_rows}
    assert label_map[_WITH_SYNS] == _WITH_SYNS_NAME
    assert label_map[_NO_SYNS] == _NO_SYNS_NAME

    # The term with synonyms: DisplayName as an exact synonym, plus each pipe-split synonym.
    assert [_WITH_SYNS, f"{OIO}:hasExactSynonym", _WITH_SYNS_NAME] in syn_rows
    for syn in _WITH_SYNS_SYNS:
        assert [_WITH_SYNS, f"{OIO}:hasExactSynonym", syn] in syn_rows

    # The no-synonym term gets exactly one synonym row (its DisplayName); no pipe-split rows.
    no_syn_rows = [r for r in syn_rows if r[0] == _NO_SYNS]
    assert no_syn_rows == [[_NO_SYNS, f"{OIO}:hasExactSynonym", _NO_SYNS_NAME]]


@pytest.mark.unit
def test_pull_gard_labels_and_synonyms_skips_non_gard_rows(tmp_path):
    """A row whose ID is not a ``GARD:`` CURIE (a malformed/trailing row the registry does not
    actually contain) is skipped, so only real GARD terms reach the labels/synonyms files.

    This is a defensive-branch test over a synthetic CSV (not a verbatim GARD record), built in
    the test so the skip path is genuinely exercised rather than trivially true against the
    GARD-only fixture. The ``URL`` column is intentionally left empty to confirm it does not gate
    inclusion.
    """
    synth = tmp_path / "synthetic.csv"
    synth.write_text(
        "ID,DisplayName,Synonyms,URL\n"
        "GARD:0000001,Real rare disease,,\n"
        "BOGUS:9999,Should be skipped,,\n"
        ",No id either,,\n",
        encoding="utf-8",
    )
    labels = str(tmp_path / "labels")
    syns = str(tmp_path / "synonyms")
    pull_gard_labels_and_synonyms(str(synth), labels, syns)

    label_rows = assert_labels_file_valid(labels)
    syn_rows = assert_synonyms_file_valid(syns)
    assert [r[0] for r in label_rows] == ["GARD:1"]
    assert all(r[0] == "GARD:1" for r in syn_rows)


# --- local-id form -------------------------------------------------------------
#
# The registry publishes GARD:0006038; DOID xrefs GARD:6038. Babel standardizes on the unpadded
# form, so the same rare disease is one identifier rather than two cliques.


@pytest.mark.unit
@pytest.mark.parametrize(
    "curie,expected",
    [
        ("GARD:0006038", "GARD:6038"),  # verbatim registry form for "Chikungunya fever"
        ("GARD:6038", "GARD:6038"),  # verbatim DOID:0050012 xref form -- already unpadded
        ("GARD:0000072", "GARD:72"),
        ("gard:0001234", "GARD:1234"),  # norm() dispatches on the upper-cased prefix; so must this
        ("GARD:0000000", "GARD:0"),  # an all-zero id is still unpadded, not left alone
        ("MONDO:0005084", "MONDO:0005084"),  # non-GARD CURIEs are untouched, zero-padding and all
        ("GARD:not-a-number", "GARD:not-a-number"),  # never seen; must not mangle
    ],
)
def test_normalize_gard_curie(curie, expected):
    """Zero-padding is stripped from GARD local ids only; everything else passes through."""
    assert normalize_gard_curie(curie) == expected


# --- ingest guards -------------------------------------------------------------
#
# A silently zeroed GARD ingest drops ~16k rare diseases from a build that still exits green, so
# the parser raises rather than logging (AGENTS.md: "A log warning is not a control").


class _FakeResponse:
    """Stand-in for urllib's response: a content type and a body, usable as a context manager."""

    def __init__(self, content_type, body, headers=None):
        self._content_type = content_type
        self._body = body
        self._headers = headers or {}
        self.headers = self

    def get_content_type(self):
        return self._content_type

    def get(self, header, default=None):
        return self._headers.get(header, default)

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _patch_opener(monkeypatch, response):
    opener = type("Opener", (), {"open": lambda self, request: response})()
    monkeypatch.setattr("src.datahandlers.gard.urllib.request.build_opener", lambda *handlers: opener)


@pytest.mark.unit
@pytest.mark.parametrize("content_type", ["text/csv", "text/plain", "application/vnd.ms-excel"])
def test_pull_gard_accepts_any_non_html_content_type(tmp_path, monkeypatch, content_type):
    """Only an HTML body is refused: a valid CSV served as text/plain or vnd.ms-excel is still a CSV,
    and the parser's header check is what decides whether the bytes are usable."""
    response = _FakeResponse(content_type, b"ID,DisplayName,Synonyms,URL\n")
    _patch_opener(monkeypatch, response)
    out = tmp_path / "gard.csv"
    assert pull_gard("https://example.invalid/gard", str(out)) == str(out)
    assert out.read_bytes() == b"ID,DisplayName,Synonyms,URL\n"


@pytest.mark.unit
def test_pull_gard_rejects_html(tmp_path, monkeypatch):
    """An expired ContentVersion link serves an HTML error page with HTTP 200; that must fail the
    rule with a message naming the config key, not write a file that parses to zero terms."""
    response = _FakeResponse("text/html", b"<html>expired</html>")
    _patch_opener(monkeypatch, response)
    out = tmp_path / "gard.csv"
    with pytest.raises(RuntimeError, match="gard_download_url"):
        pull_gard("https://example.invalid/gard", str(out))
    assert not out.exists()


@pytest.mark.unit
def test_pull_gard_labels_and_synonyms_raises_on_renamed_header(tmp_path):
    """An NCATS column rename must raise, not write an empty labels file."""
    synth = tmp_path / "renamed.csv"
    synth.write_text("GardId,Name,Synonyms,URL\nGARD:0000001,Real rare disease,,\n", encoding="utf-8")
    with pytest.raises(ValueError, match="missing expected column"):
        pull_gard_labels_and_synonyms(str(synth), str(tmp_path / "labels"), str(tmp_path / "synonyms"))


@pytest.mark.unit
@pytest.mark.parametrize("bad_value", ["Rare\tdisease", "Rare\ndisease"])
def test_pull_gard_labels_and_synonyms_raises_on_tsv_control_chars(tmp_path, bad_value):
    """A tab or newline in a name would split one TSV record into two malformed ones, so the
    writer rejects it rather than trusting that no future GARD distribution introduces one."""
    synth = tmp_path / "control_chars.csv"
    with open(synth, "w", newline="", encoding="utf-8") as f:
        csv.writer(f).writerows([["ID", "DisplayName", "Synonyms", "URL"], ["GARD:0000001", bad_value, "", ""]])
    with pytest.raises(ValueError, match="tab or newline"):
        pull_gard_labels_and_synonyms(str(synth), str(tmp_path / "labels"), str(tmp_path / "synonyms"))


@pytest.mark.unit
def test_pull_gard_labels_and_synonyms_raises_on_empty_display_name(tmp_path):
    """A GARD row with no DisplayName yields no label row, so the term would vanish from the ids
    file; that is a failed build, not a warning line scrolling past in a green one."""
    synth = tmp_path / "blank_name.csv"
    synth.write_text("ID,DisplayName,Synonyms,URL\nGARD:0000001,,,\n", encoding="utf-8")
    with pytest.raises(ValueError, match="has no DisplayName"):
        pull_gard_labels_and_synonyms(str(synth), str(tmp_path / "labels"), str(tmp_path / "synonyms"))


@pytest.mark.unit
def test_pull_gard_labels_and_synonyms_raises_when_no_terms_parsed(tmp_path):
    """A CSV with the right headers but no usable GARD rows must raise."""
    synth = tmp_path / "empty.csv"
    synth.write_text("ID,DisplayName,Synonyms,URL\nBOGUS:9999,Not a GARD term,,\n", encoding="utf-8")
    with pytest.raises(ValueError, match="yielded no terms"):
        pull_gard_labels_and_synonyms(str(synth), str(tmp_path / "labels"), str(tmp_path / "synonyms"))


# --- extra_prefixes regression -------------------------------------------------
#
# GARD is not in the Biolink Model's `disease` id_prefixes, so the disease compendium build passes
# extra_prefixes=[GARD] at its write_compendium call site (src/createcompendia/diseasephenotype.py).
# These two tests lock that linchpin in: the first asserts the precondition that makes the escape
# hatch necessary (and flips to prompt removing the line once GARD is registered upstream); the
# second asserts NodeFactory tolerates extra_prefixes=[GARD] for biolink:Disease without raising
# (the failure mode the leftover-UMLS analogue catches in test_leftover_umls.py). Both are
# network-marked because building the Biolink Model Toolkit fetches biolink-model.yaml on first
# use (for the biolink_version pinned in config.yaml).


@pytest.mark.network
def test_gard_not_in_biolink_disease_id_prefixes():
    """GARD is not in the Biolink Model's `disease` `id_prefixes` for the pinned biolink_version.

    This is why extra_prefixes=[GARD] is required. When GARD is registered upstream this assertion
    fails -- drop the extra_prefixes=[GARD] line in diseasephenotype.py and update the GARD docs.
    """
    tk = get_biolink_model_toolkit(get_config()["biolink_version"])
    assert GARD not in tk.get_element(DISEASE).id_prefixes


@pytest.mark.network
def test_disease_node_factory_keeps_gard_only_with_extra_prefixes():
    """A GARD CURIE survives NodeFactory.create_node() for biolink:Disease only when the build's
    extra_prefixes=[GARD] escape hatch is passed; without it the identifier is dropped and the
    clique disappears entirely. This is the behavior the disease build depends on."""
    factory = NodeFactory(label_dir=None, biolink_version=get_config()["biolink_version"])
    # GARD:6038 "Chikungunya fever" -- the registry term DOID:0050012 "chikungunya" xrefs. The
    # label is passed explicitly, as the build does; NodeFactory(label_dir=None) cannot read one.
    curie = "GARD:6038"
    labels = {curie: "Chikungunya fever"}

    node = factory.create_node(input_identifiers=[curie], node_type=DISEASE, labels=labels, extra_prefixes=[GARD])
    assert node["identifiers"] == [{"identifier": curie, "label": "Chikungunya fever"}]
    assert node["type"] == DISEASE

    # Without the escape hatch there is no permitted prefix left, so create_node returns None.
    assert factory.create_node(input_identifiers=[curie], node_type=DISEASE, labels=labels) is None


@pytest.mark.unit
def test_labels_and_synonyms_are_written_utf8_under_a_c_locale(tmp_path, monkeypatch):
    """Non-ASCII rare-disease names must survive the write whatever the process locale is.

    ``open(..., "w")`` inherits the locale encoding on Python < 3.14 (this repo pins >=3.11,<3.14),
    so on an HPC batch node running under LC_ALL=C an unqualified open() raises UnicodeEncodeError
    partway through and leaves a truncated labels file. GARD is full of names like these, so the
    writers pass encoding="utf-8" explicitly.

    ``monkeypatch`` of locale.getpreferredencoding is what a C locale looks like from inside the
    process; setting LC_ALL after interpreter start would not change what open() picks.
    """
    monkeypatch.setattr(locale, "getpreferredencoding", lambda do_setlocale=True: "ascii")

    # GARD:0021527 "Attenuated Chédiak-Higashi syndrome", copied verbatim from gard.csv.
    csv_file = tmp_path / "gard.csv"
    csv_file.write_text(
        "ID,DisplayName,Synonyms,URL\n"
        "GARD:0021527,Attenuated Chédiak-Higashi syndrome,attenuated chediak-higashi syndrome"
        "|atypical chediak-higashi syndrome|atypical chédiak-higashi syndrome,"
        "https://rarediseases.info.nih.gov/?gard_id=0021527\n",
        encoding="utf-8",
    )
    labels = str(tmp_path / "labels")
    syns = str(tmp_path / "synonyms")

    pull_gard_labels_and_synonyms(str(csv_file), labels, syns)

    assert Path(labels).read_text(encoding="utf-8").rstrip("\n") == "GARD:21527\tAttenuated Chédiak-Higashi syndrome"
    synonym_values = [line.split("\t")[2] for line in Path(syns).read_text(encoding="utf-8").splitlines()]
    assert "atypical chédiak-higashi syndrome" in synonym_values


# --- keeping gard_download_url current ----------------------------------------
#
# NCATS publishes the list only as a link on its About page, and each upload is a new Salesforce
# ContentVersion URL. The config pins one; these tests notice when the page has moved on.

# The anchor, verbatim from https://rarediseases.info.nih.gov/about on 2026-08-21.
_ABOUT_PAGE_ANCHOR = (
    '<a href="https://ncats.file.force.com/sfc/dist/version/download/?oid=00Dt00000004XG2&amp;ids=068SJ00001HZAaEYAX'
    '&amp;d=%2Fa%2FSJ00000BC4Xl%2FUj7U9WuHII571Akz5AUBLe6WSCeelaMBynbjWybmhuA&amp;asPdf=false" target="_blank" '
    'rel="noopener noreferrer">GARD Rare Disease List Jun2026.csv <i class="icon-gard-open-link"></i></a>'
)


@pytest.mark.unit
def test_find_gard_download_links_decodes_href_and_reads_link_text():
    """The page writes &amp; between query parameters and puts an icon inside the anchor; the
    parsed URL must equal the config value byte-for-byte and the text must carry the version."""
    page = f"<html><body><p>downloaded here: {_ABOUT_PAGE_ANCHOR}</p><a href='/other'>x</a></body></html>"
    assert find_gard_download_links(page) == [
        (get_config()["gard_download_url"], "GARD Rare Disease List Jun2026.csv"),
    ]


@pytest.mark.network
def test_gard_download_url_is_current():
    """gard_download_url must still be the link NCATS publishes on the GARD About page.

    Fails if the configured URL is no longer on the page -- get_gard will fail too, since an old
    ContentVersion link stops resolving. Warns (without failing) if the page also carries a
    distribution link that is NOT the configured one: that is most likely a newer upload, and the
    warning names it so the config can be repointed before the next release.
    """
    configured = get_config()["gard_download_url"]
    links = find_gard_download_links(fetch_gard_about_page())
    assert links, "no GARD distribution link found on the About page at all; has its markup changed?"

    others = [(url, text) for url, text in links if url != configured]
    if others:
        listing = "; ".join(f"{text!r} -> {url}" for url, text in others)
        warnings.warn(f"GARD About page carries a distribution link that is not gard_download_url: {listing}")

    assert configured in {url for url, _ in links}, (
        "gard_download_url is no longer linked from the GARD About page; repoint config.yaml to the "
        f"current link: {others}"
    )


# --- download provenance -------------------------------------------------------
#
# GARD publishes no release number, so babel_downloads/GARD/metadata.yaml has to reconstruct
# "which upload is this?" from the HTTP exchange. Both signals are visible only during the
# download, so if pull_gard does not capture them nothing downstream can.


@pytest.mark.unit
@pytest.mark.parametrize(
    "url,expected",
    [
        # The configured link's shape, abbreviated: the ContentVersion id is the `ids` parameter.
        (
            "https://ncats.file.force.com/sfc/dist/version/download/?oid=00Dt0&ids=068SJ00001HZAaEYAX&asPdf=false",
            "068SJ00001HZAaEYAX",
        ),
        ("https://example.invalid/download?ids=068ABC", "068ABC"),
        ("https://example.invalid/download?oid=00Dt0", ""),  # no ids= at all
        ("https://example.invalid/download", ""),  # no query string
    ],
)
def test_content_version_id_extracts_the_upload_identifier(url, expected):
    """The ContentVersion id names one uploaded file, which is the closest thing GARD has to a
    version: two builds sharing it read the same bytes. Missing is "" rather than an error, since
    this is provenance and must never fail a download."""
    assert content_version_id(url) == expected


@pytest.mark.unit
@pytest.mark.parametrize(
    "header,expected",
    [
        # Verbatim from the live response on 2026-08-26 -- percent-encoded, and the only place the
        # dated label ("Jun2026") appears anywhere in the exchange.
        (
            'attachment; filename="GARD%20Disease%20List%20Website%20Jun2026.csv"',
            "GARD Disease List Website Jun2026.csv",
        ),
        ("attachment; filename=plain.csv", "plain.csv"),
        ("attachment", ""),  # header present, no filename
        (None, ""),  # header absent
    ],
)
def test_published_filename_decodes_the_content_disposition(header, expected):
    """Salesforce percent-encodes the filename, and the decoded form carries the dated label the
    About page advertises. There is no Last-Modified and no ETag on this response, so losing this
    header means losing every clue about which upload a build read."""
    assert published_filename(header) == expected


@pytest.mark.unit
def test_pull_gard_writes_download_provenance(tmp_path, monkeypatch):
    """pull_gard records which upload it fetched, because nothing downstream can.

    The parse sees only the CSV bytes: no version, no filename, no content type. Whether two builds
    read the same GARD list is answerable afterwards only if this file was written at download time.
    """
    body = b"ID,DisplayName,Synonyms,URL\nGARD:0006038,Chikungunya fever,,\n"
    response = _FakeResponse(
        "text/csv",
        body,
        headers={"Content-Disposition": 'attachment; filename="GARD%20Disease%20List%20Website%20Jun2026.csv"'},
    )
    _patch_opener(monkeypatch, response)
    url = "https://ncats.file.force.com/sfc/dist/version/download/?oid=00Dt0&ids=068SJ00001HZAaEYAX"
    metadata_yaml = tmp_path / "metadata.yaml"

    pull_gard(url, str(tmp_path / "gard.csv"), str(metadata_yaml))

    metadata = yaml.safe_load(metadata_yaml.read_text())
    assert metadata["type"] == "download"
    assert metadata["url"] == url
    source = metadata["sources"][0]
    assert source["version"] == "GARD Disease List Website Jun2026.csv"
    assert source["content_version_id"] == "068SJ00001HZAaEYAX"
    assert metadata["counts"] == {"bytes": len(body), "lines": 2}
    # The whole published list is ingested; if that ever changes, this description must too.
    assert "No term is filtered out" in metadata["description"]


@pytest.mark.unit
def test_pull_gard_still_downloads_without_a_metadata_path(tmp_path, monkeypatch):
    """metadata_yaml is optional, so a caller that only wants the CSV is not forced to name one."""
    _patch_opener(monkeypatch, _FakeResponse("text/csv", b"ID,DisplayName,Synonyms,URL\n"))
    out = tmp_path / "gard.csv"
    assert pull_gard("https://example.invalid/gard", str(out)) == str(out)
    assert not (tmp_path / "metadata.yaml").exists()


@pytest.mark.unit
def test_write_download_metadata_forwards_counts(tmp_path):
    """write_download_metadata used to accept counts and then hardcode None on the way through, so
    a caller's counts were silently dropped. GARD passes counts, so pin the forwarding."""
    from src.metadata.provenance import write_download_metadata

    out = tmp_path / "metadata.yaml"
    write_download_metadata(out, name="test", counts={"bytes": 7})

    assert yaml.safe_load(out.read_text())["counts"] == {"bytes": 7}
