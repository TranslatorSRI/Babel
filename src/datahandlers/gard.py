"""NCATS GARD (Genetic and Rare Diseases) rare-disease registry data handler.

GARD is a flat registry of rare-disease terms distributed by NCATS as a single CSV (UTF-8 with
BOM, CRLF line endings) with four columns -- ``ID``, ``DisplayName``, ``Synonyms``, ``URL``:

* ``ID`` -- a ``GARD:NNNNNNN`` CURIE (zero-padded to seven digits in the distribution; see
  "Local-id form" below -- Babel emits the unpadded form).
* ``DisplayName`` -- the preferred label.
* ``Synonyms`` -- pipe-separated (``|``) alternative names; empty for many rows.
* ``URL`` -- the rarediseases.info.nih.gov page. Babel has no per-identifier URL attribute file
  (handlers emit only labels/synonyms/taxa/descriptions), so the column is not ingested.

Local-id form
-------------
The distribution zero-pads every local id to seven digits (``GARD:0006038``), but DOID -- the one
other disease source that cross-references GARD -- overwhelmingly emits the unpadded form
(``GARD:6038``: all but 29 of its 2,196 distinct GARD xrefs). Babel standardizes on the
**unpadded** form, so :func:`normalize_gard_curie` strips leading zeros both here and -- via
``LOCAL_ID_DEPENDENT_RENAMES`` in ``src/createcompendia/diseasephenotype.py``, keyed from
``config.yaml: disease_xref_prefixes`` -- on every source's GARD xref targets. Without that, ``GARD:0006038`` from the registry and
``GARD:6038`` from DOID are two identifiers for one disease and ~1,886 rare diseases normalize to
two conflicting cliques.

28 of those 29 carry the registry's 7-digit padding. The 29th, ``GARD:0418`` on
``DOID:0061030`` "hemophilia", is an upstream typo for ``GARD:10418``: it unpads to ``GARD:418``
"Essential pentosuria", which ``DOID:0111258`` "pentosuria" also xrefs. A GARD id claimed by two
DOID terms is dropped from DOID's concord by ``remove_overused_xrefs`` (``OVERUSE_FILTERED_CONCORDS``
in ``src/createcompendia/diseasephenotype.py``), and MONDO's own mapping puts ``GARD:418`` in the
pentosuria clique, so the typo costs nothing. Reported upstream as
https://github.com/DiseaseOntology/HumanDiseaseOntology/issues/1620.

GARD itself carries no cross-references to other disease vocabularies (MONDO/DOID/UMLS/...), so it
contributes identifiers and labels/synonyms only -- there is no GARD concord file. Cliques still
merge, in the other direction: MONDO's ``hasDbXref`` mappings (the ``MONDO_GARD`` concord, ~15.9k
of the ~16k registry terms) and DOID's xrefs (~2.2k) pull registry terms into existing disease
cliques. (DOID also asserts 300 GARD ids that the current registry no longer publishes; those join
their DOID clique without a label, the same as any other xref target Babel does not ingest.)

Every GARD term is typed ``biolink:Disease``. ``GARD`` is registered neither in the Biolink Model's
``disease`` ``id_prefixes`` nor in its prefix map (verified against the pinned
``biolink_version``), so the disease compendium build passes ``extra_prefixes=[GARD]`` to keep the
identifiers (``disease_extra_prefixes_by_biolink_class`` in ``config.yaml``, read by
``src/createcompendia/diseasephenotype.py``); registering GARD with the Biolink
team is the long-term fix, the same situation GTDB is in (see PR #978).

Field-shape note: a scan of the published CSV (16,214 rows) found no ``DisplayName`` or
``Synonyms`` value containing an embedded tab or newline, and no row with an empty
``DisplayName``. The labels/synonyms writers therefore emit raw values without sanitization,
matching the Orphanet/DOID handlers -- but that scan describes one distribution, not the next one,
so both findings are enforced at parse time (:func:`_reject_tsv_control_chars`, and a raise on an
empty ``DisplayName``) rather than left resting on a finding that nothing re-checks.
"""

import csv
import re
import urllib.parse
import urllib.request
from html.parser import HTMLParser

from src.babel_utils import get_user_agent
from src.metadata.provenance import write_download_metadata
from src.prefixes import GARD, OIO
from src.util import get_logger

logger = get_logger(__name__)

# Where NCATS publishes the list: the About page carries a "GARD Rare Disease List <Mon><Year>.csv"
# link, and there is no other documented data URL. The link is a Salesforce file-distribution URL
# whose `ids=068...` is a ContentVersion id -- one uploaded version of the file -- so every new
# upload is a new URL. `gard_download_url` in config.yaml pins one; find_gard_download_links() is
# how we notice it has moved.
GARD_ABOUT_PAGE_URL = "https://rarediseases.info.nih.gov/about"
_DISTRIBUTION_URL_STEM = "ncats.file.force.com/sfc/dist/version/download/"


class _DistributionLinkParser(HTMLParser):
    """Collect (href, link text) for every Salesforce distribution link on a page."""

    def __init__(self):
        super().__init__()
        self.links = []
        self._current = None

    def handle_starttag(self, tag, attrs):
        href = dict(attrs).get("href") or ""
        if tag == "a" and _DISTRIBUTION_URL_STEM in href:
            self._current = [href, ""]

    def handle_data(self, data):
        if self._current is not None:
            self._current[1] += data

    def handle_endtag(self, tag):
        if tag == "a" and self._current is not None:
            href, text = self._current
            self.links.append((href, " ".join(text.split())))
            self._current = None


def find_gard_download_links(page_html):
    """Return ``[(url, link_text), ...]`` for every GARD distribution link in ``page_html``.

    ``url`` is the href with HTML entities decoded (the page writes ``&amp;`` between query
    parameters), so it compares equal to ``gard_download_url`` in config.yaml; ``link_text`` is the
    anchor text, e.g. ``"GARD Rare Disease List Jun2026.csv"``, which is where the version lives.
    """
    parser = _DistributionLinkParser()
    parser.feed(page_html)
    return parser.links


def fetch_gard_about_page():
    """Download the GARD About page HTML (the only place the list's download link is published)."""
    request = urllib.request.Request(GARD_ABOUT_PAGE_URL, headers={"User-Agent": get_user_agent()})
    with urllib.request.urlopen(request) as response:
        return response.read().decode("utf-8", errors="replace")


def _reject_tsv_control_chars(curie, field, value):
    """Raise if ``value`` carries a character that would corrupt the TSV it is about to be written to.

    The labels/synonyms files are tab-separated with one record per line, so an embedded tab or
    newline silently splits one record into two malformed ones. The published CSV has none, but
    enforcing that at write time is what makes it safe to emit raw values -- a one-off scan of one
    distribution cannot speak for the next one.
    """
    if any(ch in value for ch in "\t\r\n"):
        raise ValueError(f"GARD term {curie} has a tab or newline in its {field} ({value!r}); it would corrupt the TSV")


def content_version_id(url):
    """The Salesforce ContentVersion id (``ids=068...``) from a GARD distribution URL, or "".

    This is the closest thing GARD publishes to a version number. It names *one uploaded file*
    rather than a release of the registry, so it changes whenever NCATS re-uploads the list --
    which is exactly what makes it worth recording: two builds with the same id read the same
    bytes, and two with different ids did not.
    """
    return urllib.parse.parse_qs(urllib.parse.urlparse(url).query).get("ids", [""])[0]


def published_filename(content_disposition):
    """The filename Salesforce declares for the download, from a ``Content-Disposition`` header.

    The header is the only place the distribution names itself: it answers
    ``attachment; filename="GARD%20Disease%20List%20Website%20Jun2026.csv"``, percent-encoded. That
    "Jun2026" is the dated label the About page advertises, and nothing else in the response carries
    it -- there is no ``Last-Modified`` and no ``ETag``. Returns "" if the header is absent or
    malformed, since it is provenance rather than something the build depends on.
    """
    if not content_disposition:
        return ""
    match = re.search(r'filename="?([^";]+)"?', content_disposition)
    return urllib.parse.unquote(match.group(1)) if match else ""


def normalize_gard_curie(curie):
    """Strip the zero-padding from a ``GARD:`` local id, leaving non-GARD CURIEs untouched.

    ``GARD:0006038`` -> ``GARD:6038``. Babel standardizes on the unpadded form because that is what
    DOID's xrefs overwhelmingly use; see the module docstring. Applied both when reading the
    registry CSV and, as the ``GARD`` entry of ``diseasephenotype.LOCAL_ID_DEPENDENT_RENAMES``, to
    DOID's and MONDO's xref targets, so the ID spaces meet.
    """
    prefix, _, local_id = curie.partition(":")
    # Case-insensitive on the prefix, because norm()/build_sets() dispatch on the upper-cased
    # prefix and MONDO does emit mixed-case prefixes (Orphanet:); int() also maps GARD:0000000 to
    # GARD:0 rather than leaving an all-zero id padded.
    if prefix.upper() != GARD or not local_id.isdigit():
        return curie
    return f"{GARD}:{int(local_id)}"


def pull_gard(url, outfile, metadata_yaml=None):
    """Download the GARD term CSV from ``url`` to ``outfile`` and return the path.

    The distribution is a Salesforce ContentVersion download link -- a single URL with a query
    string and no stable filename on the server -- so ``pull_via_urllib``'s ``url + in_file_name``
    assembly does not fit. We fetch the URL directly with redirect + User-Agent handling
    (mirroring ``pull_via_urllib``) and reject an HTML response, so an expired link serving an
    HTML error page with HTTP 200 fails the rule with a clear message (the parser's header check
    would catch it anyway, less legibly). Any other content type is accepted: the live link answers
    `text/csv`, but a valid CSV served as `text/plain` or `application/vnd.ms-excel` is still a
    CSV. The URL lives in ``config.yaml`` (``gard_download_url``) and is passed in as a
    Snakemake ``params`` value so that repointing it retriggers the download.

    When ``metadata_yaml`` is given, writes the source's provenance there -- when, from where, and
    which upload. This is the only point in the build that sees the HTTP response, and the response
    is the only place the distribution names itself (``Content-Disposition``) or identifies its
    version (the ContentVersion id in the URL); neither survives to the parse. GARD publishes no
    release number, so those two strings *are* the version.
    """
    opener = urllib.request.build_opener(urllib.request.HTTPRedirectHandler())
    request = urllib.request.Request(url, headers={"User-Agent": get_user_agent()})
    logger.info("Downloading GARD term list from %s", url)
    with opener.open(request) as response:
        content_type = response.headers.get_content_type()
        if content_type == "text/html":
            raise RuntimeError(
                f"GARD download {url} returned an HTML page, not a CSV. "
                f"The Salesforce ContentVersion link has probably expired or been repointed; "
                f"update gard_download_url in config.yaml."
            )
        filename = published_filename(response.headers.get("Content-Disposition"))
        body = response.read()
    with open(outfile, "wb") as out:
        out.write(body)
    if metadata_yaml:
        write_gard_download_metadata(metadata_yaml, url=url, content_type=content_type, filename=filename, body=body)
    return outfile


def write_gard_download_metadata(metadata_yaml, *, url, content_type, filename, body):
    """Record what this download was and what Babel does with it.

    Two things are worth having here that are lost by the time anything else runs. The
    ``Content-Disposition`` filename and the ContentVersion id are GARD's only version signals, and
    they live in the HTTP exchange. And the *whole* list is ingested -- 16,214 terms, of which only
    ~6,265 have a public page on rarediseases.info.nih.gov -- which is a deliberate choice rather
    than an oversight (docs/sources/GARD/README.md, "The published list is larger than the
    website"), and one NCATS has not confirmed is the right reading
    (https://github.com/NCATSTranslator/Babel/issues/1062).
    """
    write_download_metadata(
        metadata_yaml,
        name="GARD Rare Disease List",
        url=url,
        description=(
            "NCATS Genetic and Rare Diseases registry term list: a CSV of GARD:<id>, preferred "
            "label and pipe-separated synonyms, fetched directly from the Salesforce "
            "file-distribution link in config.yaml: gard_download_url (see pull_gard). No term is "
            "filtered out -- the whole published list is ingested, including the ~9,949 terms with "
            "no public GARD page. Rows are rejected only when malformed: the download refuses an "
            "HTML body (an expired link serves one with HTTP 200), and the parse raises on a "
            "missing ID/DisplayName/Synonyms header, an empty DisplayName, a tab or newline inside "
            "a value, or a parse yielding zero terms. Local ids are unpadded by "
            "normalize_gard_curie (GARD:0006038 -> GARD:6038) so they join DOID's xrefs."
        ),
        sources=[
            {
                "name": "GARD Rare Disease List",
                # GARD has no release number; these two strings are what distinguishes one upload
                # from the next, and neither is derivable from the CSV itself.
                "version": filename or "unknown (no Content-Disposition filename)",
                "content_version_id": content_version_id(url) or "unknown (no ids= in the URL)",
                "url": url,
                "content_type": content_type,
                "about_page": GARD_ABOUT_PAGE_URL,
            }
        ],
        counts={"bytes": len(body), "lines": body.count(b"\n")},
    )


def pull_gard_labels_and_synonyms(infile, labelfile, synonymfile):
    """Parse the GARD CSV into per-prefix ``labels`` and ``synonyms`` files.

    * ``labels`` -- one ``GARD:<id>\\t<DisplayName>`` row per term that has a name.
    * ``synonyms`` -- one ``GARD:<id>\\tOIO:hasExactSynonym\\t<synonym>`` row per synonym, with
      the ``DisplayName`` itself emitted first as an exact synonym (the Orphanet/DOID convention,
      so the preferred label is searchable as a synonym too).

    Local ids are unpadded via :func:`normalize_gard_curie` so they match DOID's xrefs.

    The CSV is UTF-8 with a BOM and CRLF line endings; ``utf-8-sig`` strips the BOM and
    ``newline=""`` lets the ``csv`` module handle embedded/CRLF quoting correctly. Rows whose
    ``ID`` is not a ``GARD:`` CURIE are skipped defensively (the registry contains only GARD ids,
    but a malformed trailing row must never abort the whole ingest).

    A missing ``ID``/``DisplayName`` header, a GARD row with an empty ``DisplayName``, or a parse
    that yields no terms raises: an NCATS format change that silently blanked names or zeroed this
    output would otherwise drop rare diseases from a build that still exits green, and a log line
    in a multi-hour build is not a control.
    """
    parsed = 0
    skipped_non_gard = 0
    with (
        open(infile, encoding="utf-8-sig", newline="") as inf,
        open(labelfile, "w", encoding="utf-8") as labels,
        open(synonymfile, "w", encoding="utf-8") as syns,
    ):
        reader = csv.DictReader(inf)
        missing = {"ID", "DisplayName", "Synonyms"} - set(reader.fieldnames or [])
        if missing:
            raise ValueError(
                f"GARD CSV {infile} is missing expected column(s) {sorted(missing)}; got {reader.fieldnames}"
            )
        for row in reader:
            curie = normalize_gard_curie((row.get("ID") or "").strip())
            if not curie.startswith(f"{GARD}:"):
                skipped_non_gard += 1
                continue
            name = (row.get("DisplayName") or "").strip()
            if not name:
                # A GARD term with no name yields no label row, so disease_gard_ids' awk (which
                # derives ids from the labels file) would never emit it and the term would be
                # silently lost. The published CSV has none; raise so a distribution that blanks
                # names is a failed build, not a green one that quietly dropped terms.
                raise ValueError(f"GARD CSV {infile}: term {curie} has no DisplayName")
            _reject_tsv_control_chars(curie, "DisplayName", name)
            parsed += 1
            labels.write(f"{curie}\t{name}\n")
            syns.write(f"{curie}\t{OIO}:hasExactSynonym\t{name}\n")
            synonyms_field = (row.get("Synonyms") or "").strip()
            if not synonyms_field:
                continue
            for syn in synonyms_field.split("|"):
                syn = syn.strip()
                if syn:
                    _reject_tsv_control_chars(curie, "synonym", syn)
                    syns.write(f"{curie}\t{OIO}:hasExactSynonym\t{syn}\n")
    if parsed == 0:
        raise ValueError(
            f"GARD CSV {infile} yielded no terms ({skipped_non_gard} non-GARD rows skipped). "
            f"Refusing to write an empty GARD ingest."
        )
    logger.info("GARD parse: %d terms written, %d non-GARD rows skipped", parsed, skipped_non_gard)
