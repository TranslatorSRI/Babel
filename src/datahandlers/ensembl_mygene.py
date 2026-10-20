"""Pull Ensembl gene/protein identifiers and cross-references from MyGene.info.

This is a lightweight alternative to the BioMart harvest in
:mod:`src.datahandlers.ensembl`. That module queries BioMart for *every* Ensembl species
dataset (200+), which is slow, flaky (see ``docs/sources/ENSEMBL/Download.md``), and
impractical on a machine that cannot run a long network harvest. MyGene.info is a BioThings
API the Translator already trusts as a registered Knowledge Provider; it aggregates Ensembl
gene/protein stable IDs together with the database cross-references Babel needs (NCBI Gene,
ZFIN, MGI, RGD, FlyBase, WormBase), and it can be paged with a scrolling query so a full
species is retrieved without any bulk download.

The pull writes the *same* on-disk artifact the BioMart pull produces —
``<ensembl_dir>/<dataset>/BioMart.tsv`` with the BioMart display-header columns — so the
downstream consumers (:func:`src.createcompendia.gene.write_ensembl_gene_ids`,
:func:`src.createcompendia.gene.build_gene_ensembl_relationships`, and
:func:`src.createcompendia.protein.write_ensembl_protein_ids`) work unchanged.

Coverage caveat (verified against the live API, 2026-07): MyGene covers the major model
organisms well (human ~83k, mouse ~78k, rat ~43k, zebrafish ~38k, worm ~47k, fly ~24k,
xenopus ~25k gene objects carrying an ``ensembl.gene``) but is near-empty for yeast (2
genes) and dicty (0). Those species stay on BioMart; see ``DEFAULT_MYGENE_TAXA``.

Testability: every network touch point is behind an injectable ``http_get`` callable, so
the parsing/normalization core is exercised offline against the committed real fixtures in
``tests/data/ensembl_mygene/``. Only :func:`_default_http_get` speaks HTTP.
"""

import itertools
import json
import os
import time

import requests

from src.util import ensure_parent_dir, get_logger

logger = get_logger(__name__)

MYGENE_BASE_URL = "https://mygene.info/v3"

# MyGene returns at most 1000 hits per page; a scrolling query (fetch_all) pages through the
# full result set 1000 at a time. 1000 is the documented maximum page size.
MYGENE_PAGE_SIZE = 1000

# Polite pause between scrolling pages. MyGene publishes no hard numeric rate limit but asks
# callers to be considerate; one request per second is well within reason. Tests pass 0.
MYGENE_PAGE_DELAY_SECS = 1.0

# How many times to retry a single HTTP request on a transient failure (HTTP 429/5xx, a
# connection error, or a malformed body) before giving up on the whole taxon. Mirrors the
# defensive posture the BioMart pull takes: a transient error should not discard an entire
# species' worth of work.
MYGENE_MAX_RETRIES = 5
MYGENE_RETRY_DELAY_SECS = 30

# The BioMart.tsv display-header columns the downstream consumers read by exact string. This
# is a hard contract: gene.build_gene_ensembl_relationships keys its column_to_prefix on
# these strings, and write_ensembl_gene_ids / write_ensembl_protein_ids call
# header.index("Gene stable ID") / header.index("Protein stable ID"). Every column is always
# emitted (possibly empty) so those lookups always succeed.
BIOMART_COLUMNS = [
    "Gene stable ID",
    "Protein stable ID",
    "NCBI gene (formerly Entrezgene) ID",
    "ZFIN ID",
    "SGD gene name ID",
    "WormBase Gene ID",
    "FlyBase ID",
    "MGI ID",
    "RGD ID",
]

# BioMart.tsv column -> MyGene.info field that supplies it. None means MyGene has no
# equivalent field (the column is emitted empty). SGD has no MyGene field, so yeast
# cross-references are not produced by this pull — one reason yeast stays on BioMart.
COLUMN_TO_MYGENE_FIELD = {
    "NCBI gene (formerly Entrezgene) ID": "entrezgene",
    "ZFIN ID": "ZFIN",
    "SGD gene name ID": None,
    "WormBase Gene ID": "WormBase",
    "FlyBase ID": "FLYBASE",
    "MGI ID": "MGI",
    "RGD ID": "RGD",
}

# MyGene returns some MOD identifiers already carrying their CURIE prefix while the
# downstream consumer prepends the prefix itself (it writes f"{prefix}:{value}"). MGI is the
# case in point: MyGene emits "MGI:3704398" but the BioMart "MGI ID" column is bare
# ("3704398"), so a naive copy would yield "MGI:MGI:3704398". For each column listed here we
# strip a leading "<prefix>:" before writing the bare value into BioMart.tsv.
_STRIP_PREFIX = {
    "MGI ID": "MGI",
}

# The MyGene fields needed to populate BIOMART_COLUMNS (plus _id/taxid/symbol/type_of_gene for
# logging and context). The ensembl sub-fields are selected with dotted paths so the payload
# carries only gene/protein ids and omits the transcript/translation arrays Babel never reads
# (verified live to preserve the dict-or-list-of-dicts ensembl shape). Keeps the scroll payload small.
MYGENE_FIELDS = ",".join(
    ["_id", "taxid", "symbol", "type_of_gene", "ensembl.gene", "ensembl.protein"]
    + sorted({f for f in COLUMN_TO_MYGENE_FIELD.values() if f})
)

# NCBI taxid -> BioMart-style dataset directory name, for the species MyGene covers well
# enough to substitute for BioMart. Directory names deliberately match the BioMart dataset
# ids so the output layout (ENSEMBL/<dataset>/BioMart.tsv) is drop-in compatible. Verified
# live 2026-07. Yeast (4932) and dicty (44689) are intentionally absent: MyGene has ~2 yeast
# genes and 0 dicty genes with an ensembl.gene, so they must stay on BioMart.
DEFAULT_MYGENE_TAXA = {
    9606: "hsapiens_gene_ensembl",
    10090: "mmusculus_gene_ensembl",
    10116: "rnorvegicus_gene_ensembl",
    7955: "drerio_gene_ensembl",
    7227: "dmelanogaster_gene_ensembl",
    6239: "celegans_gene_ensembl",
    8364: "xtropicalis_gene_ensembl",
}


def _as_list(value):
    """Normalize a MyGene field value to a list of scalars.

    MyGene represents a field as a scalar when there is one value and a list when there are
    several (e.g. ``ensembl.protein`` is ``"ENSP.."`` for a single-protein gene but a list
    for a multi-isoform gene). Missing/empty fields become the empty list.
    """
    if value is None or value == "":
        return []
    if isinstance(value, list):
        return [v for v in value if v not in (None, "")]
    return [value]


def _ensembl_entries(gene_object):
    """Yield ``(gene_id, [protein_ids])`` pairs from a MyGene gene object's ``ensembl`` field.

    The ``ensembl`` field is a single dict for most genes but a *list* of dicts when one
    MyGene gene aggregates several Ensembl gene entries (common in zebrafish). Proteins are
    scoped to their own entry — a protein belongs to the gene entry it appears under — so we
    yield them per entry rather than pooling them across entries. An entry with no
    ``protein`` key (e.g. ncRNA) yields an empty protein list.
    """
    ensembl = gene_object.get("ensembl")
    if ensembl is None:
        return
    entries = ensembl if isinstance(ensembl, list) else [ensembl]
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        gene_id = entry.get("gene")
        if not gene_id:
            continue
        yield gene_id, _as_list(entry.get("protein"))


def _xref_values(gene_object, column):
    """Return the bare cross-reference values for a BioMart column from a MyGene gene object.

    Looks up the MyGene field for ``column`` and normalizes it to a list of bare values,
    stripping any CURIE prefix the downstream consumer would re-add (see ``_STRIP_PREFIX``).
    Columns with no MyGene field yield the empty list.
    """
    field = COLUMN_TO_MYGENE_FIELD.get(column)
    if field is None:
        return []
    strip = _STRIP_PREFIX.get(column)
    values = []
    for raw in _as_list(gene_object.get(field)):
        value = str(raw)
        if strip and value.startswith(f"{strip}:"):
            value = value[len(strip) + 1 :]
        if value:
            values.append(value)
    return values


def gene_object_to_rows(gene_object):
    """Expand one MyGene gene object into BioMart.tsv row dicts keyed by display-header column.

    Rows are emitted per Ensembl gene entry, one row per protein (or a single row with an
    empty protein when the entry has none), with the gene-level cross-reference columns
    repeated across the entry's rows — mirroring how BioMart returns one row per
    gene/protein/xref combination. A cross-reference column holding multiple values expands
    into extra rows so each cell carries a single bare value. A gene object with no usable
    ``ensembl`` entry yields no rows.
    """
    # Gene-level cross-reference columns as lists of bare values (usually 0 or 1 each); an
    # empty list becomes [""] so the cartesian product still yields exactly one combination.
    expand_columns = {col: (_xref_values(gene_object, col) or [""]) for col in COLUMN_TO_MYGENE_FIELD}
    column_names = list(expand_columns)

    rows = []
    for gene_id, proteins in _ensembl_entries(gene_object):
        for combo in itertools.product(*(expand_columns[c] for c in column_names)):
            row = {"Gene stable ID": gene_id, "Protein stable ID": ""}
            row.update(zip(column_names, combo))
            for protein in proteins or [""]:
                row_with_protein = dict(row)
                row_with_protein["Protein stable ID"] = protein
                rows.append(row_with_protein)
    return rows


def render_row(row, columns=None):
    """Render one row dict as a single tab-separated line (newline-terminated).

    Columns are emitted in ``columns`` order (default ``BIOMART_COLUMNS``); any value missing
    from the row is written empty. Shared by ``rows_to_tsv`` and the streaming writer in
    ``pull_ensembl_via_mygene`` so the tested rendering is the one that runs in production.
    """
    columns = columns or BIOMART_COLUMNS
    return "\t".join(str(row.get(col, "")) for col in columns) + "\n"


def rows_to_tsv(rows, columns=None):
    """Render row dicts as a tab-separated string with the BioMart header line.

    The header is ``columns`` (default ``BIOMART_COLUMNS``) followed by one ``render_row`` line
    per row. The result ends with a trailing newline so the downstream line-oriented readers see
    a well-formed final row.
    """
    columns = columns or BIOMART_COLUMNS
    return "\t".join(columns) + "\n" + "".join(render_row(row, columns) for row in rows)


def iter_mygene_genes(
    taxid,
    http_get,
    base_url=MYGENE_BASE_URL,
    fields=MYGENE_FIELDS,
    page_size=MYGENE_PAGE_SIZE,
    page_delay=MYGENE_PAGE_DELAY_SECS,
    max_pages=None,
):
    """Yield every MyGene gene object for a taxon that carries an Ensembl gene id.

    Uses MyGene's scrolling query (``fetch_all``): the first request returns a ``_scroll_id``
    plus the first page of hits; each follow-up passes that ``scroll_id`` to fetch the next
    page until a page comes back empty. ``http_get(url, params) -> dict`` is injected so the
    paging logic is testable offline; ``page_delay`` is the inter-page pause (tests pass 0).

    ``max_pages`` bounds the scroll so a misbehaving server returning non-empty pages forever
    cannot hang the (hours-long) job silently; it defaults to a value derived from the first
    page's ``total`` (with margin), or a large cap when ``total`` is absent. Exceeding it raises.
    """
    url = f"{base_url}/query"
    page = http_get(
        url, {"q": f"taxid:{taxid} AND ensembl.gene:*", "fields": fields, "size": page_size, "fetch_all": "true"}
    )
    if max_pages is None:
        total = page.get("total")
        max_pages = (total // page_size + 2) if isinstance(total, int) else 100000
    pages_seen = 0
    while page.get("hits"):
        pages_seen += 1
        if pages_seen > max_pages:
            raise RuntimeError(
                f"MyGene scroll for taxid {taxid} exceeded {max_pages} pages (total={page.get('total')}); aborting"
            )
        yield from page["hits"]
        scroll_id = page.get("_scroll_id")
        if not scroll_id:
            # No scroll cursor means the whole result fit on the page just yielded.
            break
        if page_delay:
            time.sleep(page_delay)
        page = http_get(url, {"scroll_id": scroll_id})


def _default_http_get(url, params):
    """Fetch ``url`` with ``params`` and return the parsed JSON, retrying transient failures.

    Retries HTTP 429 (honouring ``Retry-After`` when present), 5xx, connection errors, and
    malformed bodies up to ``MYGENE_MAX_RETRIES`` times, then raises ``RuntimeError``. A
    non-429 4xx is a client error that retrying cannot fix, so it raises immediately.
    """
    last_exc = None
    for attempt in range(1, MYGENE_MAX_RETRIES + 1):
        delay = MYGENE_RETRY_DELAY_SECS
        try:
            response = requests.get(
                url, params=params, timeout=120, headers={"User-Agent": "babel-pipeline/ensembl-mygene"}
            )
            if response.status_code == 429 or response.status_code >= 500:
                retry_after = response.headers.get("Retry-After")
                if retry_after:
                    try:
                        delay = float(retry_after)
                    except ValueError:
                        # Retry-After may be an HTTP-date rather than seconds; use the default delay.
                        delay = MYGENE_RETRY_DELAY_SECS
                raise requests.HTTPError(f"HTTP {response.status_code} from {url}")
            response.raise_for_status()
            return response.json()
        except requests.HTTPError as exc:
            status = getattr(exc.response, "status_code", None)
            if status is not None and 400 <= status < 500 and status != 429:
                raise RuntimeError(f"MyGene request failed for {url}: {exc}") from exc
            last_exc = exc
        except (requests.RequestException, ValueError) as exc:
            last_exc = exc
        logger.warning(f"MyGene request failed (attempt {attempt}/{MYGENE_MAX_RETRIES}) for {url}: {last_exc}")
        if attempt < MYGENE_MAX_RETRIES:
            time.sleep(delay)
    raise RuntimeError(f"MyGene request failed after {MYGENE_MAX_RETRIES} attempts: {url}") from last_exc


def pull_ensembl_via_mygene(ensembl_dir, complete_file, taxa=None, http_get=None, page_delay=MYGENE_PAGE_DELAY_SECS):
    """Pull Ensembl ids/xrefs from MyGene.info for each taxon and write BioMart-compatible TSVs.

    For every ``taxid -> dataset`` pair in ``taxa`` (default ``DEFAULT_MYGENE_TAXA``), pages
    through MyGene, expands each gene object into BioMart rows, and writes
    ``<ensembl_dir>/<dataset>/BioMart.tsv``. A JSON summary is written to ``complete_file``
    (the Snakemake sentinel), recording per-dataset status and counts. A dataset is written
    atomically — to ``BioMart.tsv.part`` then ``os.replace``d onto ``BioMart.tsv`` only after the
    whole taxon is pulled — so a mid-pull failure never leaves a partial file that a later run would
    mistake for complete; a complete ``BioMart.tsv`` is skipped for resumability.

    :param ensembl_dir: directory to write ``<dataset>/BioMart.tsv`` files under.
    :param complete_file: sentinel JSON file written when the pull finishes.
    :param taxa: ``{taxid: dataset_name}`` to pull; defaults to ``DEFAULT_MYGENE_TAXA``.
    :param http_get: injectable ``http_get(url, params) -> dict``; defaults to the real HTTP
        client. Tests pass a fixture-backed fake to run offline.
    :param page_delay: inter-page pause forwarded to ``iter_mygene_genes`` (tests pass 0).
    :return: a report dict keyed by dataset name.
    :raises RuntimeError: if any taxon fails to pull.
    """
    if taxa is None:
        taxa = DEFAULT_MYGENE_TAXA
    if http_get is None:
        http_get = _default_http_get

    report = {}
    failed = {}
    for taxid, dataset in taxa.items():
        outfile = os.path.join(ensembl_dir, dataset, "BioMart.tsv")
        if os.path.exists(outfile):
            report[dataset] = {"status": "skipped", "output_file": outfile, "message": "already exists"}
            logger.info(f"Skipping {dataset} (taxid {taxid}); {outfile} already exists")
            continue
        logger.info(f"Pulling {dataset} (taxid {taxid}) from MyGene.info")
        partfile = outfile + ".part"
        try:
            num_genes = 0
            num_rows = 0
            ensure_parent_dir(outfile)
            with open(partfile, "w") as out:
                out.write("\t".join(BIOMART_COLUMNS) + "\n")
                for gene_object in iter_mygene_genes(taxid, http_get, page_delay=page_delay):
                    rows = gene_object_to_rows(gene_object)
                    num_genes += 1
                    num_rows += len(rows)
                    for row in rows:
                        out.write(render_row(row, BIOMART_COLUMNS))
            # Atomic publish: BioMart.tsv appears only once the whole taxon is written, so a later
            # run can never mistake a partial pull for a complete one.
            os.replace(partfile, outfile)
            report[dataset] = {
                "status": "downloaded",
                "taxid": taxid,
                "output_file": outfile,
                "num_genes": num_genes,
                "num_rows": num_rows,
            }
            logger.info(f"Pulled {dataset}: {num_genes} genes, {num_rows} rows")
        except Exception as exc:  # noqa: BLE001 - record and continue; fail at the end
            # Drop any partial output so the next run re-pulls this taxon from scratch.
            if os.path.exists(partfile):
                os.remove(partfile)
            failed[dataset] = exc
            report[dataset] = {"status": "failed", "taxid": taxid, "message": str(exc), "output_file": outfile}
            logger.error(f"Failed to pull {dataset} (taxid {taxid}) from MyGene: {exc}")

    ensure_parent_dir(complete_file)
    with open(complete_file, "w") as outf:
        json.dump(report, outf, indent=2, sort_keys=True)

    if failed:
        raise RuntimeError(f"MyGene pull failed for {len(failed)} dataset(s): {', '.join(sorted(failed))}")
    return report


if __name__ == "__main__":
    from src.util import get_config

    ensembl_dir = os.path.join(get_config()["download_directory"], "ENSEMBL")
    pull_ensembl_via_mygene(ensembl_dir, os.path.join(ensembl_dir, "BioMartDownloadComplete"))
