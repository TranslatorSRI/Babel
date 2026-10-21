#!/usr/bin/env python3
"""Draft a combined Babel/NodeNorm/NameRes release note.

Babel ships to Translator as a build plus the NodeNorm and NameRes versions deployed against it, so
a release note has to cover all three repositories. This assembles the mechanical parts of that note
-- the pull request list, the version bumps, the per-compendium count table -- and leaves the
judgement to a human: every PR comes out as an unchecked checklist item to triage, keep or delete.

Everything except the build directory comes from ``releases/releases.yaml``. The entry below the one
being drafted supplies the baselines, so the three PR ranges are derived rather than remembered::

    uv run python releases/scripts/draft_release_notes.py 2026jul22 \\
        --build-dir /path/to/2026jul22 > releases/2026jul22/README.md

PRs are collected with ``gh api .../compare/BASE...HEAD``, which asks GitHub for the commits reachable
from HEAD but not from BASE. That is the only correct way to do this: NameRes v1.5.2 was cut *after*
v1.6.2 as a maintenance release on the v1.5 line, so neither a version sort nor a date sort produces
the right set. The compare endpoint follows the actual history.
"""

from __future__ import annotations

import argparse
import csv
import http.client
import json
import re
import subprocess
import sys
import urllib.request
from dataclasses import dataclass
from pathlib import Path

import yaml

# releases/scripts/draft_release_notes.py -> repo root. Deliberately not `src.util.get_repo_root`:
# this is release engineering, not pipeline code, and importing from `src/` would need a sys.path
# hack to buy a one-liner.
REPO_ROOT = Path(__file__).resolve().parents[2]

# The three repositories a Babel release spans. Hardcoded because they are the definition of a
# Babel release, not a knob: a fourth would need its own section in the note anyway.
REPOS: dict[str, str] = {
    "Babel": "NCATSTranslator/Babel",
    "NodeNorm": "NCATSTranslator/NodeNormalization",
    "NameRes": "NCATSTranslator/NameResolution",
}

# Where a deployed build's outputs live, by build name.
BUILD_URL = "https://stars.renci.org/var/babel_outputs/{build}/"

# config.yaml keys that record an upstream data version, and how to name them in prose. Diffing
# these between the baseline tag and now turns "Updated RxNorm from 01062025 to 03032025" -- written
# by hand in every previous release note -- into something the build itself reports.
VERSION_KEYS = {
    "biolink_version": "the Biolink Model",
    "umls_version": "UMLS",
    "rxnorm_version": "RxNorm",
    "mesh_year": "MeSH",
    "drugbank_version": "DrugBank",
}

# PR titles matching any of these are routine: real changes, but not something a Babel consumer needs
# to read about. They are still listed, just folded away, because "there were no dependency bumps" and
# "the dependency bumps are collapsed" are different claims and only one of them is true.
ROUTINE_TITLE_PATTERNS = [
    re.compile(r"^increment(ed)? version", re.I),
    re.compile(r"^bump ", re.I),
    re.compile(r"^build\(deps", re.I),
    re.compile(r"^update \S+ requirement from", re.I),
    re.compile(r"^upgraded? packages", re.I),
    re.compile(r"^merge (branch|pull request)", re.I),
]
ROUTINE_AUTHORS = {"dependabot[bot]", "dependabot", "github-actions[bot]", "pre-commit-ci[bot]"}

# A squash merge leaves "Title (#123)"; the older merge commits leave "Merge pull request #123 from ...".
_SQUASH_RE = re.compile(r"^(?P<title>.*?)\s*\(#(?P<number>\d+)\)$")
_MERGE_RE = re.compile(r"^Merge pull request #(?P<number>\d+) from ")

# The `## Notable changes` section of the build's own prefix_comparison.md, up to the next heading.
_NOTABLE_RE = re.compile(r"^## Notable changes\s*\n(.*?)(?=^## |\Z)", re.M | re.S)
# That report's own statement of what it compared against: "against baseline `2025sep1`".
_BASELINE_RE = re.compile(r"against baseline `([^`]+)`")

# The `## Summary` table's Redis rows, in the order the published notes have always used. NodeNorm's
# /status names every database it has, so this list only fixes the ordering of the ones we know --
# any database not listed here is appended rather than dropped.
SUMMARY_DB_ORDER = [
    "eq_id_to_id_db",
    "id_to_eqids_db",
    "id_to_type_db",
    "curie_to_bl_type_db",
    "info_content_db",
    "gene_protein_db",
    "chemical_drug_db",
]

# Deployed instances whose /status endpoints report the numbers for the `## Summary` table. Both are
# the RENCI **Exp** deployments, which is where a release lands first: the note is written while the
# build is on Exp, before it is promoted to Dev, because Exp is publicly reachable and the note is
# what tells people they can test against it. Pointing either of these at Dev reports the *previous*
# release until the promotion happens (see docs/Deployment.md). Override per run when a release is
# staged somewhere else.
NODENORM_STATUS_URL = "https://nodenormalization-exp.apps.renci.org/status"
NAMERES_STATUS_URL = "https://name-resolution-exp.apps.renci.org/status"
STATUS_TIMEOUT_SECONDS = 30

# A compendium moving by at least this percentage gets an unchecked triage item under `Areas that
# changed substantially`. Deliberately *not* config.yaml's `prefix_comparison_warn_pct`, which is the
# same number for a different granularity: that one flags a prefix row inside the build's own report,
# this one flags a whole compendium in the published note. 25% is loose enough that a routine
# release surfaces only a handful, and tight enough to have caught Polypeptide's 97% drop.
MOVER_THRESHOLD_PCT = 25.0


@dataclass(frozen=True)
class PullRequest:
    repo_key: str  # "Babel", "NodeNorm", "NameRes"
    repo: str  # "NCATSTranslator/Babel"
    number: int
    title: str
    author: str
    # False when `title` is a merge commit's subject rather than the PR's own title (see
    # _MERGE_RE below), so the routine-folding patterns have nothing real to match against.
    title_known: bool = True

    @property
    def url(self) -> str:
        return f"https://github.com/{self.repo}/pull/{self.number}"

    @property
    def is_routine(self) -> bool:
        if self.author in ROUTINE_AUTHORS:
            return True
        if not self.title_known:
            # "Merge pull request #123 from foo/bar" matches ROUTINE_TITLE_PATTERNS, but that says
            # how the PR was merged, not what it did. Repositories that don't squash-merge would
            # otherwise fold every substantive change into the collapsed section nobody reads.
            return False
        return any(pattern.match(self.title) for pattern in ROUTINE_TITLE_PATTERNS)

    def as_markdown(self) -> str:
        return f"- [ ] [{self.repo_key} #{self.number}]({self.url}) — {self.title} (@{self.author})"


def load_manifest(path: Path) -> list[dict]:
    """Read ``releases/releases.yaml`` into a newest-first list of release entries."""
    with open(path) as f:
        return yaml.safe_load(f)["releases"]


def find_release(manifest: list[dict], release_id: str) -> tuple[dict, dict | None]:
    """Return the entry for ``release_id`` and the deployed release it should be compared against.

    The baseline is the nearest *earlier* entry that was actually deployed. Entries marked
    ``deployed: false`` are code releases that never shipped, so they carry no NodeNorm/NameRes
    versions and cannot anchor a comparison.
    """
    ids = [entry["id"] for entry in manifest]
    if release_id not in ids:
        raise ValueError(f"Release {release_id!r} is not in the manifest. Known releases: {', '.join(ids)}")
    index = ids.index(release_id)
    for candidate in manifest[index + 1 :]:
        if candidate.get("deployed", True) and candidate.get("build"):
            return manifest[index], candidate
    return manifest[index], None


def fetch_pull_requests(repo_key: str, repo: str, base: str, head: str) -> list[PullRequest]:
    """List the PRs merged into ``head`` but not ``base``, newest number last.

    Titles and authors come from the commits themselves, so this costs one paginated request per
    repository rather than one per PR.

    :raises RuntimeError: if the ``gh`` CLI is not installed, or the query fails.
    """
    try:
        result = subprocess.run(
            [
                "gh",
                "api",
                # 100 is GitHub's per_page maximum; anything larger is silently clamped.
                f"repos/{repo}/compare/{base}...{head}?per_page=100",
                "--paginate",
                "-q",
                '.commits[] | {m: (.commit.message | split("\\n")[0]), '
                "a: (.author.login // .commit.author.name)} | @json",
            ],
            capture_output=True,
            text=True,
            check=True,
        )
    except FileNotFoundError as error:
        raise RuntimeError(
            "The `gh` CLI is required to list pull requests; install it from https://cli.github.com/."
        ) from error
    except subprocess.CalledProcessError as error:
        # capture_output hides gh's diagnosis (an unknown tag, an expired token, a rate limit), and
        # that message is the whole answer -- surface it rather than a bare non-zero exit.
        raise RuntimeError(
            f"`gh api` failed comparing {base}...{head} in {repo}: {(error.stderr or '').strip()}"
        ) from error

    by_number: dict[int, PullRequest] = {}
    for line in result.stdout.splitlines():
        if not line.strip():
            continue
        commit = json.loads(line)
        message, author = commit["m"], commit["a"] or "unknown"
        squash = _SQUASH_RE.match(message)
        merge = _MERGE_RE.match(message)
        if squash:
            number, title, title_known = int(squash.group("number")), squash.group("title"), True
        elif merge:
            # A merge commit names the PR but not its title; the number is what makes it findable.
            number, title, title_known = int(merge.group("number")), message, False
        else:
            continue
        by_number[number] = PullRequest(repo_key, repo, number, title, author, title_known)
    return [by_number[n] for n in sorted(by_number)]


def read_config_versions(config_text: str) -> dict[str, str]:
    """Pull the upstream-data version keys out of a config.yaml's text."""
    config = yaml.safe_load(config_text) or {}
    return {key: str(config[key]) for key in VERSION_KEYS if key in config}


def version_bumps(repo_root: Path, baseline_tag: str) -> list[str]:
    """Prose bullets for every upstream version that changed since ``baseline_tag``."""
    try:
        previous_text = subprocess.run(
            ["git", "-C", str(repo_root), "show", f"{baseline_tag}:config.yaml"],
            capture_output=True,
            text=True,
            check=True,
        ).stdout
    except subprocess.CalledProcessError:
        return [f"_Could not read `config.yaml` at `{baseline_tag}`; fill in the version bumps by hand._"]

    previous = read_config_versions(previous_text)
    current = read_config_versions((repo_root / "config.yaml").read_text())
    return version_bump_bullets(previous, current, baseline_tag)


def version_bump_bullets(previous: dict[str, str], current: dict[str, str], baseline_tag: str) -> list[str]:
    """Prose bullets for the VERSION_KEYS that differ between two read_config_versions() results.

    A key that only the current config has -- one added to config.yaml since the baseline, as
    ``mesh_year`` was -- gets a visible to-do rather than nothing: the source was still updated (MeSH
    went from the stale 2024 dump to 2026), but the baseline cannot say from what.
    """
    bullets = []
    for key, label in VERSION_KEYS.items():
        was, now = previous.get(key), current.get(key)
        if was and now and was != now:
            bullets.append(f"- Updated {label} from {was} to {now}.")
        elif now and not was:
            bullets.append(
                f"- _Now on {label} {now}, but `config.yaml` at `{baseline_tag}` has no `{key}`;"
                f" say by hand which {label} version it was updated from._"
            )
    return bullets


def build_workarounds(repo_root: Path) -> list[str]:
    """The `build.workarounds` notes recorded in config.yaml for this build."""
    config = yaml.safe_load((repo_root / "config.yaml").read_text()) or {}
    return [str(note) for note in (config.get("build") or {}).get("workarounds") or []]


def notable_changes(build_dir: Path) -> str | None:
    """The `## Notable changes` bullets from the build's own prefix comparison report."""
    path = build_dir / "reports" / "tables" / "prefix_comparison.md"
    if not path.exists():
        return None
    match = _NOTABLE_RE.search(path.read_text())
    return match.group(1).strip() if match else None


def umls_mesh_skew_bullet(build_dir: Path) -> str | None:
    """A bullet saying how many UMLS MSH codes this build's MeSH dump has no record for, from its report.

    The build fails when the count is over ``umls_mesh_skew_max_missing_codes``, so a build that got this far
    is within the limit; the bullet is here so whoever writes the note sees how close it came, alongside the
    UMLS and MeSH version bumps it depends on. None for a build older than the report.
    """
    path = build_dir / "reports" / "umls_mesh_skew.json"
    if not path.exists():
        return None
    report = json.loads(path.read_text())
    return (
        f"- {report['umls_msh_codes_missing_from_mesh_excluding_categories']:,} MeSH codes in UMLS "
        f"{report['umls_version']} have no record in MeSH {report['mesh_year']} (`reports/umls_mesh_skew.json`, "
        f"leaving out the `U` category headings UMLS adds itself). _Delete this line unless the count is large._"
    )


def fetch_status(url: str) -> dict | None:
    """GET a NodeNorm or NameRes ``/status`` document, or None if it can't be reached.

    A release note is worth drafting even when the services are down or not yet deployed, so a
    failure here degrades to the blank table skeleton rather than aborting.

    The except clause has to be wider than ``urllib.error``: urllib wraps what happens while
    *opening* the connection, but a failure part-way through ``response.read()`` comes through raw --
    ``http.client.RemoteDisconnected`` (an ``OSError``), ``http.client.IncompleteRead`` (an
    ``HTTPException``), some ``ssl.SSLError`` paths -- and a half-read /status is exactly the failure
    a service being restarted mid-draft produces.
    """
    try:
        with urllib.request.urlopen(url, timeout=STATUS_TIMEOUT_SECONDS) as response:
            return json.loads(response.read())
    except (OSError, http.client.HTTPException, ValueError) as error:
        print(f"warning: could not read {url}: {error}", file=sys.stderr)
        return None


def check_deployed_version(status: dict | None, service: str, release_id: str) -> list[str]:
    """Warn if a service is not actually serving the release being drafted.

    Worth checking rather than assuming: at the time 2026jul22 was written, NodeNorm had been
    updated but NameRes was still answering from 2025sep1, so its Solr numbers described the
    *previous* release. Silently copying them into the table would have been wrong and invisible.
    """
    if not status:
        return []
    deployed = status.get("babel_version")
    if deployed and deployed != release_id:
        return [
            f"<!-- WARNING: {service} at this URL is serving Babel {deployed}, not {release_id}. "
            f"Its numbers below describe {deployed}; re-run once {release_id} is deployed. -->"
        ]
    return []


def _column_widths(header, rows) -> list[int]:
    """Per-column Markdown width. Tolerates no data rows -- a header-only CSV is a report that ran
    and found nothing, which should still render rather than crash the whole draft."""
    return [max([len(h), *(len(row[i]) for row in rows)]) for i, h in enumerate(header)]


def _solr_stats(nameres: dict | None) -> dict:
    """The Solr counters from a NameRes ``/status``, whichever shape that version serves.

    NameRes v1.5.2 nests them under ``solr``; v1.7.0 promotes ``numDocs``/``maxDoc``/``size`` to the
    top level. Both shapes are live right now (Dev is on v1.5.2, Exp on v1.7.0), and reading only the
    nested one yields a silently *blank* Solr row rather than an error -- which is the failure this
    table exists to prevent.
    """
    nameres = nameres or {}
    nested = nameres.get("solr")
    if isinstance(nested, dict) and "numDocs" in nested:
        return nested
    return nameres if "numDocs" in nameres else {}


def summary_table(nodenorm: dict | None, nameres: dict | None) -> list[str]:
    """The `## Summary` table of deployed database sizes, from the two services' /status endpoints."""
    rows: list[tuple[str, str, str, str]] = []

    databases = (nodenorm or {}).get("databases") or {}
    ordered = [key for key in SUMMARY_DB_ORDER if key in databases]
    ordered += [key for key in databases if key not in SUMMARY_DB_ORDER]
    for key in ordered:
        entry = databases[key]
        rows.append(
            (
                str(entry.get("dbname", "")),
                key,
                f"{entry['count']:,}" if isinstance(entry.get("count"), int) else "",
                str(entry.get("used_memory_rss_human", "")),
            )
        )

    solr = _solr_stats(nameres)
    num_docs = solr.get("numDocs")
    rows.append(
        ("Solr", "name_lookup", f"{num_docs:,}" if isinstance(num_docs, int) else "", str(solr.get("size", "")))
    )

    if not databases:
        # No NodeNorm data: emit the known row skeleton so the numbers can be filled in by hand.
        rows = [(name, key, "", "") for name, key in zip(_SKELETON_NAMES, SUMMARY_DB_ORDER)] + rows[-1:]

    header = ("Database name", "Database ID", "Number of keys", "Memory used")
    widths = _column_widths(header, rows)
    lines = [
        "| " + " | ".join(h.ljust(w) for h, w in zip(header, widths)) + " |",
        "|" + "|".join("-" * (w + 2) for w in widths) + "|",
    ]
    lines += ["| " + " | ".join(c.ljust(w) for c, w in zip(row, widths)) + " |" for row in rows]
    return lines


# Display names for the Redis databases, used only when NodeNorm is unreachable and the table has to
# be emitted blank; when it responds, /status supplies the real `dbname` for each.
_SKELETON_NAMES = [
    "id-id",
    "id-eq-id",
    "id-categories",
    "semantic-count",
    "info-content",
    "conflation-db",
    "chemical-drug-db",
]
# Zipped against SUMMARY_DB_ORDER, which silently truncates to the shorter list -- and a skeleton
# row missing its name is exactly the kind of quiet gap the blank table is meant to make obvious.
assert len(_SKELETON_NAMES) == len(SUMMARY_DB_ORDER), "_SKELETON_NAMES must name every SUMMARY_DB_ORDER database"


def _format_count(value: str) -> str:
    return f"{int(value):,}"


def _format_diff(value: str) -> str:
    """Comma-group a signed difference, escaping a leading minus so Markdown doesn't eat it."""
    number = int(value)
    if number < 0:
        # `\-` keeps the sign visible; a bare leading `-` can be read as a list bullet.
        return f"\\-{abs(number):,}"
    if number > 0:
        return f"+{number:,}"
    return "0"


def _format_percent(value: str) -> str:
    """Match the existing tables: escaped negatives, and a from-zero row reads as Infinity%."""
    if value == "NEW":
        return "Infinity%"
    return f"\\{value}" if value.startswith("-") else value


def comparison_baseline(build_dir: Path) -> str | None:
    """The release the build's own prefix comparison actually ran against, per its own report.

    Not the same thing as the manifest's previous entry: the build compared itself against whatever
    ``previous_release`` was pinned in ``config.yaml`` at build time, and a build that skipped a
    release (or was built before one shipped) compares against something older.
    """
    path = build_dir / "reports" / "tables" / "prefix_comparison.md"
    if not path.exists():
        return None
    match = _BASELINE_RE.search(path.read_text())
    return match.group(1) if match else None


def summary_of_changes(build_dir: Path, previous_id: str, release_id: str) -> str | None:
    """Rebuild the release note's `Summary of changes` table from the build's comparison CSV.

    The CSV is already a comparison of this build against the baseline pinned in config.yaml; this
    only reformats it -- no numbers are recomputed here. Which is why the baseline column is labelled
    from the report rather than from ``previous_id``: the caller's idea of the previous release comes
    from ``releases.yaml`` and the numbers come from the build, and nothing keeps those two in step.
    Label the column with the manifest's answer and a build pinned to an older `previous_release`
    silently gets the wrong year's numbers under the right name.
    """
    path = build_dir / "reports" / "tables" / "prefix_comparison_overall.csv"
    if not path.exists():
        return None

    baseline = comparison_baseline(build_dir)
    if baseline and baseline != previous_id:
        print(
            f"warning: {build_dir}'s comparison is against {baseline}, not the manifest's previous "
            f"release {previous_id}; labelling the column {baseline}. Check that is what you want.",
            file=sys.stderr,
        )
        previous_id = baseline

    rows = []
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            metric = row["Metric"]
            if metric == "All CURIEs":
                label = "Count of CURIEs in all files"
            elif metric.startswith("All cliques"):
                # Keep the source metric's "(approx)". These counts are HyperLogLog estimates
                # (~2% error, as the build's own prefix_comparison.md says), and the gap is wide
                # enough to matter: 2026jul22's table says 388,490,111 where the exact
                # `clique_count` in its CURIE summary is 398,671,998. Published notes through
                # 2025sep1 used the unqualified label and read as exact totals.
                label = "Count of cliques in all files (approx)"
            else:
                label = metric.removesuffix(" CURIEs")
            rows.append(
                (
                    label,
                    _format_count(row["Previous"]),
                    _format_count(row["Current"]),
                    _format_diff(row["Absolute change"]),
                    _format_percent(row["Percent change"]),
                )
            )

    header = ["Filename", previous_id, release_id, "Diff", "% Diff"]
    widths = _column_widths(header, rows)
    lines = [
        "| " + " | ".join(h.ljust(w) for h, w in zip(header, widths)) + " |",
        "| " + " | ".join("-" * w for w in widths) + " |",
    ]
    lines += ["| " + " | ".join(c.ljust(w) for c, w in zip(row, widths)) + " |" for row in rows]
    return "\n".join(lines)


def unexplained_movers(build_dir: Path, threshold_pct: float = MOVER_THRESHOLD_PCT) -> list[str]:
    """A checklist of every compendium that moved enough to need an explanation in the note.

    The build's own prefix comparison flags large drifts *per prefix*; nothing flags them per
    compendium, which is the granularity the note's table and its `Areas that changed substantially`
    section are written at. So a compendium can move 97% and reach publication unmentioned -- that is
    exactly what happened to `Polypeptide` in 2026jul22.

    Emitted as unchecked items rather than prose: under time pressure the honest outcome is often
    "investigated enough to file an issue, not enough to explain", and an unchecked box says that
    while a missing paragraph says nothing.
    """
    path = build_dir / "reports" / "tables" / "prefix_comparison_overall.csv"
    if not path.exists():
        return []

    movers = []
    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            metric, percent = row["Metric"], row["Percent change"]
            # The two "All ..." rows are the whole-build aggregates, explained by the rest of the list.
            if metric.startswith("All "):
                continue
            name = metric.removesuffix(" CURIEs")  # match the table's row labels
            if percent == "NEW":
                movers.append((name, "new this release"))
            elif abs(float(percent.rstrip("%"))) >= threshold_pct:
                movers.append((name, f"{percent}, {_format_diff(row['Absolute change'])} CURIEs"))
    if not movers:
        return []

    return [
        f"Every compendium that moved by {threshold_pct:.0f}% or more, for triage. Explain it above, or",
        "file an issue and link it here — but do not publish one of these unaddressed:",
        "",
        *[f"- [ ] **{name}** ({detail})" for name, detail in movers],
    ]


def is_tag(repo_root: Path, ref: str) -> bool:
    """Whether ``ref`` resolves to a tag in the local checkout (so a link to it will not 404)."""
    return (
        subprocess.run(
            ["git", "-C", str(repo_root), "rev-parse", "-q", "--verify", f"refs/tags/{ref}"],
            capture_output=True,
        ).returncode
        == 0
    )


def provenance_block(entry: dict, previous: dict | None, repo_root: Path = REPO_ROOT) -> list[str]:
    """The bullet block at the top of the note: what was built, and what was deployed with it."""
    release_id, build = entry["id"], entry.get("build")
    lines = []

    babel_bits = []
    if build:
        # `build` names the output directory, which for a modern release is *also* a git tag -- but
        # not always: 2024aug18, 2024mar24 and 2023nov5 were never tagged, and linking them anyway
        # publishes a 404 as provenance. Say "untagged" rather than guess.
        if is_tag(repo_root, build):
            babel_bits.append(f"[tagged {build}](https://github.com/{REPOS['Babel']}/releases/tag/{build})")
        else:
            babel_bits.append(f"untagged in this repository as of drafting (`{build}`)")
    if entry.get("babel"):
        version = entry["babel"]
        approx = "approx " if entry.get("approx") else ""
        babel_bits.append(f"{approx}[Babel {version}](https://github.com/{REPOS['Babel']}/releases/tag/{version})")
    if entry.get("branch"):
        babel_bits.append(f"branch `{entry['branch']}`")
    target = f"[{build}]({BUILD_URL.format(build=build)})" if build else release_id
    lines.append(f"- Babel: {target}" + (f" ({', '.join(babel_bits)})" if babel_bits else ""))
    if entry.get("biolink"):
        lines.append(f"  - Built against Biolink Model v{entry['biolink']}")
    if build:
        # The note is `releases/<build>/README.md`, so it sits beside the archive it links into.
        lines.append("  - [Summary tables](./reports/tables/)")
        lines.append("  - [CURIE summary](./reports/content/compendia_report.json)")
        lines.append("  - [Prefix report](./reports/duckdb/prefix_report.json)")

    for key, repo_key in (("nodenorm", "NodeNorm"), ("nameres", "NameRes")):
        versions = entry.get(key) or []
        if not versions:
            continue
        links = ", ".join(f"[{v}](https://github.com/{REPOS[repo_key]}/releases/tag/{v})" for v in versions)
        lines.append(f"- {repo_key}: {links}")

    lines.append("")
    lines.append("Next release: _None as yet_")
    if previous:
        # Note directories are keyed by *build*, not release id -- for the Translator-named releases
        # those differ (id TranslatorFuguJuly2024, build 2024jul13), and linking by id pointed at
        # files that were never there. See releases/ARTIFACTS.md. `find_release` skips undeployed
        # entries, so v1.11 -- the one note not at `<build>/README.md` -- never turns up here.
        lines.append(f"Previous release: [{previous['title']}](../{previous.get('build') or previous['id']}/README.md)")
    return lines


def pull_request_sections(prs_by_repo: dict[str, list[PullRequest] | str]) -> list[str]:
    """The triage checklist: every PR in the release, substantive ones first, routine ones folded.

    A ``str`` value in place of a list is the reason that repository's range could not be listed --
    no baseline version in ``releases.yaml``, or a compare that GitHub refused. Either way it gets a
    visible TODO carrying the reason rather than a missing section.
    """
    lines = [
        "## All changes in this release",
        "",
        "Every pull request between this release and the previous one, for triage: promote what",
        "matters into the sections above, then delete the rest of this section before publishing.",
    ]
    for repo_key, prs in prs_by_repo.items():
        if isinstance(prs, str):
            lines += ["", f"### {repo_key} (range unknown)", "", f"_TODO: {prs}_"]
            continue
        substantive = [pr for pr in prs if not pr.is_routine]
        routine = [pr for pr in prs if pr.is_routine]
        lines += ["", f"### {repo_key} ({len(prs)} PRs)", ""]
        lines += [pr.as_markdown() for pr in substantive] or ["_No substantive changes._"]
        if routine:
            # A plain sub-heading rather than a <details> block: rumdl's MD033 rejects inline HTML,
            # and this section is deleted before publishing anyway.
            plural = "" if len(routine) == 1 else "s"
            lines += [
                "",
                f"#### {len(routine)} routine {repo_key} change{plural}",
                "",
                "Dependency bumps, version increments and merge commits.",
                "",
            ]
            lines += [pr.as_markdown() for pr in routine]
    return lines


def draft(
    entry: dict,
    previous: dict | None,
    build_dir: Path | None,
    repo_root: Path,
    nodenorm_url: str | None = None,
    nameres_url: str | None = None,
) -> str:
    release_id = entry["id"]
    nodenorm = fetch_status(nodenorm_url) if nodenorm_url else None
    nameres = fetch_status(nameres_url) if nameres_url else None
    lines = [f"# {entry['title']}", ""]
    lines += provenance_block(entry, previous, repo_root)

    # A reason string (rather than an absent key) for a repository whose PR range can't be worked
    # out, so the checklist says so instead of quietly omitting the section -- an omitted section
    # reads as "nothing changed in that repository", which is the failure this whole checklist exists
    # to prevent. That covers a failed `gh` compare too: three of the build names in releases.yaml
    # (2024aug18, 2024mar24, 2023nov5) were never tagged in this repo, and letting that abort the run
    # trades a note with one TODO section for no note at all.
    prs_by_repo: dict[str, list[PullRequest] | str] = {}
    if previous:
        for repo_key, repo in REPOS.items():
            base, head = _range_for(repo_key, entry, previous)
            if not (base and head):
                reason = (
                    f"`releases/releases.yaml` records no baseline {repo_key} version for the "
                    f"previous release, so its pull requests could not be listed. Fill the version "
                    f"in and re-run, or list them by hand."
                )
            else:
                try:
                    prs_by_repo[repo_key] = fetch_pull_requests(repo_key, repo, base, head)
                    continue
                except RuntimeError as error:
                    reason = f"Could not list {repo_key} pull requests: {error}"
            print(f"warning: {reason}", file=sys.stderr)
            prs_by_repo[repo_key] = reason

    bumps = version_bumps(repo_root, previous["build"]) if previous else []
    skew = umls_mesh_skew_bullet(build_dir) if build_dir else None
    if skew:
        bumps.append(skew)
    workarounds = build_workarounds(repo_root)
    notable = notable_changes(build_dir) if build_dir else None
    table = summary_of_changes(build_dir, previous["id"], release_id) if build_dir and previous else None

    # `## Bugfixes` leads, deliberately empty. It is not "PRs labelled bug" -- it is the much
    # narrower set a consumer has to act on: behaviour they may have relied on that was wrong before
    # and is right now. Emitting it empty rather than omitting it makes "this release had none" a
    # decision someone made, and the section is cheap to delete when that is the answer.
    lines += [
        "",
        "## Bugfixes",
        "",
        '<!-- Wrong behaviour a consumer may have relied on, now corrected: "you used to see X, you',
        "     now see Y -- check whether X affected your downstream analyses, and whether Y is the",
        '     behaviour you want." Promote items here during triage, or delete the whole section if',
        "     this release has none. -->",
        "",
        "- _TODO_.",
    ]

    # `## Areas that changed substantially` sits directly under the bugfixes, above the Babel notes:
    # after "what must I act on", "what moved" is what a consumer reads next. It is deliberately far
    # from the two tables it explains -- a reader who wants the numbers can scroll to them, and the
    # same change is often worth a one-line headline here *and* a paragraph in the per-repository
    # notes below, which reads as repetition when the two are adjacent.
    if notable or build_dir:
        lines += [
            "",
            "## Areas that changed substantially",
            "",
            "Explanations for the movements in the `Compendium size comparison` table at the end of",
            "this note. Every large change here should trace back to a pull request in this release;",
            "one that doesn't is either an upstream data change (say so, and say how you checked) or",
            "a bug to file before this release ships.",
        ]
        if notable:
            lines += ["", "Straight from this build's own `reports/tables/prefix_comparison.md`:", "", notable]
        movers = unexplained_movers(build_dir) if build_dir else []
        if movers:
            lines += ["", *movers]

    lines += [
        "",
        "## Babel changes",
        "",
        "### Updates",
        "",
    ]
    lines += bumps or ["- _TODO_."]
    for heading in (
        "New features and identifier/mapping additions",
        "Improvements to Babel's output",
        "Development and infrastructure",
        "Minor changes and fixes",
    ):
        lines += ["", f"### {heading}", "", "- _TODO_."]

    lines += ["", "### Known issues and caveats", ""]
    lines += [f"- {note}" for note in workarounds] or ["- _TODO_."]

    # NodeNorm and NameRes get their own top-level sections, named for the store each one serves --
    # which is also how the `## Deployed database sizes` table below is laid out. The PR checklist is
    # already grouped by repository, so triage is "move each PR up into its own section" rather than
    # re-deriving the grouping by hand.
    for heading in ("NodeNorm Redis", "NameRes Solr"):
        lines += ["", f"## {heading}", "", "- _TODO_."]

    lines += ["", *pull_request_sections(prs_by_repo)]

    lines += ["", "## Deployed database sizes", ""]
    if nodenorm or nameres:
        lines += [f"Sizes of the deployed databases, read from `{nodenorm_url}` and `{nameres_url}`.", ""]
        lines += check_deployed_version(nodenorm, "NodeNorm", release_id)
        lines += check_deployed_version(nameres, "NameRes", release_id)
    else:
        lines += ["<!-- TODO: fill in from the deployed Redis and Solr instances. -->", ""]
    lines += summary_table(nodenorm, nameres)

    lines += ["", "## Compendium size comparison", ""]
    lines += [table] if table else ["_TODO: no `prefix_comparison_overall.csv` found in the build directory._"]

    return "\n".join(lines) + "\n"


def _range_for(repo_key: str, entry: dict, previous: dict) -> tuple[str | None, str | None]:
    """The (base, head) refs to compare for one repository.

    For Babel that is the two build tags. For NodeNorm and NameRes it is the *last* version deployed
    against each build, since a build can outlive several service releases.
    """
    if repo_key == "Babel":
        return previous.get("build"), entry.get("build")
    key = repo_key.lower()
    base, head = previous.get(key) or [], entry.get(key) or []
    return (base[-1] if base else None), (head[-1] if head else None)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("release", help="Release id to draft, as listed in releases/releases.yaml (e.g. 2026jul22).")
    parser.add_argument(
        "--build-dir",
        type=Path,
        help="A copy of the build's output directory, used for reports/tables/. Defaults to the "
        "release's own archive under releases/<build>/ when that exists, which holds the same files "
        "at the same paths. Without either, the count table and notable-changes section are TODOs.",
    )
    parser.add_argument(
        "--manifest",
        type=Path,
        default=None,
        help="Path to releases.yaml (default: releases/releases.yaml in the repo root).",
    )
    parser.add_argument(
        "--nodenorm-status",
        default=NODENORM_STATUS_URL,
        help=f"NodeNorm /status URL supplying the Summary table's Redis rows (default: {NODENORM_STATUS_URL}).",
    )
    parser.add_argument(
        "--nameres-status",
        default=NAMERES_STATUS_URL,
        help=f"NameRes /status URL supplying the Summary table's Solr row (default: {NAMERES_STATUS_URL}).",
    )
    parser.add_argument(
        "--offline",
        action="store_true",
        help="Skip the /status lookups and emit the Summary table blank. The GitHub queries still run.",
    )
    args = parser.parse_args(argv)

    repo_root = REPO_ROOT
    manifest_path = args.manifest or repo_root / "releases" / "releases.yaml"
    entry, previous = find_release(load_manifest(manifest_path), args.release)
    if args.build_dir and not args.build_dir.is_dir():
        parser.error(f"--build-dir {args.build_dir} is not a directory")
    if not args.build_dir and entry.get("build"):
        # The archive mirrors the build directory, so it answers the same four questions. This is
        # what makes an old note re-draftable from the repository alone.
        archived = manifest_path.parent / entry["build"]
        if archived.is_dir():
            args.build_dir = archived
            print(f"Reading build reports from the archived release {archived}.", file=sys.stderr)

    print(
        draft(
            entry,
            previous,
            args.build_dir,
            repo_root,
            nodenorm_url=None if args.offline else args.nodenorm_status,
            nameres_url=None if args.offline else args.nameres_status,
        ),
        end="",
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
