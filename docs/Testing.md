# Testing Strategy

This document describes *how* and *where* Babel's tests should be run — what to automate, what to
schedule, what to leave on-demand, and what new kinds of testing might be worth adding. It is
companion reading to [`tests/README.md`](../tests/README.md), which describes *what* tests exist
and how they are marked.

Recommendations are tagged with one of:

- **Recommended** — the author should adopt this.
- **Optional** — useful if cheap to set up; skip otherwise.
- **Probably not worth it** — included for completeness so the trade-off is on record.

## Goals and constraints

The constraints that shape this strategy:

1. **Every PR should get *some* signal**, even when the change touches a data handler whose only
   real validation is a full pipeline run.
2. **GitHub Actions minutes are finite** and the project is community-funded — long-running jobs
   should not run on every push.
3. **Pipeline tests are expensive in disk and time**, but most of that cost is in the *downloads*
   and the `write_X_ids()` steps; assertions on cached intermediates are cheap.
4. **The author has access to HPC and Kubernetes** clusters, which can host self-hosted runners
   and persist `babel_downloads/` + `babel_outputs/intermediate/` between runs.
5. **The authoritative validation is `pytest --all --regenerate`** before a full pipeline run. No
   amount of incremental testing replaces that.
6. **Test fixes themselves take time.** A weekly cadence on slow suites prevents a backlog of
   stale failures.
7. **Development is bursty.** Active sprints (a deadline, a data source update, a Biolink Model
   bump) produce many PRs in a short window. Between sprints the codebase may be untouched for
   weeks or months. Infrastructure that requires continuous maintenance — a persistent self-hosted
   runner, a triage rotation, nightly alert monitoring — carries overhead that is hard to justify
   between sprints. Prefer automation that is cheap to leave running always, and tools that are
   cheap to activate at the start of a sprint and ignore the rest of the time.

## Current state (2026-05)

- **Per PR (GitHub Actions):** `ruff`, `snakefmt`, `rumdl`,
  `pytest --collect-only`, and `pytest -m unit`. Fast and free.
- **Weekly (GitHub Actions, Wednesdays 17:00 UTC):**
  `pytest --network -m "unit or network or slow"`.
- **Pipeline tests:** never automated. Run manually before a release on the HPC cluster.
- **Full pipeline run:** 12–24 hours on HPC, performed before a release.

The big gap is pipeline tests: they only run when someone remembers to run them, and they cost
nothing to run on a machine that already has the downloads.

### "It passes locally but fails on the PR"

Usually this means the branch is behind `main`, not that a test is flaky. The `pull_request` event
runs against your branch **merged with `main`**, so it includes tests and files that only exist on
`main`; a branch-push run and your local checkout do not. The same commit can therefore produce one
passing run and one failing run, differing only in `event`, and `pytest` will collect a different
number of tests in each.

Check with `gh run view <id> --json event,headSha`. The fix is to merge `main` into the branch, at
which point local runs collect what CI does — worth doing as soon as the two disagree rather than
diagnosing each failure in turn. A branch that is behind sees none of `main`'s new guards, which is
how `tests/test_docs_links.py` first ran against PR #983's new files only on the merge.

## Recommended cadence by environment

### Per PR — GitHub Actions (unchanged from today)

**Recommended.** Keep what is there now:

- Formatting (`ruff`, `snakefmt`, `rumdl`).
- `pytest --collect-only -q`.
- `pytest -m unit --no-cov -q`.

These are the only checks that are cheap enough to run on every push and that give meaningful
signal in under a minute. Anything that needs network or pipeline state should not run on every
PR — the false-positive rate (transient network failures, missing UMLS credentials, etc.) would
train reviewers to ignore CI.

#### Optional additions per PR

- **Coverage diff** on changed files only (Codecov or `coverage-comment-action`). Catches the
  "added a new branch with no test" case without enforcing a global coverage floor. Cheap to
  add, useful, but not load-bearing.
- **Path-filtered triggers** — if a PR only touches `src/datahandlers/foo.py`, trigger an
  additional job that runs the `foo`-specific network test. Implement with
  `dorny/paths-filter` or `tj-actions/changed-files`. Requires the test files to be named or
  marked in a way that the workflow can map changed source files to tests.
- **Affected-area pipeline checks** — for changes that touch `src/createcompendia/chemicals.py`,
  run `tests/pipeline/checks/test_chemicals.py` (ID-presence checks only, which don't need
  Snakemake) on the self-hosted runner. Higher complexity; only worth it once self-hosted
  runners exist.

### Nightly — self-hosted runner on HPC (sprint-active only)

**Recommended during active sprints; not worth maintaining as always-on infrastructure.**

The command itself is simple — run on a self-hosted GitHub Actions runner on HPC with persistent
`babel_downloads/` and `babel_outputs/intermediate/`:

```bash
uv run pytest --pipeline --no-cov -q
```

Without `--regenerate`, this reuses cached intermediates and only re-runs `write_X_ids()` for
vocabularies whose intermediate files are missing. Total time should be on the order of minutes
to tens of minutes once the cache is warm.

What this catches that nothing else does:

- New vocabulary-partitioning regressions (IDs leaking between compendia) introduced by recent
  merges.
- TDD checks in `tests/pipeline/checks/` newly added by issue triage.

**Given the bursty development pattern**, the nightly cadence only delivers value when PRs are
landing frequently. Between sprints, the pipeline is unchanged and a nightly job just burns
resources. Consider activating the self-hosted runner workflow at the start of a sprint and
disabling it (or running less frequently) when the sprint ends. If the maintenance cost of the
persistent runner is too high, the pre-release manual run (below) is an acceptable fallback —
the main risk is that a regression introduced mid-sprint goes undetected until release day.

### Weekly — GitHub Actions (unchanged from today, expand slightly)

**Recommended.** Keep the existing Wednesday network test (already expanded to include `slow`
tests: `pytest --network -m "unit or network or slow"`). This runs on GitHub Actions and is
always-on with no maintenance cost.

The scheduled run is gated to the main repo (`NCATSTranslator/Babel`) via a job-level `if` check
in `test.yml`, so it does not run on forks. Fork owners can still trigger it manually with
`workflow_dispatch`.

The scheduled run is also the place where endpoints that are firewalled elsewhere get exercised:
outbound FTP (port 21) is blocked on HPC nodes, so the PANTHER FTP network test can only pass on a
host with a clean network identity. On firewalled hosts that test xfails a refused/timed-out
connection by design, and fails outright on a session that breaks *after* connecting, so an
upstream change at `ftp.pantherdb.org` surfaces as a failure rather than hiding behind the xfail
(issue #1102).

Consider adding during active sprints:

- A weekly job on the self-hosted HPC runner that runs `pytest --pipeline --regenerate` once a
  week — i.e., re-runs every `write_X_ids()` from cached *downloads*. This validates that
  intermediate-file generation is still deterministic and catches drift between source data and
  parsing code without paying full-pipeline cost. Like the nightly runner, this is most valuable
  when the codebase is actively changing.

### Pre-release — HPC, manual (unchanged from today)

**Recommended.** The full `pytest --all --regenerate` + full Snakemake run remains the
authoritative validation. Document it in a `RELEASE.md` checklist if not already.

#### Optional: continuous "shadow" full run on HPC

A monthly or quarterly full pipeline run on the latest `main` (not tied to a release), with
output diffed against the previous run. This catches slow-moving regressions before they
accumulate into a stressful pre-release scramble. Probably overkill given current release
cadence, but worth revisiting if the project ever moves to monthly releases.

## GitHub Actions vs self-hosted HPC vs Kubernetes

### GitHub Actions — best for

- Unit tests, formatting, linting. Free for public repos, no maintenance, runs in clean
  environments which catches "works on my machine" bugs.
- Network tests against public APIs. The hosted runner has a clean network identity, which is
  closer to what an external consumer of your data sources would see.
- Anything community contributors might want to run themselves.

### Self-hosted runner on HPC — best for

- Pipeline tests that benefit from cached `babel_downloads/` and `babel_outputs/intermediate/`.
- Tests requiring `UMLS_API_KEY` (avoids putting the secret in GitHub Secrets, though that is
  also acceptable). Or use GitHub Secrets and run on the hosted runner — fine either way.
- Anything that needs more than 14 GB RAM (the GitHub-hosted runner limit) without paying for
  larger runners.

Set up via the GitHub Actions self-hosted runner agent on an HPC login or service node. Pin the
runner to a label like `babel-hpc` and select it from workflow YAML with `runs-on:
[self-hosted, babel-hpc]`. The runner should clone fresh per job but mount the persistent
`babel_downloads/` and `babel_outputs/intermediate/` directories.

**Maintenance cost** to be honest about:

- The agent process needs to stay up (systemd unit, supervisord, or a Kubernetes Deployment).
- UV / Python / system packages need periodic updates.
- Disk usage on the cache directories grows; needs a cleanup policy (e.g., LRU eviction once
  the directory exceeds N GB).
- Security: a self-hosted runner accepting jobs from public PRs is a code-execution risk.
  Mitigate by restricting the workflow to `pull_request_target` from trusted forks only, or
  only running pipeline tests on `push` to `main` and tagged branches — *not* on arbitrary PR
  events.

### Kubernetes cluster — best for

- Same use cases as HPC, plus dynamic scale-up. If multiple PRs land in quick succession, a
  K8s-backed runner pool can spin up parallel jobs in a way HPC schedulers (SLURM, PBS) cannot.
- One-shot containerized pipeline experiments where each run starts from a known image.

**Probably not worth it** for Babel specifically unless you already operate a K8s cluster you'd
host the runner on. The HPC setup is closer to the production environment and already has the
data. Use the K8s runner only as a second tier — e.g., for the optional path-filtered per-PR
pipeline checks that need quick turnaround and shouldn't queue behind HPC jobs.

### Rule of thumb

- If it touches the network or external services → GitHub Actions.
- If it needs cached pipeline data → HPC self-hosted runner.
- If it needs to run many small jobs in parallel quickly → K8s self-hosted runner pool.
- If you're not sure → GitHub Actions. Cheaper to maintain.

## Other testing strategies worth considering

### Recommended

#### Compendium regression baseline

Tracked in [issue #764](https://github.com/NCATSTranslator/Babel/issues/764).
After each full pipeline run, serialize summary statistics per compendium (clique counts, clique
size distribution, per-prefix counts, identifier counts per source) to a JSON file checked into
the repo. The next full run compares against the baseline and a script flags any metric that
drifted by more than a configurable threshold.

This is the single most valuable addition for catching silent regressions, and it is especially
well-suited to bursty development: the baseline committed at the end of one sprint becomes the
reference for the start of the next. Changes that accumulated across a long gap between sprints
show up immediately on first comparison, rather than being discovered at release time. Pipeline
tests verify *correctness* on specific cases; the baseline catches *unintended distributional
changes* across the whole output.

#### Smoke test of one full pipeline target per night

In addition to `pytest --pipeline`, run `uv run snakemake --cores N anatomy` (or another small
target) end-to-end on the HPC nightly job. This catches snakefile-level breakage — missing
inputs, wrong rule wiring — that pytest pipeline tests don't exercise because they call
`write_X_ids()` directly. Pick `anatomy` because it's relatively small.

### Optional

#### Property-based tests with Hypothesis for `glom()`

`glom()` is union-find. Union-find has well-known properties: order-independence (merging
{A,B} then {B,C} should equal merging {B,C} then {A,B}), idempotence, etc. A Hypothesis-based
test that generates random pairwise relations and asserts these properties catches bugs that
case-based tests miss. Probably 1–2 days of effort to set up. Worth it only if `glom()` is
modified again; the existing case-based tests have caught the bugs found so far.

#### Snapshot / golden testing for `create_node()`

`NodeFactory.create_node()` produces a structured dict per node. A snapshot test (e.g., with
`syrupy`) captures the exact output for a curated set of inputs and fails if the structure
changes. Useful when refactoring `NodeFactory`; less useful day-to-day because biolink-model
upgrades intentionally change output shape and would require constant snapshot updates. Adopt
only if you find yourself refactoring `node.py` frequently.

#### Mini-fixture ETL tests per data handler

Already proposed in `docs/Development.md` items #8 and `tests/pipeline/README.md` under
"New `network + slow` ETL tests". This would let `pytest -m unit` cover each data handler
parser, dramatically improving per-PR signal for `src/datahandlers/` changes. Significant
upfront investment (one fixture file per handler) but high long-term payoff.

#### Diff testing across compendium versions

A script that takes two `compendia/*.jsonl` files (e.g., last release vs. current run) and
reports added/removed cliques, merged cliques, split cliques. Useful for release notes and
for understanding the impact of a Biolink Model upgrade. Half a day of effort. Not really a
"test" — more of a release-engineering tool — but it lives in the same conceptual space.

#### CodeQL

GitHub-native, free. Catches a small class of bugs (SQL injection, path traversal); for a data
pipeline that doesn't accept user input the value is low but the cost is zero. Enable via
the GitHub Security tab with no workflow changes needed.

#### Mutation testing (`mutmut`, `cosmic-ray`)

Measures whether the test suite *would catch* changes to the source code. High signal for
identifying weak tests, but slow to run and noisy (lots of equivalent mutants). Run once
manually to identify gaps, then probably not as a regular CI job.

### Probably not worth it

#### Pre-commit hooks

Running `ruff`, `snakefmt`, and `rumdl` on every `git commit` catches formatting issues before
push, but the per-PR CI job already catches them quickly. Enforcing checks on every commit adds
friction without meaningfully improving signal.

#### Cross-Python-version matrix

Babel currently advertises support for Python 3.11 through 3.13. Snakemake and bmt both
have specific version constraints, and a full cross-version matrix would add CI cost for a
project that controls its own runtime closely. Running the unit suite under multiple Python
versions could still catch portability bugs, but this is probably not worth making a regular
CI requirement. Skip.

#### Fuzz testing (`atheris`)

Babel processes data from a small number of curated sources, not adversarial input. The
risk model doesn't justify the setup cost.

#### Performance benchmarks in CI

In principle, tracking `glom()` runtime over time would catch performance regressions. In
practice, GitHub-hosted runners have variable performance, so the noise floor is high and
the data is mostly useless. If you wanted this, run it on a dedicated HPC node, not in CI.

#### A staging "mini-pipeline" that runs on every PR

A reduced-data variant of the full pipeline (item #9 in `docs/Development.md`) that runs in
~10 minutes per PR. Conceptually attractive, but the maintenance burden of keeping a second
configuration in sync with the production one is real, and the test signal it provides is
already largely covered by pipeline tests against cached intermediates. The HPC nightly job
is a better use of the same engineering effort.

## Open questions worth deciding explicitly

These questions are most relevant if and when a persistent self-hosted HPC runner is set up
(see [issue #761](https://github.com/NCATSTranslator/Babel/issues/761)). If pipeline tests
continue to run manually, they don't need answers yet.

- **Notification target.** Where should nightly HPC failures go? Slack, GitHub issue auto-file,
  email? Pick one and put a TODO comment in the workflow so it isn't forgotten.
- **Cache eviction policy on the HPC runner.** Without one, `babel_downloads/` and
  `babel_outputs/intermediate/` grow indefinitely. A simple weekly cron that deletes anything
  in `babel_outputs/intermediate/` older than N days (and lets the next pipeline test
  regenerate it) is sufficient.
- **Secret handling.** `UMLS_API_KEY` is currently set per-machine. Decide whether to also put
  it in GitHub Secrets so that network tests can hit UMLS endpoints, or keep UMLS-touching
  tests on the self-hosted runner only.

## Summary table of recommended cadence

For quick reference (one of the few places horizontal rules add value):

- **On every PR (GitHub Actions, ~1 min):** formatting + `pytest -m unit`.
- **Nightly (HPC self-hosted, ~5–30 min):** `pytest --pipeline` against cached intermediates.
- **Weekly (GitHub Actions, ~5 min):** `pytest --network -m "unit or network or slow"`.
- **Weekly (HPC self-hosted, ~hours):** `pytest --pipeline --regenerate` from cached downloads.
- **Pre-release (HPC, ~12–24 hours):** `pytest --all --regenerate` + full Snakemake run + diff
  against compendium baseline.
- **Each release:** update compendium regression baseline JSON if any drift is intentional.
