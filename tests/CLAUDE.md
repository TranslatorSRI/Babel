# CLAUDE.md — tests/

Point-of-use conventions for writing Babel tests, for Claude Code. For the test taxonomy (the four
`unit`/`network`/`slow`/`pipeline` marks, the `min_memory_gb` guard) and *where* a new test belongs,
see [`README.md`](README.md) — this file only covers how to write one once you know where it goes.

- **Test documentation** — every test docstring states the scenario and expected outcome ("should"
  phrasing works well). Group related tests with a `# LABEL` section comment; the module docstring
  describes the file overall without duplicating that section list.

- **Assertion helpers** — `tests/conftest.py` exports `assert_labels_file_valid`,
  `assert_synonyms_file_valid`, `assert_ids_file_valid`, `assert_concordance_file_valid`,
  `assert_taxa_file_valid`, and `assert_descriptions_file_valid` (plus `read_tsv`). Use these
  instead of hand-rolling TSV checks. When a handler adds a new output kind, add the matching helper
  to the root `conftest.py` rather than a private one in the test file.

- **Ruff lint in test code** — all Python must pass `uv run ruff check`. Two rules are easy to trip
  in tests: E741 (no single-letter ambiguous names like `l`/`O`/`I`) and F841 (no unread
  assignments).

- **Fixtures for a real-data bug must be copied verbatim** — when a test stands in for a specific
  record from a source file, paste that record's row exactly as it appears in the downloaded file
  and name the record's ID in a comment, so the next person can re-derive it. Never hand-compose a
  row that looks like what you think the source contains. A fabricated fixture in the #744 work
  invented a `gene_info.gz` row shape NCBI never emits and then asserted it survives, certifying a
  guarantee the real data does not provide — and it read as entirely plausible for months. If the
  real row is too long to paste, that is a reason to add a `pipeline` test over the real file, not
  a reason to invent a shorter one.

- **Collecting a repeated literal into a constant can make an assertion tautological** — and the
  test still passes, so nothing tells you. `test_gard.py` had a distribution URL and the
  ContentVersion id inside it written out separately in two places; extracting both and building the
  URL from the id turned `content_version_id(_DISTRIBUTION_URL) == _CONTENT_VERSION_ID` into a
  statement true for any string. The duplication had been carrying a realism check nobody had named.
  After any such extraction, **break each new constant in turn and confirm a test fails**; if none
  does, the constant needs tying to something independent (there, a fixture copied verbatim from the
  live page). The probe is three lines of shell and takes seconds.

- **Never put a blanket `xfail` on a `network`-marked test** — the mark already keeps it out of CI,
  so the only thing a blanket `xfail(strict=False)` adds is that the test stops reporting failures
  once the service comes back. `test_pull_ensembl` carried one reading "requires network access to
  the Ensembl BioMart service"; BioMart returned, the test ran again, and a stale assertion of its
  own (a dataset had outgrown the single-query path) was reported as a routine xfail for months.
  `--runxfail` is what finally showed it. Guard the unreachable-service case from *inside* the
  test instead, so every assertion stays live: `pytest.skip`/`pytest.xfail` in an `except` on the
  connection error, as `tests/datahandlers/test_pantherfamily.py` and the `ubergraph` fixture in
  `conftest.py` do.

- **Assert upstream-derived counts as bounds, not equalities** — a test that pins how many
  attributes/files/rows a live source exposes is asserting something the source may change without
  telling you, and the failure looks like a bug in your code. `test_pull_ensembl` now asserts
  `len(batches) >= 2` rather than `== 2` for exactly this reason, and derives the "fits in one
  query" limit from the dataset instead of hard-coding it.

- **Pin known-imperfect behavior, don't leave it unasserted** — when shipping a partial fix, assert
  the wrong-but-harmless behavior that remains, with a comment saying it pins current behavior, a
  link to the tracking issue, and an instruction to **invert** the assertion when the fix lands
  rather than delete it. Otherwise the eventual fix changes behavior nothing was watching. The
  worked example is `SPLIT_CASES` in `tests/datahandlers/test_ncbigene.py`: the #932 fix drops a
  shredded value's middle pieces by matching them against the row's intact `Full_name`, so a row
  that has *no* `Full_name` still leaks them —
  `shredded-value-without-full-name-keeps-middle-pieces` asserts that leak rather than ignoring it,
  and is the case to invert if that gap is ever closed.
