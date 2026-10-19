# Source Download Patterns

This file documents recurring patterns and trade-offs that appear across multiple data handlers in
`src/datahandlers/`. Prefer adding source-specific notes to that source's own directory; put
guidance here only when the same pattern applies to several sources.

## HTTP directory listing vs FTP for file discovery

Several upstream sources (EBI FTP, NCBI FTP, etc.) expose their file listings both as a native FTP
service and as an HTTP mirror whose URLs look like
`https://ftp.example.org/pub/databases/source/current/`.

**Current approach in Babel** — when a handler only needs to discover which files exist in a
directory and then download them, we parse the HTTP directory listing rather than connecting via
FTP. `src/datahandlers/complexportal.py` is the canonical example: `_DirectoryListingParser`
scrapes `<a href>` tags from the Apache autoindex page to enumerate TSV filenames, and then
`pull_via_urllib` downloads each file over HTTPS.

**Why HTTP over FTP for the actual download** — FTP connections can stall or time out on large
files, especially through firewalls and NAT. HTTPS is more reliable for multi-hundred-megabyte
transfers.

**Why HTTP listing instead of FTP `NLST`/`MLSD`** — EBI's autoindex format has been stable for
years and the parser is a handful of lines. `ftplib` adds a second protocol dependency and
occasional auth/passive-mode headaches. The risk is that EBI (or another host) switches to a
different listing format (e.g. Nginx JSON autoindex, a JavaScript SPA) and the parser silently
returns an empty list. The existing `fetch_complexportal_tsv_filenames()` guard (`if not
tsv_filenames: raise RuntimeError(...)`) catches this immediately at runtime.

**If the HTML listing breaks** — switch `fetch_complexportal_tsv_filenames()` (and equivalent
functions in other handlers) to use `ftplib.FTP.nlst()` or `MLSD` for discovery while keeping
`pull_via_urllib` for the actual download. That combination gives structured directory listings
without sacrificing download reliability. The FTP URL for EBI is
`ftp.ebi.ac.uk/pub/databases/intact/complex/current/complextab/`.

**When to prefer FTP listing from the start** — if the HTTP mirror does not serve an Apache-style
autoindex (no `<a href>` links to files), use FTP `NLST`/`MLSD` for discovery instead.

## Sources behind a Cloudflare bot challenge

Some sources (HMDB is the first) front their download URLs with a Cloudflare challenge page: the
request comes back as a 403 whose body is an interactive JS/Turnstile challenge rather than the file
you asked for. Cloudflare marks these responses with a
[`cf-mitigated: challenge` header](https://developers.cloudflare.com/cloudflare-challenges/challenge-types/challenge-pages/detect-response/).

**Retrying does not help, and neither does the User-Agent.** The challenge is served identically to
a real browser; only a browser that can execute the challenge gets through. Left alone, a download
would burn its whole retry budget (and, under Snakemake `retries`, the whole rule's) on a URL that
cannot succeed unattended.

**What Babel does** — `pull_via_urllib()` calls `raise_if_cloudflare_challenge()`
(`src/babel_utils.py`) before its retry/backoff path. On a challenge response it raises immediately
with a `RuntimeError` naming both the URL and the local path the file is expected at, so the
operator can download it in a browser, drop it in place, and re-run the rule. Two signals are
checked: the `cf-mitigated: challenge` header, and — as a fallback should Cloudflare rename that
header — a `content-security-policy` allowing `challenges.cloudflare.com`, which a challenge page
needs in order to load the widget at all.

**Placing the file by hand has to actually work.** `pull_via_urllib()` deletes its target before
every attempt, so a handler whose error message says "put it here and re-run" must skip the
download when the file is already present, or the manual copy is wiped and the rule fails
identically forever. `pull_hmdb()` (`src/datahandlers/hmdb.py`) is the worked example; copy that
shape rather than relying on `pull_via_urllib()` alone.

**A challenge still burns the rule's Snakemake `retries`.** Snakemake has no way to mark a failure
as non-retryable from inside the rule, so `get_HMDB` re-runs its three times. Each one fails in
well under a second, without backoff, and the last error the operator sees is the actionable one —
not worth machinery to suppress.

**When a new source starts doing this** — the detection is generic, so no code change is needed; the
rule will simply fail fast with instructions. Record the manual download in `config.yaml` under
`build.workarounds` for that release, the same way the DrugBank login restriction is recorded, so
the next person running a build knows a file needs placing by hand.
