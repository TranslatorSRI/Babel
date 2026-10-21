"""Re-derive how much the pre-#1020 parse overstated a failed job's duration.

`babel-slurm-errors` times a job from Snakemake's *submit* line to its *finish* line. Snakemake
reports a failure twice -- once as the job dies, and again in the summary it prints when the
workflow gives up -- and `parse_job_events()` used to keep the later timestamp, so a failed
attempt read as the length of the *run* rather than of the job. It now keeps the first.

This script quantifies the difference over the archived runs, and is what the numbers in
`docs/tools/Errors.md` ("A failed job is timed from when it died") come from. Its output is
committed beside it as `output.txt`; regenerate with:

    uv run python docs/tools/failed-job-durations/check_failed_durations.py > \
        docs/tools/failed-job-durations/output.txt

Run it from the repository root. It needs the two archived runs under `data/` (gitignored, ~GBs)
-- `data/2026jul22/logs/` and `data/babel-1.17/logs/`, both pulled from
<https://stars.renci.org/var/babel/>. Only the `sbatch-*.err` logs are read, so those two
directories alone are enough. `data/babel-1.18/` is deliberately *not* included: it holds a second
copy of the 2026jul22 sbatch logs, and counting them twice inflates every figure here.
"""

import statistics
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from src.tools.slurm.parse import (  # noqa: E402
    _ERROR_RE,
    _SUBMIT_RE,
    _parse_ts,
    log_relative,
    parse_job_events,
)

RUNS = ("data/2026jul22", "data/babel-1.17")


def parse_last_wins(err_file):
    """The pre-#1020 parse: every `Error in rule` line overwrites the attempt's finish timestamp."""
    current, out = {}, []
    for line in err_file.read_text(errors="replace").splitlines():
        if m := _SUBMIT_RE.search(line):
            snakemake_id = int(m.group(2))
            if snakemake_id in current:
                out.append(current[snakemake_id])
            current[snakemake_id] = {
                "rule": log_relative(m.group(4)).split("/")[0][len("rule_") :],
                "submitted_at": _parse_ts(m.group(1)),
                "finished_at": None,
                "failed": False,
            }
        elif m := _ERROR_RE.search(line):
            snakemake_id = int(m.group(3))
            if snakemake_id in current:
                current[snakemake_id]["failed"] = True
                current[snakemake_id]["finished_at"] = _parse_ts(m.group(1))
    out.extend(current.values())
    return out


def main():
    logs = [log for run in RUNS for log in sorted(Path(run).rglob("sbatch-*.err"))]
    if not logs:
        raise SystemExit(f"No sbatch-*.err logs under {' or '.join(RUNS)} -- see this script's docstring.")

    total, overstated, deltas = 0, 0, []
    worst = (0.0, "")
    for err in logs:
        # An attempt is identified by its rule and submit time; the fix cannot change either.
        fixed = {(j.rule_name, j.submitted_at): j for j in parse_job_events(err) if j.failed and j.finished_at}
        for old in parse_last_wins(err):
            if not (old["failed"] and old["finished_at"]):
                continue
            new = fixed.get((old["rule"], old["submitted_at"]))
            if new is None:
                continue
            total += 1
            old_sec = (old["finished_at"] - old["submitted_at"]).total_seconds()
            new_sec = (new.finished_at - old["submitted_at"]).total_seconds()
            if old_sec <= new_sec:
                continue
            overstated += 1
            deltas.append(old_sec - new_sec)
            if old_sec - new_sec > worst[0]:
                worst = (old_sec - new_sec, f"{old['rule']} in {err.name}: {old_sec:,.0f}s -> {new_sec:,.0f}s")

    print(f"{len(logs)} sbatch .err logs across {', '.join(RUNS)}\n")
    print(f"failed attempts carrying a duration: {total}")
    print(f"  overstated by the pre-fix parse:   {overstated}")
    print(f"  median overstatement:              {statistics.median(deltas) / 3600:.1f}h")
    print(f"  smallest / largest:                {min(deltas):,.0f}s / {max(deltas):,.0f}s")
    print(f"  worst attempt:                     {worst[1]}")


if __name__ == "__main__":
    main()
