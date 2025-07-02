#!/bin/bash
#
# This script can be used to configure and run snakemake.
#
# It accepts up to 9 additional arguments, which are passed
# to the snakemake invocation, e.g.
#   bash scripts/babel-build.sh --keep-incomplete anatomy_uberon_ids
#

# Number of cores to use.
export CORES=5

# Dry run: if true, run Snakemake in a dry run.
export DRY_RUN=1

# Verbose: if set, produce verbose output.
export VERBOSE=

# Keep going: if set, then keep going if one job errors out.
export KEEP_GOING=1

# Rerun incomplete: if set, then rerun any jobs that are incomplete.
export RERUN_INCOMPLETE=1

# Keep incomplete: if set, keep any incomplete files.
export KEEP_INCOMPLETE=

# Until: run until a particular rule or file.
export UNTIL=

# Latency wait: number of seconds to wait for a job to produce output files.
# (default: 5)
export LATENCY_WAIT=100

# Determine the location of this script, so we can change to the right directory before running this script.
SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

cd "$SCRIPT_DIR/.." || exit
snakemake \
  --cores ${CORES} \
  $([[ $DRY_RUN ]] && echo '--dry-run') \
  $([[ $VERBOSE ]] && echo '--verbose') \
  $([[ $KEEP_GOING ]] && echo '--keep-going') \
  $([[ $RERUN_INCOMPLETE ]] && echo '--rerun-incomplete') \
  $([[ $KEEP_INCOMPLETE ]] && echo '--keep-incomplete') \
  $([[ $UNTIL ]] && echo "--until ${UNTIL}") \
  --latency-wait $LATENCY_WAIT \
  $1 $2 $3 $4 $5 $6 $7 $8 $9
