#!/bin/bash
#
# This script copies babel-private from a remote location
# into input_data/private/
#

export USERNAME='username'
export HOSTNAME='hostname'
export BABEL_PRIVATE="${USERNAME}@${HOSTNAME}:~/babel-private/"
export SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

rsync -avp $BABEL_PRIVATE $SCRIPT_DIR/../input_data/private
