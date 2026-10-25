#!/bin/sh
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
#
# Install a fork wheel (and the product modules beside it) into the test
# environment of bp/test.Dockerfile and run test suites against it.
#
#   bp/run-tests.sh [--ext <package dir>] <wheel> <test dir>...
#
# Each test dir holds a tests.list: one test file per line, optionally followed
# by VAR=value assignments for its run. Every test runs as its own process,
# like the base image build ran them, with BROWSER_STREAM_TIER empty unless
# its line sets it.
set -eu

ext=""
if [ "${1:-}" = "--ext" ]; then
  ext="$2"
  shift 2
fi
wheel="$1"
shift
[ "$#" -gt 0 ] || { echo "usage: $0 [--ext <dir>] <wheel> <test dir>..." >&2; exit 2; }

. "$(dirname "$0")/flux.env"
pip install --no-cache-dir --quiet "$wheel" "uvloop==${UVLOOP_VERSION}"
if [ -n "$ext" ]; then
  pip install --no-cache-dir --quiet "$ext"
fi
pip check
python3 -c 'from pixelflux import CaptureSettings, ScreenCapture, SOFTWARE_H264_ENCODER'
python3 -c 'from pcmflux import AudioCapture, AudioCaptureSettings, AudioPlayback, AudioPlaybackSettings'
python3 -c 'from selkies.ice.ice import ice_interface_allowlist'

failed=0
for dir in "$@"; do
  dir=$(cd "$dir" && pwd)
  while read -r test vars; do
    case "$test" in ''|'#'*) continue ;; esac
    echo "==> $dir/$test"
    # shellcheck disable=SC2086
    if ! (cd /tmp && env BROWSER_STREAM_TIER= $vars python3 "$dir/$test"); then
      echo "FAILED: $dir/$test"
      failed=$((failed + 1))
    fi
  done < "$dir/tests.list"
done
[ "$failed" -eq 0 ] || { echo "$failed test file(s) failed"; exit 1; }
echo "all test files passed"
