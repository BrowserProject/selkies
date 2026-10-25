#!/bin/sh
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
#
# The package versions of a release tag, as GitHub Actions outputs:
#
#   bp/version.sh bp-7a1219d-1   python=0.0.0+bp.g7a1219d.1  npm=0.0.0-bp.g7a1219d.1
#   bp/version.sh ""             python=0.0.0.dev0           npm=0.0.0-dev
#
# The g keeps an all-digit short sha from reading as a number.
set -eu
tag="${1:-}"
if [ -z "$tag" ]; then
  echo "python=0.0.0.dev0"
  echo "npm=0.0.0-dev"
  exit 0
fi
if ! printf '%s' "$tag" | grep -Eq '^bp-[0-9a-f]{7}-[1-9][0-9]*$'; then
  echo "not a release tag (bp-<upstream short sha>-<n>): $tag" >&2
  exit 1
fi
sha=$(printf '%s' "$tag" | cut -d- -f2)
n=$(printf '%s' "$tag" | cut -d- -f3)
echo "python=0.0.0+bp.g${sha}.${n}"
echo "npm=0.0.0-bp.g${sha}.${n}"
