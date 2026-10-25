#!/bin/sh
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
#
# The headless stream check (check.mjs) and the Unix socket check
# (unix-socket.sh) against a wheel with the product modules beside it, in the
# test environment image built from bp/test.Dockerfile. Needs Docker, Node and
# Chrome (CHROME_BIN, default /usr/bin/google-chrome) on the host and
# `npm ci --prefix bp/stream-check` first.
#
#   bp/stream-check/run.sh <test image> <wheel> <product module package dir>
set -eu
image="$1"
wheel=$(cd "$(dirname "$2")" && pwd)/$(basename "$2")
here=$(cd "$(dirname "$0")" && pwd)
out=$(mktemp -d /tmp/bp-stream-check.XXXXXX)
chmod 0777 "$out"
name="bp-stream-check-$$"
cleanup() { docker rm -f "$name" >/dev/null 2>&1 || true; }
trap cleanup EXIT
mkdir "$out/wheel" && cp "$wheel" "$out/wheel/"
# Dereferenced: the product module package may be symlinks into its repository.
cp -rL "$3" "$out/ext"

docker run --rm -v "$out/wheel:/wheel:ro" -v "$out/ext:/ext:ro" -v "$here:/check:ro" -v "$out:/out" \
  "$image" /check/unix-socket.sh

transfer_key=$(od -An -tx1 -N32 /dev/urandom | tr -d ' \n')
docker run -d --name "$name" --shm-size 512m -p 127.0.0.1::3000 \
  -e SELKIES_CONNECTION_SCOPES=true -e SELKIES_TRANSFER_KEY="$transfer_key" -e SELKIES_ALLOWED_ORIGINS=http://localhost:53920 \
  -e BROWSER_STREAM_TIER=enhanced -e DISPLAY=:99 \
  -v "$out/wheel:/wheel:ro" -v "$out/ext:/ext:ro" -v "$here:/check:ro" -v "$out:/out" \
  "$image" /check/serve.sh >/dev/null
port=$(docker port "$name" 3000/tcp | head -n 1 | sed 's/.*://')
for _ in $(seq 1 120); do
  if curl -fsS "http://127.0.0.1:$port/api/status" >/dev/null 2>&1; then break; fi
  sleep 1
done
echo "Stream check evidence: $out"
if ! PORT="$port" SERVER_LOG="$out/server.log" TRANSFER_KEY="$transfer_key" node "$here/check.mjs"; then
  tail -n 40 "$out/server.log"
  exit 1
fi
