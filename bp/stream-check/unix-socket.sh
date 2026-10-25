#!/bin/sh
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
#
# Inside the test environment: Selkies listening on upstream's
# SELKIES_UNIX_SOCKET serves its HTTP routes over the socket. The wheel has no
# web bundle, so GET / is a 404 from Selkies; GET /api/status must be 200.
set -eu
pip install --no-cache-dir --quiet /wheel/*.whl
cp -r /ext /tmp/ext && pip install --no-cache-dir --quiet /tmp/ext
Xvfb :99 -screen 0 1280x720x24 -nolisten tcp >/dev/null 2>&1 &
sleep 1
mkdir -p /run/bp/selkies
SELKIES_UNIX_SOCKET=/run/bp/selkies/selkies.sock selkies --mode=websockets \
  --enable-basic-auth=false >/out/unix-server.log 2>&1 &
cd /tmp && python3 - <<'PY'
import asyncio, json, os, sys, time
import aiohttp

async def main():
    deadline = time.monotonic() + 60
    while not os.path.exists("/run/bp/selkies/selkies.sock"):
        if time.monotonic() > deadline:
            sys.exit("no socket after 60 s")
        await asyncio.sleep(0.5)
    async with aiohttp.ClientSession(connector=aiohttp.UnixConnector(path="/run/bp/selkies/selkies.sock")) as s:
        for _ in range(60):
            try:
                async with s.get("http://selkies/") as root, s.get("http://selkies/api/status") as status:
                    body = await status.json()
                    print(f"UNIX GET / {root.status}, GET /api/status {status.status} {json.dumps(body)[:160]}")
                    sys.exit(0 if status.status == 200 and root.status < 500 else 1)
            except aiohttp.ClientError:
                await asyncio.sleep(0.5)
    sys.exit("no answer over the socket")

asyncio.run(main())
PY
