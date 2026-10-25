#!/bin/sh
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
#
# Inside the test environment: install the wheel and the product modules,
# start a display with a moving picture and Selkies in WebRTC mode (which also
# serves the WebSocket sink), with the per-connection scopes enforced. Logs go
# to /out.
set -eu
pip install --no-cache-dir --quiet /wheel/*.whl
cp -r /ext /tmp/ext && pip install --no-cache-dir --quiet /tmp/ext
Xvfb :99 -screen 0 1280x720x24 +extension DAMAGE +extension MIT-SHM -nolisten tcp >/out/xvfb.log 2>&1 &
sleep 1
pulseaudio --daemonize=yes --exit-idle-time=-1 --load="module-null-sink sink_name=output" >/out/pulse.log 2>&1 || true
python3 /check/scene.py >/out/scene.log 2>&1 &
exec selkies --addr=0.0.0.0 --port=3000 --mode=webrtc --enable-basic-auth=false \
  --congestion-control=true --debug=true >/out/server.log 2>&1
