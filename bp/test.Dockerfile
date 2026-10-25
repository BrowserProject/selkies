# syntax=docker/dockerfile:1
# The fork's test environment: the Debian release, Python and native libraries
# of the BrowserProject browser images, with the pinned capture wheels. The
# fork itself is installed by bp/run-tests.sh, never baked in.
FROM debian:trixie-slim@sha256:62bd423aeee912639ca8ee681c2b762cb3e25137e0a92b20be455898a9f034ad

ARG PIXELFLUX_RELEASE
ARG PCMFLUX_RELEASE
ARG FLUX_WHEEL_TAG
ARG PIXELFLUX_WHEEL_SHA256
ARG PCMFLUX_WHEEL_SHA256
ARG PIP_VERSION

RUN apt-get update && \
  DEBIAN_FRONTEND=noninteractive apt-get install --no-install-recommends -y \
    ca-certificates \
    libgl1 \
    libopus0 \
    libpulse0 \
    libva-drm2 \
    libva-x11-2 \
    libva2 \
    libx11-6 \
    libxcb1 \
    libxext6 \
    libxfixes3 \
    libxkbcommon0 \
    libxtst6 \
    pulseaudio \
    python3 \
    python3-venv \
    x11-xserver-utils \
    xcvt \
    xdotool \
    xkb-data \
    xvfb && \
  rm -rf /var/lib/apt/lists/* && \
  python3 -m venv /opt/venv && \
  /opt/venv/bin/pip install --no-cache-dir "pip==${PIP_VERSION}" && \
  /opt/venv/bin/pip install --no-cache-dir \
    "https://github.com/selkies-project/pixelflux/releases/download/${PIXELFLUX_RELEASE}/pixelflux-2.1.0-${FLUX_WHEEL_TAG}.whl#sha256=${PIXELFLUX_WHEEL_SHA256}" \
    "https://github.com/selkies-project/pcmflux/releases/download/${PCMFLUX_RELEASE}/pcmflux-2.1.0-${FLUX_WHEEL_TAG}.whl#sha256=${PCMFLUX_WHEEL_SHA256}"

ENV PATH=/opt/venv/bin:$PATH
