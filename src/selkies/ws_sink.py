# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
"""WebSocket sink: a second output of the WebRTC service, in the websockets
wire format.

A viewer whose WebRTC attempt fails (UDP blocked, TURN unreachable, relay lost
mid-session) opens ``GET /api/websockets`` on the very same service instead of
asking the container to change transport. There is no mode switch, no second
capture and no second encoder: the sink is fed by the pipeline that already
serves the RTC peers, and it speaks the byte-for-byte protocol of the
websockets service, so the unmodified client (``selkies-ws-core.js``) cannot
tell the difference.

What rides on the sink: video, audio, cursor, input (keyboard, mouse, touch),
resize, DPI, clipboard (text and binary), settings, keyframe requests. What
does not: gamepad (``js,``), microphone PCM (``0x02``) and webcam frames are
dropped with one log line per connection, uploads run over HTTP anyway, and a
second screen stays WebRTC-only.

Threading and backpressure: the pipeline hands raw stripe frames in on the
capture thread and they reach the loop through ``call_soon_threadsafe``,
exactly as the websockets service does it. Nothing in the frame path ever
awaits a socket: each client owns a ``_VideoRelay`` (the websockets service's
own class, decoupled onto the narrow host protocol implemented here by
``_RelayHost``) which drops its backlog past a byte budget and skips ahead to
the next keyframe. Audio rides one bounded queue shared by every client.

Rate control: each client reports a smoothed goodput, a loss signal raised by
its relay's drops and its backlog, into the service's ``_congestion_control_loop``
through :meth:`WsSink.merge_congestion`, so a slow WS client lowers the shared
encoder's bitrate instead of merely losing frames. With no WS client connected
the loop is byte-for-byte what it was.

The message filter chain (`_gz` handshake, viewer allow-list, the secure-mode
input gate, the gamepad gate, the start/stop verbs) is the one the data
channel uses: it lives here as the free function
:func:`dispatch_client_message`, which ``rtc.py`` calls too, so the two
transports can never drift apart on authority.
"""

import asyncio
import base64
import gzip
import json
import logging
import time
from collections import OrderedDict, deque
from datetime import datetime
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from aiohttp import WSMsgType, web

from . import connection_scopes
from .input_handler import (
    VIEWER_ALLOWED_PREFIXES,
    VIEWER_COLLAB_EXTRA_PREFIXES,
    VIEWER_SILENT_DROP_PREFIXES,
    gamepad_slot_denied,
)
from .selkies import (
    MAX_UINT16_FRAME_ID,
    RTT_SAMPLE_SANE_MAX_MS,
    RTT_SMOOTHING_SAMPLES,
    SHARED_STREAM_SEND_TIMEOUT_SECONDS,
    VIDEO_RELAY_BUDGET_MIN_BYTES,
    VIDEO_RELAY_BUDGET_SECONDS,
    WS_GZIP_MIN_BYTES,
    _VideoRelay,
    _lookup_session_token,
    _mk_access_verdict,
    _perms_hold_input_authority,
    current_session_tokens,
)
from .settings import (
    WS_MAX_MESSAGE_BYTES,
    inflate_gz_bounded,
    settings as app_settings,
)
from .stream_server import note_pong
from .webcam import WS_OPCODE_WEBCAM

logger = logging.getLogger("ws_sink")

# pcmflux's native audio header. The WebRTC pipeline captures with
# `omit_audio_header` (RTP repacketizes), so the sink restores the two bytes the
# websockets client demuxes on: [0x01, n_red], n_red 0 being the plain layout.
AUDIO_HEADER_PLAIN = b"\x01\x00"

# Client mic PCM; never forwarded on the sink.
WS_OPCODE_MIC = 0x02
# gzip-wrapped control text, both directions.
WS_OPCODE_GZIP = 0x05

# Page session id (`vp-...`), capped as signaling_server caps it.
SESSION_ID_MAX_CHARS = 128
# The shortest session id that may bind a handover without a token: the page
# draws 122 random bits or more, so a shorter id is not one of ours.
SESSION_ID_MIN_CHARS = 32

# Reconnect debounce, the websockets service's own (`DataStreamingServer`).
RECONNECT_DEBOUNCE_MS = 500
MAX_RECENT_CLIENTS = 1000

# One keyframe request per client per three seconds, the relay's own sync floor: a
# WS client's recovery must not let it drive keyframe bursts for the RTC peers. At
# one second one lagging sink client rebuilt every peer's frame once a second.
IDR_REQUEST_FLOOR_S = 3.0

# Goodput smoothing (EWMA over one-second congestion ticks) and the loss value
# a relay drop or a standing backlog reports for its tick. 0.15 sits above the
# congestion loop's 0.10 backoff threshold.
GOODPUT_SMOOTHING = 0.6
RELAY_DROP_LOSS = 0.15

# How long one send to a sink client may stall before the client is given up
# on (video relay and audio alike). The websockets service allows a second,
# which is the right answer where a stalled client can only be dropped; a
# cancelled send leaves a half-written frame, so the socket can never be kept.
#
# Here the stall is the very signal that lowers the shared encoder, and the
# congestion loop needs about four seconds to bring it down. Meanwhile the
# client is draining a backlog of up to VIDEO_RELAY_BUDGET_SECONDS of stream at
# the OLD bitrate plus a full kernel buffer, which at a quarter of that rate is
# several seconds more. A second, or five, cuts exactly the client the rate
# control was about to rescue: it is dropped, reconnects, and the loop starts
# its recovery ramp from the top again. Fifteen covers the whole rescue; a
# socket that is genuinely dead is caught by the 30 s heartbeat, not by this.
# Memory stays bounded by the relay's byte budget throughout.
SINK_SEND_STALL_S = 15.0

# Stats cadence. The service's monitors tick once a second; the websockets
# service pushed its stats every 5 s off a 2 s collector, and 2 s keeps the
# client's bandwidth reading fresh enough to be read as one.
STATS_PUSH_INTERVAL_S = 2.0

# Smallest backlog growth over one congestion tick that counts as congestion.
# One frame in flight is normal and its size varies from tick to tick; a step
# above this (or above a hundredth of the relay budget, whichever is larger) is
# a queue that is actually building.
BACKLOG_GROWTH_MIN_BYTES = 64 * 1024

# What a client's own acks say about its path. The socket takes a frame long
# before the viewer has it: the kernel, the Go proxy and nginx buffer megabytes
# between them, so the write-side goodput and the relay backlog only see a queue
# once seconds of stream stand in it. The ack round trip sees it at once: its
# excess over the path's base round trip is queue delay, which the congestion
# loop takes like a peer's TWCC queue delay, and a frame still unacknowledged
# SINK_QUEUE_BUDGET_S past the base stops the client's video until the queue has
# drained (it resumes on a keyframe). The base is the least round trip over
# SINK_RTT_FLOOR_WINDOW_S: long enough that a standing queue never becomes the
# base, short enough to follow a new route. A hold lasts until nothing is
# outstanding, never longer than SINK_HOLD_MAX_S: a client that stops acking
# frames it received must not freeze for good.
SINK_QUEUE_BUDGET_S = 0.3
SINK_RTT_FLOOR_WINDOW_S = 120.0
SINK_HOLD_MAX_S = 2.0

# Bounded audio fan-out, sized like the websockets service's queue.
AUDIO_QUEUE_MAX = 120

# A frame that finds the client's previous one still in flight is dropped, but
# only a send outstanding for longer than this is congestion. A loop stall of a
# few frames queues them behind a send that has not even started, and reading
# that as loss held the shared encoder at its floor on a healthy link.
AUDIO_STALL_LOSS_S = 0.1

# Drain deadline for the shutdown lifecycle (patch 0005 parity).
SHUTDOWN_DRAIN_S = 5.0

# Clipboard chunking; websockets frame budget, kept a multiple of 3 so no
# chunk's base64 carries padding (the viewer concatenates the text).
CLIPBOARD_CHUNK_SIZE = 256 * 1024 // 3 * 3


def _session_id(raw: Optional[str]) -> Optional[str]:
    """One page session id off the wire, capped like the signaling server caps
    the HELLO metadata field, so the two sides can never disagree on what they
    stored."""
    return str(raw)[:SESSION_ID_MAX_CHARS] if raw else None


def viewer_is_collaborator(client_token: Optional[str]) -> bool:
    """A viewer holding the active mk (mouse+keyboard) token is a read-write
    collaborator, but only while enable_collab is on. Fail-safe: any missing
    piece means read-only. Shared by both transports."""
    if not client_token:
        return False
    if not bool(app_settings.enable_collab[0]):
        return False
    _tokens, mk = current_session_tokens()
    return mk is not None and client_token == mk


def mk_input_authorized(client_token: Optional[str]) -> bool:
    """Token-level input authority in secure mode: the active mk-token holder,
    or a controller-role token while no mk token is provisioned."""
    tokens, mk = current_session_tokens()
    if mk is not None:
        return bool(client_token) and client_token == mk
    perms = tokens.get(client_token) if client_token else None
    return bool(perms) and perms.get("role") == "controller"


def secure_input_denied(msg: str, client_token: Optional[str],
                        log: logging.Logger = logger) -> bool:
    """Secure-mode (master token configured) input authority: cmd and the
    keyboard/mouse/clipboard set are admitted only from the active mk-token
    holder, or from a controller-role token when no mk token is provisioned.
    The bare `cr` clipboard read-back is exempt (the handler direction-gates
    it itself, and it is sent at connect).

    Returns:
        True when the message must be dropped.
    """
    if not app_settings.master_token:
        return False
    if msg.split(",", 1)[0] in ("cr",):
        return False
    if (msg.split(",", 1)[0] not in ("cmd", "co")
            and not msg.startswith(VIEWER_COLLAB_EXTRA_PREFIXES)):
        return False
    authorized = mk_input_authorized(client_token)
    if not authorized:
        log.warning("Dropping unauthorized secure-mode input: %s", msg[:32])
    return not authorized


def gamepad_denied(msg: str, role: str, client_token: Optional[str],
                   client_slot: Optional[int]) -> bool:
    """Gamepad slot authority: a client may only drive the slot it holds, so a
    viewer or collaborator cannot spoof another player's controller. Under a
    master token the slot comes from the live token store (a revocation lands
    on the next message), otherwise from the connection's validated claim.

    Returns:
        True when the gamepad message must be dropped.
    """
    if not msg.startswith("js,"):
        return False
    if app_settings.master_token:
        tokens, _mk = current_session_tokens()
        perms = tokens.get(client_token) if client_token else None
        slot = perms.get("slot") if perms else None
    elif role == "viewer":
        slot = client_slot if (client_slot or 0) > 0 else None
    else:
        slot = None
    return gamepad_slot_denied(msg, role, slot, bool(app_settings.master_token))


def dispatch_client_message(
    msg: Any,
    *,
    is_viewer: bool,
    is_collaborator: Callable[[], bool],
    display_id: str,
    conn_id: Any,
    on_gz_handshake: Callable[[], None],
    on_audio_consumer_active: Optional[Callable[[Any, bool], Any]],
    on_video_consumer_active: Optional[Callable[[Any, str, bool], Any]],
    on_data_message: Callable[..., Any],
    input_denied: Callable[[str], bool],
    js_denied: Callable[[str], bool],
    scopes: connection_scopes.Scopes = None,
    log: logging.Logger = logger,
) -> Any:
    """Filter one client message and hand it to the input dispatcher.

    The chain both transports share, in order: a gzip'd payload is inflated
    with a bound (the transport's own size limit caps only the compressed
    size); a message needing a scope the connection lacks is dropped
    (`connection_scopes`); the `_gz,1` handshake marks the connection for gzip'd sends and is
    echoed; STOP_AUDIO/START_AUDIO pause this connection alone and are taken
    ahead of the viewer gate (audio is per consumer on both transports now, so
    a hidden viewer may mute itself); a viewer's SETTINGS snapshot is
    connection sync only and never applied; a viewer may send only the
    allow-listed messages, and a read-write collaborator (mk token plus
    enable_collab) additionally the keyboard/mouse/clipboard set, with
    blur/visibility noise dropped silently; the secure-mode input and gamepad
    gates apply; STOP_VIDEO/START_VIDEO pause this connection alone; and
    everything else reaches `on_data_message(msg, display_id, conn_id)`.

    Returns:
        Whatever the dispatched handler returns (possibly an awaitable), or
        None when the message was consumed or dropped here.
    """
    if isinstance(msg, (bytes, bytearray)) and bytes(msg[:2]) == b"\x1f\x8b":
        try:
            msg = inflate_gz_bounded(msg)
        except Exception:
            log.warning("Dropping undecodable compressed client message")
            return None
    if connection_scopes.message_denied(msg, scopes):
        return None
    if msg == "_gz,1":
        on_gz_handshake()
        return None
    if msg in ("STOP_AUDIO", "START_AUDIO"):
        if on_audio_consumer_active is not None:
            return on_audio_consumer_active(conn_id, msg == "START_AUDIO")
        return None
    if is_viewer and isinstance(msg, str) and msg.startswith("SETTINGS,"):
        log.debug("Ignoring SETTINGS payload from a viewer (display '%s')", display_id)
        return None
    if is_viewer and isinstance(msg, str):
        if not msg.startswith(VIEWER_ALLOWED_PREFIXES) and not (
            msg.startswith(VIEWER_COLLAB_EXTRA_PREFIXES) and is_collaborator()
        ):
            if not msg.startswith(VIEWER_SILENT_DROP_PREFIXES):
                log.warning("Dropping unauthorized viewer input: %s", msg[:32])
            return None
    if isinstance(msg, str) and input_denied(msg):
        return None
    if isinstance(msg, str) and js_denied(msg):
        log.warning("Dropping gamepad input for a slot this client does not hold: %s",
                    msg[:32])
        return None
    if msg in ("STOP_VIDEO", "START_VIDEO") and on_video_consumer_active is not None:
        return on_video_consumer_active(conn_id, display_id or "primary",
                                        msg == "START_VIDEO")
    return on_data_message(msg, display_id or "primary", conn_id=conn_id)


class _RelayHost:
    """The narrow host `_VideoRelay` needs, for one sink client.

    `_VideoRelay` was written against `DataStreamingServer`; it reads exactly
    four things off its host, and this object supplies them per client: the
    display's ack/RTT state it stamps sends into (`display_clients`), the live
    socket registry it discards a dead socket from (`clients`), the relay group
    it removes itself from when its task ends (`video_relay_groups`), and the
    sent-byte counter (`_bytes_sent_in_interval`) the goodput estimator reads.
    """

    __slots__ = ("display_clients", "clients", "video_relay_groups",
                 "_bytes_sent_in_interval")

    def __init__(self, ws: Any, display_id: str) -> None:
        self.display_clients: Dict[str, Dict[str, Any]] = {
            display_id: {
                "ws": ws,
                "acknowledged_frame_id": -1,
                "acked_sent_at": None,
                "last_sent_frame_id": 0,
                "has_sent_any_frame": False,
                "unacked_since": None,
                "sent_timestamps": OrderedDict(),
                "rtt_samples": deque(maxlen=RTT_SMOOTHING_SAMPLES),
                "smoothed_rtt": 0.0,
            }
        }
        self.clients = {ws}
        self.video_relay_groups: Dict[str, Dict[Any, Any]] = {display_id: {}}
        self._bytes_sent_in_interval = 0


class WsClient:
    """One connected sink client.

    Attributes:
        conn_id: Transport connection identity, handed to the input handler so
            per-connection state (clipboard transfer and debounce) traces back
            here.
        role: "controller" or "viewer", transport-independent.
        token: Session token in secure mode, else None.
        slot: Legacy-mode player slot claim (2..4), else None.
        display_id: The display this client renders; always "primary" (a
            second screen stays WebRTC-only).
        session_peer_id: The viewer page's own session id (`vp-...`), from
            `?session_peer_id=`; its WebRTC attempts present the same value in
            the signaling HELLO metadata. While a peer carrying that id is
            connected, this client shares ITS consumer entry rather than adding
            a second one.
        consumer: The consumer entry `_display_consumers` publishes, with the
            same `video_paused`/`audio_paused` fields a peer entry carries.
        last_ack: Last frame id this client acknowledged.
        tick_rtt: The least ack round trip of the current congestion tick.
        held_since: When the client's video was held for a full queue, else None.
        resyncing: Held video resumes on a keyframe; the deltas its relay drops
            until then are the hold's doing, not congestion.
        scopes: What the relay granted this connection (`connection_scopes`).
    """

    __slots__ = ("conn_id", "ws", "role", "token", "slot", "remote_address",
                 "audio_inflight", "audio_inflight_since",
                 "display_id", "session_peer_id", "consumer", "relay", "host",
                 "last_ack", "gz", "closing", "clip_lock", "_next_idr",
                 "goodput_bps", "loss", "_last_bytes", "_last_tick",
                 "_last_backlog", "tick_rtt", "_rtt_floor", "held_since", "resyncing",
                 "mic_notified", "cam_notified", "js_notified", "scopes")

    def __init__(self, conn_id: str, ws: web.WebSocketResponse, role: str,
                 token: Optional[str], slot: Optional[int],
                 remote_address: Tuple[Any, Any], session_peer_id: Optional[str],
                 display_id: str = "primary",
                 scopes: connection_scopes.Scopes = None) -> None:
        self.conn_id = conn_id
        self.scopes = scopes
        self.ws = ws
        self.role = role
        self.token = token
        self.slot = slot
        self.remote_address = remote_address
        self.display_id = display_id
        self.session_peer_id = session_peer_id
        self.consumer: Dict[str, Any] = {
            "display_id": display_id,
            "client_type": role,
            "client_token": token,
            "video_paused": False,
            "audio_paused": False,
            "ws_sink_client": self,
        }
        self.relay: Optional[_VideoRelay] = None
        self.host = _RelayHost(ws, display_id)
        self.last_ack = -1
        self.gz = False
        self.closing = False
        self.clip_lock = asyncio.Lock()
        self._next_idr = 0.0
        self.goodput_bps = 0.0
        self.loss = 0.0
        self._last_bytes = 0
        self._last_tick = time.monotonic()
        self._last_backlog = None
        self.tick_rtt: Optional[float] = None
        # Round trips that can still be the windowed least: increasing
        # (time, seconds) pairs, the head being the base.
        self._rtt_floor: deque = deque()
        self.held_since: Optional[float] = None
        self.resyncing = False
        self.mic_notified = False
        self.cam_notified = False
        self.js_notified = False
        # One audio frame in flight per client at a time; see _audio_fanout.
        self.audio_inflight = False
        self.audio_inflight_since = 0.0

    @property
    def video_paused(self) -> bool:
        return bool(self.consumer.get("video_paused"))

    @property
    def audio_paused(self) -> bool:
        return bool(self.consumer.get("audio_paused"))

    @property
    def display_state(self) -> Dict[str, Any]:
        """This client's ack/RTT state, the entry the relay stamps sends into."""
        return self.host.display_clients[self.display_id]

    @property
    def base_rtt(self) -> Optional[float]:
        """The path's round trip without a queue: the least ack round trip in
        SINK_RTT_FLOOR_WINDOW_S, None before the first ack."""
        return self._rtt_floor[0][1] if self._rtt_floor else None

    def note_rtt(self, rtt_s: float, now: float) -> None:
        """One ack round trip, for this tick's least and the windowed base."""
        self.tick_rtt = rtt_s if self.tick_rtt is None else min(self.tick_rtt, rtt_s)
        floor = self._rtt_floor
        while floor and floor[-1][1] >= rtt_s:
            floor.pop()
        floor.append((now, rtt_s))
        while floor[0][0] < now - SINK_RTT_FLOOR_WINDOW_S:
            floor.popleft()

    def inflight_age(self, now: float) -> float:
        """How long the oldest frame this client has not acknowledged has been
        on its way, 0 with nothing outstanding. `_note_ack` keeps only
        unacknowledged frames in `sent_timestamps`, oldest first."""
        sent = self.display_state["sent_timestamps"]
        return now - next(iter(sent.values())) if sent else 0.0

    def queue_full(self, now: float) -> bool:
        """The oldest unacknowledged frame is SINK_QUEUE_BUDGET_S past the
        path's base round trip: the buffers between here and the viewer hold
        that much stream, and anything sent now would reach it that much late.
        Never before the first ack, whose round trip is the first base."""
        base = self.base_rtt
        return base is not None and self.inflight_age(now) - base > SINK_QUEUE_BUDGET_S

    def hold_video(self, now: float) -> bool:
        """Whether this client's video is held: from a full queue until every
        frame on its way was acknowledged (or SINK_HOLD_MAX_S passed), so the
        hold does not flap at the budget's edge and drop the keyframe the
        client resumes on."""
        if self.held_since is None:
            if not self.queue_full(now):
                return False
            self.held_since = now
            self.resyncing = True
            return True
        if self.display_state["sent_timestamps"] and now - self.held_since < SINK_HOLD_MAX_S:
            return True
        self.held_since = None
        self.display_state["sent_timestamps"].clear()
        return False

    def forget_inflight(self) -> None:
        """Video paused or resumed: what was on its way is no longer awaited,
        and the video resumes on a keyframe."""
        self.display_state["sent_timestamps"].clear()
        self.tick_rtt = None
        self.held_since = None
        self.resyncing = True

    def want_idr(self) -> bool:
        """Rate-limit this client's keyframe requests to one per three seconds: an
        unthrottled recovery loop would drive keyframe bursts for the RTC peers
        that share the encoder."""
        now = time.monotonic()
        if now < self._next_idr:
            return False
        self._next_idr = now + IDR_REQUEST_FLOOR_S
        return True

    def take_window(self) -> Optional[Dict[str, float]]:
        """One congestion tick's verdict for this client, or None before a
        first full tick.

        Goodput is what the relay actually put on the socket, smoothed; loss is
        raised for a tick in which the relay dropped a backlog or is still
        holding one past half its budget; queue delay is the tick's least ack
        round trip over the base, or, without an ack in the tick, how long the
        oldest outstanding frame has been on its way. All three are what the
        shared loop already understands, so no new policy enters the loop with
        the sink.
        """
        now = time.monotonic()
        elapsed = now - self._last_tick
        if elapsed < 0.25:
            return None
        sent = self.host._bytes_sent_in_interval - self._last_bytes
        self._last_bytes = self.host._bytes_sent_in_interval
        self._last_tick = now
        bps = (sent * 8.0) / elapsed
        smoothed = (GOODPUT_SMOOTHING * self.goodput_bps
                    + (1.0 - GOODPUT_SMOOTHING) * bps) if self.goodput_bps else bps
        relay = self.relay
        if relay is not None and relay.backlog_bytes > 0 and self.goodput_bps > 0:
            # Still behind: the bytes that left in this tick are a backlog
            # draining, not room on the link. Letting that raise the estimate
            # made the loop climb again the moment the queue moved, so the
            # bitrate sawed between the ceiling and a few hundred kbps instead
            # of settling. While a backlog stands the estimate may only fall.
            smoothed = min(self.goodput_bps, smoothed)
        self.goodput_bps = smoothed
        loss = self.loss
        self.loss = 0.0
        if relay is not None:
            backlog = relay.backlog_bytes
            # A socket has no packet loss to report: a backlog that GREW over
            # this tick is the same news, and it arrives seconds before the
            # budget is reached. Without it the encoder only reacts once half a
            # budget (megabytes) has piled up, which is far too late for the
            # client whose link just got slower.
            # Hysteresis: a single frame in flight makes the backlog wobble by
            # its own size from tick to tick, and every upward wobble would
            # otherwise read as congestion and hold the encoder down on a
            # healthy link. Only growth past a real step counts, and only on a
            # backlog that already stood past that step at the last tick: one
            # keyframe still leaving at the tick boundary is gone by the next.
            step = max(BACKLOG_GROWTH_MIN_BYTES, relay.budget // 100)
            if (self._last_backlog is not None and self._last_backlog > step
                    and backlog - self._last_backlog > step):
                loss = max(loss, RELAY_DROP_LOSS)
            if backlog > relay.budget // 2:
                loss = max(loss, RELAY_DROP_LOSS)
            self._last_backlog = backlog
        else:
            self._last_backlog = None
        queue_delay = 0.0
        base = self.base_rtt
        if base is not None:
            lag = self.tick_rtt if self.tick_rtt is not None else self.inflight_age(now)
            queue_delay = max(0.0, lag - base)
        self.tick_rtt = None
        if self.video_paused:
            self.goodput_bps = 0
            self._last_backlog = None
            queue_delay = 0.0
        return {"goodput_bps": self.goodput_bps, "loss_fraction": loss,
                "queue_delay_s": queue_delay}


class WsSink:
    """The websockets-format output of the WebRTC service.

    Owns its clients, the `/api/websockets` handler, the per-client video
    relays, the shared audio queue, and the goodput the congestion loop reads.
    Everything it drives on the service is what a peer drives: the same
    consumer registration, the same capture start and grace stop, the same
    input handler and the same IDR path.
    """

    def __init__(self, service: Any) -> None:
        self.service = service
        self.clients: Dict[str, WsClient] = {}
        self._by_ws: Dict[Any, WsClient] = {}
        self._conn_seq = 0
        self._accepting = True
        self._handlers: set = set()
        self._last_connection_times: "OrderedDict[Tuple[str, str], float]" = OrderedDict()
        self._audio_queue: Optional[asyncio.Queue] = None
        self._audio_task: Optional[asyncio.Task] = None
        self._last_cursor: Optional[dict] = None
        # Last push per stats kind, so the once-a-second monitor ticks reach
        # the clients at the cadence the websockets service used.
        self._stats_sent_at: Dict[str, float] = {}
        # Captured on the loop: the capture threads hand their frames over with
        # call_soon_threadsafe and have no running loop of their own.
        self._loop: Optional[asyncio.AbstractEventLoop] = None

    # ---------------------------------------------------------------- lifecycle

    def start(self) -> None:
        """Accept sockets again after a stop (the service was restarted)."""
        self._accepting = True

    async def stop(self) -> None:
        """Patch 0005's shutdown lifecycle, from the first commit: no new
        socket is accepted from the moment a stop begins, the connected ones
        are closed, and their handlers are drained with a deadline before the
        pipeline teardown that follows can run."""
        self._accepting = False
        closing = list(self.clients.values())
        for client in closing:
            client.closing = True
        if closing:
            # Concurrently: one wedged socket must not spend its two seconds
            # before the next one is even asked to close.
            await asyncio.gather(*(
                asyncio.wait_for(
                    c.ws.close(code=1001, message=b"server shutting down"),
                    timeout=2.0)
                for c in closing), return_exceptions=True)
        handlers = [t for t in self._handlers if not t.done()]
        if handlers:
            done, pending = await asyncio.wait(handlers, timeout=SHUTDOWN_DRAIN_S)
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
        self._detach_media_hooks()
        if self._audio_task is not None:
            self._audio_task.cancel()
            try:
                await self._audio_task
            except (asyncio.CancelledError, Exception):
                pass
            self._audio_task = None
        self._audio_queue = None
        self.clients.clear()
        self._by_ws.clear()

    def client_count(self) -> int:
        """Connected sink clients, for `/api/status`."""
        return len(self.clients)

    # --------------------------------------------------------------- consumers

    def display_consumers(self, display_id: str) -> List[Dict[str, Any]]:
        """This display's sink clients as consumer entries.

        A client whose `session_peer_id` is also carried by a connected WebRTC
        peer is left out: that is ONE viewer mid-handover from the sink to a
        peer connection, the peer entry already speaks for it, so the
        capture-gating rules see one consumer throughout and no `superseded`
        close is ever needed. Both transports learn the id from the page
        itself: `?session_peer_id=` on this socket, the `session_peer_id` field
        of the signaling HELLO metadata on the peer.
        """
        live_peers = self._live_signaling_peers()
        out = []
        for client in self.clients.values():
            if client.display_id != display_id:
                continue
            if self._handed_over_to(client, live_peers) is not None:
                continue
            out.append(client.consumer)
        return out

    def _live_signaling_peers(self) -> List[Any]:
        """The signaling peers that hold a LIVE RTC connection.

        The signaling peer carries the page's session id; the RTC entry is what
        consumes the display, so a page still negotiating keeps streaming over
        the sink and counts as its own consumer.
        """
        return [peer for _, peer in self._live_signaling_peer_items()]

    def _live_signaling_peer_items(self) -> List[Tuple[Any, Any]]:
        """The same peers as `_live_signaling_peers`, with their peer ids: an
        eviction has to name the peer it closes."""
        rtc_app = getattr(self.service, "rtc_app", None)
        connections = getattr(rtc_app, "peer_connections", None) or {}
        if not connections:
            return []
        manager = getattr(self.service, "peer_manager", None)
        peers = getattr(manager, "peers", None) or {}
        return [(uid, peer) for uid, peer in list(peers.items()) if uid in connections]

    @staticmethod
    def _peer_is_controller(peer: Any) -> bool:
        """A signaling peer's role, spelled as the handshake spells it."""
        return getattr(peer, "client_type", None) != "viewer"

    @staticmethod
    def _peer_display(peer: Any) -> str:
        return getattr(peer, "display_id", "primary") or "primary"

    def _rival_controller_peer(self, client: WsClient,
                               items: Optional[List[Tuple[Any, Any]]] = None):
        """The controller peer that holds this client's display for ANOTHER
        page, or None.

        The page's own peer is not a rival: during a transport handover both
        wires belong to one session, which `_handed_over_to` proves by the
        credential rather than by the session id alone.
        """
        if client.role != "controller" or client.closing:
            return None
        if items is None:
            items = self._live_signaling_peer_items()
        if not items:
            return None
        own = self._handed_over_to(client, [peer for _, peer in items])
        for peer_id, peer in items:
            if peer is own:
                continue
            if not self._peer_is_controller(peer):
                continue
            if self._peer_display(peer) != client.display_id:
                continue
            return (peer_id, peer)
        return None

    def supersede_by_peer_controllers(self) -> None:
        """A controller peer took a display: close the sink controller on it.

        One controller per display, whatever the transport (the input authority
        rule is transport independent). Without this a second tab that reaches
        WebRTC while the first runs on the sink leaves TWO live controllers
        typing into one desktop, where two tabs on one transport have always
        superseded each other.

        The close is 1000 with the websockets service's supersede reason,
        "Superseded by new client", and deliberately WITHOUT its `KILL` line
        (see `_supersede`). The client matches on that REASON and draws its
        scrim over the frozen picture with the offer to take the session back;
        close code 4001 is its invalid-token verdict and would show a token
        error instead.
        """
        if not self.clients:
            return
        items = self._live_signaling_peer_items()
        if not items:
            return
        for client in list(self.clients.values()):
            rival = self._rival_controller_peer(client, items)
            if rival is None:
                continue
            logger.info("Sink client %s superseded by controller peer %s.",
                        client.conn_id, rival[0])
            self._schedule(self._supersede(client))

    async def _supersede(self, client: WsClient) -> None:
        """Close 1000 with the reason the client matches on.

        Deliberately WITHOUT the websockets service's `KILL` line: the client
        handles KILL by clearing its own close handler and closing the socket
        itself, which skips the supersede branch that freezes the picture and
        raises the takeover prompt, and leaves a bare "Connection Terminated"
        instead. The close reason alone gets the behaviour the plan asks for.
        """
        if client.closing:
            return
        client.closing = True
        try:
            await asyncio.wait_for(
                client.ws.close(code=1000, message=b"Superseded by new client"),
                timeout=2.0)
        except Exception:
            pass

    async def evict_peer_controllers(self, client: WsClient) -> None:
        """A new sink controller takes the display from a controller peer.

        The other direction of the same rule: the signaling server evicts a
        controller peer when a second controller peer arrives, but it never
        sees a sink client. The newest page wins, exactly as between two tabs
        on one transport, and the evicted peer gets the 4001 it already
        handles.
        """
        rival = self._rival_controller_peer(client)
        if rival is None:
            return
        peer_id, _peer = rival
        logger.info("Sink client %s takes the session from controller peer %s.",
                    client.conn_id, peer_id)
        closer = getattr(self.service, "_close_peer_signaling_ws", None)
        if callable(closer):
            try:
                await closer(peer_id, 4001, b"superseded")
            except Exception:
                logger.warning("Superseding peer %s failed", peer_id, exc_info=True)
        rtc_app = getattr(self.service, "rtc_app", None)
        stop = getattr(rtc_app, "stop_rtc_connection", None)
        if callable(stop):
            try:
                await stop(peer_id, "controller")
            except Exception:
                logger.warning("stop_rtc_connection failed for %s", peer_id, exc_info=True)

    def _handed_over_to(self, client: WsClient,
                        live_peers: Optional[List[Any]] = None) -> Optional[Any]:
        """The peer this sink client handed its consumer entry to, or None.

        The session id alone may NOT decide this. It is a value the page sends,
        so a second page that learns or guesses another session's id would
        otherwise erase that session's consumer entry and could stop its
        capture. In secure mode the peer must therefore present the identical
        non-empty session token. In legacy mode there is no credential to
        compare, so the id must be one only that page can know (at least
        SESSION_ID_MIN_CHARS of its random draw, sent over TLS and never echoed
        to another client) and the peer must claim the same role. The source
        address proves nothing here: behind the proxy mesh it is the forwarding
        node's, and the two wires of one page often reach the container through
        different nodes (measured on production: every such handover refused,
        the page's own peer then superseding its own socket).

        Returns:
            The matching peer, or None when this client is still its own
            consumer.
        """
        if not client.session_peer_id:
            return None
        if live_peers is None:
            live_peers = self._live_signaling_peers()
        for peer in live_peers:
            if getattr(peer, "session_peer_id", None) != client.session_peer_id:
                continue
            if app_settings.master_token:
                peer_token = getattr(peer, "client_token", None)
                if not peer_token or not client.token or peer_token != client.token:
                    logger.warning(
                        "Refusing a session handover for %s: the peer presents a "
                        "different token.", client.conn_id)
                    continue
            else:
                peer_role = "viewer" if getattr(peer, "client_type", None) == "viewer" \
                    else "controller"
                if len(client.session_peer_id) < SESSION_ID_MIN_CHARS:
                    logger.warning(
                        "Refusing a session handover for %s: the session id is too "
                        "short to bind it.", client.conn_id)
                    continue
                if peer_role != client.role:
                    logger.warning(
                        "Refusing a session handover for %s: the peer claims role "
                        "%s.", client.conn_id, peer_role)
                    continue
            return peer
        return None

    async def reconcile(self) -> None:
        """Apply a token-table update to the live sink clients.

        The websockets service reconciles its own sockets (`reconcile_clients`)
        and the WebRTC service its peers (`reconcile_webrtc_peers`); neither
        walks the sink, so without this a revoked share link would keep
        receiving media until its holder closed the tab. Same verdicts as both:
        a revoked token or a changed role closes the socket with 4002, a
        slot-only change is pushed as ROLE_UPDATE (the mapping lives client
        side and would silently desync), and every surviving client is re-told
        its MK_ACCESS verdict, with the cursor resent to a new holder so its
        freshly attached input context has one.

        Token-less (legacy-mode) clients are governed by their URL role alone
        and are left as they are, exactly as the other two do.
        """
        if not app_settings.master_token:
            return
        for client in list(self.clients.values()):
            if not client.token or client.closing:
                continue
            perms = _lookup_session_token(client.token)
            reason = None
            if perms is None:
                reason = "Token revoked"
            elif (perms.get("role") or "controller") != client.role:
                reason = "Permissions changed significantly"
            if reason is not None:
                logger.info("Disconnecting sink client %s: %s", client.conn_id, reason)
                client.closing = True
                try:
                    await asyncio.wait_for(
                        client.ws.close(code=4002, message=reason.encode()), timeout=2.0)
                except Exception:
                    pass
                continue
            if perms.get("slot") != client.slot:
                logger.info("Sink client %s slot change: %s -> %s", client.conn_id,
                            client.slot, perms.get("slot"))
                client.slot = perms.get("slot")
                await self._send_text(client, "ROLE_UPDATE," + json.dumps(
                    {"role": client.role, "slot": client.slot}))
            granted = _mk_access_verdict(perms, token=client.token)
            await self._send_text(client, "MK_ACCESS,1" if granted else "MK_ACCESS,0")
            if granted:
                cursor = self._current_cursor()
                if cursor:
                    await self._send_text(client, f"cursor,{json.dumps(cursor)}")

    def merge_congestion(self, per_display: Dict[str, Dict[str, Any]]) -> None:
        """Fold the sink clients' goodput, loss and queue delay into the
        congestion loop's per-display buckets, the same shape a peer's TWCC
        window produces. The loop's own rule (follow the slowest consumer, back
        off on loss or queue delay) then covers WS clients with no further
        change; with no client connected nothing is added and the loop is what
        it was."""
        for client in self.clients.values():
            try:
                window = client.take_window()
            except Exception:
                logger.exception("Sink feedback failed (display %s)", client.display_id)
                continue
            if window is None or client.video_paused or client.closing:
                continue
            bucket = per_display.setdefault(
                client.display_id, {"goodputs": [], "worst_loss": 0.0, "worst_delay": 0.0})
            if window["goodput_bps"]:
                bucket["goodputs"].append(window["goodput_bps"])
            bucket["worst_loss"] = max(bucket["worst_loss"], window["loss_fraction"])
            bucket["worst_delay"] = max(bucket.get("worst_delay", 0.0), window["queue_delay_s"])

    # ------------------------------------------------------------- media input

    def _pipeline(self, display_id: str = "primary") -> Optional[Any]:
        pipelines = getattr(self.service, "display_pipelines", None) or {}
        return pipelines.get(display_id) or getattr(self.service, "media_pipeline", None)

    def _sync_media_hooks(self) -> None:
        """Attach the capture hooks only while a client is connected: with no
        sink client the pipeline callbacks are exactly upstream's, so a
        peer-only session pays nothing at all for the sink."""
        pipeline = self._pipeline("primary")
        if pipeline is None:
            return
        try:
            self._loop = asyncio.get_running_loop()
        except RuntimeError:
            pass
        if self.clients:
            pipeline.on_ws_frame = self._capture_video
            pipeline.on_ws_audio = self._capture_audio
            if self._audio_queue is None:
                self._audio_queue = asyncio.Queue(maxsize=AUDIO_QUEUE_MAX)
            if self._audio_task is None or self._audio_task.done():
                self._audio_task = asyncio.create_task(
                    self._audio_fanout(), name="WsSinkAudio")
        else:
            self._detach_media_hooks()

    def _detach_media_hooks(self) -> None:
        pipeline = self._pipeline("primary")
        if pipeline is not None:
            pipeline.on_ws_frame = None
            pipeline.on_ws_audio = None

    def _capture_video(self, frame: Any) -> None:
        """pixelflux frame callback, on the capture thread. The frame owns its
        native buffer and carries its wire header already, so it IS the
        websockets wire format; it is handed to the loop by reference (which
        keeps it alive) and never copied."""
        loop = self._loop
        if loop is None or loop.is_closed():
            return
        try:
            loop.call_soon_threadsafe(self._offer_video, frame)
        except RuntimeError:
            pass

    def _offer_video(self, frame: Any) -> None:
        """Offer one stripe to every client's relay. Runs on the loop and never
        awaits: a relay that has to drop asks for a keyframe instead."""
        try:
            size = len(frame)
        except Exception:
            return
        if not size or size > WS_MAX_MESSAGE_BYTES:
            if size > WS_MAX_MESSAGE_BYTES:
                logger.error("Refusing to relay a %d-byte video chunk to the sink.", size)
            return
        item = {"data": memoryview(frame), "owner": frame,
                "frame_id": getattr(frame, "frame_id", 0) & 0xFFFF}
        need_idr = False
        now = time.monotonic()
        for client in list(self.clients.values()):
            if client.closing or client.video_paused:
                continue
            relay = client.relay
            if relay is None:
                relay = self._make_relay(client)
            if client.hold_video(now):
                # Its relay resumes on the next keyframe, which it asks for
                # itself once frames are offered again.
                relay.flush_for_gate()
                continue
            if relay.offer(item):
                if not client.resyncing:
                    client.loss = RELAY_DROP_LOSS
                if client.want_idr():
                    need_idr = True
            if client.resyncing and relay.live_rows:
                client.resyncing = False
            if client.ws not in client.host.clients:
                # The relay dropped a socket it could not drain.
                self._schedule(self._drop_client(client, "relay send failed"))
        if need_idr:
            self._schedule(self.service.request_idr_for_display("primary"))

    def _make_relay(self, client: WsClient) -> _VideoRelay:
        """One bounded relay per client: VIDEO_RELAY_BUDGET_SECONDS of stream at
        the display's configured bitrate, floored, exactly as the websockets
        service sizes it."""
        pipeline = self._pipeline(client.display_id)
        kbps = int(getattr(pipeline, "video_bitrate", 0) or 0)
        budget = max(VIDEO_RELAY_BUDGET_MIN_BYTES,
                     int(kbps * 125 * VIDEO_RELAY_BUDGET_SECONDS))
        relay = _VideoRelay(client.host, client.display_id, client.ws, budget,
                            send_timeout=SINK_SEND_STALL_S)
        client.host.video_relay_groups[client.display_id][client.ws] = relay
        client.relay = relay
        relay.start()
        return relay

    def _capture_audio(self, frame: Any) -> None:
        """pcmflux frame callback, on the capture thread: one bounded queue for
        every client, drop-oldest-free (a full queue drops the newest chunk,
        which RED would have covered had a peer-free session enabled it)."""
        queue = self._audio_queue
        loop = self._loop
        if queue is None or loop is None or loop.is_closed():
            return
        try:
            if not len(frame):
                return
        except Exception:
            return
        item = {"data": memoryview(frame), "owner": frame}

        def _put() -> None:
            q = self._audio_queue
            if q is None:
                return
            try:
                q.put_nowait(item)
            except asyncio.QueueFull:
                pass

        try:
            loop.call_soon_threadsafe(_put)
        except RuntimeError:
            pass

    async def _audio_fanout(self) -> None:
        """Broadcast queued Opus frames to every client that receives audio.

        The pipeline captures without pcmflux's header (RTP repacketizes), so
        the two header bytes the websockets client demuxes on are restored
        here. RED stays off: the encoder is shared with the RTC peers, which
        never advertise the websockets redundancy capability, and the
        all-consumers rule therefore resolves to plain frames.
        """
        try:
            while True:
                item = await self._audio_queue.get()
                data = item["data"]
                payload = AUDIO_HEADER_PLAIN + bytes(data)
                for client in list(self.clients.values()):
                    if client.closing or client.audio_paused:
                        continue
                    if client.audio_inflight:
                        # This client still owes the network the PREVIOUS frame.
                        # Waiting for it here (even concurrently, even once)
                        # would let the slowest socket pace the shared loop and
                        # overflow the queue for everyone else, so its frame is
                        # dropped instead and counted as loss: audio is
                        # real-time, a late frame is worth nothing, and the
                        # congestion loop reads the loss and lowers the shared
                        # bitrate that is drowning the client.
                        if time.monotonic() - client.audio_inflight_since > AUDIO_STALL_LOSS_S:
                            client.loss = max(client.loss, RELAY_DROP_LOSS)
                        continue
                    client.audio_inflight = True
                    client.audio_inflight_since = time.monotonic()
                    self._schedule(self._deliver_audio(client, payload))
                self._audio_queue.task_done()
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Sink audio fan-out stopped")

    async def _deliver_audio(self, client: WsClient, payload: bytes) -> None:
        """One Opus frame to one client, on its own task.

        The sink's stall deadline applies per client (the same one the video
        relay uses): a socket backed up behind the video it has not drained yet
        is what the rate control is about to rescue, and the websockets
        service's one second would drop exactly that client.
        """
        try:
            await asyncio.wait_for(client.ws.send_bytes(payload),
                                   timeout=SINK_SEND_STALL_S)
        except (asyncio.TimeoutError, ConnectionResetError, OSError, RuntimeError):
            self._schedule(self._drop_client(client, "audio send failed"))
        finally:
            client.audio_inflight = False

    def push_cursor(self, data: dict) -> None:
        """Push a cursor change in the websockets format. The payload comes
        from `format_pixelflux_cursor`, which is already that format."""
        self._last_cursor = data
        if not self.clients:
            return
        self._schedule(self._broadcast(f"cursor,{json.dumps(data)}"))

    def push_system(self, action: str, conn_id: Any = None) -> bool:
        """Push a `system,<json>` action, the websockets spelling of the data
        channel's `system` message (command_done, command_error, encoder,
        videoFramerate, apps_installed, reload, ...).

        Args:
            action: The action string, as the data channel carries it.
            conn_id: The connection this action answers, or None for a
                session-wide one.

        Returns:
            True when `conn_id` named a sink client, so the RTC side knows the
            requester was served here and must not broadcast the answer.
        """
        message = "system," + json.dumps({"action": action})
        if conn_id is not None:
            client = self.clients.get(conn_id)
            if client is None:
                return False
            self._schedule(self._send_text(client, message))
            return True
        if self.clients:
            self._schedule(self._broadcast(message))
        return False

    def push_system_stats(self, cpu_percent: float, mem_total: int,
                          mem_used: int) -> None:
        """Push the websockets `system_stats` message.

        The peers get these numbers over the data channel, in a shape the
        websockets client does not read. Without this its stats panel stays
        empty on the sink, which the websockets service always filled.
        """
        self._push_stats("system", {
            "type": "system_stats",
            "timestamp": datetime.now().isoformat(),
            "cpu_percent": cpu_percent,
            "mem_total": mem_total,
            "mem_used": mem_used,
        })

    def push_gpu_stats(self, load: float, memory_total: int,
                       memory_used: int, gpu_id: int = 0) -> None:
        """Push the websockets `gpu_stats` message (load is a 0..1 fraction and
        the memory figures are MiB, as the monitor reports them).

        `gpu_id` names the GPU the numbers describe, exactly as the websockets
        collector publishes it; a client on a multi-GPU host reads it to label
        the figures rather than assuming the first device.
        """
        self._push_stats("gpu", {
            "type": "gpu_stats",
            "timestamp": datetime.now().isoformat(),
            "gpu_id": int(gpu_id),
            "load": load,
            "gpu_percent": load * 100,
            "memory_total": memory_total * 1024 * 1024,
            "memory_used": memory_used * 1024 * 1024,
        })

    def push_network_stats(self) -> None:
        """Push each client its own `network_stats`.

        The websockets service derived one instance-wide bandwidth from the
        bytes it had sent; the sink already keeps that per client (the smoothed
        goodput the congestion loop reads) and the frame-ack RTT beside it, so
        every client is told about ITS OWN socket rather than about the sum.
        """
        if not self.clients or not self._stats_due("network"):
            return
        now = datetime.now().isoformat()
        for client in list(self.clients.values()):
            state = client.host.display_clients.get(client.display_id) or {}
            message = json.dumps({
                "type": "network_stats",
                "timestamp": now,
                "bandwidth_mbps": round(client.goodput_bps / 1_000_000.0, 2),
                "latency_ms": round(float(state.get("smoothed_rtt") or 0.0), 1),
            })
            self._schedule(self._send_text(client, message, drop_on_timeout=False))

    def _stats_due(self, key: str) -> bool:
        """One shared throttle per stats kind: the monitors tick every second,
        the client needs them every couple of seconds."""
        now = time.monotonic()
        if now - self._stats_sent_at.get(key, 0.0) < STATS_PUSH_INTERVAL_S:
            return False
        self._stats_sent_at[key] = now
        return True

    def _push_stats(self, key: str, payload: Dict[str, Any]) -> None:
        """One periodic stats message to every client, disposable: a client that
        cannot take it in time skips this tick instead of losing its session."""
        if not self.clients or not self._stats_due(key):
            return
        message = json.dumps(payload)
        for client in list(self.clients.values()):
            self._schedule(self._send_text(client, message, drop_on_timeout=False))

    def push_stream_resolution(self, width: int, height: int,
                               display_id: str = "primary") -> None:
        """Announce a display's realized resolution in the websockets format.

        The data channel carries this as a `system` action `resolution,WxH`,
        which the websockets client does not know; it expects the JSON
        `stream_resolution` message, tagged with its display so another
        display's size never rescales this page's canvas and input mapping.
        """
        if width <= 0 or height <= 0 or not self.clients:
            return
        message = json.dumps({"type": "stream_resolution", "width": int(width),
                              "height": int(height), "displayId": display_id})
        for client in list(self.clients.values()):
            if client.display_id == display_id:
                self._schedule(self._send_text(client, message))

    def _primary_dims(self) -> Optional[Tuple[int, int]]:
        """The primary's realized dimensions, or None before it has any (the
        client then sizes itself from its first frames).

        Three sources, most realized first: the service's own
        `_primary_dims` (the size the layout path settled on, which is what
        the peers are told), then the primary's layout rectangle, then the
        pipeline's configured capture region. They agree once a resize has
        gone through; before that the pipeline still carries the region it
        was started with, which would size a joining page wrongly.
        """
        for width, height in (self._service_primary_dims(),
                              self._layout_primary_dims(),
                              self._pipeline_primary_dims()):
            if width > 0 and height > 0:
                return (width, height)
        return None

    def _service_primary_dims(self) -> Tuple[int, int]:
        dims = getattr(self.service, "_primary_dims", None)
        if isinstance(dims, (tuple, list)) and len(dims) == 2:
            try:
                return int(dims[0] or 0), int(dims[1] or 0)
            except (TypeError, ValueError):
                return 0, 0
        return 0, 0

    def _layout_primary_dims(self) -> Tuple[int, int]:
        layouts = getattr(self.service, "display_layouts", None) or {}
        layout = layouts.get("primary") if isinstance(layouts, dict) else None
        if isinstance(layout, dict):
            try:
                return int(layout.get("w") or 0), int(layout.get("h") or 0)
            except (TypeError, ValueError):
                return 0, 0
        return 0, 0

    def _pipeline_primary_dims(self) -> Tuple[int, int]:
        pipeline = self._pipeline("primary")
        try:
            return (int(getattr(pipeline, "width", 0) or 0),
                    int(getattr(pipeline, "height", 0) or 0))
        except (TypeError, ValueError):
            return 0, 0

    async def push_clipboard(self, data: Union[str, bytes], mime_type: str,
                             reply_to: Optional[str], conn_id: Any) -> bool:
        """Deliver clipboard content in the websockets format.

        Returns:
            True when the payload belonged to a sink client alone (an answer to
            its own fetch), so the RTC fan-out can skip it; False for a
            session-wide announcement, which both transports carry.
        """
        addressed = self.clients.get(conn_id) if conn_id is not None else None
        if conn_id is not None and addressed is None:
            return False
        if addressed is not None:
            # The requester waits for its own answer.
            await self._send_clipboard(addressed, data, mime_type, reply_to)
            return True
        for client in list(self.clients.values()):
            # An announcement: the peers must not queue behind a slow sink
            # client's multipart transfer, so each one runs on its own.
            self._schedule(self._send_clipboard(client, data, mime_type, reply_to))
        return False

    async def _send_clipboard(self, client: WsClient, data: Union[str, bytes],
                              mime_type: str, reply_to: Optional[str]) -> None:
        """One payload at a time per client: the start/data/finish frames carry
        no transfer id, so two concurrent sends would interleave into one
        assembly. A client without the clipboard scope is sent nothing."""
        if not connection_scopes.allows(client.scopes, connection_scopes.CLIPBOARD):
            return
        is_binary = mime_type != "text/plain"
        payload = data.encode("utf-8") if isinstance(data, str) else data
        async with client.clip_lock:
            if reply_to:
                await self._send_text(client, f"clipboard_reply,{reply_to}")
            if len(payload) < CLIPBOARD_CHUNK_SIZE:
                encoded = base64.b64encode(payload).decode("ascii")
                await self._send_text(
                    client,
                    f"clipboard_binary,{mime_type},{encoded}" if is_binary
                    else f"clipboard,{encoded}")
                return
            await self._send_text(client, f"clipboard_start,{mime_type},{len(payload)}")
            offset = 0
            while offset < len(payload):
                chunk = payload[offset:offset + CLIPBOARD_CHUNK_SIZE]
                await self._send_text(
                    client, "clipboard_data," + base64.b64encode(chunk).decode("ascii"))
                offset += len(chunk)
                await asyncio.sleep(0)
            await self._send_text(client, "clipboard_finish")

    # ------------------------------------------------------------------- sends

    async def _send_text(self, client: WsClient, message: str,
                         drop_on_timeout: bool = True) -> bool:
        """Send one control message, gzip-wrapped when the client asked for it
        and the message is worth compressing (small, latency-critical messages
        stay raw). Never raises: a dead socket is dropped.

        The deadline is the sink's own (SINK_SEND_STALL_S), not the websockets
        service's second: a client whose socket is backed up behind a video
        frame is the one the rate control is bringing down, and a second would
        drop it mid-rescue.

        Args:
            drop_on_timeout: False for the periodic stats pushes. Those are
                disposable, and the next tick carries fresher numbers anyway,
                so a slow client skips one rather than losing its session over
                it. Errors still drop, as they do for every send.
        """
        if client.closing or client.ws.closed:
            return False
        try:
            if client.gz and len(message) >= WS_GZIP_MIN_BYTES:
                await asyncio.wait_for(
                    client.ws.send_bytes(bytes([WS_OPCODE_GZIP])
                                         + gzip.compress(message.encode("utf-8"), 6)),
                    timeout=SINK_SEND_STALL_S)
            else:
                await asyncio.wait_for(client.ws.send_str(message),
                                       timeout=SINK_SEND_STALL_S)
            return True
        except asyncio.TimeoutError:
            if drop_on_timeout:
                self._schedule(self._drop_client(client, "control send failed"))
            else:
                logger.info("Sink client %s did not take a stats push in %.0fs; "
                            "skipping it.", client.conn_id, SINK_SEND_STALL_S)
            return False
        except (ConnectionResetError, OSError, RuntimeError):
            self._schedule(self._drop_client(client, "control send failed"))
            return False

    async def _broadcast(self, message: str) -> None:
        await asyncio.gather(*(self._send_text(c, message)
                               for c in list(self.clients.values())),
                             return_exceptions=True)

    async def broadcast_server_settings(self) -> None:
        """Re-announce the server settings to every sink client (a settings
        change re-keys the client's wire-format demux)."""
        for client in list(self.clients.values()):
            await self._send_text(client, json.dumps(self._server_settings(client)))

    def _server_settings(self, client: WsClient) -> Dict[str, Any]:
        """The `server_settings` payload for one client.

        The WebRTC service runs H.264 only, so a client that asked for `jpeg`
        is told the encoder its display is actually captured with and re-keys
        its demux off that (the encoder list has always come from the server).
        `ws_max_message_bytes` is transport capacity, so the client sizes its
        multipart chunks to a whole frame.
        """
        payload = dict(self.service._server_settings_payload())
        entries = dict(payload.get("settings") or {})
        encoder = None
        getter = getattr(self.service, "_encoder_for_display", None)
        if callable(getter):
            try:
                encoder = getter(client.display_id)
            except Exception:
                encoder = None
        if encoder and isinstance(entries.get("encoder"), dict):
            entries["encoder"] = dict(entries["encoder"], value=encoder)
        entries["ws_max_message_bytes"] = {"value": WS_MAX_MESSAGE_BYTES}
        payload["settings"] = entries
        payload["type"] = "server_settings"
        return payload

    def _schedule(self, coro: Any) -> None:
        """Fire-and-forget on the loop, with a strong reference held: the loop
        keeps only weak ones."""
        if coro is None:
            return
        if not asyncio.iscoroutine(coro):
            return
        task = asyncio.ensure_future(coro)
        self._handlers.add(task)
        task.add_done_callback(self._handlers.discard)

    # ----------------------------------------------------------------- routing

    async def handle_request(self, request: web.Request) -> web.StreamResponse:
        """`GET /api/websockets` while the WebRTC service is the active mode.

        Refuses the way the websockets service refuses an inactive mode, so a
        client that reaches the wrong container state sees the same 409 it
        always did.
        """
        supervisor = getattr(self.service, "supervisor", None)
        if supervisor is not None and supervisor.current_mode != self.service.mode:
            return web.Response(status=409, text="WebSocket mode is inactive")
        if not self._accepting:
            return web.Response(status=409, text="WebSocket sink is shutting down")

        token = ""
        if app_settings.master_token:
            token = request.query.get("token") or ""
            if not token:
                return web.Response(status=401, text="Token missing in secure mode")

        # compress=False: the frames are already H.264/Opus. autoping=False: the
        # loop answers PING itself and feeds PONG to the uplink gauge.
        ws = web.WebSocketResponse(compress=False, max_msg_size=WS_MAX_MESSAGE_BYTES,
                                   heartbeat=30, autoping=False)
        handler_task = asyncio.current_task()
        if handler_task is not None:
            self._handlers.add(handler_task)
        try:
            await ws.prepare(request)
            if not self._accepting:
                await ws.close(code=4000, message=b"server shutting down")
                return ws
            peername = request.transport.get_extra_info("peername")
            remote_address = peername[:2] if peername else (request.remote, 0)
            query_role = request.query.get("role", "")
            query_slot = request.query.get("slot")
            if request.get("auth_role_ceiling") == "viewer":
                query_role = "viewer"
                query_slot = None
            await self.run_client(ws, remote_address, token,
                                  query_role=query_role, query_slot=query_slot,
                                  session_peer_id=_session_id(
                                      request.query.get("session_peer_id")),
                                  scopes=request.get(connection_scopes.REQUEST_KEY))
        finally:
            if handler_task is not None:
                self._handlers.discard(handler_task)
        return ws

    # ------------------------------------------------------------ connection

    async def run_client(self, ws: web.WebSocketResponse, remote_address: Tuple[Any, Any],
                         token: str = "", query_role: str = "",
                         query_slot: Optional[str] = None,
                         session_peer_id: Optional[str] = None,
                         scopes: connection_scopes.Scopes = None) -> None:
        """Run one sink connection from handshake to cleanup.

        The handshake is the websockets service's, in its order: AUTH_SUCCESS
        in secure mode, `MODE websockets`, the MK_ACCESS verdict, the display
        roster, the current cursor, and the server settings. Then the message
        loop, and a teardown that releases held input only when the input
        authority leaves with this socket.
        """
        role, slot, refusal, perms = self._authorize(token, query_role, query_slot)
        if refusal is not None:
            code, reason, kill = refusal
            if kill:
                try:
                    await ws.send_str(f"KILL {reason}")
                except (ConnectionResetError, OSError, RuntimeError):
                    pass
            await ws.close(code=code, message=reason.encode())
            return
        if app_settings.master_token:
            try:
                await ws.send_str("AUTH_SUCCESS," + json.dumps({"role": role, "slot": slot}))
            except (ConnectionResetError, OSError, RuntimeError):
                return

        # Every viewer of a proxied deployment shares the proxy's address, so
        # the address alone would rate-limit unrelated sessions against each
        # other. The identity the request already carries narrows it: the
        # session token, else the page session id.
        ip_address = remote_address[0] if remote_address else ""
        debounce_key = (ip_address, token or session_peer_id or "")
        last = self._last_connection_times.get(debounce_key)
        now = time.monotonic()
        if last is not None and (now - last) * 1000 < RECONNECT_DEBOUNCE_MS:
            logger.warning("Sink client %s reconnecting too quickly; rejecting.",
                           debounce_key)
            await ws.close(code=4029, message=b"Rate limited: reconnecting too quickly")
            return
        self._last_connection_times[debounce_key] = now
        if len(self._last_connection_times) > MAX_RECENT_CLIENTS:
            self._last_connection_times.popitem(last=False)

        self._conn_seq += 1
        client = WsClient(f"wssink:{self._conn_seq}", ws, role, token or None, slot,
                          remote_address, session_peer_id, scopes=scopes)
        logger.info("Sink client %s connected from %s (role %s).",
                    client.conn_id, remote_address, role)
        try:
            await self._handshake(client)
        except (ConnectionResetError, OSError, RuntimeError):
            return

        self.clients[client.conn_id] = client
        self._by_ws[ws] = client
        self._sync_media_hooks()
        supervisor = getattr(self.service, "supervisor", None)
        if supervisor is not None:
            supervisor.set_clients_present(True)
        try:
            await self.service.start_display_media(client.display_id)
            self._sync_media_hooks()
            await self.evict_peer_controllers(client)
            await self.service.handle_consumers_changed(client.display_id)
            await self.service.request_idr_for_display(client.display_id)
            await self._message_loop(client)
        finally:
            await self._teardown(client)

    def _authorize(self, token: str, query_role: str, query_slot: Optional[str]):
        """Auth exactly as the websockets service does it: the token governs in
        secure mode, otherwise the query role/slot, held to the same
        shared/player enablement the signaling server enforces.

        Returns:
            `(role, slot, refusal, perms)`; `refusal` is `(close code, reason,
            send KILL)` or None.
        """
        if app_settings.master_token:
            perms = _lookup_session_token(token)
            if perms is None:
                return "viewer", None, (4001, "Invalid authentication token", False), None
            return (perms.get("role") or "controller"), perms.get("slot"), None, perms
        role = "viewer" if query_role == "viewer" else "controller"
        slot = None
        if query_slot is not None:
            try:
                slot_num = int(query_slot)
                if 2 <= slot_num <= 4:
                    slot = slot_num
            except (ValueError, TypeError):
                pass
        if role == "viewer" and slot is None and not app_settings.enable_shared[0]:
            return role, slot, (1008, "Strict shared clients are not enabled.", True), None
        if slot is not None and not getattr(app_settings, f"enable_player{slot}", (True,))[0]:
            return role, slot, (1008, f"Player slot {slot} is not enabled.", True), None
        return role, slot, None, {"role": role, "slot": slot, "token": None}

    async def _handshake(self, client: WsClient) -> None:
        """The websockets handshake pushes, in the order the client expects."""
        await client.ws.send_str("MODE websockets")
        if app_settings.master_token:
            granted = _mk_access_verdict({"role": client.role, "token": client.token},
                                         token=client.token)
            await client.ws.send_str("MK_ACCESS,1" if granted else "MK_ACCESS,0")
        roster = self.service._display_config_payload()
        await client.ws.send_str(f"DISPLAY_CONFIG_UPDATE,{json.dumps(roster)}")
        cursor = self._current_cursor()
        if cursor:
            await client.ws.send_str(f"cursor,{json.dumps(cursor)}")
        await client.ws.send_str(json.dumps(self._server_settings(client)))
        dims = self._primary_dims()
        if dims is not None:
            # The websockets handshake ends with the display's realized size;
            # without it a joining page keeps its placeholder geometry until
            # the next resize.
            await client.ws.send_str(json.dumps({
                "type": "stream_resolution", "width": dims[0], "height": dims[1],
                "displayId": client.display_id}))

    def _current_cursor(self) -> Optional[dict]:
        """The cursor a joining client must be told about: the live one when
        the input handler can answer, else the last one pushed on either
        transport."""
        handler = getattr(self.service, "input_handler", None)
        if handler is not None:
            try:
                data = handler.get_current_cursor_data()
                if data:
                    return data
            except Exception:
                pass
        rtc_app = getattr(self.service, "rtc_app", None)
        return getattr(rtc_app, "last_cursor_sent", None) or self._last_cursor

    async def _message_loop(self, client: WsClient) -> None:
        """Read one client's messages until it goes away."""
        ws = client.ws
        async for msg in ws:
            if msg.type == WSMsgType.PING:
                await ws.pong(msg.data)
                continue
            if msg.type == WSMsgType.PONG:
                note_pong(ws, msg.data)
                continue
            if msg.type == WSMsgType.BINARY:
                if not msg.data:
                    continue
                opcode = msg.data[0]
                if opcode == WS_OPCODE_GZIP:
                    try:
                        text = inflate_gz_bounded(msg.data[1:])
                    except Exception:
                        logger.warning("Dropping undecodable client gzip frame.")
                        continue
                    await self._handle_text(client, text)
                    continue
                if opcode == WS_OPCODE_MIC:
                    if not client.mic_notified:
                        client.mic_notified = True
                        logger.info("Sink client %s: microphone is WebRTC-only; "
                                    "dropping its PCM.", client.conn_id)
                        await self._send_text(client, "MICROPHONE_DISABLED")
                    continue
                if opcode == WS_OPCODE_WEBCAM:
                    if not client.cam_notified:
                        client.cam_notified = True
                        logger.info("Sink client %s: webcam is WebRTC-only; dropping "
                                    "its frames.", client.conn_id)
                        await self._send_text(client, "WEBCAM_DISABLED")
                    continue
                continue
            if msg.type == WSMsgType.TEXT:
                await self._handle_text(client, msg.data)
                continue
            if msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSING, WSMsgType.CLOSED,
                            WSMsgType.ERROR):
                break

    async def _handle_text(self, client: WsClient, message: str) -> None:
        """One text message: the per-client verbs first, then the shared filter
        chain both transports run."""
        if message.startswith("CLIENT_FRAME_ACK"):
            self._note_ack(client, message)
            return
        if message.startswith("js,"):
            if not client.js_notified:
                client.js_notified = True
                logger.info("Sink client %s: gamepad is WebRTC-only; dropping its "
                            "input.", client.conn_id)
            return
        result = dispatch_client_message(
            message,
            is_viewer=client.role == "viewer",
            is_collaborator=lambda: viewer_is_collaborator(client.token),
            display_id=client.display_id,
            conn_id=client.conn_id,
            on_gz_handshake=lambda: self._gz_handshake(client),
            on_audio_consumer_active=lambda _cid, active: self._audio_active(client, active),
            on_video_consumer_active=lambda _cid, _did, active: self._video_active(
                client, active),
            on_data_message=self._on_data_message,
            input_denied=lambda m: secure_input_denied(m, client.token, logger),
            js_denied=lambda m: gamepad_denied(m, client.role, client.token, client.slot),
            scopes=client.scopes,
            log=logger,
        )
        if asyncio.iscoroutine(result):
            await result

    def _on_data_message(self, msg: str, display_id: str, conn_id: Any = None) -> Any:
        handler = getattr(self.service, "input_handler", None)
        if handler is None:
            return None
        return handler.on_message(msg, display_id, conn_id=conn_id)

    def _gz_handshake(self, client: WsClient) -> None:
        """Mark this connection gzip-capable and echo the verb, so the client
        compresses its own large sends too."""
        client.gz = True
        self._schedule(self._send_text(client, "_gz,1"))

    def _note_ack(self, client: WsClient, message: str) -> None:
        """`CLIENT_FRAME_ACK <id> [heldMs]`, per client: it sizes this client's
        RTT estimate, the one its goodput verdict is read alongside."""
        try:
            parts = message.split(" ")
            acked = int(parts[1])
            held_ms = max(0.0, float(parts[2])) if len(parts) >= 3 else 0.0
            if not (0 <= acked <= MAX_UINT16_FRAME_ID):
                raise ValueError("ACK frame id outside uint16 wire space.")
        except (IndexError, ValueError):
            logger.warning("Malformed CLIENT_FRAME_ACK from %s: %s", client.conn_id,
                           message[:64])
            return
        client.last_ack = acked
        state = client.display_state
        state["acknowledged_frame_id"] = acked
        state["unacked_since"] = None
        sent_ts = state.get("sent_timestamps")
        if sent_ts and acked in sent_ts:
            send_time = sent_ts[acked]
            # The ack covers every frame sent up to this one: a socket delivers
            # in order. What stays is exactly what is still on its way.
            while sent_ts and next(iter(sent_ts.values())) <= send_time:
                sent_ts.popitem(last=False)
            state["acked_sent_at"] = send_time
            now = time.monotonic()
            rtt_ms = max(0.0, (now - send_time) * 1000.0 - held_ms)
            if 0 <= rtt_ms <= RTT_SAMPLE_SANE_MAX_MS:
                samples = state["rtt_samples"]
                samples.append(rtt_ms)
                state["smoothed_rtt"] = sum(samples) / len(samples)
                client.note_rtt(rtt_ms / 1000.0, now)

    async def _video_active(self, client: WsClient, active: bool) -> None:
        """START_VIDEO / STOP_VIDEO for this client alone: it sets the same
        consumer field a peer sets, so the shared capture stops only once every
        consumer of the display is paused and restarts for the first that asks
        again. A resuming client always gets an IDR: its decoder needs the
        resync even when the capture never stopped."""
        client.consumer["video_paused"] = not active
        client.forget_inflight()
        if not active and client.relay is not None:
            client.relay.flush_for_gate()
        await self.service.handle_consumers_changed(client.display_id)
        if active:
            await self.service.request_idr_for_display(client.display_id)
            await self._send_text(client, "VIDEO_STARTED")
        else:
            await self._send_text(client, "VIDEO_STOPPED")

    async def _audio_active(self, client: WsClient, active: bool) -> None:
        """START_AUDIO / STOP_AUDIO for this client alone, the per-consumer rule
        the data channel uses; the shared capture settles on the whole set."""
        client.consumer["audio_paused"] = not active
        await self.service.handle_consumers_changed(client.display_id)

    async def _drop_client(self, client: WsClient, reason: str) -> None:
        """Close one client's socket; its handler runs the teardown."""
        if client.closing:
            return
        client.closing = True
        logger.warning("Dropping sink client %s: %s", client.conn_id, reason)
        try:
            await asyncio.wait_for(client.ws.close(code=1001, message=b"stream stalled"),
                                   timeout=2.0)
        except Exception:
            pass

    async def _teardown(self, client: WsClient) -> None:
        """Unregister one client and release what leaves with it.

        Held keys, modifiers and pointer buttons are one global desktop state,
        so a departing socket force-releases them only when it could drive
        input AND no input-capable consumer is left on either transport: a
        viewer leaving must never drop the controller's held keys or its
        in-flight drag.
        """
        client.closing = True
        self.clients.pop(client.conn_id, None)
        self._by_ws.pop(client.ws, None)
        if client.relay is not None:
            client.relay.stop()
            client.relay = None
        self._sync_media_hooks()
        handler = getattr(self.service, "input_handler", None)
        if handler is not None:
            handler.release_clipboard_for_conn(client.conn_id)
        held_authority = _perms_hold_input_authority(
            {"role": client.role, "token": client.token}, token=client.token)
        if held_authority and not self._input_capable_left():
            if handler is not None:
                for release in (handler.release_mouse_buttons, handler.reset_keyboard):
                    try:
                        await release()
                    except Exception as e:
                        logger.warning("Input release for departed sink client %s "
                                       "failed: %s", client.conn_id, e)
        logger.info("Sink client %s disconnected.", client.conn_id)
        try:
            if not self._service_consumers(client.display_id):
                # Nothing consumes the display on either transport; the primary's
                # stop is the service's deferred one, so a reconnect inside the
                # grace still finds the capture warm.
                await self.service.stop_display_media(client.display_id)
            else:
                await self.service.handle_consumers_changed(client.display_id)
        except Exception:
            logger.exception("Sink teardown could not settle display '%s'",
                             client.display_id)
        supervisor = getattr(self.service, "supervisor", None)
        if supervisor is not None and not self.clients:
            rtc_app = getattr(self.service, "rtc_app", None)
            if not getattr(rtc_app, "peer_connections", None):
                supervisor.set_clients_present(False)

    def _service_consumers(self, display_id: str) -> List[Dict[str, Any]]:
        """Every consumer of the display on BOTH transports.

        The service's own view already merges the peers with this sink's
        clients; the sink-only list is the fallback for a service that has no
        peers at all.
        """
        getter = getattr(self.service, "_display_consumers", None)
        if callable(getter):
            return getter(display_id)
        return self.display_consumers(display_id)

    def _input_capable_left(self) -> bool:
        """Whether any connection on either transport may still drive input."""
        for other in self.clients.values():
            if _perms_hold_input_authority({"role": other.role, "token": other.token},
                                           token=other.token):
                return True
        rtc_app = getattr(self.service, "rtc_app", None)
        peers = getattr(rtc_app, "peer_connections", None) or {}
        for peer in list(peers.values()):
            try:
                if rtc_app.peer_holds_input_authority(peer):
                    return True
            except Exception:
                continue
        return False
