"""WebSocket sink (patch 0007) contract tests, run during every base-image build.

The sink is the websockets-format output of the WebRTC service: one capture,
one encoder, one input handler, two kinds of client. These tests drive a real
aiohttp WebSocket against a real `WsSink` over a fake service, so the wire
contract the unmodified `selkies-ws-core.js` depends on is checked without a
display, a browser or a capture.

pixelflux/pcmflux are never started: a `MediaPipelinePixel` is constructed and
its frame callbacks are called by hand, which is exactly what the capture
threads do.
"""

import asyncio
import base64
import json
import os
import inspect
import threading
import time
import unittest
import unittest.mock
from types import SimpleNamespace

from aiohttp import WSMsgType, web
from aiohttp.test_utils import TestClient, TestServer

from selkies import ws_sink as ws_sink_module
from selkies import selkies as selkies_module
from selkies import media_pipeline as media_pipeline_module
from selkies.media_pipeline import MediaPipelinePixel
from selkies.rtc import ClientType, RTCApp
from selkies.stream_hold import StaticHold, configuration as hold_configuration
from selkies.stream_server import CentralizedStreamServer
from selkies.webrtc_mode import WebRTCService
from selkies.ws_sink import WsSink

# Page session ids as the viewer draws them: `vp-` and a random UUID.
OWN_PAGE = "vp-6f1c2a9e-1d4b-4c55-9a3e-0b7f5d2c8e41"
OTHER_PAGE = "vp-0d3e7b52-8a6f-4e19-b2c4-7c1a9f6e3d58"


class Frame(bytes):
    """A pixelflux frame: the wire bytes plus the frame id the ACK carries."""

    frame_id = 0


def stripe(picture_type: int, row: int = 0, payload: bytes = b"\x00" * 64,
           frame_id: int = 1) -> Frame:
    """One H.264 stripe in pixelflux's wire layout: [0x04][picture type][..][y]."""
    header = bytes([0x04, picture_type, 0x00, 0x00, (row >> 8) & 0xFF, row & 0xFF,
                    0x00, 0x00, 0x00, 0x00])
    frame = Frame(header + payload)
    frame.frame_id = frame_id
    return frame


class FakeInput:
    """The input handler surface the sink touches."""

    def __init__(self):
        self.messages = []
        self.released = 0
        self.clipboard_released = []
        self.cursor = {"curdata": "abc", "handle": 1}

    async def on_message(self, msg, display_id="primary", conn_id=None):
        self.messages.append((msg, display_id, conn_id))

    def get_current_cursor_data(self):
        return self.cursor

    async def release_mouse_buttons(self):
        self.released += 1

    async def reset_keyboard(self):
        self.released += 1

    def release_clipboard_for_conn(self, conn_id):
        self.clipboard_released.append(conn_id)


class FakeService:
    """A WebRTCService stand-in with the surface the sink drives.

    `handle_consumers_changed` reproduces the real all-consumers-paused rule
    over `_display_consumers`, which is what the sink relies on for capture
    gating; nothing else is simulated.
    """

    mode = "webrtc"

    def __init__(self, loop):
        self.supervisor = SimpleNamespace(current_mode="webrtc",
                                          set_clients_present=lambda present: None)
        self.input_handler = FakeInput()
        self.peer_manager = SimpleNamespace(peers={})
        self.rtc_app = SimpleNamespace(
            peer_connections={},
            last_cursor_sent=None,
            peer_holds_input_authority=lambda peer: bool(peer.get("input")),
        )
        self.media_pipeline = MediaPipelinePixel(loop, encoder="x264enc",
                                                 video_bitrate=8000)
        self.display_pipelines = {"primary": self.media_pipeline}
        self.display_clients = {}
        self.ws_sink = None
        self.idr_requests = 0
        self.started = 0
        self.stopped = 0
        self.paused = False

    def _server_settings_payload(self):
        return {"settings": {"encoder": {"value": "jpeg"},
                             "audio_enabled": {"value": True, "locked": False}}}

    def _display_config_payload(self):
        return {"type": "display_config_update", "displays": ["primary"]}

    def _encoder_for_display(self, display_id):
        return "x264enc"

    def _display_consumers(self, display_id):
        peers = [p for p in self.rtc_app.peer_connections.values()
                 if (p.get("display_id") or "primary") == display_id]
        return peers + self.ws_sink.display_consumers(display_id)

    async def request_idr_for_display(self, display_id="primary", reason="request", peer_id=None):
        self.idr_requests += 1

    async def start_display_media(self, display_id):
        self.started += 1
        self.paused = False

    async def stop_display_media(self, display_id):
        self.stopped += 1

    async def handle_consumers_changed(self, display_id):
        consumers = self.ws_sink.display_consumers(display_id)
        if consumers and all(c.get("video_paused") for c in consumers):
            self.paused = True
        elif consumers:
            self.paused = False


class SinkTestCase(unittest.IsolatedAsyncioTestCase):
    """A live sink behind an aiohttp test server."""

    async def asyncSetUp(self):
        loop = asyncio.get_running_loop()
        self.service = FakeService(loop)
        self.sink = WsSink(self.service)
        self.service.ws_sink = self.sink
        app = web.Application()
        app.router.add_get("/api/websockets", self.sink.handle_request)
        self.server = TestServer(app)
        self.client = TestClient(self.server)
        await self.client.start_server()

    async def asyncTearDown(self):
        await self.sink.stop()
        await self.client.close()

    async def connect(self, **params):
        """Open one sink socket. The reconnect debounce is per source address
        and every test client shares 127.0.0.1, so it is cleared here: it is
        exercised in its own test."""
        self.sink._last_connection_times.clear()
        return await self.client.ws_connect("/api/websockets", params=params)

    async def drain_handshake(self, ws):
        """Read the legacy-mode handshake pushes and return them in order:
        MODE, the display roster, the cursor, server_settings and the realized
        resolution (five; secure mode prepends AUTH_SUCCESS and MK_ACCESS)."""
        received = []
        while len(received) < 5:
            msg = await asyncio.wait_for(ws.receive(), timeout=5)
            if msg.type != WSMsgType.TEXT:
                continue
            received.append(msg.data)
        return received

    async def next_binary(self, ws, timeout=2.0):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                msg = await asyncio.wait_for(ws.receive(), timeout=deadline - time.monotonic())
            except asyncio.TimeoutError:
                return None
            if msg.type == WSMsgType.BINARY:
                return msg.data
            if msg.type in (WSMsgType.CLOSED, WSMsgType.CLOSE, WSMsgType.ERROR):
                return None
        return None

    async def next_text(self, ws, timeout=2.0):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                msg = await asyncio.wait_for(ws.receive(), timeout=deadline - time.monotonic())
            except asyncio.TimeoutError:
                return None
            if msg.type == WSMsgType.TEXT:
                return msg.data
        return None


class HandshakeTest(SinkTestCase):
    async def test_handshake_order_is_the_websockets_one(self):
        ws = await self.connect()
        pushes = await self.drain_handshake(ws)
        self.assertEqual(pushes[0], "MODE websockets")
        self.assertTrue(pushes[1].startswith("DISPLAY_CONFIG_UPDATE,"))
        self.assertTrue(pushes[2].startswith("cursor,"))
        settings = json.loads(pushes[3])
        self.assertEqual(settings["type"], "server_settings")
        # A WS client that asked for jpeg is told the encoder it actually gets.
        self.assertEqual(settings["settings"]["encoder"]["value"], "x264enc")
        self.assertIn("ws_max_message_bytes", settings["settings"])
        resolution = json.loads(pushes[4])
        self.assertEqual(resolution["type"], "stream_resolution")
        self.assertEqual(
            (resolution["width"], resolution["height"], resolution["displayId"]),
            (1920, 1080, "primary"))
        await ws.close()

    async def test_secure_mode_sends_auth_success_and_mk_access(self):
        real = ws_sink_module.app_settings

        class Secure:
            master_token = "master"

            def __getattr__(self, name):
                return getattr(real, name)

        ws_sink_module.app_settings = Secure()
        selkies_module.user_tokens = {"tok": {"role": "controller", "slot": None}}
        selkies_module.active_mk_token = None
        try:
            ws = await self.connect(token="tok")
            pushes = []
            while len(pushes) < 5:
                msg = await asyncio.wait_for(ws.receive(), timeout=5)
                if msg.type == WSMsgType.TEXT:
                    pushes.append(msg.data)
            self.assertTrue(pushes[0].startswith("AUTH_SUCCESS,"))
            self.assertEqual(pushes[1], "MODE websockets")
            self.assertEqual(pushes[2], "MK_ACCESS,1")
            self.assertTrue(pushes[3].startswith("DISPLAY_CONFIG_UPDATE,"))
            await ws.close()
        finally:
            ws_sink_module.app_settings = real
            selkies_module.user_tokens = {}
            selkies_module.active_mk_token = None

    async def test_reconnect_rate_limit(self):
        ws = await self.connect()
        await self.drain_handshake(ws)
        await ws.close()
        # No clear() this time: the debounce must refuse the immediate reconnect.
        again = await self.client.ws_connect("/api/websockets")
        msg = await asyncio.wait_for(again.receive(), timeout=5)
        # The frame's own code, not ws.close_code: which of the two a client
        # library fills in depends on its version.
        self.assertEqual((msg.type, msg.data), (WSMsgType.CLOSE, 4029))

    async def test_inactive_mode_answers_409(self):
        self.service.supervisor.current_mode = "websockets"
        response = await self.client.get("/api/websockets")
        self.assertEqual(response.status, 409)


class MediaTest(SinkTestCase):
    async def test_one_frame_reaches_the_peer_and_the_ws_client(self):
        peer_frames = []
        pipeline = self.service.media_pipeline
        pipeline.produce_data = lambda data, pts, kind, keyframe=True: peer_frames.append(
            (bytes(data), keyframe))
        ws = await self.connect()
        await self.drain_handshake(ws)
        frame = stripe(0x01)
        # What the pixelflux capture thread does, verbatim.
        pipeline._screen_capture_callback(frame)
        received = await self.next_binary(ws)
        self.assertEqual(received, bytes(frame), "WS client must get the RAW frame")
        self.assertEqual(len(peer_frames), 1, "the RTC path must still be fed")
        self.assertEqual(peer_frames[0][0], bytes(frame)[10:],
                         "the peer keeps getting the header-stripped payload")
        await ws.close()

    async def test_relay_gates_until_a_keyframe_and_asks_for_one(self):
        ws = await self.connect()
        await self.drain_handshake(ws)
        before = self.service.idr_requests
        # A fresh relay is fully gated: a delta decodes against a reference this
        # client never received, so it is dropped and an IDR is requested.
        self.sink._offer_video(stripe(0x00, row=0, frame_id=2))
        await asyncio.sleep(0.1)
        self.assertGreater(self.service.idr_requests, before)
        self.assertIsNone(await self.next_binary(ws, timeout=0.5))
        # The IDR opens the gate for its row, and the next delta follows it.
        idr = stripe(0x01, row=0, frame_id=3)
        self.sink._offer_video(idr)
        self.assertEqual(await self.next_binary(ws), bytes(idr))
        delta = stripe(0x00, row=0, frame_id=4)
        self.sink._offer_video(delta)
        self.assertEqual(await self.next_binary(ws), bytes(delta))
        await ws.close()

    async def test_audio_frames_carry_the_pcmflux_header(self):
        ws = await self.connect()
        await self.drain_handshake(ws)
        self.sink._capture_audio(Frame(b"opusopus"))
        received = await self.next_binary(ws)
        self.assertEqual(received, b"\x01\x00opusopus")
        await ws.close()

    async def test_one_stalled_client_does_not_hold_the_audio_of_another(self):
        """The audio loop is shared, so it may never wait on a socket: one
        client that stops reading would otherwise hold every other client's
        sound for the whole stall deadline and overflow the shared queue. A
        client that still owes its previous frame skips the new one, and the
        skip is counted as loss so the congestion loop lowers the bitrate that
        is drowning it."""
        slow = await self.connect()
        await self.drain_handshake(slow)
        await asyncio.sleep(0.2)
        fast = await self.connect()
        await self.drain_handshake(fast)
        clients = list(self.sink.clients.values())
        stalled = clients[0]

        class StalledSocket:
            """A socket that accepts a send and never completes it."""

            closed = False

            def __init__(self):
                self.sends = 0
                self.release = asyncio.Event()

            async def send_bytes(self, payload):
                self.sends += 1
                await self.release.wait()

            async def send_str(self, payload):
                await self.release.wait()

            async def close(self, code=1000, message=b""):
                self.release.set()

        socket = StalledSocket()
        stalled.ws = socket
        try:
            for _ in range(10):
                self.sink._capture_audio(Frame(b"opusopus"))
                await asyncio.sleep(0.02)
            received = 0
            while await self.next_binary(fast, timeout=0.5) is not None:
                received += 1
            self.assertGreaterEqual(received, 5,
                                    "the reading client keeps getting audio")
            self.assertEqual(socket.sends, 1,
                             "the stalled client holds exactly one frame")
            self.assertGreaterEqual(stalled.loss, ws_sink_module.RELAY_DROP_LOSS,
                                    "its skipped frames are counted as loss")
            self.assertLess(self.sink._audio_queue.qsize(), 5,
                            "the shared queue is not backing up")
        finally:
            socket.release.set()
            await asyncio.sleep(0.05)
        await slow.close()
        await fast.close()

    async def test_a_stats_push_a_client_cannot_take_does_not_drop_it(self):
        """Stats are disposable and the next tick carries fresher numbers, so a
        client that cannot take one in time skips it. Dropping it would undo
        the rescue the longer deadline exists for."""
        ws = await self.connect()
        await self.drain_handshake(ws)
        client = next(iter(self.sink.clients.values()))

        class StalledSocket:
            closed = False

            def __init__(self):
                self.release = asyncio.Event()

            async def send_str(self, payload):
                await self.release.wait()

            async def send_bytes(self, payload):
                await self.release.wait()

            async def close(self, code=1000, message=b""):
                self.release.set()

        socket = StalledSocket()
        client.ws = socket
        stall = ws_sink_module.SINK_SEND_STALL_S
        ws_sink_module.SINK_SEND_STALL_S = 0.05
        try:
            self.assertFalse(
                await self.sink._send_text(client, "x" * 8, drop_on_timeout=False))
            self.assertFalse(client.closing, "a skipped stats push is not a drop")
            self.assertIn(client.conn_id, self.sink.clients)
            # A control message the session depends on still drops it.
            self.assertFalse(await self.sink._send_text(client, "y" * 8))
            await asyncio.sleep(0.05)
            self.assertTrue(client.closing)
        finally:
            ws_sink_module.SINK_SEND_STALL_S = stall
            socket.release.set()
        await ws.close()

    async def test_cursor_is_pushed_in_the_websockets_format(self):
        ws = await self.connect()
        await self.drain_handshake(ws)
        self.sink.push_cursor({"curdata": "zzz", "handle": 7})
        text = await self.next_text(ws)
        self.assertTrue(text.startswith("cursor,"))
        self.assertEqual(json.loads(text[len("cursor,"):])["handle"], 7)
        await ws.close()

    async def test_system_actions_reach_sink_clients(self):
        ws = await self.connect()
        await self.drain_handshake(ws)
        client = next(iter(self.sink.clients.values()))
        # Addressed at this client: the RTC side must not broadcast it.
        self.assertTrue(self.sink.push_system("command_done,upload", client.conn_id))
        text = await self.next_text(ws)
        self.assertEqual(json.loads(text[len("system,"):])["action"], "command_done,upload")
        # Session-wide, and addressed at a peer id the sink does not own.
        self.assertFalse(self.sink.push_system("encoder,x264enc", None))
        self.assertFalse(self.sink.push_system("command_done,other", "client-9"))
        text = await self.next_text(ws)
        self.assertEqual(json.loads(text[len("system,"):])["action"], "encoder,x264enc")
        await ws.close()

    async def test_realized_resolution_is_sent_as_stream_resolution(self):
        ws = await self.connect()
        await self.drain_handshake(ws)
        self.sink.push_stream_resolution(1280, 720, "primary")
        payload = json.loads(await self.next_text(ws))
        self.assertEqual(payload, {"type": "stream_resolution", "width": 1280,
                                   "height": 720, "displayId": "primary"})
        # Another display's size must never rescale this page.
        self.sink.push_stream_resolution(800, 600, "display2")
        self.assertIsNone(await self.next_text(ws, timeout=0.5))
        await ws.close()

    async def test_the_realized_size_wins_over_the_capture_region(self):
        """After a resize the pipeline still carries the region it started
        with; the service's own dimensions and the primary layout are the
        realized ones and must size a joining page."""
        self.assertEqual(self.sink._primary_dims(), (1920, 1080))
        self.service.display_layouts = {"primary": {"x": 0, "y": 0,
                                                    "w": 1600, "h": 900}}
        self.assertEqual(self.sink._primary_dims(), (1600, 900))
        self.service._primary_dims = (1280, 720)
        self.assertEqual(self.sink._primary_dims(), (1280, 720))
        ws = await self.connect()
        resolution = json.loads((await self.drain_handshake(ws))[4])
        self.assertEqual((resolution["width"], resolution["height"]),
                         (1280, 720))
        await ws.close()

    async def test_stats_reach_the_client_in_the_websockets_spelling(self):
        """The peers get CPU, GPU and network numbers over the data channel,
        which a websockets client cannot read. Without these three pushes its
        stats panel stays empty on the sink, and the bandwidth reading the
        congestion loop already computes never reaches the page."""
        ws = await self.connect()
        await self.drain_handshake(ws)
        self.sink.push_system_stats(12.5, 8 * 1024 ** 3, 2 * 1024 ** 3)
        system = json.loads(await self.next_text(ws))
        self.assertEqual(system["type"], "system_stats")
        self.assertEqual((system["cpu_percent"], system["mem_used"]),
                         (12.5, 2 * 1024 ** 3))
        self.sink.push_gpu_stats(0.5, 4096, 1024)
        gpu = json.loads(await self.next_text(ws))
        self.assertEqual((gpu["type"], gpu["gpu_percent"], gpu["memory_used"]),
                         ("gpu_stats", 50.0, 1024 * 1024 * 1024))
        client = next(iter(self.sink.clients.values()))
        client.goodput_bps = 3_500_000.0
        client.host.display_clients["primary"]["smoothed_rtt"] = 42.4
        self.sink.push_network_stats()
        network = json.loads(await self.next_text(ws))
        self.assertEqual((network["type"], network["bandwidth_mbps"],
                          network["latency_ms"]),
                         ("network_stats", 3.5, 42.4))
        # The monitors tick once a second; the client is not told that often.
        self.sink.push_system_stats(99.0, 1, 1)
        self.sink.push_network_stats()
        self.assertIsNone(await self.next_text(ws, timeout=0.5))
        await ws.close()

    async def test_gpu_stats_name_their_gpu(self):
        ws = await self.connect()
        await self.drain_handshake(ws)
        self.sink._stats_sent_at.clear()
        self.sink.push_gpu_stats(0.5, 8192, 1024, gpu_id=1)
        deadline = time.monotonic() + 3
        payload = None
        while time.monotonic() < deadline and payload is None:
            text = await self.next_text(ws, timeout=1.0)
            if text and text.startswith("{") and json.loads(text).get("type") == "gpu_stats":
                payload = json.loads(text)
        self.assertIsNotNone(payload, "no gpu_stats push")
        self.assertEqual(payload["gpu_id"], 1)
        self.assertEqual(payload["gpu_percent"], 50.0)
        await ws.close()

    async def test_the_service_pushes_its_monitor_ticks_to_the_sink(self):
        """The hook, not just the method: `handle_system_monitor` and
        `handle_gpu_stats` are where the numbers exist, and a sink that is
        never called from them pushes nothing."""
        source = inspect.getsource(WebRTCService.handle_system_monitor)
        self.assertIn("push_system_stats", source)
        self.assertIn("push_network_stats", source)
        self.assertIn("push_gpu_stats",
                      inspect.getsource(WebRTCService.handle_gpu_stats))

    async def test_clipboard_addressing(self):
        ws = await self.connect()
        await self.drain_handshake(ws)
        client = next(iter(self.sink.clients.values()))
        # An answer to this client's own fetch belongs to it alone, so the RTC
        # fan-out is told to skip it.
        self.assertTrue(
            await self.sink.push_clipboard("hello", "text/plain", "cr", client.conn_id))
        self.assertEqual(await self.next_text(ws), "clipboard_reply,cr")
        self.assertEqual(await self.next_text(ws), "clipboard,aGVsbG8=")
        # A session-wide announcement reaches both transports.
        self.assertFalse(await self.sink.push_clipboard("hi", "text/plain", None, None))
        self.assertEqual(await self.next_text(ws), "clipboard,aGk=")
        # A payload for a peer is not the sink's.
        self.assertFalse(
            await self.sink.push_clipboard("x", "text/plain", "cr", "client-9"))
        self.assertIsNone(await self.next_text(ws, timeout=0.5))
        await ws.close()

    async def test_multipart_clipboard_chunks_carry_no_inner_padding(self):
        """The viewer concatenates the base64 *text* of every clipboard_data
        frame and decodes it on quartet boundaries, so a chunk size that is not
        a multiple of 3 pads mid-stream and the whole paste fails to decode."""
        self.assertEqual(ws_sink_module.CLIPBOARD_CHUNK_SIZE % 3, 0)
        payload = bytes((i * 7 + 3) & 0xFF
                        for i in range(2 * ws_sink_module.CLIPBOARD_CHUNK_SIZE + 7))
        ws = await self.connect()
        await self.drain_handshake(ws)
        await self.sink.push_clipboard(payload, "application/octet-stream", None, None)
        start = await self.next_text(ws)
        self.assertEqual(start,
                         f"clipboard_start,application/octet-stream,{len(payload)}")
        chunks = []
        while True:
            text = await self.next_text(ws, timeout=5.0)
            self.assertIsNotNone(text, "clipboard transfer never finished")
            if text == "clipboard_finish":
                break
            self.assertTrue(text.startswith("clipboard_data,"))
            chunks.append(text[len("clipboard_data,"):])
        for i, chunk in enumerate(chunks[:-1]):
            self.assertFalse("=" in chunk, f"chunk {i} carries base64 padding")
        self.assertEqual(base64.b64decode("".join(chunks)), payload)
        await ws.close()

    async def test_start_and_stop_video_drive_the_capture_gate(self):
        ws = await self.connect()
        await self.drain_handshake(ws)
        await ws.send_str("STOP_VIDEO")
        self.assertEqual(await self.next_text(ws), "VIDEO_STOPPED")
        self.assertTrue(self.service.paused, "last consumer paused must stop capture")
        before = self.service.idr_requests
        await ws.send_str("START_VIDEO")
        self.assertEqual(await self.next_text(ws), "VIDEO_STARTED")
        self.assertFalse(self.service.paused)
        self.assertGreater(self.service.idr_requests, before,
                           "a resuming consumer always gets an IDR")
        await ws.close()


# Whole pixelflux messages of the standard profile (x264, WebRTC CBR), recorded on
# the beta image: a keyframe's parameter sets, a refinement frame and two converged
# static frames that differ only in frame_num (the stream_hold_test golden vectors).
HOLD_KEY = (
    "04010000000007800438000000016742c028da01e0089f966a02020280000003008000001e478c19500000000168ce3c80")
HOLD_REFINED = (
    "0400001e00000780043800000001419ba032800a223dfad1513de0671efed0e86c763816c7bdbe2f17802740e29883ffa69e9a"
    "7683b1a076349c65345345d6e80b70e2f4d3d34ffd5a6bf03d0f7332bef7805460000001410015926e80ca0040a3debe874381"
    "6c7bc4d490921f005103dc8caa1d0fe2c7beb4544f7816c7bd7e4849002d86dff7bfdee6b43a1f907bdf5a99ea67c806bfa952"
    "0313aa70010dd52c035700000141000a8c9ba03280fa3debf7b805c07bc7a8b2340593a048490804607bd3def3c278405f0f7a"
    "d6b5a81f3aa5809c0dbef7fbdfc773231230caca005ee8")
HOLD_CONVERGED = (
    "040002c600000780043800000001419aa032800a223dfad1513de0671efed0e86c763816c7bdbe2f17802740e29883ffa69e9a"
    "7683b1a076349c65345345d6e80b70e2f4d3d34ffd5a6bf03d0f7332bef7805460000001410015926a80ca0040a3debe874381"
    "6c7bc4d490921f005103dc8caa1d0fe2c7beb4544f7816c7bd7e4849002d86dff7bfdee6b43a1f907bdf5a99ea67c801292000"
    "000141000a8c9aa03280fa3debf7b805c07bc7a8b2340593a048490804607bd3def3c278405f0f7ad6b5a8046036fbdfef7f1d"
    "cc8c48c32b28017ba0",
    "040002c700000780043800000001419ac032800a223dfad1513de0671efed0e86c763816c7bdbe2f17802740e29883ffa69e9a"
    "7683b1a076349c65345345d6e80b70e2f4d3d34ffd5a6bf03d0f7332bef7805460000001410015926b00ca0040a3debe874381"
    "6c7bc4d490921f005103dc8caa1d0fe2c7beb4544f7816c7bd7e4849002d86dff7bfdee6b43a1f907bdf5a99ea67c801292000"
    "000141000a8c9ac03280fa3debf7b805c07bc7a8b2340593a048490804607bd3def3c278405f0f7ad6b5a8046036fbdfef7f1d"
    "cc8c48c32b28017ba0")
HOLD_FLOOR = 14


def hold_message(text: str, frame_id: int) -> Frame:
    raw = bytearray.fromhex(text)
    raw[2:4] = frame_id.to_bytes(2, "big")
    frame = Frame(bytes(raw))
    frame.frame_id = frame_id
    return frame


class HoldPipelineTest(unittest.IsolatedAsyncioTestCase):
    """The static hold inside a real `MediaPipelinePixel` over a fake capture
    module: frames still reach both outputs, and every live push carries the
    effective streaming flag."""

    async def asyncSetUp(self):
        self.now = 1000.0
        self.frame_id = 0
        self.peer_frames, self.sink_frames = [], []
        self.pipeline = MediaPipelinePixel(asyncio.get_running_loop(), encoder="h264enc",
                                           use_paint_over_quality=False)
        self.pipeline.produce_data = lambda data, pts, kind, keyframe=True: self.peer_frames.append(bytes(data))
        self.pipeline.on_ws_frame = lambda frame: self.sink_frames.append(bytes(frame))
        self.capture = unittest.mock.Mock()
        self.pipeline.capture_module, self.pipeline._is_screen_capturing = self.capture, True
        self.hold = self.pipeline.static_hold = StaticHold(
            hold_configuration(""), "on", push=self.pipeline._hold_push,
            link=lambda: ((), [{"video_paused": False}]), scope=self.pipeline._hold_scope,
            clock=lambda: self.now, display="primary")
        self.hold.resume(HOLD_FLOOR)

    async def asyncTearDown(self):
        await self.pipeline.stop_screen_capture()

    async def deliver(self, text):
        """One capture tick, exactly as the pixelflux capture thread hands it over."""
        self.now += 1 / 30
        self.pipeline._screen_capture_callback(hold_message(text, self.frame_id))
        self.frame_id += 1
        await asyncio.sleep(0)

    def streaming_flags(self):
        return [call.args[0].video_streaming_mode for call in self.capture.update_tunables.call_args_list]

    async def enter_hold(self):
        await self.deliver(HOLD_KEY)
        while self.hold.state != "held" and self.frame_id < 90:
            await self.deliver(HOLD_CONVERGED[self.frame_id % 2])
        self.assertEqual(self.hold.state, "held")

    async def test_a_held_display_still_feeds_the_peer_and_the_sink(self):
        await self.enter_hold()
        self.assertEqual(self.streaming_flags(), [False])
        self.assertEqual(len(self.peer_frames), self.frame_id)
        self.assertEqual(len(self.sink_frames), self.frame_id)
        self.assertEqual(self.peer_frames[-1], self.sink_frames[-1][10:])
        self.assertIsNotNone(self.pipeline._hold_task)

    async def test_every_live_push_carries_the_effective_streaming_flag(self):
        await self.enter_hold()
        await self.pipeline.set_pointer_visible(True)
        await self.pipeline.set_video_paintover_crf(21)
        self.assertEqual(self.streaming_flags(), [False, False, False])
        await self.deliver(HOLD_REFINED)
        self.assertEqual(self.hold.state, "streaming")
        await self.pipeline.set_pointer_visible(False)
        self.assertEqual(self.streaming_flags(), [False, False, False, True, True])

    async def test_a_scope_change_streams_before_the_setting_is_pushed(self):
        await self.enter_hold()
        await self.pipeline.set_use_paint_over_quality(True)
        self.assertEqual(self.hold.state, "streaming")
        self.assertEqual(self.hold.releases["ineligible:paint-over"], 1)
        self.assertEqual(self.streaming_flags(), [False, True, True])

    async def test_a_keyframe_request_streams_before_the_idr(self):
        await self.enter_hold()
        self.capture.reset_mock()
        await self.pipeline.dynamic_idr_frame("pli", "viewer")
        self.assertEqual([name for name, _, _ in self.capture.mock_calls],
                         ["update_tunables", "request_idr_frame"])
        self.assertTrue(self.capture.update_tunables.call_args.args[0].video_streaming_mode)

    async def test_frame_rate_changes_wake_after_their_guard_and_bitrate_changes_never(self):
        await self.enter_hold()
        self.capture.reset_mock()
        await self.pipeline.set_video_bitrate(1234)
        await self.pipeline.set_framerate(self.pipeline.framerate)
        self.assertEqual(self.hold.state, "held")
        self.capture.update_tunables.assert_not_called()
        await self.pipeline.set_framerate(20)
        self.assertEqual(self.hold.state, "streaming")
        self.assertEqual([name for name, _, _ in self.capture.mock_calls],
                         ["update_video_bitrate", "update_tunables", "update_vbv_multiplier",
                          "update_framerate"])
        self.pipeline.static_hold = unittest.mock.Mock()
        self.pipeline._is_screen_capturing = False
        await self.pipeline.set_framerate(25)
        self.pipeline.static_hold.wake.assert_not_called()

    async def test_restart_builds_a_streaming_snapshot_and_pushes_again(self):
        await self.enter_hold()
        task = self.pipeline._hold_task
        self.capture.reset_mock()
        await self.pipeline.restart_screen_capture()
        started = self.capture.start_capture.call_args.args[1]
        self.assertTrue(started.video_streaming_mode, "the hold is suspended before the snapshot")
        self.assertEqual([name for name, _, _ in self.capture.mock_calls],
                         ["start_capture", "update_tunables", "update_video_bitrate"])
        self.assertTrue(self.capture.update_tunables.call_args.args[0].video_streaming_mode)
        self.capture.update_video_bitrate.assert_called_once_with(int(self.pipeline.video_bitrate))
        self.assertFalse(self.hold.suspended)
        self.assertEqual(self.hold.floor, started.video_min_qp)
        await asyncio.sleep(0)
        self.assertTrue(task.cancelled())

    async def test_a_start_pushes_what_changed_while_it_ran(self):
        started, release = threading.Event(), threading.Event()
        module = unittest.mock.Mock()

        def start_capture(callback, settings):
            started.set()
            release.wait(5)

        module.start_capture.side_effect = start_capture
        self.pipeline.capture_module, self.pipeline._is_screen_capturing = None, False
        with unittest.mock.patch.object(media_pipeline_module, "ScreenCapture", return_value=module):
            start = asyncio.ensure_future(self.pipeline.start_screen_capture())
            await asyncio.to_thread(started.wait, 5)
            await self.pipeline.set_pointer_visible(not self.pipeline.capture_cursor)
            await self.pipeline.set_video_bitrate(4321)
            release.set()
            await start
        snapshot = module.start_capture.call_args.args[1]
        pushed = module.update_tunables.call_args.args[0]
        self.assertEqual([name for name, _, _ in module.mock_calls],
                         ["set_cursor_callback", "start_capture", "update_tunables", "update_video_bitrate"])
        self.assertNotEqual(pushed.capture_cursor, snapshot.capture_cursor)
        self.assertEqual(pushed.capture_cursor, self.pipeline.capture_cursor)
        module.update_video_bitrate.assert_called_once_with(4321)
        self.assertTrue(pushed.video_streaming_mode)
        self.assertFalse(self.hold.suspended)

    async def test_a_delivered_frame_wakes_the_hold_timer(self):
        await self.enter_hold()
        # The timer task has not run yet: its first deadline is a minute away.
        self.hold.due = self.hold.pad_at = self.now + 60
        ticks = []
        tick = self.hold.tick
        self.hold.tick = lambda: ticks.append(self.now) or tick()
        for _ in range(3):
            await asyncio.sleep(0)
        self.assertEqual(ticks, [])
        await self.deliver(HOLD_CONVERGED[self.frame_id % 2])
        for _ in range(3):
            await asyncio.sleep(0)
        self.assertEqual((ticks[:1], self.hold.state), ([self.now], "held"))
        self.assertAlmostEqual(self.hold.pad_at, self.now + 1 / 30, delta=0.005)

    async def test_stop_cancels_the_hold_timers(self):
        await self.enter_hold()
        task = self.pipeline._hold_task
        timer = self.pipeline._hold_timer = asyncio.get_running_loop().call_later(60, lambda: None)
        await self.pipeline.stop_screen_capture()
        await asyncio.sleep(0)
        self.assertTrue(task.cancelled())
        self.assertTrue(timer.cancelled())
        self.assertIsNone(self.pipeline._hold_task)
        self.assertIsNone(self.pipeline._hold_timer)
        self.assertTrue(self.hold.suspended)
        self.capture.stop_capture.assert_called_once()

    async def test_a_failed_push_disables_the_hold_and_restarts_the_capture(self):
        failures = [RuntimeError("capture gone")]

        def update_tunables(settings):
            if failures:
                raise failures.pop()

        self.capture.update_tunables.side_effect = update_tunables
        with self.assertLogs("stream-hold", level="WARNING"):
            await self.deliver(HOLD_KEY)
            while not self.hold.disabled and self.frame_id < 90:
                await self.deliver(HOLD_CONVERGED[self.frame_id % 2])
        self.assertEqual((self.hold.disabled, self.hold.gated, self.hold.state), ("push", False, "streaming"))
        await self.pipeline._hold_restart
        self.capture.start_capture.assert_called_once()
        self.assertTrue(self.capture.update_tunables.call_args.args[0].video_streaming_mode)

    async def test_the_software_encode_line_is_logged_once(self):
        pipeline = MediaPipelinePixel(asyncio.get_running_loop(), encoder="h264enc", use_cpu=True,
                                      display_id="log-once")
        with self.assertLogs("display_utils", level="INFO") as logs:
            for _ in range(3):
                pipeline.generate_capture_settings()
        self.assertEqual(sum("encodes H.264 in software" in line for line in logs.output), 1)


class DispatchTest(SinkTestCase):
    async def test_viewer_whitelist_applies_on_the_sink(self):
        ws = await self.connect(role="viewer")
        await self.drain_handshake(ws)
        await ws.send_str("kd,65")
        await ws.send_str("REQUEST_KEYFRAME")
        await asyncio.sleep(0.2)
        dispatched = [m for m, _d, _c in self.service.input_handler.messages]
        self.assertNotIn("kd,65", dispatched, "a viewer may not drive the keyboard")
        self.assertIn("REQUEST_KEYFRAME", dispatched)
        await ws.close()

    async def test_controller_input_reaches_the_shared_input_handler(self):
        ws = await self.connect()
        await self.drain_handshake(ws)
        await ws.send_str("kd,65")
        await asyncio.sleep(0.2)
        self.assertEqual(self.service.input_handler.messages[0][0], "kd,65")
        self.assertTrue(self.service.input_handler.messages[0][2].startswith("wssink:"))
        await ws.close()

    async def test_gamepad_mic_and_webcam_are_dropped(self):
        ws = await self.connect()
        await self.drain_handshake(ws)
        await ws.send_str("js,0,1,1")
        await ws.send_bytes(b"\x02" + b"\x00" * 32)
        await asyncio.sleep(0.2)
        self.assertEqual([m for m, _d, _c in self.service.input_handler.messages], [])
        self.assertEqual(await self.next_text(ws), "MICROPHONE_DISABLED")
        await ws.close()

    async def test_gz_handshake_is_echoed(self):
        ws = await self.connect()
        await self.drain_handshake(ws)
        await ws.send_str("_gz,1")
        self.assertEqual(await self.next_text(ws), "_gz,1")
        await ws.close()


class ConsumerTest(SinkTestCase):
    def fake_service_self(self):
        """A WebRTCService `self` carrying only what the consumer helpers read,
        so the REAL methods are the ones under test."""
        fake = SimpleNamespace(rtc_app=self.service.rtc_app, ws_sink=self.sink)
        fake.peer_manager = self.service.peer_manager
        fake._display_consumers = lambda did: WebRTCService._display_consumers(fake, did)
        return fake

    async def test_sink_clients_are_display_consumers(self):
        ws = await self.connect()
        await self.drain_handshake(ws)
        consumers = WebRTCService._display_consumers(self.fake_service_self(), "primary")
        self.assertEqual(len(consumers), 1)
        self.assertIn("video_paused", consumers[0])
        self.assertIn("audio_paused", consumers[0])
        self.assertTrue(
            WebRTCService._primary_display_has_consumer(self.fake_service_self()))
        await ws.close()

    async def test_session_peer_handover_creates_no_second_consumer(self):
        ws = await self.connect(session_peer_id=OWN_PAGE)
        await self.drain_handshake(ws)
        # The same viewer's WebRTC attempt succeeds: the signaling peer carries
        # the page's session id, its RTC entry takes the consumer over.
        self.service.peer_manager.peers["client-1"] = SimpleNamespace(
            session_peer_id=OWN_PAGE, client_type="controller", client_token=None,
            raddr=("127.0.0.1", 5))
        self.service.rtc_app.peer_connections["client-1"] = {
            "display_id": "primary", "video_paused": False, "audio_paused": False}
        consumers = WebRTCService._display_consumers(self.fake_service_self(), "primary")
        self.assertEqual(len(consumers), 1,
                         "the peer entry and the sink client are one consumer")
        self.assertFalse(ws.closed, "no superseded close for a transport handover")
        # A peer that carries no session id hands nothing over.
        self.service.peer_manager.peers["client-2"] = SimpleNamespace(
            session_peer_id=None, client_type="viewer", client_token=None,
            raddr=("127.0.0.1", 6))
        self.service.rtc_app.peer_connections["client-2"] = {
            "display_id": "primary", "video_paused": False, "audio_paused": False}
        self.assertEqual(
            len(WebRTCService._display_consumers(self.fake_service_self(), "primary")),
            2)
        await ws.close()

    async def test_a_controller_peer_supersedes_a_sink_controller(self):
        """One controller per display, whatever the transport. Two tabs on one
        transport have always superseded each other; a second tab that reaches
        WebRTC while the first runs on the sink must not leave two live
        controllers typing into one desktop."""
        ws = await self.connect(session_peer_id=OWN_PAGE)
        await self.drain_handshake(ws)
        # The page's OWN peer (same id, same role) is a handover.
        self.service.peer_manager.peers["mine"] = SimpleNamespace(
            session_peer_id=OWN_PAGE, client_type="controller", client_token=None,
            raddr=("127.0.0.1", 5), display_id="primary")
        self.service.rtc_app.peer_connections["mine"] = {"display_id": "primary"}
        self.sink.supersede_by_peer_controllers()
        await asyncio.sleep(0.2)
        self.assertFalse(ws.closed, "a page's own upgrade is not a supersede")
        # Another tab: a controller peer of a different session.
        self.service.peer_manager.peers["other"] = SimpleNamespace(
            session_peer_id=OTHER_PAGE, client_type="controller", client_token=None,
            raddr=("10.0.0.9", 7), display_id="primary")
        self.service.rtc_app.peer_connections["other"] = {"display_id": "primary"}
        self.sink.supersede_by_peer_controllers()
        seen = []
        for _ in range(40):
            msg = await asyncio.wait_for(ws.receive(), timeout=2)
            if msg.type == WSMsgType.TEXT:
                seen.append(msg.data)
            if msg.type in (WSMsgType.CLOSE, WSMsgType.CLOSED):
                break
        # Close 1000 with the reason the client matches on, and no `KILL`:
        # the client answers KILL by closing the socket itself, which skips the
        # branch that freezes the picture and offers the takeover, and 4001 is
        # its invalid-token verdict.
        self.assertEqual(ws.close_code, 1000)
        self.assertFalse([m for m in seen if m.startswith("KILL ")], seen)

    async def test_a_viewer_peer_supersedes_nobody(self):
        ws = await self.connect(session_peer_id=OWN_PAGE)
        await self.drain_handshake(ws)
        self.service.peer_manager.peers["watcher"] = SimpleNamespace(
            session_peer_id=OTHER_PAGE, client_type="viewer", client_token=None,
            raddr=("10.0.0.9", 7), display_id="primary")
        self.service.rtc_app.peer_connections["watcher"] = {"display_id": "primary"}
        self.sink.supersede_by_peer_controllers()
        await asyncio.sleep(0.2)
        self.assertFalse(ws.closed)
        await ws.close()

    async def test_a_new_sink_controller_takes_the_session_from_a_peer(self):
        """The other direction: the signaling server evicts a controller peer
        when a second controller peer arrives, but it never sees a sink
        client. The newest page wins there too."""
        closed = []
        self.service._close_peer_signaling_ws = lambda pid, code, msg: closed.append((pid, code, msg))
        self.service.rtc_app.stop_rtc_connection = lambda pid, role: closed.append((pid, role))
        self.service.peer_manager.peers["other"] = SimpleNamespace(
            session_peer_id=OTHER_PAGE, client_type="controller", client_token=None,
            raddr=("10.0.0.9", 7), display_id="primary")
        self.service.rtc_app.peer_connections["other"] = {"display_id": "primary"}
        ws = await self.connect(session_peer_id=OWN_PAGE)
        await self.drain_handshake(ws)
        await asyncio.sleep(0.3)
        self.assertEqual(closed[0][:2], ("other", 4001))
        self.assertIn(("other", "controller"), closed)
        await ws.close()

    async def test_handover_needs_more_than_the_session_id(self):
        """The session id is a value the PAGE sends, so it may not decide a
        handover on its own: a peer that presents another session's id must not
        remove that session's consumer entry (and with it its capture)."""
        ws = await self.connect(session_peer_id=OWN_PAGE)
        await self.drain_handshake(ws)
        # Legacy mode: a viewer-role peer is not the same client.
        self.service.peer_manager.peers["client-9"] = SimpleNamespace(
            session_peer_id=OWN_PAGE, client_type="viewer", client_token=None,
            raddr=("127.0.0.1", 5))
        self.service.rtc_app.peer_connections["client-9"] = {"display_id": "primary"}
        self.assertEqual(
            len(WebRTCService._display_consumers(self.fake_service_self(), "primary")),
            2, "a role mismatch is not the same client")
        # The real page, its peer forwarded by another proxy node: the address
        # is the node's, so it decides nothing.
        self.service.peer_manager.peers["client-9"].client_type = "controller"
        self.service.peer_manager.peers["client-9"].raddr = ("10.9.9.9", 5)
        self.assertEqual(
            len(WebRTCService._display_consumers(self.fake_service_self(), "primary")),
            1)
        await ws.close()

    async def test_a_short_session_id_binds_no_handover(self):
        """Without a token only an id no other page can guess binds the two
        wires; a short one is not the page's random draw."""
        ws = await self.connect(session_peer_id="vp-abc")
        await self.drain_handshake(ws)
        self.service.peer_manager.peers["client-9"] = SimpleNamespace(
            session_peer_id="vp-abc", client_type="controller", client_token=None,
            raddr=("127.0.0.1", 5), display_id="primary")
        self.service.rtc_app.peer_connections["client-9"] = {"display_id": "primary"}
        self.assertEqual(
            len(WebRTCService._display_consumers(self.fake_service_self(), "primary")),
            2)
        await ws.close()

    async def test_handover_needs_the_same_token_in_secure_mode(self):
        real = ws_sink_module.app_settings

        class Secure:
            master_token = "master"

            def __getattr__(self, name):
                return getattr(real, name)

        ws_sink_module.app_settings = Secure()
        selkies_module.user_tokens = {"tok": {"role": "controller", "slot": None}}
        selkies_module.active_mk_token = None
        try:
            ws = await self.connect(token="tok", session_peer_id=OWN_PAGE)
            while True:
                msg = await asyncio.wait_for(ws.receive(), timeout=5)
                if msg.type == WSMsgType.TEXT and msg.data.startswith("{") and \
                        json.loads(msg.data).get("type") == "stream_resolution":
                    break
            self.service.peer_manager.peers["client-9"] = SimpleNamespace(
                session_peer_id=OWN_PAGE, client_type="viewer",
                client_token="another", raddr=("127.0.0.1", 5))
            self.service.rtc_app.peer_connections["client-9"] = {"display_id": "primary"}
            self.assertEqual(
                len(WebRTCService._display_consumers(self.fake_service_self(), "primary")),
                2, "a different token is a different client")
            self.service.peer_manager.peers["client-9"].client_token = "tok"
            self.assertEqual(
                len(WebRTCService._display_consumers(self.fake_service_self(), "primary")),
                1)
            await ws.close()
        finally:
            ws_sink_module.app_settings = real
            selkies_module.user_tokens = {}
            selkies_module.active_mk_token = None

    async def test_leaving_while_a_peer_stays_keeps_the_capture(self):
        ws = await self.connect()
        await self.drain_handshake(ws)
        self.service.rtc_app.peer_connections["client-5"] = {
            "display_id": "primary", "video_paused": False, "audio_paused": False}
        await ws.close()
        for _ in range(50):
            if not self.sink.clients:
                break
            await asyncio.sleep(0.05)
        await asyncio.sleep(0.1)
        self.assertEqual(self.service.stopped, 0,
                         "a peer still consumes the display; no release")

    async def test_teardown_releases_input_when_authority_leaves(self):
        ws = await self.connect()
        await self.drain_handshake(ws)
        await ws.close()
        for _ in range(50):
            if self.service.input_handler.released:
                break
            await asyncio.sleep(0.05)
        self.assertEqual(self.service.input_handler.released, 2,
                         "held keys and buttons are released with the last authority")
        self.assertEqual(self.service.stopped, 1, "the display is released too")

    async def test_teardown_drops_the_clients_clipboard_transfer(self):
        ws = await self.connect()
        await self.drain_handshake(ws)
        conn_id = next(iter(self.sink.clients))
        await ws.close()
        for _ in range(50):
            if self.service.input_handler.clipboard_released:
                break
            await asyncio.sleep(0.05)
        self.assertEqual(self.service.input_handler.clipboard_released, [conn_id])

    async def test_teardown_keeps_input_while_a_peer_still_drives(self):
        ws = await self.connect()
        await self.drain_handshake(ws)
        self.service.rtc_app.peer_connections["peer-2"] = {
            "display_id": "primary", "input": True}
        await ws.close()
        await asyncio.sleep(0.3)
        self.assertEqual(self.service.input_handler.released, 0)


class ReconcileTest(SinkTestCase):
    """A token-table replacement must reach the sink's clients: neither
    reconcile_clients (websockets sockets) nor reconcile_webrtc_peers (peers)
    walks them, so a revoked share link would keep receiving media."""

    def secure(self):
        real = ws_sink_module.app_settings

        class Secure:
            master_token = "master"

            def __getattr__(self, name):
                return getattr(real, name)

        ws_sink_module.app_settings = Secure()
        return real

    async def connect_tokened(self, token="tok", role="controller", slot=None):
        selkies_module.user_tokens = {token: {"role": role, "slot": slot}}
        selkies_module.active_mk_token = None
        ws = await self.connect(token=token)
        seen = 0
        while seen < 7:
            msg = await asyncio.wait_for(ws.receive(), timeout=5)
            if msg.type == WSMsgType.TEXT:
                seen += 1
                if msg.data.startswith("{") and \
                        json.loads(msg.data).get("type") == "stream_resolution":
                    break
        return ws

    async def test_revoked_token_closes_the_socket(self):
        real = self.secure()
        try:
            ws = await self.connect_tokened()
            selkies_module.user_tokens = {"other": {"role": "controller"}}
            await self.sink.reconcile()
            msg = await asyncio.wait_for(ws.receive(), timeout=5)
            self.assertEqual((msg.type, msg.data), (WSMsgType.CLOSE, 4002))
        finally:
            ws_sink_module.app_settings = real
            selkies_module.user_tokens = {}

    async def test_role_change_closes_and_slot_change_updates(self):
        real = self.secure()
        try:
            ws = await self.connect_tokened(slot=2)
            selkies_module.user_tokens = {"tok": {"role": "controller", "slot": 3}}
            await self.sink.reconcile()
            self.assertEqual(await self.next_text(ws),
                             'ROLE_UPDATE,{"role": "controller", "slot": 3}')
            self.assertEqual(await self.next_text(ws), "MK_ACCESS,1")
            # The new holder gets the cursor with its freshly attached context.
            self.assertTrue((await self.next_text(ws)).startswith("cursor,"))
            selkies_module.user_tokens = {"tok": {"role": "viewer", "slot": 3}}
            await self.sink.reconcile()
            msg = await asyncio.wait_for(ws.receive(), timeout=5)
            self.assertEqual((msg.type, msg.data), (WSMsgType.CLOSE, 4002))
        finally:
            ws_sink_module.app_settings = real
            selkies_module.user_tokens = {}

    async def test_mk_handoff_is_pushed(self):
        real = self.secure()
        try:
            ws = await self.connect_tokened()
            selkies_module.user_tokens = {"tok": {"role": "controller"},
                                          "mk": {"role": "controller"}}
            selkies_module.active_mk_token = "mk"
            await self.sink.reconcile()
            self.assertEqual(await self.next_text(ws), "MK_ACCESS,0")
            selkies_module.active_mk_token = None
            await self.sink.reconcile()
            self.assertEqual(await self.next_text(ws), "MK_ACCESS,1")
            self.assertTrue((await self.next_text(ws)).startswith("cursor,"))
            await ws.close()
        finally:
            ws_sink_module.app_settings = real
            selkies_module.user_tokens = {}
            selkies_module.active_mk_token = None


class CongestionTest(SinkTestCase):
    async def test_first_goodput_sample_is_not_diluted_by_unobserved_zeroes(self):
        ws = await self.connect()
        await self.drain_handshake(ws)
        client = next(iter(self.sink.clients.values()))
        client.host._bytes_sent_in_interval = 625000
        relay = SimpleNamespace(backlog_bytes=10000, budget=4000000)
        with unittest.mock.patch.object(client, 'relay', relay), unittest.mock.patch.object(ws_sink_module.time, 'monotonic', return_value=client._last_tick + 1):
            window = client.take_window()
        # A float division by the tick interval: assert the rate, not its last
        # bit (5 Mbps comes back as 4999999.999999929).
        self.assertAlmostEqual(window['goodput_bps'], 5000000, delta=1)
        await ws.close()


    async def test_first_backlog_is_a_baseline_not_measured_queue_growth(self):
        ws = await self.connect()
        await self.drain_handshake(ws)
        client = next(iter(self.sink.clients.values()))
        relay = SimpleNamespace(backlog_bytes=262144, budget=4000000)
        with unittest.mock.patch.object(client, 'relay', relay):
            client.host._bytes_sent_in_interval = 625000
            client._last_tick -= 1
            self.assertEqual(client.take_window()['loss_fraction'], 0)
            relay.backlog_bytes += 262144
            client._last_tick -= 1
            self.assertGreater(client.take_window()['loss_fraction'], 0)
        await ws.close()

    async def test_first_window_still_reports_real_drops_and_excessive_backlog(self):
        ws = await self.connect()
        await self.drain_handshake(ws)
        client = next(iter(self.sink.clients.values()))
        for backlog, loss in [(262144, .15), (3000000, 0)]:
            client._last_backlog = None
            client.loss = loss
            client._last_tick -= 1
            with unittest.mock.patch.object(client, 'relay', SimpleNamespace(backlog_bytes=backlog, budget=4000000)):
                self.assertGreater(client.take_window()['loss_fraction'], 0)
        await ws.close()

    async def test_paused_audio_only_consumer_does_not_limit_video(self):
        ws = await self.connect()
        await self.drain_handshake(ws)
        client = next(iter(self.sink.clients.values()))
        client.consumer['video_paused'] = True
        client.host._bytes_sent_in_interval = 8000
        client._last_tick -= 1.0
        client.loss = ws_sink_module.RELAY_DROP_LOSS
        buckets = {}
        self.sink.merge_congestion(buckets)
        self.assertEqual(buckets, {})
        self.assertEqual(client.loss, 0)
        self.assertEqual(client.goodput_bps, 0)
        await ws.close()


    async def test_goodput_and_loss_reach_the_congestion_buckets(self):
        ws = await self.connect()
        await self.drain_handshake(ws)
        client = next(iter(self.sink.clients.values()))
        client.host._bytes_sent_in_interval = 125_000
        client._last_tick -= 1.0
        client.loss = ws_sink_module.RELAY_DROP_LOSS
        buckets = {}
        self.sink.merge_congestion(buckets)
        self.assertIn("primary", buckets)
        self.assertTrue(buckets["primary"]["goodputs"])
        self.assertGreater(buckets["primary"]["goodputs"][0], 0)
        self.assertGreaterEqual(buckets["primary"]["worst_loss"],
                                ws_sink_module.RELAY_DROP_LOSS)
        await ws.close()

    async def test_a_growing_backlog_is_the_loss_signal(self):
        """A socket cannot report packet loss, so a backlog that grew over the
        tick has to be the news that the client is falling behind. Waiting for
        half the byte budget (megabytes) would mean many seconds of a picture
        the client cannot take before the encoder is asked to come down."""
        ws = await self.connect()
        await self.drain_handshake(ws)
        client = next(iter(self.sink.clients.values()))
        relay = self.sink._make_relay(client)
        try:
            client._last_tick -= 1.0
            client.take_window()
            # One tick of growth can be a keyframe still leaving the socket.
            relay.backlog_bytes = 4 * ws_sink_module.BACKLOG_GROWTH_MIN_BYTES
            client._last_tick -= 1.0
            self.assertEqual(client.take_window()["loss_fraction"], 0.0)
            relay.backlog_bytes = 8 * ws_sink_module.BACKLOG_GROWTH_MIN_BYTES
            client._last_tick -= 1.0
            first = client.take_window()
            self.assertGreaterEqual(first["loss_fraction"],
                                    ws_sink_module.RELAY_DROP_LOSS)
            # Draining again: a backlog that is no longer growing is not loss.
            relay.backlog_bytes = 32 * 1024
            client._last_tick -= 1.0
            self.assertEqual(client.take_window()["loss_fraction"], 0.0)
            # One frame in flight makes the backlog wobble by its own size per
            # tick; reading that as a queue would hold the encoder down.
            relay.backlog_bytes = 32 * 1024 + 4096
            client._last_tick -= 1.0
            self.assertEqual(client.take_window()["loss_fraction"], 0.0)
        finally:
            relay.stop()
        await ws.close()

    async def test_a_keyframe_leaving_at_the_tick_is_not_loss(self):
        """A keyframe larger than the growth step can still sit in the relay
        when the tick fires; it is gone by the next one. Read as congestion,
        it pinned the shared encoder at its floor on a healthy link."""
        ws = await self.connect()
        await self.drain_handshake(ws)
        client = next(iter(self.sink.clients.values()))
        relay = self.sink._make_relay(client)
        try:
            for backlog in (0, 120 * 1024, 0, 120 * 1024, 0):
                relay.backlog_bytes = backlog
                client._last_tick -= 1.0
                self.assertEqual(client.take_window()["loss_fraction"], 0.0)
        finally:
            relay.stop()
        await ws.close()

    async def test_frames_queued_by_a_loop_stall_are_not_loss(self):
        """A stall of the event loop queues several audio frames; the fan-out
        takes them in one pass, before the first send has even started. That
        is not a client falling behind."""
        ws = await self.connect()
        await self.drain_handshake(ws)
        client = next(iter(self.sink.clients.values()))
        for _ in range(3):
            self.sink._capture_audio(Frame(b"opusopus"))
        self.assertEqual(await self.next_binary(ws), b"\x01\x00opusopus")
        self.assertEqual(client.loss, 0)
        await ws.close()

    async def test_a_draining_backlog_never_raises_the_estimate(self):
        """A tick that drains a queue moves a burst of bytes; counting that as
        capacity made the loop climb back to the ceiling the moment the queue
        moved, and the bitrate sawed instead of settling. While a backlog
        stands, the estimate may only fall."""
        ws = await self.connect()
        await self.drain_handshake(ws)
        client = next(iter(self.sink.clients.values()))
        relay = self.sink._make_relay(client)
        try:
            client.goodput_bps = 500_000.0
            relay.backlog_bytes = 256 * 1024
            client.host._bytes_sent_in_interval = 1_000_000  # 8 Mbit in a tick
            client._last_tick -= 1.0
            self.assertLessEqual(client.take_window()["goodput_bps"], 500_000.0)
            # Caught up: a real tick may raise it again.
            relay.backlog_bytes = 0
            client.host._bytes_sent_in_interval += 1_000_000
            client._last_tick -= 1.0
            self.assertGreater(client.take_window()["goodput_bps"], 500_000.0)
        finally:
            relay.stop()
        await ws.close()

    async def test_a_slow_client_gets_time_instead_of_a_disconnect(self):
        """The sink's relay allows a longer stalled send than the websockets
        service's. There a stalled client can only be dropped; here the stall
        is what lowers the shared encoder, and dropping the client would end
        the session rather than slow it down."""
        ws = await self.connect()
        await self.drain_handshake(ws)
        client = next(iter(self.sink.clients.values()))
        relay = self.sink._make_relay(client)
        try:
            self.assertEqual(relay.send_timeout, ws_sink_module.SINK_SEND_STALL_S)
            self.assertGreater(ws_sink_module.SINK_SEND_STALL_S,
                               selkies_module.SHARED_STREAM_SEND_TIMEOUT_SECONDS)
            # The websockets service keeps its own, shorter, deadline.
            plain = selkies_module._VideoRelay(client.host, "primary", ws, 4096)
            self.assertEqual(plain.send_timeout,
                             selkies_module.SHARED_STREAM_SEND_TIMEOUT_SECONDS)
        finally:
            relay.stop()
        await ws.close()

    async def test_no_ws_client_adds_nothing(self):
        buckets = {}
        self.sink.merge_congestion(buckets)
        self.assertEqual(buckets, {})


class DeliveryTest(SinkTestCase):
    """The buffers between the sink and the viewer (kernel, Go proxy, nginx)
    hold seconds of stream before the socket or the relay feel anything, so
    what is still on its way is read off the client's own acks."""

    async def client_with_keyframe(self, ws):
        await self.drain_handshake(ws)
        client = next(iter(self.sink.clients.values()))
        idr = stripe(0x01, frame_id=3)
        self.sink._offer_video(idr)
        self.assertEqual(await self.next_binary(ws), bytes(idr))
        return client

    async def test_an_ack_leaves_only_what_is_still_on_its_way(self):
        ws = await self.connect()
        client = await self.client_with_keyframe(ws)
        for fid in (4, 5):
            self.sink._offer_video(stripe(0x00, frame_id=fid))
            self.assertIsNotNone(await self.next_binary(ws))
        self.assertEqual(list(client.display_state["sent_timestamps"]), [3, 4, 5])
        self.sink._note_ack(client, "CLIENT_FRAME_ACK 4 0")
        self.assertEqual(list(client.display_state["sent_timestamps"]), [5])
        self.assertIsNotNone(client.base_rtt)
        await ws.close()

    async def test_ack_round_trip_over_the_base_reaches_the_loop_as_queue_delay(self):
        ws = await self.connect()
        client = await self.client_with_keyframe(ws)
        now = time.monotonic()
        client.note_rtt(0.04, now - 5)
        client._last_tick -= 1.0
        client.take_window()
        client.note_rtt(0.44, now)
        client._last_tick -= 1.0
        buckets = {}
        self.sink.merge_congestion(buckets)
        self.assertAlmostEqual(buckets["primary"]["worst_delay"], 0.4, places=6)
        # The base is the window's least round trip, so a standing queue never
        # becomes it.
        self.assertAlmostEqual(client.base_rtt, 0.04)
        client.note_rtt(0.05, now + ws_sink_module.SINK_RTT_FLOOR_WINDOW_S + 1)
        self.assertAlmostEqual(client.base_rtt, 0.05)
        await ws.close()

    async def test_a_full_queue_stops_the_video_until_it_drained(self):
        ws = await self.connect()
        client = await self.client_with_keyframe(ws)
        self.sink._note_ack(client, "CLIENT_FRAME_ACK 3 0")
        # Frame 9 has been on its way far longer than the budget allows.
        late = ws_sink_module.SINK_QUEUE_BUDGET_S + 0.5
        client.display_state["sent_timestamps"][9] = time.monotonic() - late
        self.assertTrue(client.queue_full(time.monotonic()))
        self.sink._offer_video(stripe(0x01, frame_id=10))
        self.assertIsNone(await self.next_binary(ws, timeout=0.3))
        # Drained: the client resumes on the keyframe it asks for.
        before = self.service.idr_requests
        self.sink._note_ack(client, "CLIENT_FRAME_ACK 9 0")
        self.assertFalse(client.queue_full(time.monotonic()))
        self.sink._offer_video(stripe(0x00, frame_id=11))
        await asyncio.sleep(0.1)
        self.assertGreater(self.service.idr_requests, before)
        self.assertIsNone(await self.next_binary(ws, timeout=0.3))
        idr = stripe(0x01, frame_id=12)
        self.sink._offer_video(idr)
        self.assertEqual(await self.next_binary(ws), bytes(idr))
        await ws.close()

    async def test_a_hold_lasts_until_the_queue_drained_and_lets_the_keyframe_through(self):
        """At the budget's edge the queue dips below and rises above it with
        every ack; a hold that followed it dropped the keyframe the client was
        to resume on and froze the picture until the next one was allowed."""
        ws = await self.connect()
        client = await self.client_with_keyframe(ws)
        self.sink._note_ack(client, "CLIENT_FRAME_ACK 3 0")
        sent = client.display_state["sent_timestamps"]
        late = ws_sink_module.SINK_QUEUE_BUDGET_S + 0.5
        sent[8] = time.monotonic() - late
        sent[9] = time.monotonic()
        self.sink._offer_video(stripe(0x00, frame_id=10))
        self.assertIsNotNone(client.held_since)
        # Frame 8 arrived: under budget again, but frame 9 is still on its way.
        self.sink._note_ack(client, "CLIENT_FRAME_ACK 8 0")
        self.assertFalse(client.queue_full(time.monotonic()))
        self.sink._offer_video(stripe(0x01, frame_id=11))
        self.assertIsNone(await self.next_binary(ws, timeout=0.3))
        self.sink._note_ack(client, "CLIENT_FRAME_ACK 9 0")
        client._next_idr = 0.0
        self.sink._offer_video(stripe(0x00, frame_id=12))
        await asyncio.sleep(0.1)
        self.assertIsNone(client.held_since)
        # The deltas dropped until the keyframe are the hold's, not congestion.
        self.assertEqual(client.loss, 0)
        idr = stripe(0x01, frame_id=13)
        self.sink._offer_video(idr)
        self.assertEqual(await self.next_binary(ws), bytes(idr))
        self.assertFalse(client.resyncing)
        await ws.close()

    async def test_a_hold_ends_after_its_limit_even_without_acks(self):
        ws = await self.connect()
        client = await self.client_with_keyframe(ws)
        self.sink._note_ack(client, "CLIENT_FRAME_ACK 3 0")
        # A whole second keeps (now + SINK_HOLD_MAX_S) - now exactly the limit.
        # Just below a power of two (a fresh CI VM's uptime near 128 s) the sum
        # rounds down by one ulp, and the hold would still run at its limit.
        now = float(int(time.monotonic()))
        client.display_state["sent_timestamps"][9] = now - 1.0
        self.assertTrue(client.hold_video(now))
        self.assertTrue(client.hold_video(now + ws_sink_module.SINK_HOLD_MAX_S - 0.1))
        self.assertFalse(client.hold_video(now + ws_sink_module.SINK_HOLD_MAX_S))
        self.assertFalse(client.display_state["sent_timestamps"])
        await ws.close()

    async def test_nothing_is_held_back_before_the_first_ack(self):
        ws = await self.connect()
        client = await self.client_with_keyframe(ws)
        client.display_state["sent_timestamps"][9] = time.monotonic() - 10
        self.assertFalse(client.queue_full(time.monotonic()))
        await ws.close()

    async def test_pausing_forgets_what_was_on_its_way(self):
        ws = await self.connect()
        client = await self.client_with_keyframe(ws)
        self.sink._note_ack(client, "CLIENT_FRAME_ACK 3 0")
        client.display_state["sent_timestamps"][9] = time.monotonic() - 10
        await self.sink._video_active(client, False)
        await self.sink._video_active(client, True)
        self.assertFalse(client.queue_full(time.monotonic()))
        await ws.close()


class ServiceContractTest(unittest.TestCase):
    """FakeService stands in for WebRTCService. Every name it re-implements
    must still exist on the real service with a compatible signature, or these
    tests would keep passing over an interface the sink no longer has."""

    NAMES = ("start_display_media", "stop_display_media", "handle_consumers_changed",
             "request_idr_for_display", "_server_settings_payload",
             "_display_config_payload", "_encoder_for_display", "_display_consumers")

    def test_fake_service_matches_the_real_one(self):
        for name in self.NAMES:
            with self.subTest(name=name):
                real = getattr(WebRTCService, name, None)
                self.assertTrue(callable(real), f"WebRTCService.{name} is gone")
                fake = getattr(FakeService, name)
                real_params = list(inspect.signature(real).parameters)[1:]
                fake_params = list(inspect.signature(fake).parameters)[1:]
                self.assertEqual(real_params, fake_params,
                                 f"{name} signature drifted")
                self.assertEqual(inspect.iscoroutinefunction(real),
                                 inspect.iscoroutinefunction(fake),
                                 f"{name} changed between sync and async")

    def test_the_sink_hooks_the_service_actually_exposes(self):
        for name in ("_fanout_cursor", "_reconcile_sink_clients"):
            self.assertTrue(hasattr(WebRTCService, name),
                            f"WebRTCService.{name} is gone")

    def test_the_service_still_builds_the_sink_attribute(self):
        # `ws_sink` is per instance and constructing a real service needs a
        # display, so the assignment is read off the compiled __init__
        # (`self.<name> = ...` puts the name in co_names).
        self.assertIn("ws_sink", WebRTCService.__init__.__code__.co_names,
                      "WebRTCService.__init__ no longer creates ws_sink")


class DispatchExtractionTest(unittest.IsolatedAsyncioTestCase):
    """The data channel must keep running the SAME filter chain the sink runs:
    it is one implementation of authority (ws_sink.dispatch_client_message) and
    RTCApp._on_input_channel_message is its other caller."""

    def rtc_self(self, seen):
        return SimpleNamespace(
            on_audio_consumer_active=lambda pid, active: seen.append(("audio", pid, active)),
            on_video_consumer_active=lambda pid, did, active: seen.append(("video", pid, active)),
            on_data_message=lambda msg, did, conn_id=None: seen.append((msg, did, conn_id)),
            _viewer_is_collaborator=lambda token: False,
            _secure_input_denied=lambda msg, token: False,
            _gamepad_denied=lambda msg, ctype, token, slot: False,
        )

    async def test_chain_order_is_preserved(self):
        seen = []
        rtc = self.rtc_self(seen)
        call = lambda msg, ctype=ClientType.CONTROLLER: RTCApp._on_input_channel_message(
            rtc, msg, channel=None, client_type=ctype, client_token=None,
            display_id="primary", peer_id="peer-1", client_slot=None)
        call("kd,65")
        self.assertEqual(seen[-1], ("kd,65", "primary", "peer-1"))
        call("START_AUDIO")
        self.assertEqual(seen[-1], ("audio", "peer-1", True))
        call("STOP_VIDEO")
        self.assertEqual(seen[-1], ("video", "peer-1", False))
        before = len(seen)
        # A viewer's SETTINGS is connection sync only, and its keyboard input
        # is refused; its own pause verb is not.
        call("SETTINGS,{}", ClientType.VIEWER)
        call("kd,65", ClientType.VIEWER)
        self.assertEqual(len(seen), before)
        call("REQUEST_KEYFRAME", ClientType.VIEWER)
        self.assertEqual(seen[-1], ("REQUEST_KEYFRAME", "primary", "peer-1"))


class SupervisorTest(unittest.IsolatedAsyncioTestCase):
    def supervisor(self, sink):
        return SimpleNamespace(
            services={"webrtc": SimpleNamespace(ws_sink=sink), "websockets": object()},
            current_mode="webrtc",
            settings=SimpleNamespace(enable_dual_mode=(True,), subfolder=""),
            _viewer_ceiling=lambda request: False,
            switch_to_mode=None,
        )

    async def test_status_reports_the_sink_and_its_clients(self):
        sink = SimpleNamespace(client_count=lambda: 2)
        status = CentralizedStreamServer._get_status(self.supervisor(sink))
        self.assertTrue(status["ws_sink"])
        self.assertEqual(status["ws_clients"], 2)

    async def test_status_without_a_sink(self):
        status = CentralizedStreamServer._get_status(self.supervisor(None))
        self.assertFalse(status["ws_sink"])
        self.assertEqual(status["ws_clients"], 0)

    async def test_switch_is_refused_while_the_sink_serves(self):
        supervisor = self.supervisor(SimpleNamespace(client_count=lambda: 0))
        request = SimpleNamespace(headers={"X-Selkies-Switch-Token": "tok"})
        with unittest.mock.patch.dict(os.environ, {"SELKIES_SWITCH_TOKEN": "tok"}):
            response = await CentralizedStreamServer.handle_switch(supervisor, request)
        self.assertEqual(response.status, 409)
        self.assertEqual(json.loads(response.body), {"error": "ws_sink_active"})

    async def test_route_is_taken_only_with_an_enabled_sink(self):
        sink = SimpleNamespace(handle_request="handler")
        supervisor = self.supervisor(sink)
        request = SimpleNamespace(path="/api/websockets",
                                  app={"settings": supervisor.settings})
        self.assertEqual(
            CentralizedStreamServer._ws_sink_handler(supervisor, request), "handler")
        # A WebRTC service always owns the sink route.
        self.assertIsNone(
            CentralizedStreamServer._ws_sink_handler(self.supervisor(None), request))
        # So does any other path.
        other = SimpleNamespace(path="/api/status", app={"settings": supervisor.settings})
        self.assertIsNone(CentralizedStreamServer._ws_sink_handler(supervisor, other))

if __name__ == "__main__":
    unittest.main(verbosity=2)
