"""Per-connection scopes (connection_scopes): what the relay grants one
connection holds on every channel of it, the data WebSocket, the WebSocket
sink, the signaling socket, the WebRTC data channel and the file routes."""

import os

# Read once at import, like the container reads it at start.
os.environ["SELKIES_CONNECTION_SCOPES"] = "true"

import asyncio
import json
import unittest
import unittest.mock
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

from aiohttp import WSMsgType, web
from aiohttp.test_utils import TestClient, TestServer, make_mocked_request

from selkies import connection_scopes
from selkies import selkies as selkies_module
from selkies import webrtc_mode
from selkies.rtc import ClientType, RTCApp
from selkies.selkies import DataStreamingServer, SelkiesStreamingApp
from selkies.stream_server import CentralizedStreamServer
from selkies.ws_sink import WsSink, dispatch_client_message

from signaling_keepalive_test import manager
from ws_sink_test import FakeService

VIEW = frozenset({"stream"})
INPUT_ONLY = frozenset({"stream", "input"})
FULL = frozenset({"stream", "input", "clipboard", "files"})

INPUT_MESSAGES = ["kd,65", "ku,65", "kh,65", "kr", "m,10,10,1,0", "m2,1,1,0,0",
                  "co,end,typed", "js,b,0,0,1", "cmd,xterm", "ctl,take"]
CLIPBOARD_MESSAGES = ["cw,c2VjcmV0", "cb,image/png,AAAA", "cws,1,text,6", "cr",
                      "REQUEST_CLIPBOARD", "_ebc,true"]
VIEW_MESSAGES = ["REQUEST_KEYFRAME", "STOP_VIDEO", "START_VIDEO", "_f,30", "_l,12", "ctl,status"]


def dispatch(msg, scopes, is_viewer=False):
    seen = []
    dispatch_client_message(
        msg, is_viewer=is_viewer, is_collaborator=lambda: True, display_id="primary",
        conn_id="c1", on_gz_handshake=lambda: seen.append("_gz"),
        on_audio_consumer_active=None,
        on_video_consumer_active=None,
        on_data_message=lambda m, d, conn_id=None: seen.append(m),
        input_denied=lambda m: False, js_denied=lambda m: False, scopes=scopes)
    return seen


class HeaderTest(unittest.TestCase):
    def request(self, *values):
        headers = [(connection_scopes.SCOPES_HEADER, v) for v in values]
        return make_mocked_request("GET", "/api/websockets", headers=headers)

    def test_one_header_names_the_scopes(self):
        self.assertEqual(connection_scopes.from_request(self.request("stream, input")), INPUT_ONLY)

    def test_unknown_names_grant_nothing(self):
        self.assertEqual(connection_scopes.from_request(self.request("stream,admin,cdp")), VIEW)

    def test_a_missing_or_repeated_header_grants_nothing(self):
        self.assertEqual(connection_scopes.from_request(self.request()), frozenset())
        self.assertEqual(connection_scopes.from_request(self.request("input", "input")), frozenset())

    def test_scopes_that_never_reached_a_gate_grant_nothing(self):
        self.assertFalse(connection_scopes.allows(None, connection_scopes.INPUT))

    def test_without_enforcement_everything_is_upstream_behaviour(self):
        with unittest.mock.patch.object(connection_scopes, "ENFORCED", False):
            self.assertIsNone(connection_scopes.from_request(self.request("stream")))
            self.assertTrue(connection_scopes.allows(None, connection_scopes.CLIPBOARD))
            self.assertFalse(connection_scopes.message_denied("kd,65", VIEW))
            handler = AsyncMock()
            self.assertIs(connection_scopes.guard(self.request(), handler, ""), handler)


class GuardTest(unittest.IsolatedAsyncioTestCase):
    async def run_guarded(self, path, scopes, credential_ceiling="controller"):
        headers = {connection_scopes.SCOPES_HEADER: scopes} if scopes is not None else {}
        request = make_mocked_request("GET", path, headers=headers)
        seen = {}

        async def handler(req):
            seen["ceiling"] = req.get("auth_role_ceiling")
            return web.json_response({"status": "ok"})

        guarded = connection_scopes.guard(request, handler, "")
        # A credential gate assigns its ceiling just before it calls the route.
        request["auth_role_ceiling"] = credential_ceiling
        response = await guarded(request)
        return response, seen, request

    async def test_file_routes_need_the_files_scope(self):
        for path in ("/api/upload", "/api/files", "/api/files/report.pdf"):
            response, seen, _ = await self.run_guarded(path, "stream,input,clipboard")
            self.assertEqual(response.status, 403, path)
            self.assertEqual(json.loads(response.body), {"status": "denied", "code": "scope"})
            self.assertEqual(seen, {})
            response, _, _ = await self.run_guarded(path, "stream,input,files")
            self.assertEqual(response.status, 200, path)

    async def test_without_input_the_connection_is_a_viewer_whatever_its_credential(self):
        _, seen, request = await self.run_guarded("/api/websockets", "stream,clipboard")
        self.assertEqual(seen["ceiling"], "viewer")
        self.assertEqual(request[connection_scopes.REQUEST_KEY], frozenset({"stream", "clipboard"}))
        _, seen, _ = await self.run_guarded("/api/ws", None)
        self.assertEqual(seen["ceiling"], "viewer")

    async def test_input_leaves_the_credential_ceiling_alone(self):
        _, seen, _ = await self.run_guarded("/api/websockets", "stream,input")
        self.assertEqual(seen["ceiling"], "controller")

    async def test_the_auth_middleware_runs_every_route_under_the_scopes(self):
        app = web.Application()
        app["settings"] = SimpleNamespace(subfolder="")
        server = SimpleNamespace(_ws_sink_handler=lambda request: None,
                                 _authorize=lambda request, handler: handler(request))
        reached = []

        async def route(request):
            reached.append(request.get("auth_role_ceiling"))
            return web.json_response({"status": "ok"})

        upload = make_mocked_request("POST", "/api/upload", app=app,
                                     headers={connection_scopes.SCOPES_HEADER: "stream,input"})
        response = await CentralizedStreamServer._auth_middleware(server, upload, route)
        self.assertEqual(response.status, 403)
        socket = make_mocked_request("GET", "/api/ws", app=app,
                                     headers={connection_scopes.SCOPES_HEADER: "stream"})
        response = await CentralizedStreamServer._auth_middleware(server, socket, route)
        self.assertEqual(response.status, 200)
        self.assertEqual(reached, ["viewer"])
        self.assertEqual(socket[connection_scopes.REQUEST_KEY], VIEW)


class DispatchTest(unittest.TestCase):
    """The filter chain the WebSocket sink and the WebRTC data channel share."""

    def test_a_view_only_connection_sends_no_input_and_no_clipboard(self):
        for msg in INPUT_MESSAGES + CLIPBOARD_MESSAGES:
            self.assertEqual(dispatch(msg, VIEW), [], msg)
        for msg in VIEW_MESSAGES:
            self.assertEqual(dispatch(msg, VIEW), [msg], msg)

    def test_input_without_clipboard_keeps_the_clipboard_closed(self):
        for msg in INPUT_MESSAGES:
            self.assertEqual(dispatch(msg, INPUT_ONLY), [msg], msg)
        for msg in CLIPBOARD_MESSAGES:
            self.assertEqual(dispatch(msg, INPUT_ONLY), [], msg)

    def test_a_full_grant_passes_everything(self):
        for msg in INPUT_MESSAGES + CLIPBOARD_MESSAGES + VIEW_MESSAGES:
            self.assertEqual(dispatch(msg, FULL), [msg], msg)

    def test_a_collaborator_viewer_still_needs_the_scopes(self):
        self.assertEqual(dispatch("kd,65", VIEW, is_viewer=True), [])
        self.assertEqual(dispatch("kd,65", FULL, is_viewer=True), ["kd,65"])

    def test_unknown_scopes_fail_closed(self):
        self.assertEqual(dispatch("kd,65", None), [])
        self.assertEqual(dispatch("REQUEST_KEYFRAME", None), ["REQUEST_KEYFRAME"])


class DataChannelTest(unittest.TestCase):
    def rtc_self(self, seen):
        return SimpleNamespace(
            on_audio_consumer_active=lambda pid, active: seen.append(("audio", active)),
            on_video_consumer_active=lambda pid, did, active: seen.append(("video", active)),
            on_data_message=lambda msg, did, conn_id=None: seen.append(msg),
            _viewer_is_collaborator=lambda token: False,
            _secure_input_denied=lambda msg, token: False,
            _gamepad_denied=lambda msg, ctype, token, slot: False,
            peer_connections={},
        )

    def call(self, scopes, msg):
        seen = []
        RTCApp._on_input_channel_message(
            self.rtc_self(seen), msg, channel=None, client_type=ClientType.CONTROLLER,
            client_token=None, display_id="primary", peer_id="peer-1", client_slot=None,
            scopes=scopes)
        return seen

    def test_the_data_channel_drops_what_the_scopes_do_not_cover(self):
        for msg in INPUT_MESSAGES + CLIPBOARD_MESSAGES:
            self.assertEqual(self.call(VIEW, msg), [], msg)
            self.assertEqual(self.call(FULL, msg), [msg], msg)
        self.assertEqual(self.call(VIEW, "STOP_VIDEO"), [("video", False)])

    def test_the_channel_handler_is_built_with_the_peer_scopes(self):
        # The data channel never passes the relay: its handler carries the
        # scopes of the signaling connection that negotiated it.
        code = RTCApp._start_rtc_pipeline.__code__
        self.assertIn("scopes", code.co_varnames[:code.co_argcount])
        self.assertIn("scopes", RTCApp._on_input_channel_message.__code__.co_varnames)

    def test_start_rtc_connection_hands_the_scopes_on(self):
        rtc = SimpleNamespace(_start_rtc_pipeline=AsyncMock())
        asyncio.run(RTCApp.start_rtc_connection(rtc, "peer-1", "viewer", None, "primary", None, VIEW))
        rtc._start_rtc_pipeline.assert_awaited_once_with("peer-1", "viewer", None, "primary", None, VIEW)


class SignalingTest(unittest.IsolatedAsyncioTestCase):
    async def test_the_peer_keeps_its_connection_scopes(self):
        peers = manager()
        sent = []
        hello = json.dumps({"client_type": "controller", "client_slot": -1})
        ws = SimpleNamespace(closed=False, send_str=AsyncMock(side_effect=sent.append),
                             receive=AsyncMock(return_value=SimpleNamespace(
                                 type=WSMsgType.TEXT, data=f"HELLO client {hello}")))
        puid, _ptype, client_type, _slot, _strict = await peers.hello_peer(
            ws, ("127.0.0.1", 1), "viewer", VIEW)
        self.assertEqual(peers.peers[puid].scopes, VIEW)
        self.assertEqual(client_type, "viewer")
        self.assertEqual(sent, ["HELLO"])

    async def test_session_start_passes_the_peer_scopes_to_the_peer_connection(self):
        rtc_app = SimpleNamespace(start_rtc_connection=AsyncMock())
        service = SimpleNamespace(
            peer_manager=SimpleNamespace(peers={"client-1": SimpleNamespace(client_slot=None, scopes=VIEW)}),
            rtc_app=rtc_app, args=SimpleNamespace(enable_webrtc_statistics=False), metrics=None)
        await webrtc_mode.WebRTCService.handle_session_start(service, "client-1", "controller")
        rtc_app.start_rtc_connection.assert_awaited_once_with(
            "client-1", "controller", None, "primary", None, VIEW)


class FakeChannel:
    readyState = "open"
    bufferedAmount = 0

    def __init__(self):
        self.sent = []

    def send(self, payload):
        self.sent.append(payload)


class ClipboardOutboundTest(unittest.IsolatedAsyncioTestCase):
    """The server's clipboard reaches only connections holding the scope."""

    async def test_data_channel_peers_without_the_scope_get_no_clipboard(self):
        rtc = RTCApp.__new__(RTCApp)
        rtc.on_ws_clipboard = None
        rtc.on_ws_system = None
        view, full = FakeChannel(), FakeChannel()
        connected = SimpleNamespace(connectionState="connected")
        rtc.peer_connections = {
            "view": {"peer_conn": connected, "data_channel": view, "scopes": VIEW},
            "full": {"peer_conn": connected, "data_channel": full, "scopes": FULL},
        }
        await rtc.send_clipboard_data("secret")
        await rtc.send_clipboard_data("answer", reply_to="cr", peer_id="view")
        self.assertEqual(view.sent, [])
        self.assertEqual([json.loads(p)["type"] for p in full.sent], ["clipboard-msg"])

    async def test_websockets_clients_without_the_scope_get_no_clipboard(self):
        class Socket:
            closed = False
            _ws_gz = False

            def __init__(self):
                self.send_str = AsyncMock()
        view, full = Socket(), Socket()
        app = SelkiesStreamingApp.__new__(SelkiesStreamingApp)
        app.data_streaming_server = SimpleNamespace(clients={view, full}, enable_binary_clipboard=True)
        with unittest.mock.patch.dict(selkies_module.client_permissions,
                                      {view: {"scopes": VIEW}, full: {"scopes": FULL}}):
            await app.send_ws_clipboard_data("secret")
        view.send_str.assert_not_awaited()
        full.send_str.assert_awaited()

    async def test_the_websockets_service_takes_the_scopes_from_the_request(self):
        server = SimpleNamespace(
            supervisor=SimpleNamespace(current_mode="websockets"), mode="websockets",
            _accepting_ws_handlers=True, cli_args=SimpleNamespace(master_token=""),
            _ws_handler_tasks=set(), ws_handler=AsyncMock(),
            _report_client_presence=lambda: None)
        request = make_mocked_request("GET", "/api/websockets")
        request[connection_scopes.REQUEST_KEY] = VIEW
        with unittest.mock.patch("selkies.selkies.web.WebSocketResponse",
                                 return_value=SimpleNamespace(prepare=AsyncMock())):
            await DataStreamingServer.data_ws_handler(server, request)
        self.assertEqual(server.ws_handler.await_args.kwargs["scopes"], VIEW)


class SinkTest(unittest.IsolatedAsyncioTestCase):
    """A live WebSocket sink behind the scope guard."""

    async def asyncSetUp(self):
        self.service = FakeService(asyncio.get_running_loop())
        self.sink = WsSink(self.service)
        self.service.ws_sink = self.sink

        @web.middleware
        async def scoped(request, handler):
            return await connection_scopes.guard(request, handler, "")(request)

        app = web.Application(middlewares=[scoped])
        app.router.add_get("/api/websockets", self.sink.handle_request)
        self.client = TestClient(TestServer(app))
        await self.client.start_server()

    async def asyncTearDown(self):
        await self.sink.stop()
        await self.client.close()

    async def connect(self, scopes):
        self.sink._last_connection_times.clear()
        headers = {connection_scopes.SCOPES_HEADER: scopes} if scopes is not None else {}
        ws = await self.client.ws_connect("/api/websockets", headers=headers)
        for _ in range(5):
            await asyncio.wait_for(ws.receive(), timeout=5)
        return ws

    def dispatched(self):
        return [m for m, _d, _c in self.service.input_handler.messages]

    async def send_all(self, ws):
        for msg in INPUT_MESSAGES + CLIPBOARD_MESSAGES + ["REQUEST_KEYFRAME"]:
            await ws.send_str(msg)
        await asyncio.sleep(0.3)

    async def test_a_view_only_sink_client_reaches_only_the_view_verbs(self):
        ws = await self.connect("stream")
        await self.send_all(ws)
        self.assertEqual(self.dispatched(), ["REQUEST_KEYFRAME"])
        await ws.close()

    async def test_a_sink_client_without_scopes_is_view_only(self):
        ws = await self.connect(None)
        await self.send_all(ws)
        self.assertEqual(self.dispatched(), ["REQUEST_KEYFRAME"])
        await ws.close()

    async def test_a_full_grant_drives_input_and_clipboard(self):
        ws = await self.connect("stream,input,clipboard")
        await ws.send_str("kd,65")
        await ws.send_str("cw,c2VjcmV0")
        await asyncio.sleep(0.3)
        self.assertEqual(self.dispatched(), ["kd,65", "cw,c2VjcmV0"])
        await ws.close()

    async def test_clipboard_announcements_skip_clients_without_the_scope(self):
        view = await self.connect("stream,input")
        full = await self.connect("stream,input,clipboard")
        await self.sink.push_clipboard("secret", "text/plain", None, None)

        async def texts(ws):
            out = []
            while True:
                try:
                    msg = await asyncio.wait_for(ws.receive(), timeout=0.5)
                except asyncio.TimeoutError:
                    return out
                if msg.type == WSMsgType.TEXT:
                    out.append(msg.data)
        self.assertFalse([t for t in await texts(view) if t.startswith("clipboard")])
        self.assertIn("clipboard,c2VjcmV0", await texts(full))
        await view.close()
        await full.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
