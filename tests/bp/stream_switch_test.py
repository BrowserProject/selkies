import asyncio
import json
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

from selkies.selkies import DataStreamingServer
from selkies.stream_server import CentralizedStreamServer


class SwitchCredentialTest(unittest.IsolatedAsyncioTestCase):
    async def call_switch(self, configured, presented, viewer=False):
        request = SimpleNamespace(
            headers={} if presented is None else {"X-Selkies-Switch-Token": presented},
            json=AsyncMock(return_value={"mode": "websockets"}),
        )
        server = SimpleNamespace(
            _viewer_ceiling=lambda request: viewer,
            settings=SimpleNamespace(enable_dual_mode=(True,)),
            switch_to_mode=AsyncMock(),
        )
        with patch.dict("os.environ", {"SELKIES_SWITCH_TOKEN": configured}):
            response = await CentralizedStreamServer.handle_switch(server, request)
        return response, server

    async def test_missing_configuration_fails_closed(self):
        response, server = await self.call_switch("", "")
        self.assertEqual(response.status, 403)
        server.switch_to_mode.assert_not_awaited()

    async def test_missing_wrong_and_nonascii_headers_cannot_switch(self):
        for presented in (None, "wrong", "ümlaut"):
            with self.subTest(presented=presented):
                response, server = await self.call_switch("test-private-token", presented)
                self.assertEqual(response.status, 403)
                server.switch_to_mode.assert_not_awaited()

    async def test_private_api_credential_switches(self):
        response, server = await self.call_switch("test-private-token", "test-private-token")
        self.assertEqual(response.status, 200)
        self.assertEqual(json.loads(response.body), {"status": "success", "mode": "websockets"})
        server.switch_to_mode.assert_awaited_once_with("websockets")

    async def test_viewer_ceiling_is_still_enforced(self):
        response, server = await self.call_switch("test-private-token", "test-private-token", True)
        self.assertEqual(response.status, 403)
        server.switch_to_mode.assert_not_awaited()


class FlipStormTest(unittest.IsolatedAsyncioTestCase):
    """Back-to-back mode switches must hand the display over cleanly.

    `switch_to_mode` starts a service with `asyncio.create_task`, and a
    service's `start()` clears its own shutdown event before it awaits
    anything. A stop that lands between the two is therefore erased by the
    start it was meant to cancel: the service waits on an event nobody sets
    again, and the next switch spends the full 15 s grace period before
    force-cancelling a half-started service. That is what a viewer sees as a
    container that answers "connected" and then never sends a frame.
    """

    class Service:
        """Upstream's service contract, verbatim (`WebRTCService`,
        `DataStreamingServer`): start() clears the shutdown event and waits on
        it, stop() sets it."""

        def __init__(self, name, events):
            self.name = name
            self.events = events
            self.shutdown_event = asyncio.Event()
            self.live = False
            self.force_cancelled = False

        async def start(self):
            self.shutdown_event.clear()
            self.live = True
            self.events.append(f"start:{self.name}")
            try:
                await self.shutdown_event.wait()
            except asyncio.CancelledError:
                self.force_cancelled = True
                raise
            finally:
                self.live = False
                self.events.append(f"stopped:{self.name}")

        async def stop(self):
            self.shutdown_event.set()

    def make_supervisor(self, services):
        supervisor = SimpleNamespace(
            services=services,
            current_mode=None,
            active_task=None,
            lock=asyncio.Lock(),
            settings=SimpleNamespace(
                mode="websockets",
                apply_webrtc_encoder_filter=lambda: None,
                resolve_rate_control_default=lambda: None,
            ),
        )
        supervisor._stop_service = lambda: CentralizedStreamServer._stop_service(
            supervisor
        )
        return supervisor

    async def _flip_storm(self, supervisor, services):
        """Eight alternating switches with nothing awaited in between.

        Nothing here yields to the event loop except the switches themselves,
        which is the point: a switch that leaves the loop a turn to run the
        service it just created would hide the race.
        """
        for index in range(8):
            mode = "webrtc" if index % 2 else "websockets"
            await CentralizedStreamServer.switch_to_mode(supervisor, mode)
            self.assertEqual(supervisor.current_mode, mode)
            self.assertTrue(services[mode].live, f"{mode} not running after switch")
            other = "websockets" if mode == "webrtc" else "webrtc"
            self.assertFalse(
                services[other].live, f"{other} still running after switch to {mode}"
            )
        # Leave nothing running behind the test.
        await CentralizedStreamServer._stop_service(supervisor)

    async def test_flip_storm_hands_the_display_over_every_time(self):
        events = []
        services = {
            name: self.Service(name, events) for name in ("websockets", "webrtc")
        }
        supervisor = self.make_supervisor(services)

        # A ceiling far below the supervisor's 15 s teardown grace: a storm
        # that has to wait one out is the defect, not slow teardown.
        await asyncio.wait_for(self._flip_storm(supervisor, services), timeout=5)

        for name, service in services.items():
            self.assertFalse(service.force_cancelled, f"{name} was force-cancelled")
        # Every start was followed by its own stop before the next start.
        live = None
        for event in events:
            kind, _, name = event.partition(":")
            if kind == "start":
                self.assertIsNone(live, f"{name} started while {live} was still live")
                live = name
            else:
                self.assertEqual(live, name)
                live = None
        self.assertEqual(events.count("start:websockets"), 4)
        self.assertEqual(events.count("start:webrtc"), 4)

    async def test_immediate_start_failure_is_reported_to_the_switch_caller(self):
        class FailingService(self.Service):
            async def start(self):
                self.shutdown_event.clear()
                raise RuntimeError("startup failed")

        events = []
        services = {
            "websockets": self.Service("websockets", events),
            "webrtc": FailingService("webrtc", events),
        }
        supervisor = self.make_supervisor(services)

        with self.assertRaisesRegex(RuntimeError, "startup failed"):
            await CentralizedStreamServer.switch_to_mode(supervisor, "webrtc")

        self.assertIsNone(supervisor.current_mode)
        self.assertIsNone(supervisor.active_task)


class WebSocketShutdownLifecycleTest(unittest.IsolatedAsyncioTestCase):
    class Socket:
        def __init__(self, on_close=None):
            self.on_close = on_close

        async def close(self, code=None, message=None):
            if self.on_close:
                self.on_close()

    def make_server(self, socket):
        server = DataStreamingServer.__new__(DataStreamingServer)
        server._shutdown_called = False
        server.RECONNECT_GRACE_S = 0.01
        server.clients = {socket}
        server.video_paused_clients = set()
        server.display_clients = {"primary": {"ws": socket}}
        server.capture_instances = {}
        server._persistent_capture_modules = {}
        server._tasks_to_run = []
        server._ws_handler_tasks = set()
        server._display_teardown_tasks = set()
        server.input_handler = None
        server.metrics = None
        server.app = object()
        server._report_client_presence = lambda: None
        server.shutdown_pipelines = AsyncMock()
        return server

    async def test_shutdown_drains_handlers_and_deferred_teardown(self):
        closed = asyncio.Event()
        socket = self.Socket(closed.set)
        server = self.make_server(socket)
        natural_cancelled = False
        cleanup_lock = asyncio.Lock()
        late_grace_ran = False

        async def natural_handler():
            nonlocal late_grace_ran
            nonlocal natural_cancelled
            try:
                await closed.wait()
                async with cleanup_lock:
                    pass
                async def late_grace():
                    nonlocal late_grace_ran
                    await asyncio.sleep(0.02)
                    late_grace_ran = True
                task = asyncio.create_task(late_grace())
                server._display_teardown_tasks.add(task)
            except asyncio.CancelledError:
                natural_cancelled = True
                raise

        natural = asyncio.create_task(natural_handler())
        await asyncio.sleep(0)
        server._ws_handler_tasks.add(natural)

        lock = asyncio.Lock()
        entered = asyncio.Event()

        async def stuck_handler():
            async with lock:
                entered.set()
                await asyncio.Future()

        stuck = asyncio.create_task(stuck_handler())
        await entered.wait()
        server._ws_handler_tasks.add(stuck)

        stale_grace_ran = False
        grace_entered = asyncio.Event()

        async def stale_grace():
            nonlocal stale_grace_ran
            async with cleanup_lock:
                grace_entered.set()
                await asyncio.Future()
                stale_grace_ran = True

        grace = asyncio.create_task(stale_grace())
        await grace_entered.wait()
        server._display_teardown_tasks.add(grace)

        await DataStreamingServer.shutdown(server)
        await asyncio.sleep(0.03)

        self.assertTrue(natural.done())
        self.assertFalse(natural_cancelled)
        self.assertTrue(stuck.cancelled())
        self.assertFalse(lock.locked())
        self.assertTrue(grace.cancelled())
        self.assertFalse(stale_grace_ran)
        self.assertFalse(late_grace_ran)
        self.assertEqual(server._ws_handler_tasks, set())
        self.assertEqual(server._display_teardown_tasks, set())

    async def test_shutdown_cancels_a_handler_suspended_during_prepare(self):
        socket = self.Socket()
        server = self.make_server(socket)
        server.clients.clear()
        server.display_clients.clear()
        server._accepting_ws_handlers = True
        server.mode = "websockets"
        server.supervisor = SimpleNamespace(current_mode="websockets")
        server.cli_args = SimpleNamespace(master_token="")
        prepare_started = asyncio.Event()

        class PreparingSocket(self.Socket):
            async def prepare(self, request):
                prepare_started.set()
                await asyncio.Future()

        preparing_socket = PreparingSocket()
        with patch("selkies.selkies.web.WebSocketResponse", return_value=preparing_socket):
            handler = asyncio.create_task(
                DataStreamingServer.data_ws_handler(server, SimpleNamespace())
            )
            await prepare_started.wait()
            self.assertIn(handler, server._ws_handler_tasks)
            await DataStreamingServer.shutdown(server)

        self.assertTrue(handler.cancelled())
        self.assertEqual(server._ws_handler_tasks, set())

    async def test_stopped_service_rejects_before_websocket_prepare(self):
        server = self.make_server(self.Socket())
        server._accepting_ws_handlers = False
        server.mode = "websockets"
        server.supervisor = SimpleNamespace(current_mode="websockets")
        server.cli_args = SimpleNamespace(master_token="")

        with patch("selkies.selkies.web.WebSocketResponse") as websocket:
            response = await DataStreamingServer.data_ws_handler(
                server, SimpleNamespace()
            )

        self.assertEqual(response.status, 409)
        websocket.assert_not_called()


class DisplayTakeoverTest(unittest.IsolatedAsyncioTestCase):
    def make_server(self, socket):
        state = {
            "ws": socket,
            "video_active": False,
            "acknowledged_frame_id": 42,
            "acked_sent_at": 1.0,
            "unacked_since": 2.0,
            "stall_gated_at": 3.0,
            "sent_timestamps": {42: 4.0},
            "rtt_samples": [5.0],
            "smoothed_rtt": 6.0,
        }
        server = SimpleNamespace(
            display_clients={"primary": state},
            _video_start_state=lambda ws, display_id: True,
            _schedule_idr_for_display=unittest.mock.Mock(),
        )
        return server, state

    async def test_current_owner_settings_do_not_reset_the_pipeline(self):
        current = SimpleNamespace(send_str=AsyncMock())
        server, state = self.make_server(current)
        before = dict(state)
        before["sent_timestamps"] = dict(state["sent_timestamps"])
        before["rtt_samples"] = list(state["rtt_samples"])

        changed = await DataStreamingServer._take_over_display_if_needed(
            server, current, "primary", False
        )

        self.assertFalse(changed)
        self.assertEqual(state, before)
        current.send_str.assert_not_awaited()
        server._schedule_idr_for_display.assert_not_called()

    async def test_replacement_socket_still_resets_and_claims_the_display(self):
        current = SimpleNamespace(send_str=AsyncMock())
        replacement = SimpleNamespace(send_str=AsyncMock())
        server, state = self.make_server(current)

        changed = await DataStreamingServer._take_over_display_if_needed(
            server, replacement, "primary", True
        )

        self.assertTrue(changed)
        self.assertIs(state["ws"], replacement)
        self.assertTrue(state["video_active"])
        self.assertEqual(state["acknowledged_frame_id"], -1)
        self.assertEqual(state["sent_timestamps"], {})
        self.assertEqual(state["rtt_samples"], [])
        replacement.send_str.assert_awaited_once_with("PIPELINE_RESETTING primary")
        server._schedule_idr_for_display.assert_called_once_with("primary")


if __name__ == "__main__":
    unittest.main()
