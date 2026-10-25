import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock

from selkies.rtc import RTCApp
from selkies.webrtc_mode import PEER_CLOSED_CLOSE_CODE, PEER_CLOSED_CLOSE_REASON, WebRTCService


def app(state):
    order = []
    peer = {'peer_conn': SimpleNamespace(connectionState=state), 'client_type': 'controller',
            'display_id': 'primary'}
    rtc = SimpleNamespace(
        peer_connections={'peer-1': peer},
        _reap_peer=AsyncMock(side_effect=lambda *args: order.append('reap')),
        on_peer_closed=AsyncMock(side_effect=lambda peer_id: order.append(('closed', peer_id))),
        on_peer_connection_established=AsyncMock())
    return rtc, order


class PeerClosedSignalTest(unittest.IsolatedAsyncioTestCase):
    async def test_a_peer_the_server_closed_is_signalled_after_its_reap(self):
        rtc, order = app('closed')
        await RTCApp.on_connectionstatechange(rtc, 'peer-1')
        self.assertEqual(order, ['reap', ('closed', 'peer-1')])
        self.assertNotIn('peer-1', rtc.peer_connections)

    async def test_an_explicit_stop_sends_no_peer_closed_signal(self):
        rtc, order = app('closed')
        rtc.peer_connections.clear()
        await RTCApp.on_connectionstatechange(rtc, 'peer-1')
        self.assertEqual(order, [])

    async def test_other_states_send_no_peer_closed_signal(self):
        for state in ('connecting', 'connected', 'disconnected'):
            rtc, _ = app(state)
            await RTCApp.on_connectionstatechange(rtc, 'peer-1')
            rtc.on_peer_closed.assert_not_awaited()

    async def test_a_failing_hook_is_logged_not_raised(self):
        rtc, _ = app('closed')
        rtc.on_peer_closed = AsyncMock(side_effect=RuntimeError('socket gone'))
        with self.assertLogs('rtc', level='ERROR'):
            await RTCApp.on_connectionstatechange(rtc, 'peer-1')

    async def test_the_viewer_signaling_socket_closes_with_the_peer_closed_code(self):
        ws = SimpleNamespace(closed=False, close=AsyncMock())
        other = SimpleNamespace(closed=False, close=AsyncMock())
        service = SimpleNamespace(peer_manager=SimpleNamespace(
            lock=asyncio.Lock(), peers={'peer-1': SimpleNamespace(ws=ws), 'peer-2': SimpleNamespace(ws=other)}))
        service._close_peer_signaling_ws = lambda *args: WebRTCService._close_peer_signaling_ws(service, *args)
        with self.assertLogs('webrtc', level='INFO') as logs:
            await WebRTCService.handle_peer_closed(service, 'peer-1')
        ws.close.assert_awaited_once_with(code=PEER_CLOSED_CLOSE_CODE, message=PEER_CLOSED_CLOSE_REASON)
        other.close.assert_not_awaited()
        self.assertEqual((PEER_CLOSED_CLOSE_CODE, PEER_CLOSED_CLOSE_REASON), (4003, b'Peer connection closed'))
        self.assertIn('Closed signaling of peer peer-1 with 4003', logs.output[0])

    async def test_a_peer_whose_signaling_already_left_is_a_no_op(self):
        gone = SimpleNamespace(closed=True, close=AsyncMock())
        for peers in ({}, {'peer-1': SimpleNamespace(ws=gone)}):
            service = SimpleNamespace(peer_manager=SimpleNamespace(lock=asyncio.Lock(), peers=peers))
            service._close_peer_signaling_ws = lambda *args, s=service: WebRTCService._close_peer_signaling_ws(s, *args)
            with self.assertNoLogs('webrtc', level='INFO'):
                await WebRTCService.handle_peer_closed(service, 'peer-1')
        gone.close.assert_not_awaited()


if __name__ == '__main__':
    unittest.main()
