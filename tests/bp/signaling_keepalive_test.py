import unittest
from types import SimpleNamespace
from unittest.mock import Mock

from aiohttp import WSMessage, WSMsgType

from selkies import webrtc_mode
from selkies.signaling_server import WebRTCPeerManagement

# The longest idle the web filters seen dropping viewers' signaling sockets allow.
FILTER_IDLE_S = 15


def manager():
    service = SimpleNamespace(supervisor=SimpleNamespace(set_clients_present=Mock()), args=SimpleNamespace(
        rtc_config_json=None, turn_shared_secret=None, turn_host=None, turn_port=None, turn_protocol='udp',
        turn_tls=False, turn_rest_username_auth_header='x-auth-user', stun_host=None, stun_port=None,
        enable_sharing=True, enable_shared=True, enable_player2=False, enable_player3=False,
        enable_player4=False))
    webrtc_mode.WebRTCService.create_peer_manager(service, rtc_config={'iceServers': []})
    return service.peer_manager


class SignalingKeepaliveTest(unittest.IsolatedAsyncioTestCase):
    def test_an_idle_signaling_socket_is_pinged_before_a_web_filter_drops_it(self):
        self.assertEqual(manager().keepalive_timeout, webrtc_mode.SIGNALING_KEEPALIVE_S)
        self.assertLess(webrtc_mode.SIGNALING_KEEPALIVE_S, FILTER_IDLE_S)

    async def test_a_socket_lost_without_a_close_frame_is_a_peer_close(self):
        ws = SimpleNamespace(closed=False, close_code=None)

        async def receive():
            # aiohttp after the TCP connection ended without a close frame.
            ws.closed, ws.close_code = True, 1006
            return WSMessage(WSMsgType.CLOSED, None, None)
        ws.receive = receive
        with self.assertLogs('signaling', level='INFO') as logs:
            await manager().peer_connection_handler(ws, ('198.51.100.7', 1), 'client-1', 'client', 'controller')
        self.assertIn("Peer connection closed: 'client-1'", '\n'.join(logs.output))
        self.assertFalse([line for line in logs.output if line.startswith(('WARNING', 'ERROR'))])


if __name__ == '__main__':
    unittest.main()
