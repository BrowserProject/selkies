import asyncio
import unittest
from unittest.mock import Mock
from selkies.rtc import PipelineBridge


class BridgeTest(unittest.IsolatedAsyncioTestCase):
    async def test_short_scheduling_burst_preserves_every_reference(self):
        request = Mock()
        bridge = PipelineBridge(maxsize=3, request_keyframe=request)
        bridge.set_data(b'I', keyframe=True)
        bridge.set_data(b'P1', keyframe=False)
        bridge.set_data(b'P2', keyframe=False)
        self.assertEqual([await asyncio.wait_for(bridge.get_data(), 1) for _ in range(3)], [b'I', b'P1', b'P2'])
        request.assert_not_called()

    async def test_overflow_keeps_decodable_prefix_and_gates_missing_reference(self):
        request = Mock()
        bridge = PipelineBridge(maxsize=3, request_keyframe=request)
        for data in (b'I', b'P1', b'P2', b'P3', b'P4'):
            bridge.set_data(data, keyframe=data == b'I')
        self.assertEqual([await asyncio.wait_for(bridge.get_data(), 1) for _ in range(3)], [b'I', b'P1', b'P2'])
        self.assertTrue(bridge.empty())
        request.assert_called_once()
        bridge.set_data(b'I2', keyframe=True)
        bridge.set_data(b'P5', keyframe=False)
        self.assertEqual([await asyncio.wait_for(bridge.get_data(), 1) for _ in range(2)], [b'I2', b'P5'])

    async def test_full_queue_recovery_keyframe_does_not_leave_orphan_deltas(self):
        bridge = PipelineBridge(maxsize=3, request_keyframe=Mock())
        for data in (b'I', b'P1', b'P2', b'I2'):
            bridge.set_data(data, keyframe=data.startswith(b'I'))
        self.assertEqual(await asyncio.wait_for(bridge.get_data(), 1), b'I2')
        self.assertTrue(bridge.empty())
        self.assertEqual(bridge.dropped, 3)

    async def test_unread_bridge_asks_for_no_keyframe(self):
        clock = Mock(return_value=0.0)
        request = Mock()
        bridge = PipelineBridge(maxsize=3, request_keyframe=request, clock=clock)
        bridge.set_data(b'I', keyframe=True)
        for i in range(150):
            clock.return_value = i / 30
            bridge.set_data(b'P', keyframe=False)
        request.assert_not_called()
        self.assertEqual([await asyncio.wait_for(bridge.get_data(), 1) for _ in range(3)], [b'I', b'P', b'P'])
        request.assert_called_once()

    async def test_gate_timeout_runs_from_the_first_read(self):
        clock = Mock(return_value=0.0)
        bridge = PipelineBridge(maxsize=1, request_keyframe=Mock(), clock=clock)
        bridge.set_data(b'I', keyframe=True)
        bridge.set_data(b'P1', keyframe=False)
        clock.return_value = 10.0
        self.assertEqual(await asyncio.wait_for(bridge.get_data(), 1), b'I')
        clock.return_value = 10.5
        bridge.set_data(b'P2', keyframe=False)
        self.assertTrue(bridge.empty())
        bridge.set_data(b'I2', keyframe=True)
        self.assertEqual(await asyncio.wait_for(bridge.get_data(), 1), b'I2')

    async def test_reader_that_stopped_asks_no_more(self):
        clock = Mock(return_value=0.0)
        request = Mock()
        bridge = PipelineBridge(maxsize=1, request_keyframe=request, clock=clock)
        bridge.set_data(b'I', keyframe=True)
        self.assertEqual(await asyncio.wait_for(bridge.get_data(), 1), b'I')
        bridge.set_data(b'P1', keyframe=False)
        bridge.set_data(b'P2', keyframe=False)
        request.assert_called_once()
        for i in range(30):
            clock.return_value = 1 + i / 30
            bridge.set_data(b'P', keyframe=False)
        request.assert_called_once()

    async def test_audio_retains_drop_oldest_behavior(self):
        bridge = PipelineBridge(maxsize=2)
        for data in (b'a', b'b', b'c'):
            bridge.set_data(data)
        self.assertEqual([await asyncio.wait_for(bridge.get_data(), 1) for _ in range(2)], [b'b', b'c'])


if __name__ == '__main__':
    unittest.main()
