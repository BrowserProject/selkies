import errno
import os
import tempfile
import unittest
from unittest.mock import patch
from selkies.webrtc_utils import RTCConfigFileMonitor


class RTCConfigFileMonitorTests(unittest.IsolatedAsyncioTestCase):
    """Patch 0019: a refused watch costs the live reload, never the WebRTC service."""

    async def asyncSetUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.rtc_file = os.path.join(self.directory.name, 'rtc.json')
        with open(self.rtc_file, 'w') as f:
            f.write('{"iceServers": []}')

    async def asyncTearDown(self):
        self.directory.cleanup()

    async def test_inotify_limit_disables_the_monitor(self):
        monitor = RTCConfigFileMonitor(self.rtc_file)
        refused = OSError(errno.EMFILE, 'inotify instance limit reached')
        with patch.object(monitor.observer, 'start', side_effect=refused):
            with self.assertLogs('rtcice', level='WARNING') as logs:
                await monitor.start()
        self.assertFalse(monitor.enabled)
        self.assertIn('inotify instance limit reached', logs.output[0])
        await monitor.stop()

    async def test_other_failures_still_propagate(self):
        monitor = RTCConfigFileMonitor(self.rtc_file)
        with patch.object(monitor.observer, 'start', side_effect=RuntimeError('broken observer')):
            with self.assertRaises(RuntimeError):
                await monitor.start()

    async def test_a_granted_watch_runs_until_stopped(self):
        monitor = RTCConfigFileMonitor(self.rtc_file)
        await monitor.start()
        self.assertTrue(monitor.enabled)
        self.assertTrue(monitor.observer.is_alive())
        await monitor.stop()
        self.assertFalse(monitor.observer.is_alive())


if __name__ == '__main__':
    unittest.main()
