import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock
from selkies.webrtc_mode import WebRTCService


class ResizeTest(unittest.IsolatedAsyncioTestCase):
    def service(self):
        service = SimpleNamespace(
            args=SimpleNamespace(enable_resize=True, congestion_control=True, manual_resolution=False),
            _server_locked_dims=lambda: None, display_clients={},
            _stream_pixel_caps={'primary': 518400}, _stream_requested_dims={},
            _resize_primary_display=AsyncMock(),
        )
        service._display_setting = lambda did, key: service.display_clients.get(did, {}).get(key, getattr(service.args, key))
        return service

    async def test_auto_resize_remembers_requested_geometry_and_caps_encoder_geometry(self):
        service=self.service()
        await WebRTCService._on_resize_handler(service, '1920x1080')
        self.assertEqual(service._stream_requested_dims['primary'], (1920,1080))
        service._resize_primary_display.assert_awaited_once_with('960x540')
        await WebRTCService._on_resize_handler(service, '960x540', adaptive=True)
        self.assertEqual(service._stream_requested_dims['primary'], (1920,1080))

    async def test_manual_resolution_is_not_replaced_by_congestion_policy(self):
        service=self.service()
        service.args.manual_resolution=True
        await WebRTCService._on_resize_handler(service, '1920x1080')
        service._resize_primary_display.assert_awaited_once_with('1920x1080')

    async def test_secondary_manual_policy_is_independent_of_primary(self):
        service = self.service()
        service.display_clients = {'display2': {'manual_resolution': True}}
        service._stream_pixel_caps['display2'] = 518400
        service._display_setting = lambda did, key: service.display_clients.get(did, {}).get(key, getattr(service.args, key, False))
        service.reconfigure_displays = AsyncMock()
        await WebRTCService._on_resize_handler(service, '1280x720', 'display2')
        self.assertEqual(service.display_clients['display2']['width'], 1280)
        self.assertEqual(service.display_clients['display2']['height'], 720)
        service.args.manual_resolution = True
        service.display_clients['display2']['manual_resolution'] = False
        await WebRTCService._on_resize_handler(service, '1280x720', 'display2')
        self.assertEqual(service.display_clients['display2']['width'], 960)
        self.assertEqual(service.display_clients['display2']['height'], 540)

    async def test_invalid_and_disabled_resize_do_not_change_geometry(self):
        service=self.service()
        for value in ('0x0', '-1x1080', 'broken'):
            await WebRTCService._on_resize_handler(service, value)
        service.args.enable_resize=False
        await WebRTCService._on_resize_handler(service, '1920x1080')
        service._resize_primary_display.assert_not_awaited()
        self.assertEqual(service._stream_requested_dims, {})

    async def test_user_and_adaptive_resize_are_serialized(self):
        events=[]
        async def resize(res, did, adaptive):
            events.append(('start',res))
            await asyncio.sleep(0.01)
            events.append(('end',res))
        service=SimpleNamespace(_stream_resize_lock=asyncio.Lock(), _on_resize_handler=resize)
        await asyncio.gather(WebRTCService.on_resize_handler(service,'1920x1080'),
                             WebRTCService.on_resize_handler(service,'1280x720',adaptive=True))
        self.assertEqual(events,[('start','1920x1080'),('end','1920x1080'),
                                 ('start','1280x720'),('end','1280x720')])


if __name__ == '__main__':
    unittest.main()
