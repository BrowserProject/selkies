import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from selkies.media_pipeline import MediaPipelinePixel, RateControlMode
from selkies.stream_control import CONFIG as STREAM_CONFIG, StreamController
from selkies.webrtc_mode import WebRTCService
from selkies.ws_sink import WsSink
from selkies.rtc import ClientType, RTCApp
from selkies.stream_hold import CONFIG as HOLD_CONFIG, RTX_TIME_MS, LinkSample, budget


class ConsumerTest(unittest.IsolatedAsyncioTestCase):
    def peer(self, goodput=4_000_000, loss=.2, paused=False, error=None):
        transport = SimpleNamespace(take_twcc_window=Mock(
            return_value={'goodput_bps': goodput, 'loss_fraction': loss, 'queue_delay_s': 0},
            side_effect=error))
        return {'peer_conn': SimpleNamespace(sctp=SimpleNamespace(transport=transport)),
                'display_id': 'primary', 'video_paused': paused}

    async def tick(self, peers, controllers=None):
        pipeline = SimpleNamespace(rc_mode=RateControlMode.CBR, video_bitrate=4000,
                                   width=1920, height=1080, framerate=30,
                                   encoded_delta_bytes=1_500_000, encoded_keyframes=2,
                                   set_video_bitrate=AsyncMock(), set_framerate=AsyncMock())
        # Pipelines are identity-keyed by the production controller.
        class Pipeline:
            pass
        actual = Pipeline()
        actual.__dict__.update(pipeline.__dict__)
        service = SimpleNamespace(
            rtc_app=SimpleNamespace(peer_connections=dict(enumerate(peers))),
            metrics=None, ws_sink=None, display_pipelines={'primary': actual}, display_clients={},
            args=SimpleNamespace(congestion_control=True, enable_resize=False),
            _stream_controllers={} if controllers is None else controllers,
            _ensure_pacer=Mock(), _display_setting=lambda did, key: 8000 if key == 'video_bitrate' else 30)
        with patch('selkies.webrtc_mode.asyncio.sleep', new=AsyncMock(side_effect=[None, asyncio.CancelledError])):
            with self.assertRaises(asyncio.CancelledError):
                await WebRTCService._congestion_control_loop(service)
        return actual

    async def test_paused_and_retired_peers_cannot_request_shared_keyframes(self):
        request = AsyncMock()
        peer = {'display_id': 'secondary', 'video_paused': True}
        app = SimpleNamespace(peer_connections={'viewer': peer}, request_idr_frame=request,
                              async_event_loop=asyncio.get_running_loop())
        for _ in range(20):
            RTCApp.on_pli(app, 'viewer', 'viewer')
            RTCApp.on_pli(app, 'retired', 'viewer')
        await asyncio.sleep(.01)
        request.assert_not_called()
        peer['video_paused'] = False
        RTCApp.on_pli(app, 'viewer', 'viewer')
        await asyncio.sleep(.01)
        request.assert_awaited_once_with('secondary', reason='pli', peer_id='viewer')

    async def test_audio_only_hidden_peer_cannot_collapse_active_video(self):
        paused = self.peer(goodput=64000, loss=0, paused=True)
        pipeline = await self.tick([self.peer(), paused])
        pipeline.set_video_bitrate.assert_awaited_once_with(3000)
        paused['peer_conn'].sctp.transport.take_twcc_window.assert_called_once()

    async def test_hidden_peer_loss_does_not_congest_active_video(self):
        pipeline = await self.tick([self.peer(loss=0), self.peer(goodput=64000, loss=1, paused=True)])
        pipeline.set_video_bitrate.assert_awaited_once_with(5000)

    def test_failed_socket_feedback_does_not_hide_healthy_consumers(self):
        bad = SimpleNamespace(display_id='primary', take_window=Mock(side_effect=RuntimeError('closed sink')))
        good = SimpleNamespace(display_id='primary', video_paused=False, closing=False,
                               take_window=Mock(return_value={'goodput_bps': 4_000_000, 'loss_fraction': .2, 'queue_delay_s': .1}))
        buckets = {}
        WsSink.merge_congestion(SimpleNamespace(clients={1: bad, 2: good}), buckets)
        self.assertEqual(buckets['primary']['goodputs'], [4_000_000])
        self.assertEqual(buckets['primary']['worst_loss'], .2)
        self.assertEqual(buckets['primary']['worst_delay'], .1)

    async def test_one_failed_feedback_reader_does_not_stop_other_peers(self):
        pipeline = await self.tick([self.peer(error=RuntimeError('closed feedback reader')), self.peer()])
        pipeline.set_video_bitrate.assert_awaited_once_with(3000)

    async def test_the_loop_hands_the_encoder_counters_to_the_controller(self):
        controllers = {}
        pipeline = await self.tick([self.peer()], controllers)
        self.assertEqual(controllers[pipeline].encoded[0], (1_500_000, 2))

    async def test_the_pipeline_counts_delta_bytes_and_keyframes_apart(self):
        pipeline = MediaPipelinePixel(asyncio.get_running_loop(), encoder='x264enc')
        pipeline.produce_data = Mock()
        pipeline.static_hold = Mock(active=False)
        pipeline._on_video_frame(b'k' * 900, 0, True, b'', 0.0)
        pipeline._on_video_frame(b'd' * 120, 1, False, b'', 0.0)
        pipeline._on_video_frame(b'd' * 80, 2, False, b'', 0.0)
        self.assertEqual((pipeline.encoded_delta_bytes, pipeline.encoded_keyframes), (200, 1))

    async def test_the_loop_keeps_its_controllers_in_the_service_map(self):
        controllers = {}
        pipeline = await self.tick([self.peer()], controllers)
        self.assertEqual(list(controllers), [pipeline])
        self.assertTrue(controllers[pipeline].congested)


class FreshStartTest(unittest.IsolatedAsyncioTestCase):
    """A display whose last consumer left starts the next one where a new
    session starts; a display someone still consumes keeps its stream."""

    BOUND = ('_reset_stream_adaptation', '_display_setting', '_display_consumers',
             '_primary_display_has_consumer', '_schedule_primary_stop_grace')

    def service(self, peers=(), sink=(), rc_mode=RateControlMode.CBR, ceiling=20000,
                congestion=True, capturing=True, running=False):
        pipeline = MediaPipelinePixel(asyncio.get_running_loop(), encoder='x264enc', video_bitrate=ceiling)
        pipeline.rc_mode = rc_mode
        # Where the departed consumer's adaptation left the shared encoder.
        pipeline.video_bitrate, pipeline.framerate = 19360, 8
        self.capture = Mock()
        if capturing:
            pipeline.capture_module, pipeline._is_screen_capturing = self.capture, True
        if running:
            # The capture keeps running through the reconnect grace.
            pipeline._running = True

            async def stop_media_pipeline():
                pipeline._running = pipeline._is_screen_capturing = False
            pipeline.stop_media_pipeline = stop_media_pipeline
        self.controller = StreamController()
        self.peers = {f'p{i}': p for i, p in enumerate(peers)}
        service = SimpleNamespace(
            display_pipelines={'primary': pipeline}, media_pipeline=pipeline, display_clients={},
            rtc_app=SimpleNamespace(peer_connections=self.peers),
            ws_sink=SimpleNamespace(display_consumers=lambda did: list(sink) if did == 'primary' else []),
            args=SimpleNamespace(congestion_control=congestion, video_bitrate=ceiling, framerate=30),
            _stream_controllers={pipeline: self.controller}, _stream_pixel_caps={'primary': 518400},
            _primary_stop_grace_task=None, RECONNECT_GRACE_S=0.01)
        for name in self.BOUND:
            setattr(service, name, getattr(WebRTCService, name).__get__(service))
        return service, pipeline

    def live_pushes(self):
        return [(c[0], c.args) for c in self.capture.method_calls
                if c[0] in ('update_video_bitrate', 'update_framerate')]

    def assert_ramp_kept(self, service, pipeline):
        self.assertEqual((pipeline.video_bitrate, pipeline.framerate), (19360, 8))
        self.assertIs(service._stream_controllers[pipeline], self.controller)
        self.assertEqual(service._stream_pixel_caps, {'primary': 518400})

    async def test_the_last_consumer_leaving_keeps_the_stream_through_the_grace(self):
        service, pipeline = self.service(running=True)
        await WebRTCService.stop_display_media(service, 'primary')
        self.assert_ramp_kept(service, pipeline)
        self.assertEqual(self.live_pushes(), [])
        await service._primary_stop_grace_task

    async def test_an_unclaimed_display_starts_again_at_the_startup_state(self):
        service, pipeline = self.service(running=True)
        await WebRTCService.stop_display_media(service, 'primary')
        await service._primary_stop_grace_task
        self.assertFalse(pipeline.is_media_pipeline_running())
        self.assertEqual((pipeline.video_bitrate, pipeline.framerate), (STREAM_CONFIG['startup_kbps'], 30))
        self.assertEqual(service._stream_controllers, {}, 'the next consumer gets a fresh controller')
        self.assertEqual(service._stream_pixel_caps, {})

    async def test_a_consumer_back_within_the_grace_keeps_the_ramp(self):
        # A reload, or a peer rebuilt after its signaling socket dropped.
        service, pipeline = self.service(running=True)
        await WebRTCService.stop_display_media(service, 'primary')
        self.peers['again'] = {'display_id': 'primary'}
        await service._primary_stop_grace_task
        self.assertTrue(pipeline.is_media_pipeline_running())
        self.assert_ramp_kept(service, pipeline)

    async def test_the_last_consumer_of_a_stopped_capture_restores_the_startup_state(self):
        # Every consumer was paused: the capture already stopped and no grace runs.
        service, pipeline = self.service()
        await WebRTCService.stop_display_media(service, 'primary')
        self.assertIsNone(service._primary_stop_grace_task)
        self.assertEqual((pipeline.video_bitrate, pipeline.framerate), (STREAM_CONFIG['startup_kbps'], 30))
        self.assertEqual(self.live_pushes(), [('update_video_bitrate', (STREAM_CONFIG['startup_kbps'],)),
                                              ('update_framerate', (30.0,))],
                         'bitrate first, so the frame-rate reopen keys at the startup rate')
        self.assertEqual(service._stream_controllers, {}, 'the next consumer gets a fresh controller')
        self.assertEqual(service._stream_pixel_caps, {})

    async def test_a_stopped_capture_starts_again_at_the_startup_state(self):
        service, pipeline = self.service(capturing=False)
        await WebRTCService.stop_display_media(service, 'primary')
        self.assertEqual((pipeline.video_bitrate, pipeline.framerate), (STREAM_CONFIG['startup_kbps'], 30))
        self.assertEqual(self.live_pushes(), [])

    async def test_a_ceiling_below_the_startup_bitrate_is_kept(self):
        service, pipeline = self.service(ceiling=2000)
        await WebRTCService.stop_display_media(service, 'primary')
        self.assertEqual(pipeline.video_bitrate, 2000)

    async def test_a_remaining_consumer_keeps_the_running_stream(self):
        for peers, sink in (([{'display_id': 'primary'}], ()), ((), [{'video_paused': True}])):
            with self.subTest(peers=peers, sink=sink):
                service, pipeline = self.service(peers=peers, sink=sink)
                await WebRTCService.stop_display_media(service, 'primary')
                self.assert_ramp_kept(service, pipeline)
                self.assertEqual(self.live_pushes(), [])

    async def test_without_congestion_control_only_the_controller_is_dropped(self):
        for kwargs in ({'congestion': False}, {'rc_mode': RateControlMode.CRF}):
            with self.subTest(**kwargs):
                service, pipeline = self.service(**kwargs)
                await WebRTCService.stop_display_media(service, 'primary')
                self.assertEqual((pipeline.video_bitrate, pipeline.framerate), (19360, 8))
                self.assertEqual(service._stream_controllers, {})


class StaticHoldWiringTest(unittest.IsolatedAsyncioTestCase):
    """The service and RTC side of the static hold: link samples, padding, PLI
    attribution, the viewer's path report and the SDP's rtx-time."""

    def service(self, peers, sink_consumers=()):
        pipeline = Mock(static_hold=SimpleNamespace(period=1 / 30), dynamic_idr_frame=AsyncMock())
        sink = SimpleNamespace(display_consumers=lambda did: list(sink_consumers) if did == 'primary' else [],
                               supersede_by_peer_controllers=lambda: None)
        return SimpleNamespace(rtc_app=SimpleNamespace(peer_connections=peers), ws_sink=sink,
                               display_pipelines={'primary': pipeline}, _last_idr_request_times={})

    def peer(self, sample=None, path='udp', display='primary', paused=False):
        sender = Mock(link_sample=Mock(return_value=sample), send_padding=AsyncMock(return_value=True))
        return {'display_id': display, 'video_paused': paused, 'video_sender': sender, 'path_protocol': path}

    def test_link_lists_every_unpaused_peer_of_the_display(self):
        sample = (0.02, 0.03, 0.5, 0.001, 0.0, 0.4)
        peers = {'ready': self.peer(sample), 'joining': self.peer(path=None),
                 'hidden': self.peer(sample, paused=True), 'other': self.peer(sample, display='display2')}
        service = self.service(peers, sink_consumers=[{'video_paused': False}, {'video_paused': True}])
        samples, sinks = WebRTCService._hold_link(service, 'primary')
        self.assertEqual(samples, [LinkSample('ready', 'udp', *sample), LinkSample('joining', None)])
        self.assertEqual(sinks, [{'video_paused': False}])
        self.assertEqual(budget(HOLD_CONFIG, 30, samples, sinks), (None, 'path:unknown'),
                         'a peer without a report keeps the display streaming')
        samples, sinks = WebRTCService._hold_link(service, 'display2')
        self.assertEqual((samples, sinks), ([LinkSample('other', 'udp', *sample)], []))

    async def test_padding_reaches_every_unpaused_sender_of_the_display(self):
        broken = self.peer()
        broken['video_sender'].send_padding.side_effect = ConnectionError('closed')
        peers = {'broken': broken, 'viewer': self.peer(), 'hidden': self.peer(paused=True),
                 'other': self.peer(display='display2')}
        await WebRTCService._hold_padding(self.service(peers), 'primary', 12.5)
        broken['video_sender'].send_padding.assert_awaited_once_with(12.5)
        peers['viewer']['video_sender'].send_padding.assert_awaited_once_with(12.5)
        peers['hidden']['video_sender'].send_padding.assert_not_awaited()
        peers['other']['video_sender'].send_padding.assert_not_awaited()

    async def test_keyframe_requests_carry_their_reason_and_peer_to_the_pipeline(self):
        service = self.service({})
        await WebRTCService.request_idr_for_display(service, 'primary', reason='pli', peer_id='viewer')
        service.display_pipelines['primary'].dynamic_idr_frame.assert_awaited_once_with('pli', 'viewer')
        await WebRTCService.request_idr_for_display(service)
        service.display_pipelines['primary'].dynamic_idr_frame.assert_awaited_once()

    async def test_consumer_changes_reevaluate_the_hold(self):
        service = self.service({})
        service._display_consumers = lambda did: []
        await WebRTCService.handle_consumers_changed(service, 'primary')
        service.display_pipelines['primary'].static_hold_reevaluate.assert_called_once_with('consumers')

    async def test_a_resuming_peer_reevaluates_before_its_resync_keyframe(self):
        order = []
        peers = {'viewer': self.peer(paused=True)}
        service = self.service(peers)
        pipeline = service.display_pipelines['primary']
        pipeline.static_hold_reevaluate.side_effect = lambda *args: order.append(('reevaluate',) + args)
        service._resume_display_capture = AsyncMock(side_effect=lambda *args, **kw: order.append(('resume',)))
        await WebRTCService.handle_video_consumer_active(service, 'viewer', 'primary', True)
        self.assertEqual(order, [('reevaluate', 'resume', 'viewer'), ('resume',)])
        self.assertFalse(peers['viewer']['video_paused'])

    def test_the_path_report_is_validated_and_consumed_for_viewers_too(self):
        seen, changes = [], []
        peers = {'viewer': {'display_id': 'display2'}}
        rtc = SimpleNamespace(peer_connections=peers,
                              on_data_message=lambda msg, did, conn_id=None: seen.append(msg),
                              on_path_protocol=lambda peer_id, display_id: changes.append((peer_id, display_id)))
        for ctype in (ClientType.VIEWER, ClientType.CONTROLLER):
            for msg, expected in (('_path,tcp', 'tcp'), ('_path,unknown', None), ('_path,', None),
                                  ('_path,udp', 'udp'), ('_path,quic', None), ('_path,udp', 'udp'),
                                  ('_path,udp', 'udp')):
                result = RTCApp._on_input_channel_message(rtc, msg, client_type=ctype, peer_id='viewer')
                self.assertIsNone(result)
                self.assertEqual(peers['viewer'].get('path_protocol'), expected, msg)
        self.assertEqual(changes, [('viewer', 'display2')] * 10, 'only a changed protocol is announced')
        RTCApp._on_input_channel_message(rtc, '_path,tls', client_type=ClientType.VIEWER, peer_id='gone')
        RTCApp._on_input_channel_message(rtc, '_path,tls', client_type=ClientType.VIEWER, peer_id=None)
        self.assertNotIn('gone', peers)
        self.assertEqual(len(changes), 10)
        self.assertEqual(seen, [], 'a path report never reaches the input dispatcher')
        rtc.on_path_protocol = None
        RTCApp._on_input_channel_message(rtc, '_path,tls', client_type=ClientType.VIEWER, peer_id='viewer')
        self.assertEqual(peers['viewer']['path_protocol'], 'tls')

    def test_a_new_path_reevaluates_the_hold_of_the_peer_display(self):
        service = self.service({})
        WebRTCService._hold_path_changed(service, 'viewer', 'primary')
        service.display_pipelines['primary'].static_hold_reevaluate.assert_called_once_with('path', 'viewer')
        WebRTCService._hold_path_changed(service, 'viewer', 'display9')
        service.display_pipelines['primary'].static_hold_reevaluate.assert_called_once()

    def test_the_offer_carries_the_hold_rtx_time(self):
        app = SimpleNamespace(encoder='h264enc', _munge_video_bandwidth=lambda sdp: sdp)
        rtx_time = f'rtx-time={RTX_TIME_MS}'
        for fmtp in ('a=fmtp:97 apt=96', 'a=fmtp:97 apt=96;rtx-time=300'):
            sdp = f'm=video 9 UDP/TLS/RTP/SAVPF 96 97\r\n{fmtp}\r\n'
            munged = RTCApp.munge_sdp(app, sdp, encoder='h264enc', fullcolor=False, use_cpu=True)
            self.assertIn(f'a=fmtp:97 apt=96;{rtx_time}\r\n', munged)
            self.assertNotIn('rtx-time=300', munged)


if __name__ == '__main__':
    unittest.main()
