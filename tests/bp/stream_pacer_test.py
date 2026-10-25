import time
import unittest
from selkies.stream_control import CONFIG
from selkies.webrtc.pacer import (RtpPacer, PACE_FACTOR, AIMD_FLOOR_FACTOR, CLASS_VIDEO, GOODPUT_MAX_AGE_S,
                                  GOODPUT_WARMUP_S)

PACKET = 1000


class PacerTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        async def send(data):
            pass
        self.keyreqs = 0

        def request_keyframe():
            self.keyreqs += 1
        self.pacer = RtpPacer(8000000, send, request_keyframe=request_keyframe)

    async def asyncTearDown(self):
        await self.pacer.close()

    async def test_overflow_cannot_pace_below_encoder_injection(self):
        p = self.pacer
        p._enabled_at -= GOODPUT_WARMUP_S + 1
        p.set_goodput_bps(100000)
        for _ in range(10):
            p._brake_hold_until = 0
            p._on_overflow()
            self.assertGreaterEqual(p._pace_bps, p._encoder_bps * AIMD_FLOOR_FACTOR)
        self.assertLessEqual(p._pace_bps, p._encoder_bps * PACE_FACTOR)

    async def test_stale_feedback_and_gop_resets_do_not_prevent_recovery(self):
        p = self.pacer
        p._enabled_at -= GOODPUT_WARMUP_S + 1
        p.set_goodput_bps(100000)
        p._on_overflow()
        p._goodput_at -= GOODPUT_MAX_AGE_S + 1
        for _ in range(10):
            p._last_pace_update_at -= 0.3
            p._maybe_recover_pace()
            p._reset_gop()
        self.assertEqual(p._pace_bps, p._encoder_bps * PACE_FACTOR)

    async def test_queue_accommodates_bounded_vbv_after_live_rate_change(self):
        p = self.pacer
        for bitrate in (20000000, 500000, 3000000):
            p.set_encoder_bps(bitrate)
            self.assertGreaterEqual(p._video_cap_bytes(), bitrate / 8 * CONFIG['vbv_seconds'])
            self.assertLessEqual(p._pace_bps, bitrate * PACE_FACTOR)

    def queue_video(self, nbytes):
        """At least nbytes of video the wire has not drained yet, as the drain
        leaves it."""
        p = self.pacer
        for _ in range(-(-nbytes // PACKET)):
            p._queues[CLASS_VIDEO].append(bytes(PACKET))
            p._video_ts.append(time.monotonic())
            p._video_bytes += PACKET
            p._bytes_queued += PACKET
        return p._video_bytes

    async def send_frame(self, nbytes):
        for _ in range(nbytes // PACKET):
            await self.pacer.send(bytes(PACKET), CLASS_VIDEO)

    async def test_an_overflow_drops_the_whole_dead_gop(self):
        p = self.pacer
        self.queue_video(p._video_cap_bytes() - PACKET // 2)
        await p.send(bytes(PACKET), CLASS_VIDEO)
        self.assertTrue(p._gop_dead)
        self.assertEqual((p._video_bytes, len(p._queues[CLASS_VIDEO]), len(p._video_ts)), (0, 0, 0))
        self.assertEqual(p._bytes_queued, 0)
        self.assertEqual((p.stats['gop_resets'], self.keyreqs), (1, 1))

    async def test_the_recovery_keyframe_after_an_overflow_goes_out_whole(self):
        p = self.pacer
        cap = p._video_cap_bytes()
        self.queue_video(cap - PACKET // 2)
        await p.send(bytes(PACKET), CLASS_VIDEO)
        await self.send_frame(4 * PACKET)
        dropped = p.stats['video_dropped']
        keyframe = cap // 3
        p.note_keyframe(keyframe, natural=False)
        await self.send_frame(keyframe)
        self.assertFalse(p._gop_dead)
        self.assertEqual(p.stats['gop_resets'], 1)
        self.assertEqual(p.stats['video_dropped'], dropped)

    async def test_a_keyframe_that_would_overflow_drops_the_video_queued_ahead_of_it(self):
        p = self.pacer
        cap = p._video_cap_bytes()
        self.queue_video(cap - 2 * PACKET)
        keyframe = cap // 2
        p.note_keyframe(keyframe)
        self.assertEqual(p._video_bytes, 0)
        await self.send_frame(keyframe)
        self.assertEqual(p.stats['gop_resets'], 0)
        self.assertFalse(p._gop_dead)

    async def test_a_keyframe_is_measured_with_its_wire_overhead(self):
        p = self.pacer
        cap = p._video_cap_bytes()
        keyframe = cap // 4
        self.queue_video(cap - keyframe - 2 * PACKET)
        p.note_keyframe(keyframe)
        self.assertEqual(p._video_cap_bytes(), cap)
        self.assertEqual(p._video_bytes, 0)

    async def test_a_keyframe_that_fits_keeps_the_video_queued_ahead_of_it(self):
        p = self.pacer
        cap = p._video_cap_bytes()
        queued = self.queue_video(cap // 4)
        p.note_keyframe(cap // 4)
        self.assertEqual(p._video_bytes, queued)


if __name__ == '__main__':
    unittest.main()
