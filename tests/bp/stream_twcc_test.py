import random
import struct
import unittest
from unittest.mock import patch
from selkies.webrtc.rtcdtlstransport import RTCDtlsTransport
from selkies.webrtc.rtp import pack_twcc_fci, RtcpPacket, RtcpRtpfbPacket, RTCP_RTPFB_TWCC


def transport():
    tr = object.__new__(RTCDtlsTransport)
    tr._twcc_pending_loss = {}
    tr._twcc_seq = 0
    tr._twcc_history = {}
    tr.twcc_estimate = None
    tr._pacer = None
    tr._twcc_window = tr._twcc_window_zero()
    return tr


def deliver(tr, base, arrivals):
    for i in range(len(arrivals)):
        tr._twcc_history[(base + i) & 65535] = (1200, 0)
    fci = pack_twcc_fci(base, [v * 1000 if v is not None else None for v in arrivals], 0)
    tr._twcc_process_feedback(fci)
    return fci


class TwccTest(unittest.TestCase):
    def test_rate_is_independent_of_reference_time_offset(self):
        rates = []
        for offset in (0, 0.017, 0.063):
            tr = transport()
            for i in range(10):
                deliver(tr, i * 20, [100 + offset + (i * 20 + j) * 0.001 for j in range(20)])
            rates.append(tr.take_twcc_window()['goodput_bps'])
        self.assertEqual(rates, [int(200 * 1200 * 8 / 0.199)] * 3)

    def test_feedback_gaps_and_reference_wrap(self):
        tr = transport()
        wrap = (1 << 24) * 0.064
        deliver(tr, 65534, [wrap - 0.002, wrap - 0.001])
        deliver(tr, 0, [wrap + 0.100, wrap + 0.101])
        result = tr.take_twcc_window()
        self.assertEqual(result['received'], 4)
        self.assertAlmostEqual(result['goodput_bps'], 4 * 1200 * 8 / .103, delta=1)
        self.assertIsNone(tr.take_twcc_window())

    def test_reordering_negative_delta_and_duplicate_feedback(self):
        tr = transport()
        fci = deliver(tr, 10, [100.010, 100.009, 100.012])
        tr._twcc_process_feedback(fci)
        result = tr.take_twcc_window()
        self.assertEqual(result['received'], 3)
        self.assertEqual(result['lost'], 0)

    def test_malformed_feedback_does_not_consume_history(self):
        tr = transport()
        tr._twcc_history[10] = (1200, 0)
        for fci in (b'', struct.pack('!HH', 10, 1) + bytes(4),
                    struct.pack('!HH', 10, 1) + bytes(4) + struct.pack('!H', 0x2001)):
            tr._twcc_process_feedback(fci)
            self.assertIn(10, tr._twcc_history)
            self.assertIsNone(tr.take_twcc_window())

    def test_total_loss_is_reported(self):
        tr = transport()
        deliver(tr, 1, [100.0, None, None])
        with patch("selkies.webrtc.rtcdtlstransport.time.monotonic", return_value=10**12):
            result = tr.take_twcc_window()
        self.assertEqual(result['lost'], 2)
        self.assertAlmostEqual(result['loss_fraction'], 2 / 3)

    def test_audio_overtaking_paced_video_is_not_packet_loss(self):
        tr = transport()
        deliver(tr, 10, [100.0, None, 100.002])
        tr._twcc_process_feedback(pack_twcc_fci(11, [100003.0], 1))
        with patch("selkies.webrtc.rtcdtlstransport.time.monotonic", return_value=10**12):
            result = tr.take_twcc_window()
        self.assertEqual(result['lost'], 0)
        self.assertEqual(result['received'], 3)

    def test_chrome_padded_feedback_is_accepted_after_padding_is_removed(self):
        payload = struct.pack('!LL', 1, 2) + bytes(11)
        packet = struct.pack('!BBH', 0xA0 | RTCP_RTPFB_TWCC, 205, 5) + payload + bytes([1])
        parsed = RtcpPacket.parse(packet)
        self.assertEqual(len(parsed), 1)
        self.assertIsInstance(parsed[0], RtcpRtpfbPacket)
        self.assertEqual(parsed[0].fci, bytes(11))

    def test_queue_delay_is_independent_of_clock_offset_and_clears_after_drain(self):
        for offset in (100, 100000):
            tr = transport()
            for tick, delay in enumerate((0, .15, 0)):
                base = tick * 100
                for i in range(100):
                    tr._twcc_history[base + i] = (1200, tick + i * .001)
                arrivals = [(offset + tick + i * .001 + delay) * 1000 for i in range(100)]
                tr._twcc_process_feedback(pack_twcc_fci(base, arrivals, tick))
                result = tr.take_twcc_window()
                self.assertAlmostEqual(result['queue_delay_s'], .149 if tick == 1 else 0, delta=.0003)
                self.assertEqual(result['lost'], 0)

    def test_stable_longer_route_does_not_look_like_endless_queue_growth(self):
        tr = transport()
        delays = []
        for tick in range(16):
            latency = 0 if tick == 0 else .12
            for i in range(100):
                tr._twcc_history[tick * 100 + i] = (1200, tick + i * .001)
            tr._twcc_process_feedback(pack_twcc_fci(tick * 100, [(100 + tick + i * .001 + latency) * 1000 for i in range(100)], tick))
            delays.append(tr.take_twcc_window()['queue_delay_s'])
        self.assertGreater(delays[1], .05)
        self.assertLess(delays[-1], .05)

    def test_sustained_queue_growth_remains_congestion(self):
        tr = transport()
        for tick in range(20):
            for i in range(100):
                tr._twcc_history[tick * 100 + i] = (1200, tick + i * .001)
            tr._twcc_process_feedback(pack_twcc_fci(tick * 100, [(100 + tick * 1.04 + i * .001) * 1000 for i in range(100)], tick))
            delay = tr.take_twcc_window()['queue_delay_s']
            if tick >= 3:
                self.assertGreater(delay, .05)

    def test_clock_drift_does_not_accumulate_as_congestion(self):
        tr = transport()
        for tick in range(300):
            for i in range(10):
                tr._twcc_history[(tick * 10 + i) & 65535] = (1200, tick + i * .001)
            tr._twcc_process_feedback(pack_twcc_fci(tick * 10, [(100 + tick * 1.0005 + i * .001) * 1000 for i in range(10)], tick & 255))
            self.assertLess(tr.take_twcc_window()['queue_delay_s'], .001)

    def test_seeded_feedback_roundtrips_loss_reordering_and_wrap(self):
        rng = random.Random(947)
        for _ in range(300):
            tr = transport()
            base = rng.randrange(65536)
            start = ((1 << 24) * 0.064) + rng.randrange(-100, 100)
            arrivals = [start + rng.randrange(400) * .001 if rng.random() > .2 else None for _ in range(100)]
            fci = deliver(tr, base, arrivals)
            tr._twcc_process_feedback(fci)
            with patch("selkies.webrtc.rtcdtlstransport.time.monotonic", return_value=10**12):
                result = tr.take_twcc_window()
            self.assertEqual(result['received'], sum(v is not None for v in arrivals))
            self.assertEqual(result['lost'], arrivals.count(None))
            self.assertEqual(result['received'] + result['lost'], 100)
            self.assertGreaterEqual(result['goodput_bps'], 0)
            self.assertIsNone(tr.take_twcc_window())

    def test_malformed_fci_fuzz_is_bounded_and_does_not_raise(self):
        rng = random.Random(212)
        tr = transport()
        for _ in range(2000):
            tr._twcc_process_feedback(rng.randbytes(rng.randrange(256)))
        self.assertEqual(tr._twcc_history, {})
        self.assertEqual(tr._twcc_pending_loss, {})
        self.assertIsNone(tr.take_twcc_window())

    def test_send_history_prunes_by_age_across_sequence_wrap(self):
        tr = transport()
        tr._twcc_seq = 65000
        for _ in range(2100):
            newest = tr._twcc_next(1200)
        self.assertIn(newest, tr._twcc_history)
        self.assertNotIn(65000, tr._twcc_history)


if __name__ == '__main__':
    unittest.main()
