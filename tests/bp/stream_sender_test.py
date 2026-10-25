import asyncio
import math
import time
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from selkies.webrtc.mediastreams import MediaStreamError
from selkies.webrtc.rtcrtpsender import RTCRtpSender, RTCEncodedFrame
from selkies.webrtc.rtcrtpparameters import RTCRtpParameters, RTCRtpHeaderExtensionParameters
from selkies.webrtc.rtp import RTCP_RTPFB_NACK, RtcpReceiverInfo, RtcpRrPacket, RtcpRtpfbPacket, RtpPacket


class SenderTest(unittest.IsolatedAsyncioTestCase):
    def sender(self, negotiated):
        transport = SimpleNamespace(state='connected', _twcc_next=Mock(return_value=123), _send_rtp=AsyncMock())
        sender = RTCRtpSender('audio', transport)
        mapping = sender._RTCRtpSender__rtp_header_extensions_map
        extensions = [RTCRtpHeaderExtensionParameters(id=5, uri='http://www.ietf.org/id/draft-holmer-rmcat-transport-wide-cc-extensions-01')] if negotiated else []
        mapping.configure(RTCRtpParameters(headerExtensions=extensions))
        return sender, transport, mapping

    async def test_original_packets_reserve_twcc_numbers_only_when_negotiated(self):
        for negotiated in (False, True):
            sender, transport, mapping = self.sender(negotiated)
            sender._RTCRtpSender__track = SimpleNamespace(stop=lambda: None)
            sender._next_encoded_frame = AsyncMock(side_effect=[RTCEncodedFrame([b'audio'], 0, 0), MediaStreamError()])
            await asyncio.wait_for(sender._run_rtp(SimpleNamespace(payloadType=96, clockRate=48000)), 1)
            self.assertEqual(transport._twcc_next.call_count, int(negotiated))
            transport._send_rtp.assert_awaited_once()
            packet = RtpPacket.parse(transport._send_rtp.call_args.args[0], mapping)
            self.assertEqual(packet.extensions.transport_sequence_number, 123 if negotiated else None)

    async def test_retransmissions_reserve_twcc_numbers_only_when_negotiated(self):
        for negotiated in (False, True):
            sender, transport, mapping = self.sender(negotiated)
            packet = RtpPacket(payload_type=96, sequence_number=7)
            packet.payload = b'audio'
            sender._RTCRtpSender__rtp_history.add(packet, time.time())
            await sender._retransmit(7)
            self.assertEqual(transport._twcc_next.call_count, int(negotiated))
            transport._send_rtp.assert_awaited_once()
            received = RtpPacket.parse(transport._send_rtp.call_args.args[0], mapping)
            self.assertEqual(received.extensions.transport_sequence_number, 123 if negotiated else None)


MID = 'urn:ietf:params:rtp-hdrext:sdes:mid'
ABS_SEND_TIME = 'http://www.webrtc.org/experiments/rtp-hdrext/abs-send-time'
TWCC = 'http://www.ietf.org/id/draft-holmer-rmcat-transport-wide-cc-extensions-01'
VIDEO = SimpleNamespace(payloadType=96, clockRate=90000)
RTX_PT = 97


class PaddingTest(unittest.IsolatedAsyncioTestCase):
    """Static hold padding: one empty packet on the media stream, repairable by RTX."""

    def sender(self, kind='video', negotiated=True, on_send=None):
        sent = []

        async def send_rtp(data, rtc_class=None):
            sent.append(data)
            if on_send is not None:
                await on_send()

        transport = SimpleNamespace(state='connected', _stats_id='transport', _send_rtp=send_rtp,
                                    _twcc_next=Mock(return_value=321))
        sender = RTCRtpSender(kind, transport)
        uris = [MID, ABS_SEND_TIME] + ([TWCC] if negotiated else [])
        mapping = sender._RTCRtpSender__rtp_header_extensions_map
        mapping.configure(RTCRtpParameters(headerExtensions=[
            RTCRtpHeaderExtensionParameters(id=i + 1, uri=uri) for i, uri in enumerate(uris)]))
        sender._RTCRtpSender__mid = '0'
        sender._RTCRtpSender__started = True
        sender._RTCRtpSender__rtx_payload_type = RTX_PT
        return sender, transport, mapping, sent

    async def start(self, sender, frames):
        """Start the RTP loop, feed it `frames` and leave it waiting for the next one."""
        queue = asyncio.Queue()
        sender._RTCRtpSender__track = SimpleNamespace(stop=lambda: None)
        sender._next_encoded_frame = lambda codec: queue.get()
        task = asyncio.ensure_future(sender._run_rtp(VIDEO))
        self.addAsyncCleanup(self.stop, task)
        await self.feed(queue, frames)
        return queue

    async def feed(self, queue, frames):
        for frame in frames:
            queue.put_nowait(frame)
        for _ in range(50):
            await asyncio.sleep(0)

    async def stop(self, task):
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

    async def test_padding_packet_format(self):
        for negotiated in (True, False):
            sender, transport, mapping, sent = self.sender(negotiated=negotiated)
            await self.start(sender, [RTCEncodedFrame([b'first', b'last'], 9000, None)])
            media = [RtpPacket.parse(data, mapping) for data in sent]
            sender._RTCRtpSender__last_media_at = time.monotonic() - 0.1
            transport._twcc_next.reset_mock()
            self.assertTrue(await sender.send_padding(time.monotonic()))
            self.assertEqual(sent[-1][0] & 0x20, 0x20, 'the P bit')
            self.assertEqual(sent[-1][-1], 1, 'one padding octet, the count itself')
            packet = RtpPacket.parse(sent[-1], mapping)
            self.assertEqual((packet.payload, packet.padding_size, packet.marker), (b'', 1, 0))
            self.assertEqual(packet.sequence_number, (media[-1].sequence_number + 1) & 0xFFFF)
            self.assertEqual((packet.ssrc, packet.payload_type), (sender._ssrc, 96))
            self.assertEqual(packet.extensions.mid, '0')
            self.assertIsNotNone(packet.extensions.abs_send_time)
            if negotiated:
                transport._twcc_next.assert_called_once_with(0)
                self.assertEqual(packet.extensions.transport_sequence_number, 321)
            else:
                transport._twcc_next.assert_not_called()
                self.assertIsNone(packet.extensions.transport_sequence_number)
            advance = (packet.timestamp - media[-1].timestamp) & 0xFFFFFFFF
            self.assertTrue(9000 <= advance < 9000 + 900, 'the timestamp runs on from the last frame')
            self.assertEqual(sender._RTCRtpSender__rtp_timestamp, media[-1].timestamp,
                             'the sender report keeps the media RTP/NTP mapping')
            self.assertEqual(sender._RTCRtpSender__packet_count, 3)
            self.assertEqual(sender._RTCRtpSender__octet_count, len(b'firstlast'))

    async def test_padding_is_repaired_by_rtx_with_the_original_sequence_number_only(self):
        sender, transport, mapping, sent = self.sender()
        await self.start(sender, [RTCEncodedFrame([b'frame'], 0, None)])
        self.assertTrue(await sender.send_padding(math.inf))
        padding = RtpPacket.parse(sent[-1], mapping)
        await sender._retransmit(padding.sequence_number)
        rtx = RtpPacket.parse(sent[-1], mapping)
        self.assertEqual((rtx.payload_type, rtx.ssrc), (RTX_PT, sender._rtx_ssrc))
        self.assertEqual(rtx.payload, padding.sequence_number.to_bytes(2, 'big'))
        self.assertEqual(rtx.padding_size, 0)

    async def test_media_sequence_continues_after_padding(self):
        sender, transport, mapping, sent = self.sender()
        queue = await self.start(sender, [RTCEncodedFrame([b'a', b'b'], 0, None)])
        self.assertTrue(await sender.send_padding(math.inf))
        self.assertTrue(await sender.send_padding(math.inf))
        await self.feed(queue, [RTCEncodedFrame([b'c'], 3000, None)])
        numbers = [RtpPacket.parse(data, mapping).sequence_number for data in sent]
        self.assertEqual(numbers, [(numbers[0] + i) & 0xFFFF for i in range(5)])

    async def test_no_padding_where_it_does_not_belong(self):
        audio, _, _, _ = self.sender(kind='audio')
        await self.start(audio, [RTCEncodedFrame([b'opus'], 0, None)])
        self.assertFalse(await audio.send_padding(math.inf), 'audio')

        before_media, _, _, sent = self.sender()
        self.assertFalse(await before_media.send_padding(math.inf), 'no media sent yet')
        self.assertEqual(sent, [])

        disabled, _, _, _ = self.sender()
        await self.start(disabled, [RTCEncodedFrame([b'frame'], 0, None)])
        disabled._enabled = False
        self.assertFalse(await disabled.send_padding(math.inf), 'a disabled sender')

        stopped, _, _, _ = self.sender()
        await self.start(stopped, [RTCEncodedFrame([b'frame'], 0, None)])
        stopped._RTCRtpSender__rtp_exited.set()
        self.assertFalse(await stopped.send_padding(math.inf), 'an RTP loop that ended')

    async def test_padding_on_every_tick_nothing_else_left_in(self):
        sender, _, _, sent = self.sender()
        boundary = time.monotonic()
        queue = await self.start(sender, [RTCEncodedFrame([b'frame'], 0, None)])
        self.assertFalse(await sender.send_padding(boundary), 'media left in this tick')
        # A tick right after the media, however little of it has passed, pads.
        boundary = time.monotonic()
        self.assertTrue(await sender.send_padding(boundary))
        self.assertFalse(await sender.send_padding(boundary), 'padding left in this tick')
        await self.feed(queue, [RTCEncodedFrame([b'next'], 3000, None)])
        boundary = time.monotonic()
        self.assertTrue(await sender.send_padding(boundary))
        self.assertEqual(len(sent), 4)

    async def test_no_padding_inside_a_frame(self):
        state = {'asking': False, 'answers': []}

        async def pad_during_send():
            if state['asking']:
                state['answers'].append(await sender.send_padding(math.inf))

        sender, _, mapping, sent = self.sender(on_send=pad_during_send)
        queue = await self.start(sender, [RTCEncodedFrame([b'a'], 0, None)])
        # Padding asked for while each packet of the next frame is being sent.
        state['asking'] = True
        await self.feed(queue, [RTCEncodedFrame([b'b', b'c'], 3000, None)])
        state['asking'] = False
        self.assertEqual(state['answers'], [False, False])
        self.assertEqual([RtpPacket.parse(data, mapping).payload for data in sent], [b'a', b'b', b'c'])
        self.assertTrue(await sender.send_padding(math.inf), 'after the frame')

    async def report(self, sender, lsr, highest=0, lost=0, fraction_lost=0, jitter=0, dlsr=0):
        info = RtcpReceiverInfo(ssrc=sender._ssrc, fraction_lost=fraction_lost, packets_lost=lost,
                                highest_sequence=highest, jitter=jitter, lsr=lsr, dlsr=dlsr)
        await sender._handle_rtcp_packet(RtcpRrPacket(ssrc=1, reports=[info]))

    async def test_link_sample(self):
        sender, _, _, _ = self.sender()
        self.assertIsNone(sender.link_sample(), 'no receiver report yet')
        sender._RTCRtpSender__lsr = 0x1234
        sender._RTCRtpSender__lsr_time = time.time() - 0.05
        await self.report(sender, lsr=0x999, highest=1000, fraction_lost=2, jitter=900)
        srtt, rtt, rtt_age, jitter, loss, rr_age = sender.link_sample()
        self.assertEqual((srtt, rtt, rtt_age), (None, None, None), 'an unmatched LSR is no RTT sample')
        self.assertAlmostEqual(jitter, 0.01)
        self.assertIsNone(loss, 'unknown until the reports cover enough packets')
        self.assertLess(rr_age, 1)

        await self.report(sender, lsr=0x1234, highest=2000, lost=1, dlsr=int(0.01 * 65536))
        srtt, rtt, rtt_age, jitter, loss, rr_age = sender.link_sample()
        self.assertAlmostEqual(rtt, 0.04, delta=0.02)
        self.assertEqual(srtt, rtt)
        self.assertLess(rtt_age, 1)
        self.assertEqual(jitter, 0)
        self.assertAlmostEqual(loss, 0.001, msg='1 of 1000 expected packets')

        # A report on every NACK or PLI covers only its own packets.
        for step in range(1, 21):
            await self.report(sender, lsr=0x999, highest=2000 + 50 * step, lost=1 + (step >= 10), fraction_lost=8)
        self.assertAlmostEqual(sender.link_sample()[4], 2 / 2000)

        # A gap libwebrtc takes for a stream restart leaves its cumulative loss alone but is NACKed.
        await sender._handle_rtcp_packet(RtcpRtpfbPacket(fmt=RTCP_RTPFB_NACK, ssrc=1, media_ssrc=sender._ssrc,
                                                          lost=list(range(3000, 3500)) + [3000]))
        await self.report(sender, lsr=0x999, highest=3500, lost=2)
        self.assertAlmostEqual(sender.link_sample()[4], 502 / 2500)

        sender._RTCRtpSender__lsr_time = time.time() + 1
        await self.report(sender, lsr=0x1234, highest=3000, lost=2, dlsr=1)
        self.assertEqual(sender.link_sample()[1], rtt, 'a sample at or below zero is ignored')
        sender._RTCRtpSender__lsr_time = time.time() - 5
        await self.report(sender, lsr=0x1234, highest=3000, lost=2, dlsr=1)
        self.assertEqual(sender.link_sample()[:2], (srtt, rtt), 'a sample past the RTT bound is ignored')


if __name__ == '__main__':
    unittest.main()
