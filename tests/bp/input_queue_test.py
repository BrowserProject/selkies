import asyncio
import unittest
from functools import partial
from types import SimpleNamespace

from selkies import input_handler
from selkies.rtc import RTCApp


class Channel:
    """An RTCDataChannel's event surface: the SCTP transport emits every
    message of a backlog synchronously, before the consumer runs once."""

    label = 'input'

    def __init__(self):
        self.listeners = {}

    def on(self, event, listener):
        self.listeners.setdefault(event, []).append(listener)

    def emit(self, event, *args):
        for listener in self.listeners.get(event, []):
            listener(*args)


def serialize(handler):
    rtc = SimpleNamespace(async_event_loop=asyncio.get_running_loop())
    rtc._drain_channel_queue = partial(RTCApp._drain_channel_queue, rtc)
    channel = Channel()
    consumer = RTCApp._serialize_channel(rtc, channel, handler)
    return channel, consumer


async def settle(condition, timeout=2.0):
    deadline = asyncio.get_running_loop().time() + timeout
    while not condition():
        if asyncio.get_running_loop().time() > deadline:
            raise AssertionError('timed out')
        await asyncio.sleep(0.005)


class BacklogTest(unittest.IsolatedAsyncioTestCase):
    """Patch 0030: a burst on the ordered input channel never drops a state change."""

    async def asyncSetUp(self):
        self.seen = []
        self.channel, self.consumer = serialize(self.seen.append)
        self.addCleanup(self.consumer.cancel)

    def burst(self, messages):
        for message in messages:
            self.channel.emit('message', message)

    async def test_a_backlog_folds_motion_and_keeps_every_key_and_button(self):
        moves = [f'm,{i},{i},0,0' for i in range(1500)]
        with self.assertNoLogs('rtc', level='WARNING'):
            self.burst(['kd,97', *moves, 'ku,97', 'm,5,5,1,0', 'm,5,5,0,0'])
            await settle(lambda: len(self.seen) >= 6)
            await asyncio.sleep(0.05)
        # The channel's first move is kept: no earlier mask shows it is no press.
        self.assertEqual(self.seen, ['kd,97', 'm,0,0,0,0', 'm,1499,1499,0,0', 'ku,97',
                                     'm,5,5,1,0', 'm,5,5,0,0'])

    async def test_a_press_keeps_its_position_and_a_drag_folds_behind_it(self):
        self.burst(['m,1,1,0,0', 'm,2,2,0,0', 'm,3,3,1,0', 'm,4,4,1,0', 'm,9,9,1,0',
                    'm,9,9,0,0', 'm,9,9,8,3', 'm,9,9,0,3', 'm,9,9,0,0', 'm,10,10,0,0'])
        await settle(lambda: len(self.seen) >= 8)
        await asyncio.sleep(0.05)
        self.assertEqual(self.seen, ['m,1,1,0,0', 'm,2,2,0,0', 'm,3,3,1,0', 'm,9,9,1,0',
                                     'm,9,9,0,0', 'm,9,9,8,3', 'm,9,9,0,3', 'm,10,10,0,0'])

    async def test_relative_deltas_add_up_and_only_the_latest_heartbeat_and_viewport_count(self):
        self.burst(['m2,0,0,0,0', 'm2,3,-1,0,0', 'kh,97', 'vp,0,0,1,1', 'm2,4,2,0,0', 'm2,-1,1,0,0',
                    'kh,97,98', 'm2,1,1,1,0', 'vp,10,20,1.5,1.5', 'm,7,7,1,0', 'm2,2,2,1,0'])
        await settle(lambda: len(self.seen) >= 8)
        await asyncio.sleep(0.05)
        self.assertEqual(self.seen, ['m2,0,0,0,0', 'm2,3,-1,0,0', 'kh,97,98', 'vp,10,20,1.5,1.5',
                                     'm2,3,3,0,0', 'm2,1,1,1,0', 'm,7,7,1,0', 'm2,2,2,1,0'])

    async def test_beyond_the_cap_the_held_input_is_released_once_and_logged_once(self):
        from selkies.input_queue import MAX_MESSAGES
        presses = [f'k{"du"[i % 2]},98' for i in range(MAX_MESSAGES + 500)]
        with self.assertLogs('rtc', level='WARNING') as logs:
            self.burst(['kd,97', *presses])
            await settle(lambda: 'm2,0,0,0,0' in self.seen)
        self.assertEqual(len(logs.output), 1)
        self.assertIn('dropped 501 messages', logs.output[0])
        self.assertEqual(self.seen[:2], ['kd,97', 'kd,98'])
        self.assertEqual(len(self.seen), MAX_MESSAGES + 2)
        self.assertEqual(self.seen[-2:], ['kr', 'm2,0,0,0,0'])
        self.burst(['kd,99'])
        await settle(lambda: self.seen[-1] == 'kd,99')

    async def test_the_byte_cap_sheds_like_the_count_cap(self):
        from selkies.input_queue import MAX_BYTES
        chunk = 'cwd,1,' + 'A' * 200000
        with self.assertLogs('rtc', level='WARNING'):
            self.burst([chunk] * (MAX_BYTES // len(chunk) + 3))
            await settle(lambda: 'kr' in self.seen)
        self.assertEqual(self.seen.count(chunk), MAX_BYTES // len(chunk))

    async def test_the_byte_cap_counts_encoded_bytes(self):
        from selkies.input_queue import MAX_BYTES
        chunk = 'cwd,1,' + 'é' * 100000
        size = len(chunk.encode())
        with self.assertLogs('rtc', level='WARNING'):
            self.burst([chunk] * (MAX_BYTES // size + 3))
            await settle(lambda: 'kr' in self.seen)
        self.assertEqual(self.seen.count(chunk), MAX_BYTES // size)


class ClipboardReplyTest(unittest.IsolatedAsyncioTestCase):
    """Patch 0030: a clipboard reply draining over a slow link never holds up input."""

    async def test_input_behind_a_clipboard_read_is_handled_while_the_reply_drains(self):
        handler = input_handler.WebRTCInput(rtc_app=None)
        handler.enable_clipboard = 'true'
        handled, sending, release = [], asyncio.Event(), asyncio.Event()

        async def read_clipboard(use_binary=False):
            return 'x' * 4_000_000, 'text/plain'

        async def send_clipboard_data(data, mime_type='text/plain', reply_to=None, conn_id=None):
            sending.set()
            await release.wait()
            handled.append(('clipboard', reply_to, len(data)))

        async def keypress(keysym, down=True):
            handled.append(('key', keysym, down))

        async def mouse(x, y, mask, magnitude, relative=False, display_id='primary'):
            handled.append(('mouse', x, y, mask))
        handler.read_clipboard = read_clipboard
        handler.send_clipboard_data = send_clipboard_data
        handler.send_x11_keypress = keypress
        handler.send_x11_mouse = mouse

        channel, consumer = serialize(handler.on_message)
        self.addCleanup(consumer.cancel)
        for message in ('cr', 'kd,97', 'ku,97', 'm,10,10,1,0'):
            channel.emit('message', message)
        await settle(lambda: len(handled) >= 3 and sending.is_set())
        self.assertEqual(handled, [('key', 97, True), ('key', 97, False), ('mouse', 10, 10, 1)])
        release.set()
        await settle(lambda: len(handled) == 4)
        self.assertEqual(handled[-1], ('clipboard', 'cr', 4_000_000))


class ClipboardReplyBoundTest(unittest.IsolatedAsyncioTestCase):
    """Patch 0030: a connection has at most one `cr` reply in flight."""

    async def asyncSetUp(self):
        self.handler = input_handler.WebRTCInput(rtc_app=None)
        self.handler.enable_clipboard = 'true'
        self.reads, self.sent = [], []

        async def read_clipboard(use_binary=False):
            done = asyncio.Event()
            self.reads.append(done)
            await done.wait()
            return 'x', 'text/plain'

        async def send_clipboard_data(data, mime_type='text/plain', reply_to=None, conn_id=None):
            self.sent.append(conn_id)
        self.handler.read_clipboard = read_clipboard
        self.handler.send_clipboard_data = send_clipboard_data

    async def cr(self, conn_id, times=1):
        for _ in range(times):
            await self.handler.on_message('cr', conn_id=conn_id)
        await asyncio.sleep(0.01)

    async def test_a_burst_starts_one_read_and_a_cr_after_it_finished_another(self):
        await self.cr('a', 50)
        self.assertEqual(len(self.reads), 1)
        await self.cr('b')
        self.assertEqual(len(self.reads), 2)
        for done in self.reads:
            done.set()
        await settle(lambda: len(self.sent) == 2)
        self.assertEqual(sorted(self.sent), ['a', 'b'])
        await self.cr('a')
        self.assertEqual(len(self.reads), 3)
        self.reads[-1].set()
        await settle(lambda: len(self.sent) == 3)
        self.assertEqual(self.handler._clipboard_replies, {})

    async def test_teardown_frees_the_connection_and_a_late_reply_keeps_the_new_marker(self):
        await self.cr('a')
        self.handler.release_clipboard_for_conn('a')
        await self.cr('a')
        self.assertEqual(len(self.reads), 2)
        self.reads[0].set()
        await settle(lambda: len(self.sent) == 1)
        await self.cr('a', 5)
        self.assertEqual(len(self.reads), 2)
        self.reads[1].set()
        await settle(lambda: len(self.sent) == 2)
        self.assertEqual(self.handler._clipboard_replies, {})


if __name__ == '__main__':
    unittest.main()
