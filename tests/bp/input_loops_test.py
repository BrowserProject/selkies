import asyncio
import unittest
from unittest.mock import patch
from selkies import input_handler


class InputLoopTests(unittest.IsolatedAsyncioTestCase):
    """Patch 0015: the key-repeat and stale-key loops park while nothing is held."""

    async def asyncSetUp(self):
        self.real_sleep = asyncio.sleep
        self.ticks = {'repeat': 0, 'sweep': 0}
        self.handler = input_handler.WebRTCInput(rtc_app=None)
        self.events = []

        async def keypress(keysym, down=True):
            self.events.append((keysym, down))
        self.handler.send_x11_keypress = keypress

        async def counting_sleep(delay, *args, **kwargs):
            if delay == self.handler.key_repeat_tick:
                self.ticks['repeat'] += 1
            elif delay == self.handler.key_sweep_interval:
                self.ticks['sweep'] += 1
            return await self.real_sleep(delay, *args, **kwargs)
        sleeper = patch.object(input_handler.asyncio, 'sleep', counting_sleep)
        sleeper.start()
        self.addCleanup(sleeper.stop)
        self.handler.key_sweep_task = asyncio.create_task(self.handler._key_stale_sweep())
        self.handler.key_repeat_task = asyncio.create_task(self.handler._key_repeat_loop())

    async def asyncTearDown(self):
        self.handler.key_sweep_task.cancel()
        self.handler.key_repeat_task.cancel()
        await asyncio.gather(self.handler.key_sweep_task, self.handler.key_repeat_task, return_exceptions=True)

    async def idle_ticks(self, seconds=0.5):
        before = dict(self.ticks)
        await self.real_sleep(seconds)
        return {key: self.ticks[key] - before[key] for key in before}

    async def test_idle_loops_park_and_held_key_still_repeats(self):
        self.assertEqual(await self.idle_ticks(), {'repeat': 0, 'sweep': 0})
        await self.handler.on_message('kd,97')
        for _ in range(12):
            await self.real_sleep(0.1)
            await self.handler.on_message('kh,97')
        await self.handler.on_message('ku,97')
        self.assertGreaterEqual(len([event for event in self.events if event == (97, True)]), 5)
        self.assertEqual(self.events[-1], (97, False))
        await self.real_sleep(0.3)
        self.assertEqual(await self.idle_ticks(), {'repeat': 0, 'sweep': 0})

    async def test_lost_key_up_is_still_released(self):
        await self.handler.on_message('kd,98')
        for _ in range(40):
            await self.real_sleep(0.1)
            if (98, False) in self.events:
                break
        self.assertIn((98, False), self.events)
        self.assertNotIn(98, self.handler.pressed_keys)
        await self.real_sleep(0.3)
        self.assertEqual(await self.idle_ticks(), {'repeat': 0, 'sweep': 0})


if __name__ == '__main__':
    unittest.main()
