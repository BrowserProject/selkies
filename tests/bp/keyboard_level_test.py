import unittest
from unittest.mock import patch
from selkies import input_handler

EURO = 0x20AC
SHIFT_L = 0xFFE1
E_KEYCODE = 26
SHIFT_KEYCODE = 50


class FakeDisplay:
    """A German or Swiss keymap: EuroSign on the AltGr level of the E key."""

    def __init__(self, euro_on_e=True):
        self.columns = {E_KEYCODE: [0x65, 0x45, 0x65, 0x45, EURO if euro_on_e else 0], SHIFT_KEYCODE: [SHIFT_L]}

    def keysym_to_keycode(self, keysym):
        for keycode, syms in self.columns.items():
            if keysym in syms:
                return keycode
        return 0

    def keycode_to_keysym(self, keycode, index):
        syms = self.columns.get(keycode, [])
        return syms[index] if index < len(syms) else 0

    def flush(self):
        pass


class FakeKeyboard:
    def __init__(self):
        self.calls = []

    def outside_base_group(self, keysym):
        return False

    def bind_ahead(self, keysym):
        pass

    def settle_delay(self):
        return 0

    def press(self, keysym, neutralize=False, held_keysyms=()):
        self.calls.append(('press', keysym))

    def release(self, keysym):
        self.calls.append(('release', keysym))


class KeyboardLevelTests(unittest.IsolatedAsyncioTestCase):
    """Patch 0023: a keysym on an AltGr level goes through the shim with its modifiers."""

    async def asyncSetUp(self):
        self.handler = input_handler.WebRTCInput(rtc_app=None)
        self.handler.is_wayland = False
        self.handler.keyboard = FakeKeyboard()
        self.injected = []
        fake_xtest = type('FakeXtest', (), {'fake_input': staticmethod(lambda d, kind, kc: self.injected.append((kind, kc)))})
        patcher = patch.object(input_handler, 'xtest', fake_xtest)
        patcher.start()
        self.addCleanup(patcher.stop)

    async def test_euro_on_an_altgr_level_is_pressed_with_its_level(self):
        self.handler.xdisplay = FakeDisplay()
        await self.handler.send_x11_keypress(EURO, True)
        await self.handler.send_x11_keypress(EURO, False)
        self.assertEqual(self.injected, [], 'a bare E keycode would type "e"')
        self.assertEqual(self.handler.keyboard.calls, [('press', EURO), ('release', EURO)])

    async def test_a_base_level_key_keeps_the_bare_keycode(self):
        self.handler.xdisplay = FakeDisplay()
        await self.handler.send_x11_keypress(SHIFT_L, True)
        self.assertEqual(self.injected, [(input_handler.X.KeyPress, SHIFT_KEYCODE)])
        self.assertEqual(self.handler.keyboard.calls, [])

    async def test_a_keymap_without_the_euro_binds_it_through_the_shim(self):
        self.handler.xdisplay = FakeDisplay(euro_on_e=False)
        await self.handler.send_x11_keypress(EURO, True)
        self.assertEqual(self.injected, [])
        self.assertEqual(self.handler.keyboard.calls, [('press', EURO)])


if __name__ == '__main__':
    unittest.main()
