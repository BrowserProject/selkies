"""Patch 0027 on a real Xvfb: a resize past the screen maximum realizes the largest size that fits."""
import asyncio
import os
import subprocess
import time
import unittest
from unittest.mock import patch

DISPLAY = ":96"
os.environ["DISPLAY"] = DISPLAY

from selkies import display_utils  # noqa: E402


def mode_names():
    with display_utils._x11_lock:
        _, _, _, _, names = display_utils._connected_output_state(display_utils._module_display())
    return set(names.values())


class ResizePastScreenMaximum(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # An agent session's Xvfb: the framebuffer, and so the RandR maximum, is its start size.
        cls.xvfb = subprocess.Popen(
            ["Xvfb", DISPLAY, "-screen", "0", "1280x800x24", "+extension", "RANDR", "-nolisten", "tcp", "-noreset"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for _ in range(100):
            if os.path.exists(f"/tmp/.X11-unix/X{DISPLAY[1:]}"):
                break
            time.sleep(0.05)
        else:
            raise RuntimeError("Xvfb did not start")

    @classmethod
    def tearDownClass(cls):
        display_utils._drop_module_display()
        cls.xvfb.terminate()
        cls.xvfb.wait()

    def test_request_past_the_maximum_realizes_the_largest_fitting_size_on_every_retry(self):
        for _ in range(2):
            self.assertEqual(asyncio.run(display_utils.resize_display("1920x1080")), (1280, 800))
        self.assertNotIn("1920x1080", mode_names())

    def test_request_within_the_maximum_is_unchanged(self):
        self.assertEqual(asyncio.run(display_utils.resize_display("1024x768")), (1024, 768))
        self.assertEqual(asyncio.run(display_utils.resize_display("1280x720")), (1280, 720))

    def test_width_is_cut_to_the_cvt_cell_below_the_maximum(self):
        with patch.object(display_utils, "_sync_screen_max", return_value=(1366, 768)):
            self.assertEqual(asyncio.run(display_utils._fit_screen_max("1920x1080")), "1360x768")
            self.assertEqual(asyncio.run(display_utils._fit_screen_max("1366x768")), "1366x768")
            self.assertEqual(asyncio.run(display_utils._fit_screen_max("1024x900")), "1024x768")

    def test_unknown_maximum_leaves_the_request(self):
        with patch.object(display_utils, "_sync_screen_max", side_effect=RuntimeError("no X")):
            self.assertEqual(asyncio.run(display_utils._fit_screen_max("1920x1080")), "1920x1080")


if __name__ == "__main__":
    unittest.main()
