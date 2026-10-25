import asyncio
import unittest
from types import SimpleNamespace
from unittest.mock import patch
from selkies import display_utils, stream_profiles

RATE = stream_profiles.DISPLAY_REFRESH_HZ


class DisplayRefreshTests(unittest.TestCase):
    """Patch 0017: every display mode the session creates refreshes at exactly the configured rate."""

    def test_rate_is_validated(self):
        for raw in ("29", "61", "", "x", "30.5", "-1"):
            with self.assertRaises(ValueError):
                stream_profiles.display_refresh(raw)
        self.assertEqual(stream_profiles.display_refresh("60"), 60)

    def test_native_modes_refresh_exactly(self):
        created = {}

        def create_mode(root, info, name):
            created.update(info)
            return SimpleNamespace(mode=7)
        with patch.object(display_utils.randr, "create_mode", create_mode), \
                patch.object(display_utils.randr, "add_output_mode", lambda *args: None):
            display_utils._ensure_mode_on_display(
                None, None, SimpleNamespace(modes=[]), SimpleNamespace(modes=[]), 1, {}, "1600x900", 1600, 900)
        self.assertEqual(created["dot_clock"], RATE * created["h_total"] * created["v_total"])

    def test_subprocess_modelines_refresh_exactly(self):
        for size in ("1920x1080", "1366x768"):
            params = asyncio.run(display_utils.generate_xrandr_gtf_modeline(size))[1].split()
            self.assertAlmostEqual(float(params[0]) * 1e6 / (int(params[4]) * int(params[8])), RATE, places=4)


if __name__ == "__main__":
    unittest.main()
