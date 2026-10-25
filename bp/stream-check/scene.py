# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
"""A scrolling page of text and colour on $DISPLAY: every frame changes the
whole screen, so the encoder has a busy picture and the congestion loop moves
the bitrate."""
import os
import time

from selkies.Xlib import X, display

WIDTH, HEIGHT = 1280, 720
screen = display.Display(os.environ.get("DISPLAY", ":99")).screen()
window = screen.root.create_window(0, 0, WIDTH, HEIGHT, 0, screen.root_depth, X.InputOutput,
                                   X.CopyFromParent, background_pixel=0xffffff)
window.map()
gc = window.create_gc()
page = window.create_pixmap(WIDTH, HEIGHT * 2, screen.root_depth)
gc.change(foreground=0xffffff)
page.fill_rectangle(gc, 0, 0, WIDTH, HEIGHT * 2)
for y in range(0, HEIGHT * 2, 24):
    gc.change(foreground=0x202020)
    for x in range(0, WIDTH, 320):
        page.draw_text(gc, x, y + 18, b"Readable screen text 0123456789")
for i in range(300):
    gc.change(foreground=(i * 179879) % 0xffffff)
    page.fill_rectangle(gc, (i * 79) % (WIDTH - 110), (i * 133) % (HEIGHT * 2 - 35), 110, 35)
step = 0
while True:
    window.copy_area(gc, page, 0, (step * 6) % HEIGHT, WIDTH, HEIGHT, 0, 0)
    screen.root.display.flush()
    step += 1
    time.sleep(1 / 30)
