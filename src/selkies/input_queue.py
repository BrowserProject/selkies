# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.
"""The ordered input data channel's receive queue.

After a lost packet the SCTP association hands over the whole backlog of the
ordered channel in one synchronous burst, so the queue sees hundreds of
messages before its consumer runs once. Nothing that changes state is ever
dropped for that: key and button changes, clipboard chunks and every other
message keep their place and order. Only what is provably the latest state
folds into its predecessor:

- a pointer move (``m`` absolute, ``m2`` relative) folds into the message
  queued last when that is a move of the same kind with the same button
  mask, neither carries a scroll (magnitude 0) and the queued one changed no
  button itself: the newer absolute position replaces it, relative deltas
  add up;
- a key heartbeat (``kh``) or viewport report (``vp``) replaces the one of
  its kind still queued.

Past ``MAX_MESSAGES`` or ``MAX_BYTES`` (UTF-8 encoded) the queue sheds: the
message that does not fit and every later one until the consumer has caught
up are dropped, and at the point the shedding began the consumer receives one
``Overflow`` item, which carries the number dropped and stands for releasing
every held key and button, because the releases may be among the dropped
messages.
"""

import asyncio
from collections import deque
from typing import Any, Deque, Dict, Optional

MAX_MESSAGES = 4096
MAX_BYTES = 16 * 1024 * 1024
# The wire messages that release every held key and pointer button.
RELEASE_MESSAGES = ("kr", "m2,0,0,0,0")
_POINTER_KINDS = ("m", "m2")
_LATEST_KINDS = ("kh", "vp")
# XTEST carries a relative move in an Int16; a larger sum stays two moves.
_DELTA_LIMIT = 32767


class Overflow:
    """Queue item marking where shedding began; ``dropped`` counts what it shed."""

    __slots__ = ("dropped",)

    def __init__(self) -> None:
        self.dropped = 1


class _Entry:
    __slots__ = ("msg", "kind", "size", "pointer", "mask_before")

    def __init__(self, msg: Any, kind: Optional[str], size: int,
                 pointer: Optional[list], mask_before: Optional[int]) -> None:
        self.msg = msg
        self.kind = kind
        self.size = size
        self.pointer = pointer
        self.mask_before = mask_before


def _pointer_fields(msg: str) -> Optional[list]:
    """``[x, y, mask, magnitude]`` of a well-formed pointer message, else None."""
    parts = msg.split(",")
    if len(parts) != 5:
        return None
    try:
        return [int(p) for p in parts[1:]]
    except ValueError:
        return None


class InputQueue:
    """One channel's receive queue: ``put`` is synchronous and never raises,
    ``get`` awaits the next message or ``Overflow`` item in arrival order."""

    def __init__(self) -> None:
        self._items: Deque[Any] = deque()
        self._bytes = 0
        self._ready = asyncio.Event()
        self._latest: Dict[str, _Entry] = {}
        self._mask: Optional[int] = None
        self._shedding: Optional[Overflow] = None

    def put(self, msg: Any) -> None:
        if self._shedding is not None:
            self._shedding.dropped += 1
            return
        entry = self._entry(msg)
        if self._fold(entry):
            return
        if len(self._items) >= MAX_MESSAGES or self._bytes + entry.size > MAX_BYTES:
            self._shedding = Overflow()
            self._items.append(self._shedding)
            self._latest.clear()
            self._mask = 0
            self._ready.set()
            return
        if entry.pointer is not None:
            self._mask = entry.pointer[2]
        if entry.kind in _LATEST_KINDS:
            self._latest[entry.kind] = entry
        self._items.append(entry)
        self._bytes += entry.size
        self._ready.set()

    async def get(self) -> Any:
        while not self._items:
            self._ready.clear()
            await self._ready.wait()
        item = self._items.popleft()
        if item is self._shedding:
            self._shedding = None
            return item
        self._bytes -= item.size
        if self._latest.get(item.kind) is item:
            del self._latest[item.kind]
        return item.msg

    def _entry(self, msg: Any) -> _Entry:
        if not isinstance(msg, str):
            size = len(msg) if isinstance(msg, (bytes, bytearray)) else 0
            return _Entry(msg, None, size, None, None)
        kind = msg.partition(",")[0]
        pointer = _pointer_fields(msg) if kind in _POINTER_KINDS else None
        size = len(msg) if msg.isascii() else len(msg.encode("utf-8", "surrogatepass"))
        return _Entry(msg, kind, size, pointer, self._mask)

    def _fold(self, entry: _Entry) -> bool:
        """Fold ``entry`` into a queued message it supersedes; True when folded."""
        if entry.kind in _LATEST_KINDS:
            queued = self._latest.get(entry.kind)
            if queued is None:
                return False
            self._replace(queued, entry.msg, entry.size)
            return True
        if entry.pointer is None or not self._items:
            return False
        tail = self._items[-1]
        if (not isinstance(tail, _Entry) or tail.kind != entry.kind
                or tail.pointer is None or tail.mask_before != tail.pointer[2]):
            return False
        x, y, mask, magnitude = entry.pointer
        if mask != tail.pointer[2] or magnitude or tail.pointer[3]:
            return False
        msg = entry.msg
        if entry.kind == "m2":
            x += tail.pointer[0]
            y += tail.pointer[1]
            if abs(x) > _DELTA_LIMIT or abs(y) > _DELTA_LIMIT:
                return False
            msg = f"m2,{x},{y},{mask},0"
        tail.pointer[0], tail.pointer[1] = x, y
        self._replace(tail, msg, len(msg))
        return True

    def _replace(self, queued: _Entry, msg: str, size: int) -> None:
        self._bytes += size - queued.size
        queued.msg = msg
        queued.size = size
