# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at https://mozilla.org/MPL/2.0/.

"""Per-connection scopes granted by the relay in front of Selkies.

With ``SELKIES_CONNECTION_SCOPES=true`` the relay that terminates the viewer's
credential (the BrowserProject edge) names in ``X-BP-Scopes`` what one
connection may do, and every channel of that connection is held to it: the
data WebSocket, the WebSocket sink, the WebRTC signaling socket and the data
channel it negotiates (which never passes the relay), and the file routes.

- ``stream``: watch the stream; nothing else.
- ``input``: keyboard, pointer, gamepad, typing, commands and the desktop
  control verbs. A connection without it is capped at the viewer role, so
  every role gate (mode switch, SETTINGS, microphone, webcam, controller
  slots) treats it as a viewer, and its input messages are dropped on top of
  that. In secure mode (a master token) the session tokens keep deciding the
  role; the input and clipboard verbs are dropped there all the same. As a
  viewer it is a shared viewer, so ``enable_shared`` (on by default) must stay
  on for the WebSocket transports to admit it.
- ``clipboard``: clipboard writes and reads in both directions, including the
  server's clipboard announcements.
- ``files``: ``/api/upload`` and ``/api/files``.

Enforcement fails closed: a missing header, several headers, or a connection
whose scopes did not reach a gate count as no scopes at all. Unset, nothing
changes and upstream's own gates decide alone.
"""

import logging
import os
from typing import Awaitable, Callable, FrozenSet, Optional

from aiohttp import web

logger = logging.getLogger("connection_scopes")

ENFORCE_ENV = "SELKIES_CONNECTION_SCOPES"
SCOPES_HEADER = "X-BP-Scopes"
REQUEST_KEY = "connection_scopes"

STREAM = "stream"
INPUT = "input"
CLIPBOARD = "clipboard"
FILES = "files"
KNOWN_SCOPES = frozenset((STREAM, INPUT, CLIPBOARD, FILES))

Scopes = Optional[FrozenSet[str]]

# The verbs of the shared input protocol (input_handler) by the scope they need.
CLIPBOARD_VERBS = frozenset((
    "cws", "cbs", "cwd", "cbd", "cwe", "cbe", "cw", "cb", "cr",
    "REQUEST_CLIPBOARD", "_ebc",
))
INPUT_VERBS = frozenset((
    "kd", "ku", "kh", "kr", "chord", "m", "m2", "co", "js", "cmd", "ctl",
))
# Reading the agent/human control state changes nothing.
VIEW_MESSAGES = frozenset(("ctl,status",))

ENFORCED = os.environ.get(ENFORCE_ENV, "").strip().lower() in ("1", "true", "yes")


def parse(values: list) -> FrozenSet[str]:
    """The scopes of exactly one header value; anything else grants none."""
    if len(values) != 1:
        return frozenset()
    return frozenset(s.strip() for s in values[0].split(",")) & KNOWN_SCOPES


def from_request(request: web.Request) -> Scopes:
    """The connection's scopes, or None while enforcement is off."""
    if not ENFORCED:
        return None
    return parse(request.headers.getall(SCOPES_HEADER, []))


def allows(scopes: Scopes, scope: str) -> bool:
    """Whether a connection holding ``scopes`` may use ``scope``."""
    if not ENFORCED:
        return True
    return scopes is not None and scope in scopes


def message_denied(msg: object, scopes: Scopes) -> bool:
    """Whether one client message needs a scope its connection lacks."""
    if not ENFORCED or not isinstance(msg, str):
        return False
    verb = msg.split(",", 1)[0]
    if msg in VIEW_MESSAGES:
        return False
    if verb in CLIPBOARD_VERBS:
        needed = CLIPBOARD
    elif verb in INPUT_VERBS:
        needed = INPUT
    else:
        return False
    if allows(scopes, needed):
        return False
    logger.debug("Dropping %s message without the %s scope", verb[:16], needed)
    return True


def is_file_route(path: str, api_prefix: str) -> bool:
    return path == f"{api_prefix}/api/upload" or path == f"{api_prefix}/api/files" \
        or path.startswith(f"{api_prefix}/api/files/")


def guard(
    request: web.Request,
    handler: Callable[[web.Request], Awaitable[web.StreamResponse]],
    api_prefix: str,
) -> Callable[[web.Request], Awaitable[web.StreamResponse]]:
    """Wrap the route handler the auth gates call last.

    Records the scopes on the request for the transports, refuses a file route
    without ``files``, and caps a connection without ``input`` at the viewer
    role right before the handler runs, so no credential gate can raise it.
    """
    scopes = from_request(request)
    request[REQUEST_KEY] = scopes
    if not ENFORCED:
        return handler

    async def scoped(req: web.Request) -> web.StreamResponse:
        if is_file_route(req.path, api_prefix) and not allows(scopes, FILES):
            logger.warning("Refused a file request without the files scope")
            return web.json_response({"status": "denied", "code": "scope"}, status=403)
        if not allows(scopes, INPUT):
            req["auth_role_ceiling"] = "viewer"
        return await handler(req)

    return scoped
