import base64
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, call

from selkies import input_handler
from selkies.webrtc_mode import WebRTCService

# Both viewer cores number their first multi-part transfer 1.
TRANSFER_ID = "1"
CHUNK = 4


def chunks(data):
    return [base64.b64encode(data[i:i + CHUNK]).decode() for i in range(0, len(data), CHUNK)]


class ClipboardTransferTest(unittest.IsolatedAsyncioTestCase):
    """Patch 0029: each connection receives its own multi-part clipboard transfer."""

    async def asyncSetUp(self):
        self.handler = input_handler.WebRTCInput(
            rtc_app=None, enable_clipboard="true", enable_binary_clipboard="true")
        self.handler.write_clipboard = AsyncMock(return_value=True)

    async def send(self, conn_id, msg):
        await self.handler.on_message(msg, "primary", conn_id=conn_id)

    async def start(self, conn_id, data, mime="image/png"):
        await self.send(conn_id, f"cbs,{TRANSFER_ID},{mime},{len(data)}")

    async def test_two_connections_with_the_same_transfer_id_both_complete(self):
        socket_image, peer_image = b"socket-side image", b"the webrtc peer's image"
        await self.start("wssink:1", socket_image)
        await self.start("peer-1", peer_image)
        socket_chunks, peer_chunks = chunks(socket_image), chunks(peer_image)
        for i in range(max(len(socket_chunks), len(peer_chunks))):
            if i < len(socket_chunks):
                await self.send("wssink:1", f"cbd,{TRANSFER_ID},{socket_chunks[i]}")
            if i < len(peer_chunks):
                await self.send("peer-1", f"cbd,{TRANSFER_ID},{peer_chunks[i]}")
        await self.send("wssink:1", f"cbe,{TRANSFER_ID}")
        await self.send("peer-1", f"cbe,{TRANSFER_ID}")
        self.assertEqual(self.handler.write_clipboard.await_args_list, [
            call(socket_image, mime_type="image/png"),
            call(peer_image, mime_type="image/png"),
        ])

    async def test_a_closed_connections_transfer_is_dropped(self):
        image = b"half sent before the socket closed"
        await self.start("wssink:1", image)
        await self.send("wssink:1", f"cbd,{TRANSFER_ID},{chunks(image)[0]}")
        await self.start("peer-1", image)
        self.handler.release_clipboard_for_conn("wssink:1")
        for chunk in chunks(image)[1:]:
            await self.send("wssink:1", f"cbd,{TRANSFER_ID},{chunk}")
        await self.send("wssink:1", f"cbe,{TRANSFER_ID}")
        self.handler.write_clipboard.assert_not_awaited()
        self.assertEqual(list(self.handler._clipboard_transfers), ["peer-1"])

    async def test_a_departed_peer_releases_its_transfer(self):
        await self.start("peer-1", b"image")
        service = SimpleNamespace(input_handler=self.handler, rtc_app=None)
        await WebRTCService.handle_peer_gone(service, "peer-1", {})
        self.assertEqual(self.handler._clipboard_transfers, {})

    async def test_transfers_in_flight_are_capped(self):
        cap = input_handler.MULTIPART_CLIPBOARD_MAX_TRANSFERS
        text = b"text"
        for n in range(cap + 1):
            await self.send(f"conn-{n}", f"cws,{TRANSFER_ID},{len(text)}")
        self.assertEqual(len(self.handler._clipboard_transfers), cap)
        for n in (0, cap):
            await self.send(f"conn-{n}", f"cwd,{TRANSFER_ID},{chunks(text)[0]}")
            await self.send(f"conn-{n}", f"cwe,{TRANSFER_ID}")
        self.assertEqual(self.handler.write_clipboard.await_args_list,
                         [call(text, mime_type="text/plain")])


if __name__ == "__main__":
    unittest.main()
