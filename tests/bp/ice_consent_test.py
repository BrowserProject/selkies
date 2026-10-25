import asyncio
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch

from selkies.ice import ice, stun

INTERVAL_S = 0.1
TIMEOUT_S = 1.0
CONSENT_TIMEOUT_S = ice.CONSENT_TIMEOUT


class Peer:
    """A nominated pair's protocol: answers every consent check after `rtt`
    seconds (None never answers), through the real STUN transaction."""

    def __init__(self, rtt):
        self.rtt = rtt
        self.sends = []
        self.transactions = {}

    async def request(self, request, addr, integrity_key=None, retransmissions=None):
        transaction = stun.Transaction(request, addr, self, retransmissions=retransmissions)
        self.transactions[request.transaction_id] = transaction
        return await transaction.run()

    def send_stun(self, message, addr):
        self.sends.append(message.transaction_id)
        if self.rtt is None:
            return
        response = stun.Message(message_method=stun.Method.BINDING, message_class=stun.Class.RESPONSE,
                                transaction_id=message.transaction_id)
        asyncio.get_running_loop().call_later(
            self.rtt, self.transactions[message.transaction_id].response_received, response, addr)


class ConsentTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.patches = [patch.object(ice, 'CONSENT_INTERVAL', INTERVAL_S),
                        patch.object(ice, 'CONSENT_TIMEOUT', TIMEOUT_S)]
        for p in self.patches:
            p.start()

    async def asyncTearDown(self):
        for p in self.patches:
            p.stop()

    def connection(self, peer):
        conn = ice.Connection(ice_controlling=True)
        conn.remote_username, conn.remote_password = 'remote', 'remote-password'
        conn._nominated[1] = SimpleNamespace(protocol=peer, remote_addr=('198.51.100.7', 3478), component=1)
        conn.closed_at = None

        async def close():
            conn.closed_at = time.monotonic()
        conn.close = close
        return conn

    def test_consent_lasts_the_thirty_seconds_rfc_7675_allows(self):
        self.assertEqual(CONSENT_TIMEOUT_S, 30)

    async def test_answers_slower_than_the_first_rto_keep_consent(self):
        peer = Peer(rtt=stun.RETRY_RTO * 1.6)
        conn = self.connection(peer)
        task = asyncio.create_task(conn.query_consent())
        # Six failed checks in a row ended the old consent within 3.6 s here.
        await asyncio.sleep(TIMEOUT_S * 4.5)
        self.assertIsNone(conn.closed_at)
        self.assertFalse(task.done())
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task

    async def test_a_silent_peer_loses_consent_once_the_timeout_passed(self):
        peer = Peer(rtt=None)
        conn = self.connection(peer)
        started = time.monotonic()
        await asyncio.wait_for(conn.query_consent(), TIMEOUT_S * 3)
        elapsed = conn.closed_at - started
        self.assertGreaterEqual(elapsed, TIMEOUT_S)
        self.assertLess(elapsed, TIMEOUT_S + INTERVAL_S * 1.2 + 0.2)
        self.assertIsNone(conn._query_consent_task)
        retransmitted = max(peer.sends.count(tid) for tid in set(peer.sends))
        self.assertGreater(retransmitted, 1, 'a check is retransmitted')
        await asyncio.sleep(stun.RETRY_RTO * 2)
        sent = len(peer.sends)
        await asyncio.sleep(stun.RETRY_RTO * 4)
        self.assertEqual(len(peer.sends), sent, 'no check outlives the expiry')

    async def test_the_timeout_counts_from_the_last_answer(self):
        peer = Peer(rtt=0.05)
        conn = self.connection(peer)
        task = asyncio.create_task(conn.query_consent())
        await asyncio.sleep(TIMEOUT_S * 1.5)
        self.assertIsNone(conn.closed_at)
        peer.rtt = None
        silent_from = time.monotonic()
        await asyncio.wait_for(task, TIMEOUT_S * 3)
        self.assertGreaterEqual(conn.closed_at - silent_from, TIMEOUT_S - INTERVAL_S * 1.2 - 0.1)


if __name__ == '__main__':
    unittest.main()
