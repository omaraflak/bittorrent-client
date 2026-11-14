import unittest
from bittorrent.ip import IpAndPort
from bittorrent.peer import AsyncPeer, PeerMessage


class MockPieceManager:
    def __init__(self):
        self.peer_bitfields = {}
        self.chunks_received = []

    def is_finished(self):
        return False

    def remove_peer(self, peer_id):
        pass

    def update_peer_have(self, peer_id, idx):
        pass

    def update_peer_bitfield(self, peer_id, bf):
        pass

    def on_chunk_received(self, p_index, begin, data):
        self.chunks_received.append((p_index, begin, data))


class TestAsyncPeer(unittest.IsolatedAsyncioTestCase):
    async def test_adaptive_pipelining(self):
        pm = MockPieceManager()
        peer = AsyncPeer(
            IpAndPort('127.0.0.1', 6881),
            info_hash=b'1' * 20,
            peer_id=b'2' * 20,
            piece_manager=pm,
            initial_pipeline=16,
            max_pipeline=64,
            min_pipeline=4
        )

        self.assertEqual(peer.pipeline_depth, 16)

        # Simulate receiving 8 consecutive chunks -> pipeline should expand by 4
        for i in range(8):
            piece_msg = PeerMessage(
                PeerMessage.PIECE,
                (0).to_bytes(4, 'big') +
                (i * 16384).to_bytes(4, 'big') + b'X' * 16384
            )
            await peer._handle_message(piece_msg)

        # Pipeline expanded from 16 to 20!
        self.assertEqual(peer.pipeline_depth, 20)

        # Simulate choke message -> pipeline depth should halve (20 -> 10)
        choke_msg = PeerMessage(PeerMessage.CHOKE)
        await peer._handle_message(choke_msg)
        self.assertTrue(peer.choked)
        self.assertEqual(peer.pipeline_depth, 10)

        # Simulate unchoke
        unchoke_msg = PeerMessage(PeerMessage.UNCHOKE)
        await peer._handle_message(unchoke_msg)
        self.assertFalse(peer.choked)


if __name__ == '__main__':
    unittest.main()
