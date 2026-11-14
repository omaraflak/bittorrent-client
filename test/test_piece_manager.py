import shutil
import tempfile
import hashlib
import unittest
from bittorrent.torrent import Torrent, Piece, File
from bittorrent.peer import Bitfield
from bittorrent.storage import Storage
from bittorrent.piece_manager import PieceManager, CHUNK_SIZE


class TestPieceManager(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def _create_fixture(self):
        # 3 pieces, 32KB each (2 chunks of 16KB per piece)
        piece_size = CHUNK_SIZE * 2
        pieces = []
        piece_payloads = []
        for i in range(3):
            data = bytes([i] * piece_size)
            piece_payloads.append(data)
            pieces.append(Piece(i, piece_size, hashlib.sha1(data).digest()))

        file_info = File(0, 0, piece_size * 3, ['data.bin'])
        torrent = Torrent(
            name='test_torrent',
            size=piece_size * 3,
            piece_size=piece_size,
            piece_count=3,
            info_hash=b'3' * 20,
            trackers=[],
            files=[file_info],
            pieces=pieces,
            is_multi_file=False
        )
        storage = Storage(torrent, self.test_dir)
        pm = PieceManager(torrent, storage)
        return pm, piece_payloads, storage

    def test_rarest_first_selection(self):
        pm, _, _ = self._create_fixture()

        # Peer 1 has piece 0 and 1
        bf1 = Bitfield()
        bf1.set_piece(0)
        bf1.set_piece(1)
        pm.update_peer_bitfield('peer_1', bf1)

        # Peer 2 has piece 0 only
        bf2 = Bitfield()
        bf2.set_piece(0)
        pm.update_peer_bitfield('peer_2', bf2)

        # Swarm availability:
        # Piece 0: count = 2
        # Piece 1: count = 1 (rarest!)
        # Piece 2: count = 0
        availability = pm.get_piece_availability()
        self.assertEqual(availability[0], 2)
        self.assertEqual(availability[1], 1)
        self.assertEqual(availability[2], 0)

        # Peer 1 asks for chunk: should get piece 1 (the rarest piece available from peer 1)
        chunk = pm.get_next_chunk_request('peer_1', bf1)
        self.assertIsNotNone(chunk)
        self.assertEqual(chunk.piece_index, 1)

    def test_block_sharing_across_peers(self):
        pm, payloads, storage = self._create_fixture()

        # Both peers have all pieces
        bf = Bitfield()
        bf.set_piece(0)
        bf.set_piece(1)
        bf.set_piece(2)
        pm.update_peer_bitfield('peer_1', bf)
        pm.update_peer_bitfield('peer_2', bf)

        # Peer 1 requests chunk 0 of active piece
        c1 = pm.get_next_chunk_request('peer_1', bf)
        self.assertIsNotNone(c1)

        # Peer 2 requests chunk: should share the piece and get chunk 1 (block sharing!)
        c2 = pm.get_next_chunk_request('peer_2', bf)
        self.assertIsNotNone(c2)
        self.assertEqual(c1.piece_index, c2.piece_index)
        self.assertNotEqual(c1.chunk_index, c2.chunk_index)

        # Peer 1 delivers chunk 0
        p_idx = c1.piece_index
        p_data = payloads[p_idx]
        done1 = pm.on_chunk_received(
            p_idx, c1.begin, p_data[c1.begin: c1.begin + c1.length])
        self.assertFalse(done1)  # Only 1 chunk received

        # Peer 2 delivers chunk 1
        done2 = pm.on_chunk_received(
            p_idx, c2.begin, p_data[c2.begin: c2.begin + c2.length])
        self.assertTrue(done2)  # Piece complete and written directly to disk!
        self.assertIn(p_idx, pm.completed_pieces)

        # Verify piece was written directly to disk
        read_back = storage.read_piece(p_idx)
        self.assertEqual(read_back, p_data)


if __name__ == '__main__':
    unittest.main()
