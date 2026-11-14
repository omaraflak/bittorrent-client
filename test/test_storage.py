import os
import shutil
import tempfile
import hashlib
import unittest
from bittorrent.torrent import Torrent, Piece, File
from bittorrent.storage import Storage


class TestStorage(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_direct_to_disk_single_file(self):
        # 1 piece of 100 bytes
        p_data = b'X' * 100
        p_sha1 = hashlib.sha1(p_data).digest()
        piece = Piece(0, 100, p_sha1)
        file_info = File(0, 0, 100, ['file.dat'])

        torrent = Torrent(
            name='single_direct',
            size=100,
            piece_size=100,
            piece_count=1,
            info_hash=b'1' * 20,
            trackers=[],
            files=[file_info],
            pieces=[piece],
            is_multi_file=False
        )

        storage = Storage(torrent, self.test_dir)
        # Pre-allocation should have created the file
        expected_path = os.path.join(self.test_dir, 'file.dat')
        self.assertTrue(os.path.exists(expected_path))

        # Write piece directly to disk
        storage.write_piece(0, p_data)

        # Verify contents on disk
        with open(expected_path, 'rb') as f:
            self.assertEqual(f.read(), p_data)

        # Read back via storage
        read_back = storage.read_piece(0)
        self.assertEqual(read_back, p_data)

        # Verify existing pieces detects it
        verified = storage.verify_existing_pieces()
        self.assertEqual(verified, {0})

    def test_direct_to_disk_multi_file_boundary_crossing(self):
        # Piece size: 50. Total: 120 bytes. 3 pieces: 50, 50, 20.
        # File 1: 40 bytes (piece 0: 0..40)
        # File 2: 80 bytes (piece 0: 40..50, piece 1: 0..50, piece 2: 0..20)
        f1_data = b'A' * 40
        f2_data = b'B' * 80
        total_data = f1_data + f2_data

        p0_data = total_data[0:50]
        p1_data = total_data[50:100]
        p2_data = total_data[100:120]

        pieces = [
            Piece(0, 50, hashlib.sha1(p0_data).digest()),
            Piece(1, 50, hashlib.sha1(p1_data).digest()),
            Piece(2, 20, hashlib.sha1(p2_data).digest())
        ]

        files = [
            File(0, 0, 40, ['f1.txt']),
            File(1, 40, 80, ['sub', 'f2.txt'])
        ]

        torrent = Torrent(
            name='multi_direct',
            size=120,
            piece_size=50,
            piece_count=3,
            info_hash=b'2' * 20,
            trackers=[],
            files=files,
            pieces=pieces,
            is_multi_file=True
        )

        storage = Storage(torrent, self.test_dir)

        # Write pieces in non-sequential order (piece 1, then 2, then 0)
        storage.write_piece(1, p1_data)
        storage.write_piece(2, p2_data)
        storage.write_piece(0, p0_data)

        # Check target files on disk
        base_dir = os.path.join(self.test_dir, 'multi_direct')
        with open(os.path.join(base_dir, 'f1.txt'), 'rb') as f:
            self.assertEqual(f.read(), f1_data)

        with open(os.path.join(base_dir, 'sub', 'f2.txt'), 'rb') as f:
            self.assertEqual(f.read(), f2_data)

        # Verify all pieces detected as complete
        verified = storage.verify_existing_pieces()
        self.assertEqual(verified, {0, 1, 2})


if __name__ == '__main__':
    unittest.main()
