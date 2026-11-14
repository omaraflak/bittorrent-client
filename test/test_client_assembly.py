import os
import shutil
import tempfile
import hashlib
import unittest
from bittorrent.torrent import Torrent, Piece, File
from bittorrent.client import Client


class TestClientAssembly(unittest.TestCase):
    def setUp(self):
        self.test_dir = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def test_assemble_small_file_inside_one_piece(self):
        # A file of 100 bytes inside a piece of 1000 bytes
        # The old buggy code would write the entire 1000 bytes into the 100-byte file!
        piece_size = 1000
        full_piece_data = b'A' * 100 + b'B' * 900
        sha1 = hashlib.sha1(full_piece_data).digest()

        piece = Piece(0, len(full_piece_data), sha1)
        file_info = File(0, 0, 100, ['small.txt'])

        torrent = Torrent(
            name='small_test',
            size=100,
            piece_size=piece_size,
            piece_count=1,
            info_hash=b'1' * 20,
            trackers=[],
            files=[file_info],
            pieces=[piece],
            is_multi_file=False
        )

        client = Client(torrent)
        client.tmp = os.path.join(self.test_dir, '.tmp')
        os.makedirs(client.tmp, exist_ok=True)

        part_file = os.path.join(client.tmp, 'piece_0.part')
        with open(part_file, 'wb') as pf:
            pf.write(full_piece_data)

        out_path = os.path.join(self.test_dir, 'small.txt')
        client._assemble_file(file_info, out_path)

        with open(out_path, 'rb') as f:
            written_data = f.read()

        # MUST be exactly 100 bytes of 'A', not 1000 bytes!
        self.assertEqual(len(written_data), 100)
        self.assertEqual(written_data, b'A' * 100)

    def test_assemble_multi_file_spanning_pieces(self):
        # Piece size: 50 bytes. 3 pieces total = 150 bytes.
        # File 1: 30 bytes (piece 0: 0..30)
        # File 2: 60 bytes (piece 0: 30..50, piece 1: 0..40)
        # File 3: 60 bytes (piece 1: 40..50, piece 2: 0..50)
        p0 = b'1' * 30 + b'2' * 20
        p1 = b'2' * 40 + b'3' * 10
        p2 = b'3' * 50

        pieces = [
            Piece(0, len(p0), hashlib.sha1(p0).digest()),
            Piece(1, len(p1), hashlib.sha1(p1).digest()),
            Piece(2, len(p2), hashlib.sha1(p2).digest())
        ]

        files = [
            File(0, 0, 30, ['f1.bin']),
            File(1, 30, 60, ['sub', 'f2.bin']),
            File(2, 90, 60, ['f3.bin'])
        ]

        torrent = Torrent(
            name='multi_test',
            size=150,
            piece_size=50,
            piece_count=3,
            info_hash=b'2' * 20,
            trackers=[],
            files=files,
            pieces=pieces,
            is_multi_file=True
        )

        client = Client(torrent)
        client.tmp = os.path.join(self.test_dir, '.tmp')
        os.makedirs(client.tmp, exist_ok=True)

        for p, data in zip(pieces, [p0, p1, p2]):
            with open(os.path.join(client.tmp, f'piece_{p.index}.part'), 'wb') as f:
                f.write(data)

        client._assemble_files(self.test_dir)

        base_dir = os.path.join(self.test_dir, 'multi_test')
        with open(os.path.join(base_dir, 'f1.bin'), 'rb') as f:
            self.assertEqual(f.read(), b'1' * 30)

        with open(os.path.join(base_dir, 'sub', 'f2.bin'), 'rb') as f:
            self.assertEqual(f.read(), b'2' * 60)

        with open(os.path.join(base_dir, 'f3.bin'), 'rb') as f:
            self.assertEqual(f.read(), b'3' * 60)

        # Temporary directory should be removed after assembly
        self.assertFalse(os.path.exists(client.tmp))

    def test_human_friendly_bytes_str(self):
        self.assertEqual(Client._human_friendly_bytes_str(500), '500 B')
        self.assertEqual(Client._human_friendly_bytes_str(1024), '1.0 KiB')
        self.assertEqual(Client._human_friendly_bytes_str(1048576), '1.0 MiB')
        self.assertEqual(
            Client._human_friendly_bytes_str(1073741824), '1.0 GiB')
        self.assertEqual(Client._human_friendly_bytes_str(
            1099511627776), '1.0 TiB')


if __name__ == '__main__':
    unittest.main()
