import hashlib
import unittest
from bittorrent.bencode import bencode
from bittorrent.torrent import Torrent


class TestTorrent(unittest.TestCase):
    def test_single_file_torrent(self):
        info = {
            'name': 'test.iso',
            'piece length': 16384,
            'pieces': b'01234567890123456789' * 2,
            'length': 25000
        }
        info_bytes = bencode(info)
        expected_hash = hashlib.sha1(info_bytes).digest()

        torrent_dict = {
            'announce': 'http://tracker.example.com:80/announce',
            'info': info
        }
        raw = bencode(torrent_dict)
        t = Torrent.from_bytes(raw)

        self.assertEqual(t.name, 'test.iso')
        self.assertEqual(t.size, 25000)
        self.assertEqual(t.piece_size, 16384)
        self.assertEqual(t.piece_count, 2)
        self.assertEqual(t.info_hash, expected_hash)
        self.assertFalse(t.is_multi_file)
        self.assertEqual(len(t.files), 1)
        self.assertEqual(t.files[0].size, 25000)
        self.assertEqual(t.pieces[0].size, 16384)
        self.assertEqual(t.pieces[1].size, 25000 - 16384)

    def test_multi_file_torrent(self):
        files_data = [
            {'length': 1000, 'path': ['docs', 'readme.txt']},
            {'length': 5000, 'path': ['bin', 'app.bin']}
        ]
        info = {
            'name': 'my_package',
            'piece length': 4096,
            'pieces': b'a' * 20 + b'b' * 20,
            'files': files_data
        }
        torrent_dict = {
            'announce': 'udp://tracker.opentrackr.org:1337/announce',
            'announce-list': [
                ['udp://tracker.opentrackr.org:1337/announce'],
                ['http://tracker.backup.com:8080/announce']
            ],
            'info': info
        }
        raw = bencode(torrent_dict)
        t = Torrent.from_bytes(raw)

        self.assertEqual(t.name, 'my_package')
        self.assertTrue(t.is_multi_file)
        self.assertEqual(t.size, 6000)
        self.assertEqual(len(t.files), 2)
        self.assertEqual(t.files[0].path, ['docs', 'readme.txt'])
        self.assertEqual(t.files[0].start, 0)
        self.assertEqual(t.files[0].size, 1000)
        self.assertEqual(t.files[1].path, ['bin', 'app.bin'])
        self.assertEqual(t.files[1].start, 1000)
        self.assertEqual(t.files[1].size, 5000)

        # Trackers should be deduplicated
        self.assertEqual(len(t.trackers), 2)
        udp_trackers = t.trackers_by_protocol('udp')
        self.assertEqual(len(udp_trackers), 1)
        self.assertEqual(udp_trackers[0].ip, 'tracker.opentrackr.org')
        self.assertEqual(udp_trackers[0].port, 1337)

    def test_path_traversal_sanitization(self):
        files_data = [
            {'length': 100, 'path': ['..', '..', 'etc', 'passwd']},
            {'length': 200, 'path': ['/var', 'log', 'test.log']}
        ]
        info = {
            'name': '../../malicious',
            'piece length': 16384,
            'pieces': b'x' * 20,
            'files': files_data
        }
        torrent_dict = {'info': info}
        raw = bencode(torrent_dict)
        t = Torrent.from_bytes(raw)

        self.assertEqual(t.name, 'malicious')
        self.assertNotIn('..', t.files[0].path)
        self.assertEqual(t.files[0].path, ['etc', 'passwd'])
        self.assertEqual(t.files[1].path, ['var', 'log', 'test.log'])

    def test_default_ports(self):
        info = {
            'name': 'test',
            'piece length': 16384,
            'pieces': b'0' * 20,
            'length': 100
        }
        torrent_dict = {
            'announce': 'http://tracker.example.com/announce',
            'announce-list': [
                ['https://secure.example.com/announce'],
                ['udp://udp.example.com/announce']
            ],
            'info': info
        }
        t = Torrent.from_bytes(bencode(torrent_dict))
        http = t.trackers_by_protocol('http')
        https = t.trackers_by_protocol('https')
        udp = t.trackers_by_protocol('udp')

        self.assertEqual(http[0].port, 80)
        self.assertEqual(https[0].port, 443)
        self.assertEqual(udp[0].port, 80)


if __name__ == '__main__':
    unittest.main()
