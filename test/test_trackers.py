import unittest
from bittorrent.trackers import (
    _ConnectRequest,
    _ConnectResponse,
    _AnnounceRequest,
    _AnnounceResponse
)
from bittorrent.ip import IpAndPort


class TestTrackers(unittest.TestCase):
    def test_connect_packet_serialization(self):
        req = _ConnectRequest(transaction_id=12345)
        raw = req.to_bytes()
        self.assertEqual(len(raw), 16)

        resp_raw = (0).to_bytes(4, 'big') + (12345).to_bytes(4,
                                                             'big') + (987654321).to_bytes(8, 'big')
        resp = _ConnectResponse.from_bytes(resp_raw)
        self.assertIsNotNone(resp)
        self.assertEqual(resp.transaction_id, 12345)
        self.assertEqual(resp.connection_id, 987654321)

    def test_announce_packet_serialization(self):
        req = _AnnounceRequest(
            connection_id=987654321,
            transaction_id=54321,
            info_hash=b'i' * 20,
            peer_id=b'p' * 20,
            downloaded=100,
            left=200,
            uploaded=0
        )
        raw = req.to_bytes()
        self.assertEqual(len(raw), 98)

    def test_announce_response_parsing(self):
        # Action=1 (announce), trans_id=54321, interval=1800, leechers=2, seeders=5
        header = (1).to_bytes(4, 'big') + (54321).to_bytes(4, 'big') + \
            (1800).to_bytes(4, 'big') + \
            (2).to_bytes(4, 'big') + (5).to_bytes(4, 'big')
        # Peer 1: 192.168.1.1:6881 (0xC0A80101, 0x1AE1)
        # Peer 2: 10.0.0.1:51413 (0x0A000001, 0xC8D5)
        peer1 = bytes([192, 168, 1, 1]) + (6881).to_bytes(2, 'big')
        peer2 = bytes([10, 0, 0, 1]) + (51413).to_bytes(2, 'big')

        raw = header + peer1 + peer2
        resp = _AnnounceResponse.from_bytes(raw)
        self.assertIsNotNone(resp)
        self.assertEqual(resp.transaction_id, 54321)
        self.assertEqual(resp.interval, 1800)
        self.assertEqual(resp.leechers, 2)
        self.assertEqual(resp.seeders, 5)
        self.assertEqual(len(resp.peers), 2)
        self.assertEqual(resp.peers[0], IpAndPort('192.168.1.1', 6881))
        self.assertEqual(resp.peers[1], IpAndPort('10.0.0.1', 51413))

    def test_announce_error_response_handling(self):
        # Action=3 (error)
        header = (3).to_bytes(4, 'big') + \
            (54321).to_bytes(4, 'big') + b'torrent not found'
        resp = _AnnounceResponse.from_bytes(header)
        self.assertIsNone(resp)


if __name__ == '__main__':
    unittest.main()
