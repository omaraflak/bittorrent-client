import socket
import unittest
from bittorrent.peer import Bitfield, PeerMessage


class TestBitfield(unittest.TestCase):
    def test_set_and_has_piece(self):
        bf = Bitfield()
        self.assertFalse(bf.has_piece(0))
        self.assertFalse(bf.has_piece(10))

        bf.set_piece(0)
        self.assertTrue(bf.has_piece(0))
        self.assertFalse(bf.has_piece(1))

        bf.set_piece(7)
        self.assertTrue(bf.has_piece(7))

        bf.set_piece(15)
        self.assertTrue(bf.has_piece(15))
        self.assertFalse(bf.has_piece(14))

    def test_repeated_set_piece_does_not_clear(self):
        # Crucial test: verifying bitwise OR instead of XOR
        bf = Bitfield()
        bf.set_piece(5)
        self.assertTrue(bf.has_piece(5))

        # Second set_piece on same index must keep it set
        bf.set_piece(5)
        self.assertTrue(bf.has_piece(5))


class TestPeerMessage(unittest.TestCase):
    def test_write_and_read_keep_alive(self):
        s_client, s_server = socket.socketpair()
        try:
            msg = PeerMessage(PeerMessage.KEEP_ALIVE)
            msg.write(s_client)
            read_msg = PeerMessage.read(s_server)
            self.assertEqual(read_msg.message_id, PeerMessage.KEEP_ALIVE)
        finally:
            s_client.close()
            s_server.close()

    def test_write_and_read_messages(self):
        messages = [
            PeerMessage(PeerMessage.CHOKE),
            PeerMessage(PeerMessage.UNCHOKE),
            PeerMessage(PeerMessage.INTERESTED),
            PeerMessage(PeerMessage.NOT_INTERESTED),
            PeerMessage(PeerMessage.HAVE, (42).to_bytes(4, 'big')),
            PeerMessage(PeerMessage.BITFIELD, b'\xff\x00\xaa'),
            PeerMessage(PeerMessage.PIECE, (1).to_bytes(
                4, 'big') + (0).to_bytes(4, 'big') + b'chunk_data')
        ]

        for original in messages:
            s_client, s_server = socket.socketpair()
            try:
                original.write(s_client)
                decoded = PeerMessage.read(s_server)
                self.assertEqual(decoded.message_id, original.message_id)
                self.assertEqual(decoded.payload, original.payload)
            finally:
                s_client.close()
                s_server.close()


if __name__ == '__main__':
    unittest.main()
