import socket
import struct
import random
import logging
from typing import Optional
from dataclasses import dataclass
from client.torrent import Torrent


@dataclass
class IpAndPort:
    ip: str
    port: int


@dataclass
class _ConnectRequest:
    _PROTOCOL_ID = 0x41727101980
    _ACTION = 0

    transaction_id: int

    def to_bytes(self) -> bytes:
        # Offset  Size            Name            Value
        # 0       64-bit integer  protocol_id     0x41727101980 // magic constant
        # 8       32-bit integer  action          0 // connect
        # 12      32-bit integer  transaction_id
        return struct.pack(
            "!QII",
            _ConnectRequest._PROTOCOL_ID,
            _ConnectRequest._ACTION,
            self.transaction_id
        )


@dataclass
class _ConnectResponse:
    transaction_id: int
    connection_id: int


    @classmethod
    def from_bytes(cls, data: bytes) -> '_ConnectResponse':
        # Offset  Size            Name            Value
        # 0       32-bit integer  action          0 // connect
        # 4       32-bit integer  transaction_id
        # 8       64-bit integer  connection_id
        action, transaction_id, connection_id = struct.unpack("!IIQ", data)
        assert action == _ConnectRequest._ACTION
        return _ConnectResponse(transaction_id, connection_id)


    @staticmethod
    def size() -> int:
        return 16


@dataclass
class _AnnounceRequest:
    _ACTION = 1
    EVENT_COMPLETE = 1
    EVENT_START = 2
    EVENT_STOP = 3

    connection_id: int
    transaction_id: int
    info_hash: int
    peer_id: bytes
    downloaded: bytes
    left: int
    event: int = 0
    uploaded: int = 0
    key: int = 0
    num_want: int  = -1
    ip_address: int = 0
    port: int = 0


    def to_bytes(self) -> bytes:
        # Offset  Size    Name    Value
        # 0       64-bit integer  connection_id
        # 8       32-bit integer  action          1 // announce
        # 12      32-bit integer  transaction_id
        # 16      20-byte string  info_hash
        # 36      20-byte string  peer_id
        # 56      64-bit integer  downloaded
        # 64      64-bit integer  left
        # 72      64-bit integer  uploaded
        # 80      32-bit integer  event           0 // 0: none; 1: completed; 2: started; 3: stopped
        # 84      32-bit integer  IP address      0 // default
        # 88      32-bit integer  key
        # 92      32-bit integer  num_want        -1 // default
        # 96      16-bit integer  port
        return struct.pack(
            "!QII20s20sQQQIIIiH",
            self.connection_id,
            _AnnounceRequest._ACTION,
            self.transaction_id,
            self.info_hash,
            self.peer_id,
            self.downloaded,
            self.left,
            self.uploaded,
            self.event,
            self.ip_address,
            self.key,
            self.num_want,
            self.port
        )


@dataclass
class _AnnounceResponse:
    transaction_id: int
    interval: int
    leechers: int
    seeders: int
    peers: list[IpAndPort]


    @classmethod
    def from_bytes(cls, data: bytes) -> '_AnnounceResponse':
        # Offset      Size            Name            Value
        # 0           32-bit integer  action          1 // announce
        # 4           32-bit integer  transaction_id
        # 8           32-bit integer  interval
        # 12          32-bit integer  leechers
        # 16          32-bit integer  seeders
        # 20 + 6 * n  32-bit integer  IP address
        # 24 + 6 * n  16-bit integer  TCP port
        peers_count = (len(data) - 20) // 6
        (
            action,
            transaction_id,
            interval,
            leechers,
            seeders,
            *ips_and_ports
        ) = struct.unpack("!IIIII" + peers_count * "IH", data)
        assert action == _AnnounceRequest._ACTION
        peers = [
            IpAndPort(
                _AnnounceResponse._parse_ip(ips_and_ports[i]),
                ips_and_ports[i + 1]
            )
            for i in range(0, len(ips_and_ports), 2)
        ]
        return _AnnounceResponse(transaction_id, interval, leechers, seeders, peers)


    @staticmethod
    def _parse_ip(ip: int) -> str:
        return socket.inet_ntoa(struct.pack('!L', ip))


    @staticmethod
    def size(peers: int) -> int:
        return 20 + 6 * peers


class Client:
    _PEER_ID_LENGTH = 20


    def __init__(
        self,
        torrent: Torrent,
        max_peers: int = 10,
        ip: Optional[str] = None,
        port: int = 0
    ):
        self.torrent = torrent
        self.max_peers = max_peers
        self.listen_ip = ip
        self.listen_port = port
        self.peer_id = random.randbytes(Client._PEER_ID_LENGTH)
        self.left = self.torrent.piece_count * self.torrent.piece_size # self.torrent.file_size
        self.downloaded = 0


    def start(self):
        tracker = random.choice(self.torrent.trackers_with_scheme("udp"))
        response = self._announce(tracker, _AnnounceRequest.EVENT_START)
        if not response:
            logging.error(f'Failed to announce START to tracker {tracker[0]}:{tracker[1]}')
            return

        for peer in response.peers:
            if peer.port == 0:
                continue
            if self._download_from_peer(peer):
                break

        response = self._announce(tracker, _AnnounceRequest.EVENT_STOP)
        if not response:
            logging.error(f'Failed to announce STOP to tracker {tracker[0]}:{tracker[1]}')
            return


    def _announce(self, tracker: tuple[str, int], event: int) -> Optional[_AnnounceResponse]:
        logging.info(f'Announcing to {tracker[0]}:{tracker[1]} ...')

        self._new_transaction()

        # open socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(10)

        try:
            # connect
            connect_request = _ConnectRequest(self.transaction_id)
            sock.sendto(connect_request.to_bytes(), tracker)
            connect_response = _ConnectResponse.from_bytes(sock.recv(_ConnectResponse.size()))
            if self.transaction_id != connect_response.transaction_id:
                logging.error('Tracker did not return the expected transaction id')
                sock.close()
                return None

            # announce
            announce_request = _AnnounceRequest(
                connect_response.connection_id,
                self.transaction_id,
                self.torrent.info_hash,
                self.peer_id,
                self.downloaded,
                self.left,
                event
            )
            sock.sendto(announce_request.to_bytes(), tracker)
            announce_response = _AnnounceResponse.from_bytes(sock.recv(_AnnounceResponse.size(self.max_peers)))
            if self.transaction_id != announce_response.transaction_id:
                logging.error('Tracker did not return the expected transaction id')
                sock.close()
                return None

            logging.info('Announced to tracker successfully!')

            sock.close()
            return announce_response
        except socket.error as e:
            logging.error('Announce failed: %s', e)
            sock.close()
            return None


    def _new_transaction(self):
        self.transaction_id = int.from_bytes(random.randbytes(4), 'big')


    def _connect_to_peer(self, peer: IpAndPort) -> Optional[socket.socket]:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(4)
        try:
            logging.info(f'Connecting to {peer.ip}:{peer.port} ...')
            sock.connect((peer.ip, peer.port))
            logging.info('Connected!')
            if self._peer_handshake(sock):
                logging.info('Handshake successful!')
                return sock
            logging.error('Handshake failed')
        except socket.error as e:
            logging.error('Connection failed: %s', e)
        finally:
            sock.close()
            return None


    def _download_from_peer(self, peer: IpAndPort) -> bool:
        sock = self._connect_to_peer(peer)
        if not sock:
            return False

        # download file
        sock.close()
        return True


    def _peer_handshake(self, sock: socket.socket) -> bool:
        header = struct.pack('!b', 19) + b'BitTorrent protocol'
        handshake = header + struct.pack('!Q20s20s', 0, self.torrent.info_hash, self.peer_id)
        logging.info('Sent handshake: %s', header + struct.pack('!Q20s20s', 0, self.torrent.info_hash, self.peer_id))
        sock.send(handshake)
        data = sock.recv(len(header) + 48)
        _header, _, _info_hash, _ = struct.unpack('!20sQ20s20s', data)
        logging.info('Recv handshake: %s', data)
        return (
            _header == header and
            _info_hash == self.torrent.info_hash
        )


    def _send_peer_message(self, sock: socket.socket, data: bytes):
        sock.send(struct.pack('!I', len(data)) + data)


    def _recv_peer_message(self, sock: socket.socket) -> bytes:
        return sock.recv(struct.unpack('!I', sock.recv(4)))