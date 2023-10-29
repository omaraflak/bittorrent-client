import os
import time
import socket
import struct
import random
import logging
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Optional, Callable
from dataclasses import dataclass
from client.ip import IpAndPort
from client.torrent import Torrent, Piece
from client.peer import PeerRequest, PeerResult, Peer


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
            '!QII',
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
        action, transaction_id, connection_id = struct.unpack('!IIQ', data)
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
            '!QII20s20sQQQIIIiH',
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
        ) = struct.unpack('!IIIII' + peers_count * 'IH', data)
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
        max_workers: int = 1
    ):
        self.torrent = torrent
        self.max_workers = max_workers
        self.peer_id = random.randbytes(Client._PEER_ID_LENGTH)
        self.left = self.torrent.file_size
        self.downloaded = 0


    def download(self, output_directory: str):
        tracker = random.choice(self.torrent.get_trackers('udp'))
        response = self._announce(tracker, _AnnounceRequest.EVENT_START)
        if not response:
            logging.error(f'Failed to announce START to tracker {tracker.ip}:{tracker.port}')
            return

        self.lock = threading.Lock()
        self.available_peers = {peer for peer in response.peers if peer.port != 0}
        self.pieces_received: list[tuple[Piece, bytes]] = []

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for piece in self.torrent.pieces():
                worker = Peer(lambda: self._get_peer_request(piece))
                executor.submit(self._with_callback(worker.start, executor))

        logging.info('Writing file ...')
        with open(os.path.join(output_directory, self.torrent.file_name), 'wb') as file:
            for _, data in sorted(self.pieces_received, key=lambda x: x[0].index):
                file.write(data)

        logging.info('File written to disk!')

        response = self._announce(tracker, _AnnounceRequest.EVENT_STOP)
        if not response:
            logging.error(f'Failed to announce STOP to tracker {tracker.ip}:{tracker.port}')
            return


    def _get_peer_request(self, piece: Piece) -> PeerRequest:
        while not self.available_peers:
            logging.info('No worker available, waiting 3 seconds ...')
            time.sleep(3)

        with self.lock:
            return PeerRequest(
                self.available_peers.pop(),
                self.torrent.info_hash,
                self.peer_id,
                piece
            )


    def _on_peer_result(self, result: PeerResult, executor: ThreadPoolExecutor):
        if result.error:
            worker = Peer(lambda: self._get_peer_request(result.request.piece))
            executor.submit(self._with_callback(worker.start))
            return

        with self.lock:
            self.pieces_received.append((result.request.piece, result.data))
            self.available_peers.add(result.request.peer)

        progress = int(100 * len(self.pieces_received) / self.torrent.piece_count)
        logging.info(f'{progress}% downloaded')


    def _with_callback(self, fun: Callable[[], PeerResult], executor: ThreadPoolExecutor) -> Callable[[], PeerResult]:
        def _fun() -> PeerResult:
            result = fun()
            self._on_peer_result(result, executor)
            return result
        return _fun


    def _announce(self, tracker: IpAndPort, event: int) -> Optional[_AnnounceResponse]:
        logging.info(f'Announcing to {tracker.ip}:{tracker.port} ...')

        self.transaction_id = int.from_bytes(random.randbytes(4), 'big')

        # open socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(10)

        try:
            # connect
            connect_request = _ConnectRequest(self.transaction_id)
            sock.sendto(connect_request.to_bytes(), (tracker.ip, tracker.port))
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
            sock.sendto(announce_request.to_bytes(), (tracker.ip, tracker.port))
            announce_response = _AnnounceResponse.from_bytes(sock.recv(_AnnounceResponse.size(self.max_workers)))
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
