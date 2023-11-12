import time
import socket
import logging
import struct
import hashlib
from typing import Optional, Callable
from dataclasses import dataclass
from bittorrent.ip import IpAndPort
from bittorrent.torrent import Piece


@dataclass
class PieceData:
    piece: Piece
    data: bytes


@dataclass
class PeerMessage:
    KEEP_ALIVE = -1
    CHOKE = 0
    UNCHOKE = 1
    INTERESTED = 2
    NOT_INTERESTED = 3
    HAVE = 4
    BITFIELD = 5
    REQUEST = 6
    PIECE = 7
    CANCEL = 8

    message_id: int
    payload: bytes = b''


    def write(self, sock: socket.socket):
        data = self.message_id.to_bytes(1, 'big') + self.payload
        sock.sendall(len(data).to_bytes(4, 'big') + data)


    @classmethod
    def read(cls, sock: socket.socket) -> Optional['PeerMessage']:
        try:
            message_size_bytes = PeerMessage.read_bytes(sock, 4)
            if not message_size_bytes:
                return None
            message_size = int.from_bytes(message_size_bytes, 'big')
            if message_size == 0:
                return PeerMessage(PeerMessage.KEEP_ALIVE)
            data = PeerMessage.read_bytes(sock, message_size)
            if not data:
                return None
            return PeerMessage(data[0], data[1:])
        except socket.error:
            return None


    @staticmethod
    def read_bytes(sock: socket.socket, size: int) -> Optional[bytes]:
        data = bytearray()
        while len(data) != size:
            length = size - len(data)
            tmp = sock.recv(length)
            if tmp == b'':
                return None
            data.extend(tmp)
        return bytes(data)


@dataclass
class Bitfield:
    value: bytearray = bytearray()


    def set_piece(self, index: int):
        q = index // 8
        r = index % 8
        if q >= len(self.value):
            diff = q - len(self.value) + 1
            self.value.extend(bytes(diff))

        byte = self.value[q]
        mask = 1 << (7 - r)
        self.value[q] = byte ^ mask


    def has_piece(self, index: int) -> bool:
        q = index // 8
        r = index % 8
        if q >= len(self.value):
            return False

        byte = self.value[q]
        mask = 1 << (7 - r)
        return mask & byte > 0


    @property
    def size(self) -> int:
        return len(self.value)


class Peer:
    def __init__(
        self,
        peer: IpAndPort,
        get_work: Callable[[Bitfield], Optional[Piece]],
        put_work: Callable[[Piece], None],
        put_result: Callable[[PieceData], None],
        has_finished: Callable[[], bool],
        info_hash: bytes,
        peer_id: bytes,
        piece_count: int,
        chunk_size: int,
        max_batch_requests: int
    ):
        self.peer = peer
        self.get_work = get_work
        self.put_work = put_work
        self.put_result = put_result
        self.has_finished = has_finished
        self.info_hash = info_hash
        self.peer_id = peer_id
        self.piece_count = piece_count
        self.chunk_size = chunk_size
        self.max_batch_requests = max_batch_requests
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.bitfield = Bitfield()
        self.choked = True
        self.cancel = False


    def start(self):
        self.sock.settimeout(5)
        if not self._connect():
            self.sock.close()
            return

        self.sock.settimeout(30)
        while not self.has_finished():
            work = self.get_work(self.bitfield)
            if not work:
                logging.warning('No work in queue')
                time.sleep(30)
                continue

            try:
                self._download(work)
            except socket.error as e:
                logging.error('Socket error: %s', e)
                self.put_work(work)
                return

        logging.debug('Shutdown peer...')
        self.sock.close()


    def cancel_work(self):
        self.cancel = True


    def _connect(self) -> bool:
        try:
            self.sock.connect((self.peer.ip, self.peer.port))
            logging.debug(f'Connected to {self.peer.ip}:{self.peer.port}!')
            if self._handshake():
                logging.info(f'Handshake with {self.peer.ip}:{self.peer.port}!')
                return True
            logging.error(f'Failed handshake with {self.peer.ip}:{self.peer.port}')
        except socket.error as e:
            logging.error(f'Failed to connect to {self.peer.ip}:{self.peer.port}: %s', e)
        return False


    def _handshake(self) -> bool:
        header = struct.pack('!b', 19) + b'BitTorrent protocol'
        handshake = header + struct.pack('!Q20s20s', 0, self.info_hash, self.peer_id)
        logging.debug('Sent handshake: %s', header + struct.pack('!Q20s20s', 0, self.info_hash, self.peer_id))
        self.sock.send(handshake)
        data = PeerMessage.read_bytes(self.sock, len(header) + 48)
        if not data:
            return False

        _header, _, _info_hash, _ = struct.unpack('!20sQ20s20s', data)
        logging.debug('Recv handshake: %s', data)
        return (
            _header == header and
            _info_hash == self.info_hash
        )


    def _download(self, work: Piece):
        downloaded_data = bytearray(work.size)
        downloaded_bytes = 0
        should_request_chunks = True
        requests_received = 0

        logging.debug('Start download...')
        PeerMessage(PeerMessage.UNCHOKE).write(self.sock)
        PeerMessage(PeerMessage.INTERESTED).write(self.sock)

        while True:
            message = PeerMessage.read(self.sock)

            if not message:
                self.put_work(work)
                return

            elif message.message_id == PeerMessage.KEEP_ALIVE:
                time.sleep(3)

            elif message.message_id == PeerMessage.CHOKE:
                logging.debug('Choked!')
                self.choked = True

            elif message.message_id == PeerMessage.UNCHOKE:
                logging.debug('Unchoked!')
                self.choked = False

            elif message.message_id == PeerMessage.INTERESTED:
                logging.debug('_INTERESTED')

            elif message.message_id == PeerMessage.NOT_INTERESTED:
                logging.debug('_NOT_INTERESTED')

            elif message.message_id == PeerMessage.HAVE:
                logging.debug('_HAVE')
                index = int.from_bytes(message.payload, 'big')
                self.bitfield.set_piece(index)

            elif message.message_id == PeerMessage.BITFIELD:
                logging.debug('_BITFIELD')
                self.bitfield.value = bytearray(message.payload)
                if not self.bitfield.has_piece(work.index):
                    logging.warning(f'Peer does not have data')
                    self.put_work(work)
                    return

            elif message.message_id == PeerMessage.REQUEST:
                logging.debug('_REQUEST')

            elif message.message_id == PeerMessage.PIECE:
                index = int.from_bytes(message.payload[0:4], 'big')
                start = int.from_bytes(message.payload[4:8], 'big')
                data = message.payload[8:]
                downloaded_data[start : start + len(data)] = data
                downloaded_bytes += len(data)
                progress = int(100 * downloaded_bytes / work.size)
                requests_received += 1
                if requests_received == self.max_batch_requests:
                    should_request_chunks = True

                logging.debug(f'Piece #{work.index}: {downloaded_bytes}/{work.size} bytes downloaded ({progress}%).')

                if downloaded_bytes == work.size:
                    if self._sha1(downloaded_data) == work.sha1:
                        PeerMessage(PeerMessage.HAVE, work.index.to_bytes(4, 'big')).write(self.sock)
                        self.put_result(PieceData(work, downloaded_data))
                        return
                    else:
                        logging.warning('Piece corrupted!')
                        self.put_work(work)
                        return

            elif message.message_id == PeerMessage.CANCEL:
                logging.debug('_CANCEL')

            if self.cancel:
                return

            if should_request_chunks and not self.choked:
                logging.debug(f'Sending request to {self.peer.ip} for piece #{work.index}...')
                should_request_chunks = False
                requests_received = 0
                tmp = downloaded_bytes
                for _ in range(self.max_batch_requests):
                    length = min(self.chunk_size, work.size - tmp)
                    payload = struct.pack('!III', work.index, tmp, length)
                    PeerMessage(PeerMessage.REQUEST, payload).write(self.sock)
                    tmp += length


    def _sha1(self, data: bytes) -> bytes:
        hasher = hashlib.sha1()
        hasher.update(data)
        return hasher.digest()