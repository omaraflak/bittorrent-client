import time
import socket
import logging
import struct
import hashlib
from collections import deque
from typing import Optional
from dataclasses import dataclass
from client.ip import IpAndPort
from client.torrent import Piece


@dataclass
class PieceData:
    piece: Piece
    data: bytes


@dataclass
class PeerMessage:
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
        sock.send(len(data).to_bytes(4, 'big') + data)


    @classmethod
    def read(cls, sock: socket.socket) -> Optional['PeerMessage']:
        try:
            message_size = int.from_bytes(sock.recv(4), 'big')
            if message_size == 0:
                return None
            data = sock.recv(message_size)
            return PeerMessage(data[0], data[1:])
        except socket.error:
            return None


class Peer:
    def __init__(
        self,
        peer: IpAndPort,
        work_queue: deque[Piece],
        result_stack: list[PieceData],
        info_hash: bytes,
        peer_id: bytes,
        piece_count: int,
        chunk_size: int = 2 ** 14,
        max_batch_requests: int = 1
    ):
        self.peer = peer
        self.work_queue = work_queue
        self.result_stack = result_stack
        self.info_hash = info_hash
        self.peer_id = peer_id
        self.piece_count = piece_count
        self.chunk_size = chunk_size
        self.max_batch_requests = max_batch_requests
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.bitfield = bytearray()
        self.chocked = True


    def start(self):
        self.sock.settimeout(5)
        if not self._connect():
            self.sock.close()
            return

        self.sock.settimeout(30)
        while len(self.result_stack) != self.piece_count:
            try:
                self._download()
            except socket.error as e:
                logging.error('Socket error: %s', e)
                return

        logging.debug('Shutdown peer...')
        self.sock.close()


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
        data = self.sock.recv(len(header) + 48)
        _header, _, _info_hash, _ = struct.unpack('!20sQ20s20s', data)
        logging.debug('Recv handshake: %s', data)
        return (
            _header == header and
            _info_hash == self.info_hash
        )


    def _download(self):
        try:
            work = self.work_queue.popleft()
            if len(self.bitfield) > 0 and not self._has_piece(work.piece_index):
                self.work_queue.append(work)
                logging.warning(f'Peer {self.peer.ip} does not have piece #{work.piece_index}, dropping work.')
                time.sleep(5)
                return
        except IndexError:
            # the work queue may be empty because all pieces are being processed
            # however, we don't want peers to shutdown in case the peers processing
            # the pieces fail
            logging.warning('No work in queue')
            time.sleep(30)
            return

        downloaded_data = bytearray(work.piece_size)
        downloaded_bytes = 0
        should_request_chunks = True
        requests_received = 0

        logging.debug('Start download...')
        PeerMessage(PeerMessage.UNCHOKE).write(self.sock)
        PeerMessage(PeerMessage.INTERESTED).write(self.sock)

        while True:
            message = PeerMessage.read(self.sock)

            if not message:
                # keep alive
                time.sleep(3)

            elif message.message_id == PeerMessage.CHOKE:
                logging.debug('Choked!')
                self.chocked = True

            elif message.message_id == PeerMessage.UNCHOKE:
                logging.debug('Unchoked!')
                self.chocked = False

            elif message.message_id == PeerMessage.INTERESTED:
                logging.debug('_INTERESTED')

            elif message.message_id == PeerMessage.NOT_INTERESTED:
                logging.debug('_NOT_INTERESTED')

            elif message.message_id == PeerMessage.HAVE:
                logging.debug('_HAVE')
                index = int.from_bytes(message.payload, 'big')
                self._set_has_piece(index)

            elif message.message_id == PeerMessage.BITFIELD:
                logging.debug('_BITFIELD')
                self.bitfield = bytearray(message.payload)
                if not self._has_piece(work.piece_index):
                    logging.warning(f'Peer does not have data')
                    self.work_queue.append(work)
                    return

            elif message.message_id == PeerMessage.REQUEST:
                logging.debug('_REQUEST')

            elif message.message_id == PeerMessage.PIECE:
                index = int.from_bytes(message.payload[0:4], 'big')
                start = int.from_bytes(message.payload[4:8], 'big')
                data = message.payload[8:]
                downloaded_data[start : start + len(data)] = data
                downloaded_bytes += len(data)
                progress = int(100 * downloaded_bytes / work.piece_size)
                requests_received += 1
                if requests_received == self.max_batch_requests:
                    should_request_chunks = True

                logging.info(f'Piece #{work.piece_index}: {downloaded_bytes}/{work.piece_size} bytes downloaded ({progress}%).')

                if downloaded_bytes == work.piece_size:
                    logging.info(f'Received piece {work.piece_index}!')
                    PeerMessage(PeerMessage.HAVE, work.piece_index.to_bytes(4, 'big')).write(self.sock)
                    hasher = hashlib.sha1()
                    hasher.update(downloaded_data)
                    piece_hash = hasher.digest()
                    logging.debug(f'downloaded hash: {piece_hash}')
                    logging.debug(f'expected hash: {work.piece_hash}')
                    if piece_hash == work.piece_hash:
                        logging.debug('Piece hash matches')
                        self.result_stack.append(PieceData(work, downloaded_data))
                        return
                    else:
                        logging.warning('Piece corrupted!')
                        self.work_queue.append(work)
                        return

            elif message.message_id == PeerMessage.CANCEL:
                logging.debug('_CANCEL')
                self.work_queue.append(work)
                return

            if should_request_chunks and not self.chocked and self._has_piece(work.piece_index):
                logging.debug(f'Sending request to {self.peer.ip} for piece #{work.piece_index}...')
                should_request_chunks = False
                requests_received = 0
                tmp = downloaded_bytes
                for _ in range(self.max_batch_requests):
                    length = min(self.chunk_size, work.piece_size - tmp)
                    payload = struct.pack('!III', work.piece_index, tmp, length)
                    PeerMessage(PeerMessage.REQUEST, payload).write(self.sock)
                    tmp += length


    def _set_has_piece(self, index: int):
        q = index // 8
        r = index % 8
        if q >= len(self.bitfield):
            diff = q - len(self.bitfield) + 1
            self.bitfield.extend(bytes(diff))

        byte = self.bitfield[q]
        mask = 1 << (7 - r)
        self.bitfield[q] = byte ^ mask


    def _has_piece(self, index: int) -> bool:
        q = index // 8
        r = index % 8
        if q >= len(self.bitfield):
            return False

        byte = self.bitfield[q]
        mask = 1 << (7 - r)
        return mask & byte > 0