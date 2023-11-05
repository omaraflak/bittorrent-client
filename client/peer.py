import time
import socket
import logging
import struct
import hashlib
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
        data = self.payload + self.message_id.to_bytes(1, 'big')
        sock.send(len(data).to_bytes(4, 'big') + data)


    @classmethod
    def read(cls, sock: socket.socket) -> Optional['PeerMessage']:
        try:
            message_size = sock.recv(4)
            if len(message_size) == 0 or message_size == 0:
                return None
            data = sock.recv(int.from_bytes(message_size, 'big'))
            return PeerMessage(data[0], data[1:])
        except socket.error:
            return None


class Peer:
    def __init__(
        self,
        peer: IpAndPort,
        work_queue: list[Piece],
        result_queue: list[PieceData],
        info_hash: bytes,
        peer_id: bytes,
        piece_count: int,
        chunk_size: int = 2 ** 14
    ):
        self.peer = peer
        self.work_queue = work_queue
        self.result_queue = result_queue
        self.info_hash = info_hash
        self.peer_id = peer_id
        self.piece_count = piece_count
        self.chunk_size = chunk_size
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.has_piece: list[bool] = [False] * piece_count
        self.chocked = True


    def start(self):
        self.sock.settimeout(5)
        if not self._connect():
            self.sock.close()
            return

        self.sock.settimeout(30)
        while len(self.result_queue) != self.piece_count:
            try:
                self._download()
            except socket.error as e:
                logging.error('Socket error: %s', e)
                return

        logging.debug('Shutdown peer...')
        self.sock.close()


    def _connect(self) -> bool:
        try:
            logging.debug(f'Connecting to {self.peer.ip}:{self.peer.port}...')
            self.sock.connect((self.peer.ip, self.peer.port))
            logging.debug(f'Connected to {self.peer.ip}:{self.peer.port}!')
            if self._handshake():
                logging.info(f'Handshake with {self.peer.ip}:{self.peer.port}!')
                return True
            logging.error('Handshake failed')
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
            work = self.work_queue.pop()
        except IndexError:
            # the work queue may be empty because all pieces are being processed
            # however, we don't want peers to shutdown in case the peers processing
            # the pieces fail
            time.sleep(30)
            return

        downloaded_data = bytearray(work.piece_size)
        downloaded_bytes = 0
        received_chunk = True
        requested_chunk = False

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
                self.has_piece[index] = True

            elif message.message_id == PeerMessage.BITFIELD:
                logging.debug('_BITFIELD')
                bitfield = bin(int.from_bytes(message.payload, 'big'))[2:]
                for index, bit in enumerate(bitfield):
                    self.has_piece[index] = bit == '1'

                if not self.has_piece[work.piece_index]:
                    logging.warning(f'Peer does not have data')
                    self.work_queue.append(work)
                    return

            elif message.message_id == PeerMessage.REQUEST:
                logging.debug('_REQUEST')

            elif message.message_id == PeerMessage.PIECE:
                index, = struct.unpack('!I', message[1:5])
                start, = struct.unpack('!I', message[5:9])
                data = message[9:]
                downloaded_data[start : start + len(data)] = data
                downloaded_bytes += len(data)
                progress = int(100 * downloaded_bytes / work.piece_size)
                received_chunk = True
                logging.info(f'piece#{work.piece_index}: {downloaded_bytes}/{work.piece_size} bytes downloaded ({progress}%).')

                if downloaded_bytes == 0 or downloaded_bytes == work.piece_size:
                    logging.info(f'Received piece {work.piece_index}!')
                    PeerMessage(PeerMessage.HAVE, work.piece_index.to_bytes(4, 'big')).write(self.sock)
                    hasher = hashlib.sha1()
                    hasher.update(downloaded_data)
                    piece_hash = hasher.digest()
                    logging.debug(f'downloaded hash: {piece_hash}')
                    logging.debug(f'expected hash: {work.piece_hash}')
                    if piece_hash == work.piece_hash:
                        logging.debug('Piece hash matches')
                        self.result_queue.append(PieceData(work, downloaded_data))
                        return
                    else:
                        logging.warning('Piece corrupted!')
                        self.work_queue.append(work)
                        return

            elif message.message_id == PeerMessage.CANCEL:
                logging.debug('_CANCEL')
                self.work_queue.append(work)
                return

            if received_chunk and not requested_chunk and not self.chocked:
                received_chunk = False
                requested_chunk = True
                length = min(self.chunk_size, work.piece_size - downloaded_bytes)
                payload = struct.pack('!III', work.piece_index, downloaded_bytes, length)
                PeerMessage(PeerMessage.REQUEST, payload).write(self.sock)
