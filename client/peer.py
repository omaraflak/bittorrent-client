import time
import socket
import logging
import struct
import hashlib
from abc import ABC, abstractclassmethod
from enum import Enum
from dataclasses import dataclass
from client.ip import IpAndPort
from client.torrent import Piece


class PeerError(Enum):
    CONNECTION = 1
    HANDSHAKE = 2
    CORRUPTED = 3
    MISSING_DATA = 4
    CANCEL = 5
    UNKNOWN = 6


@dataclass
class PeerResult:
    data: bytes


class PeerIO(ABC):
    @abstractclassmethod
    def get_peer(self) -> IpAndPort:
        raise NotImplementedError()


    @abstractclassmethod
    def on_result(self, result: PeerResult, peer: IpAndPort, piece: Piece):
        raise NotImplementedError()


    @abstractclassmethod
    def on_error(self, error: PeerError, peer: IpAndPort, piece: Piece):
        raise NotImplementedError()


class Peer:
    _CHUNK_SIZE = 2 ** 14
    _CHOKE = 0
    _UNCHOKE = 1
    _INTERESTED = 2
    _NOT_INTERESTED = 3
    _HAVE = 4
    _BITFIELD = 5
    _REQUEST = 6
    _PIECE = 7
    _CANCEL = 8


    def __init__(
        self,
        io: PeerIO,
        info_hash: bytes,
        peer_id: bytes,
        piece: Piece
    ):
        self.io = io
        self.info_hash = info_hash
        self.peer_id = peer_id
        self.piece = piece


    def start(self):
        peer = self.io.get_peer()
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)

        if not self._connect(sock, peer):
            return

        # check index presence
        message = self._recv_message(sock)
        logging.debug('Waiting for bitfield ...')
        if message[0] == Peer._BITFIELD:
            bitfield = message[1:]
            bitfield_bin = bin(int.from_bytes(bitfield, 'big'))
            logging.debug(f'Received bitfield: {bitfield_bin}')
            logging.debug(f'Checking index {self.piece.index}')
            if bitfield_bin[self.piece.index] == '0':
                logging.warning(f'Peer does not have data')
                sock.close()
                self.io.on_error(PeerError.MISSING_DATA, peer, self.piece)
                return
        else:
            logging.error(f'Expected bitfield, got {message}')
            self.io.on_error(PeerError.UNKNOWN, peer, self.piece)
            return

        # receive loop
        downloaded_data = bytearray(self.piece.max_piece_size)
        downloaded_bytes = 0

        logging.debug('Starting download ...')
        self._send_message(sock, struct.pack('!b', Peer._INTERESTED))
        data = struct.pack('!bIII', Peer._REQUEST, self.piece.index, downloaded_bytes, Peer._CHUNK_SIZE)
        self._send_message(sock, data)

        while True:
            message = self._recv_message(sock)

            if len(message) == 0:
                logging.debug('Keep alive ...')
                time.sleep(3)

            elif message[0] == Peer._CHOKE:
                logging.debug('Choked! Sleeping 10 seconds ...')
                time.sleep(10)

            elif message[0] == Peer._UNCHOKE:
                logging.debug('Unchoked')
                length = min(Peer._CHUNK_SIZE, self.piece.max_piece_size - downloaded_bytes)
                data = struct.pack('!bIII', Peer._REQUEST, self.piece.index, downloaded_bytes, length)
                self._send_message(sock, data)

            elif message[0] == Peer._INTERESTED:
                logging.debug('Received _INTERESTED')

            elif message[0] == Peer._NOT_INTERESTED:
                logging.debug('Received _NOT_INTERESTED')

            elif message[0] == Peer._HAVE:
                logging.debug('Received _HAVE')

            elif message[0] == Peer._BITFIELD:
                logging.debug('Received _BITFIELD')

            elif message[0] == Peer._REQUEST:
                logging.debug('Received _REQUEST')

            elif message[0] == Peer._PIECE:
                index, = struct.unpack('!I', message[1:5])
                start, = struct.unpack('!I', message[5:9])
                data = message[9:]
                downloaded_data[start : start + len(data)] = data
                downloaded_bytes += len(data)
                progress = int(100 * downloaded_bytes / self.piece.max_piece_size)
                logging.debug(f'Received chunk! {downloaded_bytes}/{self.piece.max_piece_size} bytes downloaded ({progress}%).')

                if downloaded_bytes == self.piece.max_piece_size or downloaded_bytes == 0:
                    logging.debug('Received piece! Closing peer connection.')
                    sock.close()
                    hasher = hashlib.sha1()
                    hasher.update(downloaded_data)
                    piece_hash = hasher.digest()
                    logging.debug(f'downloaded hash: {piece_hash}')
                    logging.debug(f'expected hash: {self.piece.piece_hash}')
                    if piece_hash == self.piece.piece_hash:
                        logging.debug('Piece hash matches')
                        self.io.on_result(PeerResult(data), peer, self.piece)
                        return
                    else:
                        logging.debug('Piece corrupted!')
                        self.io.on_error(PeerError.CORRUPTED, peer, self.piece)
                        return

                length = min(Peer._CHUNK_SIZE, self.piece.max_piece_size - downloaded_bytes)
                data = struct.pack('!bIII', Peer._REQUEST, self.piece.index, downloaded_bytes, length)
                self._send_message(sock, data)

            elif message[0] == Peer._CANCEL:
                logging.debug('Received _CANCEL')
                sock.close()
                self.io.on_error(PeerError.CANCEL, peer, self.piece)
                return

            time.sleep(3)


    def _connect(self, sock: socket.socket, peer: IpAndPort) -> bool:
        try:
            logging.debug(f'Connecting to {peer.ip}:{peer.port} ...')
            sock.connect((peer.ip, peer.port))
            logging.debug('Connected!')
            if self._handshake(sock):
                logging.debug('Handshake successful!')
                return True
            logging.error('Handshake failed')
            sock.close()
            self.io.on_error(PeerError.HANDSHAKE, peer, self.piece)
        except socket.error as e:
            logging.error('Connection failed: %s', e)
            sock.close()
            self.io.on_error(PeerError.CONNECTION, peer, self.piece)
        return False


    def _handshake(self, sock: socket.socket) -> bool:
        header = struct.pack('!b', 19) + b'BitTorrent protocol'
        handshake = header + struct.pack('!Q20s20s', 0, self.info_hash, self.peer_id)
        logging.debug('Sent handshake: %s', header + struct.pack('!Q20s20s', 0, self.info_hash, self.peer_id))
        sock.send(handshake)
        data = sock.recv(len(header) + 48)
        _header, _, _info_hash, _ = struct.unpack('!20sQ20s20s', data)
        logging.debug('Recv handshake: %s', data)
        return (
            _header == header and
            _info_hash == self.info_hash
        )


    def _send_message(self, sock: socket.socket, data: bytes):
        sock.send(struct.pack('!I', len(data)) + data)


    def _recv_message(self, sock: socket.socket) -> bytes:
        size, = struct.unpack('!I', sock.recv(4))
        return sock.recv(size)