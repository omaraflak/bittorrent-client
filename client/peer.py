import time
import socket
import logging
import struct
import hashlib
from enum import Enum
from dataclasses import dataclass
from typing import Optional, Callable
from client.ip import IpAndPort
from client.torrent import Piece


@dataclass
class PeerRequest:
    peer: IpAndPort
    info_hash: bytes
    peer_id: bytes
    piece: Piece


class PeerError(Enum):
    CONNECTION = 1
    HANDSHAKE = 2
    CORRUPTED = 3
    MISSING_DATA = 4
    CANCEL = 5
    UNKNOWN = 6


@dataclass
class PeerResult:
    request: PeerRequest
    data: bytes
    error: Optional[PeerError] = None


class Peer:
    _CHOKE = 0
    _UNCHOKE = 1
    _INTERESTED = 2
    _NOT_INTERESTED = 3
    _HAVE = 4
    _BITFIELD = 5
    _REQUEST = 6
    _PIECE = 7
    _CANCEL = 8


    def __init__(self, get_request: Callable[[], PeerRequest]):
        self.get_request = get_request


    def start(self) -> PeerResult:
        request = self.get_request()
        piece = request.piece
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)

        error = self._connect(sock, request)
        if error is not None:
            return PeerResult(request, b'', error)

        # check index presence
        message = self._recv_message(sock)
        logging.debug('Waiting for bitfield ...')
        if message[0] == Peer._BITFIELD:
            bitfield = message[1:]
            bitfield_bin = bin(int.from_bytes(bitfield, 'big'))
            logging.debug(f'Received bitfield: {bitfield_bin}')
            logging.debug(f'Checking index {piece.index}')
            if bitfield_bin[piece.index] == '0':
                logging.warning(f'Peer does not have data')
                sock.close()
                return PeerResult(request, b'', PeerError.MISSING_DATA)
        else:
            logging.error(f'Expected bitfield, got {message}')
            return PeerResult(request, b'', PeerError.UNKNOWN)

        # receive loop
        downloaded_data = bytearray(piece.max_piece_size)
        downloaded_bytes = 0

        logging.debug('Starting download ...')
        self._send_message(sock, struct.pack('!b', Peer._INTERESTED))

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
                length = min(2 ** 14, piece.max_piece_size - downloaded_bytes)
                data = struct.pack('!bIII', Peer._REQUEST, piece.index, downloaded_bytes, length)
                self._send_message(sock, data)

            elif message[0] == Peer._INTERESTED:
                logging.debug('Received _INTERESTED')
                continue

            elif message[0] == Peer._NOT_INTERESTED:
                logging.debug('Received _NOT_INTERESTED')
                continue

            elif message[0] == Peer._HAVE:
                logging.debug('Received _HAVE')
                continue

            elif message[0] == Peer._BITFIELD:
                logging.debug('Received _BITFIELD')
                continue

            elif message[0] == Peer._REQUEST:
                logging.debug('Received _REQUEST')
                continue

            elif message[0] == Peer._PIECE:
                index, = struct.unpack('!I', message[1:5])
                start, = struct.unpack('!I', message[5:9])
                data = message[9:]
                downloaded_data[start : start + len(data)] = data
                downloaded_bytes += len(data)
                progress = int(100 * downloaded_bytes / piece.max_piece_size)
                logging.debug(f'Received chunk! {downloaded_bytes}/{piece.max_piece_size} bytes downloaded ({progress}%).')

                if downloaded_bytes == piece.max_piece_size or downloaded_bytes == 0:
                    logging.debug('Received piece! Closing peer connection.')
                    sock.close()
                    hasher = hashlib.sha1()
                    hasher.update(downloaded_data)
                    if hasher.digest() == piece.piece_hash:
                        logging.debug('Piece hash matches')
                        return PeerResult(request, data)
                    else:
                        logging.debug('Piece corrupted!')
                        return PeerResult(request, b'', PeerError.CORRUPTED)

                length = min(2 ** 14, piece.max_piece_size - downloaded_bytes)
                data = struct.pack('!bIII', Peer._REQUEST, piece.index, downloaded_bytes, length)
                self._send_message(sock, data)

            elif message[0] == Peer._CANCEL:
                sock.close()
                return PeerResult(request, b'', PeerError.CANCEL)


    def _connect(self, sock: socket.socket, request: PeerRequest) -> Optional[PeerError]:
        peer = request.peer
        try:
            logging.debug(f'Connecting to {peer.ip}:{peer.port} ...')
            sock.connect((peer.ip, peer.port))
            logging.debug('Connected!')
            if self._handshake(sock, request):
                logging.debug('Handshake successful!')
                return None
            logging.error('Handshake failed')
            sock.close()
            return PeerError.HANDSHAKE
        except socket.error as e:
            logging.error('Connection failed: %s', e)
            sock.close()
            return PeerError.CONNECTION


    def _handshake(self, sock: socket.socket, request: PeerRequest) -> bool:
        header = struct.pack('!b', 19) + b'BitTorrent protocol'
        handshake = header + struct.pack('!Q20s20s', 0, request.info_hash, request.peer_id)
        logging.debug('Sent handshake: %s', header + struct.pack('!Q20s20s', 0, request.info_hash, request.peer_id))
        sock.send(handshake)
        data = sock.recv(len(header) + 48)
        _header, _, _info_hash, _ = struct.unpack('!20sQ20s20s', data)
        logging.debug('Recv handshake: %s', data)
        return (
            _header == header and
            _info_hash == request.info_hash
        )


    def _send_message(self, sock: socket.socket, data: bytes):
        sock.send(struct.pack('!I', len(data)) + data)


    def _recv_message(self, sock: socket.socket) -> bytes:
        size, = struct.unpack('!I', sock.recv(4))
        return sock.recv(size)