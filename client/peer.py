import socket
import logging
import struct
from enum import Enum
from dataclasses import dataclass
from typing import Union, Optional
from client.ip import IpAndPort
from client.torrent import ChunkId


@dataclass
class PeerRequest:
    peer: IpAndPort
    info_hash: bytes
    peer_id: bytes
    chunk_id: ChunkId


@dataclass
class PeerResult:
    data: bytes


class PeerErrorType(Enum):
    CONNECTION = 1
    HANDSHAKE = 2


@dataclass
class PeerError:
    error: PeerErrorType
    request: PeerRequest


class Peer:
    def start(self, request: PeerRequest) -> Union[PeerResult, PeerError]:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
    
        error = self._connect(sock, request)
        if error:
            return error
        
        # download
        sock.close()
        return PeerResult(b'')


    def _connect(self, sock: socket.socket, request: PeerRequest) -> Optional[PeerError]:
        peer = request.peer
        try:
            logging.info(f'Connecting to {peer.ip}:{peer.port} ...')
            sock.connect((peer.ip, peer.port))
            logging.info('Connected!')
            if self._handshake(sock, request):
                logging.info('Handshake successful!')
                return None
            logging.error('Handshake failed')
            sock.close()
            return PeerError(PeerErrorType.HANDSHAKE, request)
        except socket.error as e:
            logging.error('Connection failed: %s', e)
            sock.close()
            return PeerError(PeerErrorType.CONNECTION, request)


    def _handshake(self, sock: socket.socket, request: PeerRequest) -> bool:
        header = struct.pack('!b', 19) + b'BitTorrent protocol'
        handshake = header + struct.pack('!Q20s20s', 0, request.info_hash, request.peer_id)
        logging.info('Sent handshake: %s', header + struct.pack('!Q20s20s', 0, request.info_hash, request.peer_id))
        sock.send(handshake)
        data = sock.recv(len(header) + 48)
        _header, _, _info_hash, _ = struct.unpack('!20sQ20s20s', data)
        logging.info('Recv handshake: %s', data)
        return (
            _header == header and
            _info_hash == request.info_hash
        )


    def _send_message(self, sock: socket.socket, data: bytes):
        sock.send(struct.pack('!I', len(data)) + data)


    def _recv_message(self, sock: socket.socket) -> bytes:
        return sock.recv(struct.unpack('!I', sock.recv(4)))