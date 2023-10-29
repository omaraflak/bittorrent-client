import hashlib
from urllib.parse import urlparse
from dataclasses import dataclass
from typing import Iterator, Optional
from functools import cached_property
from client.ip import IpAndPort
from client.bencode import bencode, decode_bencode


@dataclass
class ChunkId:
    index: int
    start: int
    length: int


class Torrent:
    _SHA1_SIZE = 20
    _TRACKER = 'announce'
    _TRACKER_LIST = 'announce-list'
    _INFO = 'info'
    _FILE_BYTES = 'length'
    _FILE_NAME = 'name'
    _FILE_PIECES = 'pieces'
    _PIECE_BYTES = 'piece length'


    def __init__(self, data: bytes):
        self._decoded = decode_bencode(data)


    def get_trackers(self, scheme: Optional[str] = 'udp') -> list[IpAndPort]:
        result: list[tuple[str, int]] = []
        for url in self.trackers:
            res = urlparse(url)
            if not scheme or scheme == res.scheme.lower():
                result.append(IpAndPort(res.hostname, res.port))
        return result


    def chunks(self, chunk_size: int = 2 ** 14) -> Iterator[ChunkId]:
        chunk_size = min(chunk_size, self.piece_size)
        for i in range(self.piece_count):
            if i < self.piece_count - 1:
                piece_size = self.piece_size
            else:
                piece_size = self.file_size - self.piece_size * (self.piece_count - 1)

            q = piece_size // chunk_size
            r = piece_size % chunk_size
            for j in range(q):
                yield ChunkId(i, j * chunk_size, chunk_size)
            
            if r > 0:
                yield ChunkId(i, q * chunk_size, r)


    @cached_property
    def trackers(self) -> list[str]:
        if Torrent._TRACKER_LIST in self._decoded:
            return [
                url.decode()
                for tier in self._decoded[Torrent._TRACKER_LIST]
                for url in tier
            ]
        return self._decoded[Torrent._TRACKER]


    @cached_property
    def info_hash(self) -> bytes:
        hasher = hashlib.sha1()
        hasher.update(bencode(self._decoded[Torrent._INFO]))
        return hasher.digest()


    @cached_property
    def file_name(self) -> str:
        return self._decoded[Torrent._INFO][Torrent._FILE_NAME].decode()


    @cached_property
    def file_size(self) -> int:
        computed_size = self.piece_size * self.piece_count
        return int(self._decoded[Torrent._INFO].get(Torrent._FILE_BYTES, computed_size))


    @cached_property
    def hash_pieces(self) -> list[bytes]:
        parts = self._decoded[Torrent._INFO][Torrent._FILE_PIECES]
        hashes: list[bytes] = []
        for i in range(0, len(parts), Torrent._SHA1_SIZE):
            hashes.append(parts[i:i + Torrent._SHA1_SIZE])
        return hashes


    @cached_property
    def piece_count(self) -> int:
        return len(self._decoded[Torrent._INFO][Torrent._FILE_PIECES]) // Torrent._SHA1_SIZE


    @cached_property
    def piece_size(self) -> int:
        return int(self._decoded[Torrent._INFO][Torrent._PIECE_BYTES])


    @classmethod
    def from_file(cls, filepath: str) -> 'Torrent':
        with open(filepath, 'rb') as file:
            return Torrent(file.read())
