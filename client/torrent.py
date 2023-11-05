import hashlib
from urllib.parse import urlparse
from dataclasses import dataclass, field
from dataclasses_json import DataClassJsonMixin, config
from typing import Iterator, Optional
from functools import cached_property
from client.ip import IpAndPort
from client.bencode import bencode, decode_bencode


@dataclass
class InfoFile(DataClassJsonMixin):
    path: list[bytes]
    length: int


@dataclass
class Info(DataClassJsonMixin):
    name: bytes
    pieces: bytes
    piece_length: int = field(metadata=config(field_name='piece length'))
    length: Optional[int] = None
    files: Optional[list[InfoFile]] = None


@dataclass
class MetaInfo(DataClassJsonMixin):
    trackers: Optional[bytes] = field(metadata=config(field_name='announce'))
    trackers_list: Optional[list[list[bytes]]] = field(metadata=config(field_name='announce-list'))
    info: Info


@dataclass
class File:
    index: int
    start: int
    size: int
    path: list[str]


@dataclass
class Piece:
    index: int
    size: int
    sha1: bytes


class Torrent:
    _SHA1_SIZE = 20


    def __init__(self, data: bytes):
        decoded = decode_bencode(data)
        self._metainfo = MetaInfo.from_dict(decoded)
        hasher = hashlib.sha1()
        hasher.update(bencode(decoded['info']))
        self.info_hash = hasher.digest()


    def get_trackers(self, scheme: Optional[str] = 'udp') -> list[IpAndPort]:
        result: list[tuple[str, int]] = []
        for url in self.trackers:
            res = urlparse(url)
            if not res.port:
                continue
            if not scheme or scheme == res.scheme.lower():
                result.append(IpAndPort(res.hostname, res.port))
        return result


    def pieces(self) -> Iterator[Piece]:
        for i in range(self.piece_count):
            if i < self.piece_count - 1:
                piece_size = self.piece_size
            else:
                piece_size = self.file_size - self.piece_size * (self.piece_count - 1)

            yield Piece(i, self.hash_pieces[i], piece_size)


    @cached_property
    def trackers(self) -> list[str]:
        result: list[str] = list()

        if self._metainfo.trackers:
            result.append(self._metainfo.trackers.decode())

        if self._metainfo.trackers_list:
            for tier in self._metainfo.trackers_list:
                for url in tier:
                    result.append(url.decode())

        return result


    @cached_property
    def files(self) -> list[File]:
        if self._metainfo.info.length:
            return File(0, 0, self._metainfo.info.length, [self._metainfo.info.name.decode()])

        result: list[File] = list()
        start = 0
        for index, file in enumerate(self._metainfo.info.files):
            paths = [path.decode() for path in file.path]
            result.append(File(index, start, file.length, paths))
            start += file.length

        return result


    @cached_property
    def file_size(self) -> int:
        return self._metainfo.info.length or sum([file.length for file in self._metainfo.info.files])


    @cached_property
    def hash_pieces(self) -> list[bytes]:
        parts = self._metainfo.info.pieces
        hashes: list[bytes] = []
        for i in range(0, len(parts), Torrent._SHA1_SIZE):
            hashes.append(parts[i : i + Torrent._SHA1_SIZE])
        return hashes


    @cached_property
    def piece_count(self) -> int:
        return len(self._metainfo.info.pieces) // Torrent._SHA1_SIZE


    @cached_property
    def piece_size(self) -> int:
        return self._metainfo.info.piece_length


    @classmethod
    def from_file(cls, filepath: str) -> 'Torrent':
        with open(filepath, 'rb') as file:
            return Torrent(file.read())
