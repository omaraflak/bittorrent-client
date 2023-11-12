import hashlib
from urllib.parse import urlparse
from dataclasses import dataclass, field
from dataclasses_json import DataClassJsonMixin, config
from typing import Optional
from bittorrent.ip import IpAndPort
from bittorrent.bencode import bencode, decode_bencode


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


    def __hash__(self) -> int:
        return hash((self.index, self.size, self.sha1))


@dataclass
class Torrent:
    name: str
    size: int
    piece_size: int
    piece_count: int
    info_hash: bytes
    trackers: list[str]
    files: list[File]
    pieces: list[Piece]


    def trackers_by_protocol(self, protocol: Optional[str] = None) -> list[IpAndPort]:
        result: list[tuple[str, int]] = []
        for url in self.trackers:
            res = urlparse(url)
            if not res.port:
                continue
            if not protocol or protocol.lower() == res.scheme.lower():
                result.append(IpAndPort(res.hostname, res.port))
        return result


    @classmethod
    def from_file(cls, filepath: str) -> 'Torrent':
        with open(filepath, 'rb') as file:
            return Torrent.from_bytes(file.read())


    @classmethod
    def from_bytes(cls, data: bytes) -> 'Torrent':
        decoded = decode_bencode(data)
        metainfo = MetaInfo.from_dict(decoded)
        info_hash = Torrent.sha1(bencode(decoded['info']))
        file_size = metainfo.info.length or sum([file.length for file in metainfo.info.files])
        piece_count = len(metainfo.info.pieces) // 20

        # trackers
        trackers: list[str] = list()
        if metainfo.trackers:
            trackers.append(metainfo.trackers.decode())
        if metainfo.trackers_list:
            for tier in metainfo.trackers_list:
                for url in tier:
                    trackers.append(url.decode())

        # files
        files: list[File] = list()
        if metainfo.info.length:
            files.append(File(0, 0, metainfo.info.length, [metainfo.info.name.decode()]))
        else:
            start = 0
            for index, file in enumerate(metainfo.info.files):
                paths = [path.decode() for path in file.path]
                files.append(File(index, start, file.length, paths))
                start += file.length

        # pieces
        hashes = [
            metainfo.info.pieces[i : i + 20]
            for i in range(0, len(metainfo.info.pieces), 20)
        ]
        pieces: list[Piece] = list()
        for i in range(piece_count):
            if i < piece_count - 1:
                piece_size = metainfo.info.piece_length
            else:
                piece_size = file_size - metainfo.info.piece_length * (piece_count - 1)
            pieces.append(Piece(i, piece_size, hashes[i]))
    
        return Torrent(
            metainfo.info.name.decode(),
            file_size,
            metainfo.info.piece_length,
            piece_count,
            info_hash,
            trackers,
            files,
            pieces
        )


    @staticmethod
    def sha1(data: bytes) -> bytes:
        hasher = hashlib.sha1()
        hasher.update(data)
        return hasher.digest()
