import hashlib
from urllib.parse import urlparse
from dataclasses import dataclass, field
from dataclasses_json import DataClassJsonMixin, config
from typing import Optional
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


@dataclass
class Torrent:
    _SHA1_SIZE = 20

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
            if not protocol or protocol == res.scheme.lower():
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
        piece_count = len(metainfo.info.pieces) // Torrent._SHA1_SIZE

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
        pieces: list[Piece] = list()
        piece_index = 0
        for i in range(0, len(metainfo.info.pieces), Torrent._SHA1_SIZE):
            if i < piece_count - 1:
                piece_size = metainfo.info.piece_length
            else:
                piece_size = file_size - metainfo.info.piece_length * (piece_count - 1)

            sha1 = metainfo.info.pieces[i : i + Torrent._SHA1_SIZE]
            pieces.append(Piece(piece_index, piece_size, sha1))
            piece_index += 1
    
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
