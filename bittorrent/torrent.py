import os
import hashlib
from urllib.parse import urlparse
from dataclasses import dataclass
from typing import Optional
from bittorrent.ip import IpAndPort
from bittorrent.bencode import bencode, decode_bencode_with_raw


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
    is_multi_file: bool = False

    def trackers_by_protocol(self, protocol: Optional[str] = None) -> list[IpAndPort]:
        result: list[IpAndPort] = []
        seen: set[IpAndPort] = set()

        for url in self.trackers:
            try:
                res = urlparse(url)
            except Exception:
                continue

            if not res.hostname:
                continue

            scheme = (res.scheme or '').lower()
            if protocol and scheme != protocol.lower():
                continue

            port = res.port
            if not port:
                if scheme == 'https':
                    port = 443
                elif scheme == 'http':
                    port = 80
                elif scheme == 'udp':
                    port = 80
                else:
                    continue

            endpoint = IpAndPort(res.hostname, port)
            if endpoint not in seen:
                seen.add(endpoint)
                result.append(endpoint)

        return result

    @classmethod
    def from_file(cls, filepath: str) -> 'Torrent':
        with open(filepath, 'rb') as file:
            return Torrent.from_bytes(file.read())

    @classmethod
    def from_bytes(cls, data: bytes) -> 'Torrent':
        decoded, raw_slices = decode_bencode_with_raw(data)
        if not isinstance(decoded, dict):
            raise ValueError(
                'Invalid torrent file: root element must be a bencoded dictionary.')

        info = decoded.get('info')
        if not isinstance(info, dict):
            raise ValueError(
                'Invalid torrent file: missing or invalid "info" dictionary.')

        # Compute SHA-1 info_hash directly from the raw byte slice of the info dictionary
        if 'info' in raw_slices:
            info_hash = hashlib.sha1(raw_slices['info']).digest()
        else:
            info_hash = hashlib.sha1(bencode(info)).digest()

        # Extract and sanitize torrent name
        raw_name = info.get('name', b'download')
        if isinstance(raw_name, bytes):
            name = raw_name.decode('utf-8', errors='replace')
        else:
            name = str(raw_name)
        # Prevent directory traversal in name
        name = os.path.basename(name.replace('\\', '/')) or 'download'

        # Extract trackers and deduplicate preserving order
        trackers: list[str] = []
        seen_trackers: set[str] = set()

        def add_tracker(raw_url: object):
            if not raw_url:
                return
            if isinstance(raw_url, bytes):
                url_str = raw_url.decode('utf-8', errors='replace').strip()
            elif isinstance(raw_url, str):
                url_str = raw_url.strip()
            else:
                return
            if url_str and url_str not in seen_trackers:
                seen_trackers.add(url_str)
                trackers.append(url_str)

        add_tracker(decoded.get('announce'))

        announce_list = decoded.get('announce-list')
        if isinstance(announce_list, list):
            for tier in announce_list:
                if isinstance(tier, list):
                    for url in tier:
                        add_tracker(url)
                else:
                    add_tracker(tier)

        # Parse files (single-file vs multi-file)
        files: list[File] = []
        is_multi_file = False
        raw_files = info.get('files')

        if isinstance(raw_files, list) and len(raw_files) > 0:
            is_multi_file = True
            current_start = 0
            for idx, file_info in enumerate(raw_files):
                if not isinstance(file_info, dict):
                    continue
                file_length = int(file_info.get('length', 0))
                raw_path_list = file_info.get('path', [])
                if not isinstance(raw_path_list, list):
                    raw_path_list = [raw_path_list]

                clean_paths: list[str] = []
                for p in raw_path_list:
                    p_str = p.decode(
                        'utf-8', errors='replace') if isinstance(p, bytes) else str(p)
                    # Sanitize path component: no '..' or absolute paths
                    clean_p = os.path.basename(p_str.replace('\\', '/'))
                    if clean_p and clean_p != '..':
                        clean_paths.append(clean_p)

                if not clean_paths:
                    clean_paths = [f'file_{idx}']

                files.append(
                    File(idx, current_start, file_length, clean_paths))
                current_start += file_length
            file_size = current_start
        else:
            is_multi_file = False
            file_size = int(info.get('length', 0))
            files.append(File(0, 0, file_size, [name]))

        # Parse piece hashes and pieces
        piece_length = int(info.get('piece length', 0))
        pieces_blob = info.get('pieces', b'')
        if not isinstance(pieces_blob, bytes):
            pieces_blob = bytes(pieces_blob)

        hashes = [
            pieces_blob[i:i + 20]
            for i in range(0, len(pieces_blob), 20)
            if len(pieces_blob[i:i + 20]) == 20
        ]
        piece_count = len(hashes)

        pieces: list[Piece] = []
        if piece_count > 0:
            for i in range(piece_count):
                if i < piece_count - 1:
                    cur_piece_size = piece_length
                else:
                    cur_piece_size = file_size - \
                        piece_length * (piece_count - 1)
                pieces.append(Piece(i, max(0, cur_piece_size), hashes[i]))

        return Torrent(
            name=name,
            size=file_size,
            piece_size=piece_length,
            piece_count=piece_count,
            info_hash=info_hash,
            trackers=trackers,
            files=files,
            pieces=pieces,
            is_multi_file=is_multi_file
        )

    @staticmethod
    def _sha1(data: bytes) -> bytes:
        return hashlib.sha1(data).digest()
