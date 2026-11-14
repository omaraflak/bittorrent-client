import os
import hashlib
from bittorrent.torrent import Torrent, File


class Storage:
    """
    Direct-to-disk sparse file storage manager.
    Eliminates temporary .part files and assembly overhead by writing verified
    piece data directly to target destination files at their exact byte offsets.
    """

    def __init__(self, torrent: Torrent, output_directory: str):
        self.torrent = torrent
        self.output_directory = output_directory
        self._init_files()

    def get_file_path(self, file_info: File) -> str:
        """Resolve the safe absolute path for a file in the torrent."""
        if self.torrent.is_multi_file:
            base_dir = os.path.join(self.output_directory, self.torrent.name)
            return os.path.join(base_dir, *file_info.path)
        else:
            return os.path.join(self.output_directory, file_info.path[-1])

    def _init_files(self):
        """Create directories and pre-allocate target files (sparse files)."""
        for file_info in self.torrent.files:
            file_path = self.get_file_path(file_info)
            parent_dir = os.path.dirname(file_path)
            if parent_dir:
                os.makedirs(parent_dir, exist_ok=True)

            if not os.path.exists(file_path):
                with open(file_path, 'wb') as f:
                    if file_info.size > 0:
                        f.seek(file_info.size - 1)
                        f.write(b'\x00')

    def write_piece(self, piece_index: int, piece_data: bytes):
        """
        Write verified piece data directly into target files at their exact offsets.
        Handles piece boundaries that span multiple files accurately.
        """
        piece_size = self.torrent.piece_size
        piece_start = piece_index * piece_size
        piece_end = piece_start + len(piece_data)

        for file_info in self.torrent.files:
            file_start = file_info.start
            file_end = file_start + file_info.size

            if piece_start >= file_end or piece_end <= file_start:
                continue

            overlap_start = max(piece_start, file_start)
            overlap_end = min(piece_end, file_end)

            slice_data = piece_data[overlap_start -
                                    piece_start: overlap_end - piece_start]
            file_offset = overlap_start - file_start

            file_path = self.get_file_path(file_info)
            with open(file_path, 'r+b') as f:
                f.seek(file_offset)
                f.write(slice_data)

    def read_piece(self, piece_index: int) -> bytes:
        """Read a piece directly from disk across file boundaries."""
        if piece_index >= len(self.torrent.pieces):
            return b''

        piece = self.torrent.pieces[piece_index]
        piece_size = self.torrent.piece_size
        piece_start = piece_index * piece_size
        piece_end = piece_start + piece.size

        buf = bytearray(piece.size)

        for file_info in self.torrent.files:
            file_start = file_info.start
            file_end = file_start + file_info.size

            if piece_start >= file_end or piece_end <= file_start:
                continue

            overlap_start = max(piece_start, file_start)
            overlap_end = min(piece_end, file_end)

            file_offset = overlap_start - file_start
            length = overlap_end - overlap_start

            file_path = self.get_file_path(file_info)
            if not os.path.exists(file_path):
                return b''

            try:
                with open(file_path, 'rb') as f:
                    f.seek(file_offset)
                    chunk = f.read(length)
                    buf[overlap_start - piece_start: overlap_end -
                        piece_start] = chunk
            except OSError:
                return b''

        return bytes(buf)

    def verify_existing_pieces(self) -> set[int]:
        """
        Verify which pieces already exist on disk and match their expected SHA-1.
        Allows instant resume without any temporary files.
        """
        completed = set()
        for file_info in self.torrent.files:
            if not os.path.exists(self.get_file_path(file_info)):
                return completed

        for piece in self.torrent.pieces:
            data = self.read_piece(piece.index)
            if len(data) == piece.size and hashlib.sha1(data).digest() == piece.sha1:
                completed.add(piece.index)

        return completed
