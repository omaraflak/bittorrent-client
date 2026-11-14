import time
import random
import hashlib
import logging
from typing import Optional
from bittorrent.torrent import Torrent, Piece
from bittorrent.peer import Bitfield
from bittorrent.storage import Storage

CHUNK_SIZE = 16384  # Standard 16 KiB BitTorrent chunk


class Chunk:
    __slots__ = ('piece_index', 'chunk_index', 'begin', 'length',
                 'is_received', 'in_flight_peer', 'requested_time')

    def __init__(self, piece_index: int, chunk_index: int, begin: int, length: int):
        self.piece_index = piece_index
        self.chunk_index = chunk_index
        self.begin = begin
        self.length = length
        self.is_received = False
        self.in_flight_peer: Optional[str] = None
        self.requested_time = 0.0


class PieceState:
    def __init__(self, piece: Piece, chunk_size: int = CHUNK_SIZE):
        self.piece = piece
        self.chunk_size = chunk_size
        num_chunks = (piece.size + chunk_size -
                      1) // chunk_size if piece.size > 0 else 0
        self.chunks = [
            Chunk(
                piece.index,
                i,
                i * chunk_size,
                min(chunk_size, piece.size - i * chunk_size)
            )
            for i in range(num_chunks)
        ]
        self.buffer = bytearray(piece.size)
        self.is_done = False
        self.received_count = 0

    @property
    def is_complete(self) -> bool:
        return self.received_count == len(self.chunks)


class PieceManager:
    """
    Coordinates chunk-level scheduling, rarest-first piece prioritization,
    multi-peer block sharing, and endgame request duplication.
    """

    def __init__(self, torrent: Torrent, storage: Storage):
        self.torrent = torrent
        self.storage = storage
        self.pieces = [PieceState(p) for p in torrent.pieces]
        self.completed_pieces: set[int] = set()
        self.peer_bitfields: dict[str, Bitfield] = {}

    def mark_piece_completed(self, piece_index: int):
        """Mark a piece as already completed (e.g. from existing files on disk)."""
        if piece_index < len(self.pieces):
            ps = self.pieces[piece_index]
            ps.is_done = True
            ps.received_count = len(ps.chunks)
            ps.buffer = bytearray()
            for c in ps.chunks:
                c.is_received = True
            self.completed_pieces.add(piece_index)

    def update_peer_bitfield(self, peer_id: str, bitfield: Bitfield):
        self.peer_bitfields[peer_id] = bitfield

    def update_peer_have(self, peer_id: str, piece_index: int):
        if peer_id in self.peer_bitfields:
            self.peer_bitfields[peer_id].set_piece(piece_index)

    def remove_peer(self, peer_id: str):
        if peer_id in self.peer_bitfields:
            del self.peer_bitfields[peer_id]
        # Release in-flight chunks reserved by this peer
        for p in self.pieces:
            if not p.is_done:
                for c in p.chunks:
                    if c.in_flight_peer == peer_id and not c.is_received:
                        c.in_flight_peer = None
                        c.requested_time = 0.0

    def get_piece_availability(self) -> list[int]:
        """Compute the availability count (number of connected peers having each piece)."""
        counts = [0] * len(self.pieces)
        for bf in self.peer_bitfields.values():
            for i in range(len(self.pieces)):
                if bf.has_piece(i):
                    counts[i] += 1
        return counts

    def get_next_chunk_request(self, peer_id: str, bitfield: Bitfield) -> Optional[Chunk]:
        """
        Selects the optimal next chunk to request from a peer using:
        1. Block Sharing: Pending chunks in currently active in-progress pieces.
        2. Rarest-First: Rarest available unactivated pieces.
        3. Endgame: Chunk duplication across remaining available peers.
        """
        now = time.time()

        # Step 1: Block Sharing - check currently in-progress pieces for unrequested chunks
        # Multiple peers collaborate on the same piece to finish it faster
        in_progress_pieces = [
            p for p in self.pieces
            if not p.is_done and any(c.is_received or c.in_flight_peer is not None for c in p.chunks)
            and (bitfield.size == 0 or bitfield.has_piece(p.piece.index))
        ]
        for p in in_progress_pieces:
            for c in p.chunks:
                if not c.is_received:
                    if c.in_flight_peer is None or (now - c.requested_time > 6.0):
                        c.in_flight_peer = peer_id
                        c.requested_time = now
                        return c

        # Step 2: Rarest-First piece activation
        availability = self.get_piece_availability()
        candidate_pieces = [
            p for p in self.pieces
            if not p.is_done and p.received_count == 0 and (bitfield.size == 0 or bitfield.has_piece(p.piece.index))
        ]

        if candidate_pieces:
            # Sort by swarm availability count ascending, random tie-breaker
            candidate_pieces.sort(key=lambda p: (
                # Prioritize pieces with known seeders
                availability[p.piece.index] == 0,
                availability[p.piece.index],
                random.random()
            ))

            for chosen_piece in candidate_pieces:
                for c in chosen_piece.chunks:
                    if not c.is_received and c.in_flight_peer is None:
                        c.in_flight_peer = peer_id
                        c.requested_time = now
                        return c

        # Step 3: Endgame mode - all pieces are active or in-flight
        # Duplicate remaining unreceived chunks across all available peers
        incomplete_pieces = [
            p for p in self.pieces
            if not p.is_done and (bitfield.size == 0 or bitfield.has_piece(p.piece.index))
        ]
        for p in incomplete_pieces:
            for c in p.chunks:
                if not c.is_received and c.in_flight_peer != peer_id:
                    c.requested_time = now
                    return c

        return None

    def on_chunk_received(self, piece_index: int, begin: int, data: bytes) -> bool:
        """
        Processes a chunk block received from any peer.
        If all chunks of the piece are assembled, verifies the piece's SHA-1 hash,
        writes it directly to destination files on disk, and releases memory.
        Returns True if the piece was successfully completed and written to disk.
        """
        if piece_index >= len(self.pieces):
            return False

        ps = self.pieces[piece_index]
        if ps.is_done:
            return False

        chunk_idx = begin // ps.chunk_size
        if chunk_idx >= len(ps.chunks):
            return False

        chunk = ps.chunks[chunk_idx]
        if not chunk.is_received:
            chunk.is_received = True
            ps.buffer[begin: begin + len(data)] = data
            ps.received_count += 1

        if ps.is_complete:
            # Verify SHA-1 hash
            digest = hashlib.sha1(ps.buffer).digest()
            if digest == ps.piece.sha1:
                # Direct-to-disk write: zero copy, zero .tmp files!
                self.storage.write_piece(piece_index, bytes(ps.buffer))
                ps.is_done = True
                self.completed_pieces.add(piece_index)
                ps.buffer = bytearray()  # Free memory immediately
                return True
            else:
                logging.warning(
                    f'Piece #{piece_index} hash mismatch. Discarding corrupt data and re-queueing.')
                for c in ps.chunks:
                    c.is_received = False
                    c.in_flight_peer = None
                    c.requested_time = 0.0
                ps.received_count = 0
                return False

        return False

    def is_finished(self) -> bool:
        return len(self.completed_pieces) >= len(self.pieces)
