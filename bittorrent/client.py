import os
import math
import random
import logging
from threading import Lock
from typing import Optional
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from bittorrent.trackers import Trackers
from bittorrent.torrent import Torrent, Piece, File
from bittorrent.peer import Peer, Bitfield


class Client:
    def __init__(
        self,
        torrent: Torrent,
        max_peer_workers: int = 1000,
        max_tracker_workers: int = 100,
        max_peers_per_tracker: int = 5000,
        max_peers_per_piece: int = 5,
        max_peer_batch_requests: int = 10,
        piece_chunk_size: int = 2 ** 14
    ):
        self.torrent = torrent
        self.max_peer_workers = max_peer_workers
        self.max_peer_batch_requests = max_peer_batch_requests
        self.max_tracker_workers = max_tracker_workers
        self.max_peers_per_tracker = max_peers_per_tracker
        self.piece_chunk_size = piece_chunk_size
        self.max_peers_per_piece = max_peers_per_piece
        self.peer_id = random.randbytes(20)
        self.work_queue: set[Piece] = set()
        self.work_done: set[Piece] = set()
        self.workers_per_work: dict[Piece, list[Peer]] = defaultdict(list)
        self.lock = Lock()


    def download(self, output_directory: str):
        trackers = Trackers(
            self.torrent,
            self.peer_id,
            self.max_tracker_workers,
            self.max_peers_per_tracker
        )
        peers = trackers.get_peers()

        files_directory = os.path.join(output_directory, self.torrent.name)
        self.tmp = os.path.join(files_directory, '.tmp')
        os.makedirs(self.tmp, exist_ok=True)
        self._read_downloaded_parts()

        with ThreadPoolExecutor(max_workers=self.max_peer_workers) as executor:
            executor.map(
                Peer.start,
                [
                    Peer(
                        peer,
                        lambda bitfield: self._get_work(peer, bitfield),
                        lambda work: self._put_work(peer, work),
                        lambda piece, data: self._put_result(peer, piece, data),
                        self._has_finished,
                        self.torrent.info_hash,
                        self.peer_id,
                        self.torrent.piece_count,
                        self.piece_chunk_size,
                        self.max_peer_batch_requests
                    )
                    for peer in peers
                ]
            )

        if not self._has_finished():
            logging.error('Could not download file.')
            return

        self._write_files(files_directory)


    def _get_work(self, peer: Peer, bitfield: Bitfield) -> Optional[Piece]:
        with self.lock:
            candidates = [
                work
                for work in self.work_queue
                if (
                    (bitfield.size == 0 or bitfield.has_piece(work.index)) and
                    len(self.workers_per_work[work]) < self.max_peers_per_piece
                )
            ]

            if not candidates:
                candidates = [
                    work
                    for work, workers in self.workers_per_work.items()
                    if len(workers) < self.max_peers_per_piece
                ]
                if not candidates:
                    return None

            work = random.choice(candidates)
            self.workers_per_work[work].append(peer)
            if work in self.work_queue:
                self.work_queue.remove(work)

            return work


    def _put_work(self, peer: Peer, work: Piece):
        with self.lock:
            self.work_queue.add(work)
            self.workers_per_work[work].remove(peer)


    def _put_result(self, peer: Peer, piece: Piece, data: bytes):
        with self.lock:
            if piece in self.work_queue:
                self.work_queue.remove(piece)

            for worker in self.workers_per_work[piece]:
                if worker != peer:
                    worker.cancel()

            del self.workers_per_work[piece]

            self.work_done.add(piece)
            percent = int(100 * len(self.work_done) / self.torrent.piece_count)
            human = Client._human_friendly_bytes_str(len(self.work_done) * self.torrent.piece_size)
            logging.info(f'Progress: {len(self.work_done)}/{self.torrent.piece_count} ({percent}%) {human}')
        
        filepath = os.path.join(self.tmp, piece.sha1.hex())
        with open(filepath, 'wb') as file:
            file.write(data)


    def _has_finished(self) -> bool:
        return len(self.work_done) == self.torrent.piece_count


    def _read_downloaded_parts(self):
        parts_downloaded = {os.path.basename(path) for path in os.listdir(self.tmp)}
        for piece in self.torrent.pieces:
            if piece.sha1.hex() in parts_downloaded:
                self.work_done.add(piece)
            else:
                self.work_queue.add(piece)


    def _write_files(self, output_directory: str):
        for file in self.torrent.files:
            file_directoy = os.path.join(output_directory, *file.path[:-1])
            os.makedirs(file_directoy, exist_ok=True)
            file_path = os.path.join(file_directoy, file.path[-1])
            self._assemble_file(file, file_path)

        logging.info('Files written to disk!')


    def _assemble_file(self, file: File, output: str):
        logging.info(f'Assembling {os.path.basename(output)}...')
        piece_size = self.torrent.piece_size
        start_index = file.start // piece_size
        start_offset = file.start % piece_size

        written = 0
        with open(output, 'wb') as f:
            for piece in self.torrent.pieces[start_index:]:
                part_path = os.path.join(self.tmp, piece.sha1.hex())
                with open(part_path, 'rb') as part:
                    data = part.read()
                    if piece.index == start_index:
                        written += f.write(data[start_offset:])
                    else:
                        rest = min(piece_size, file.size - written)
                        written += f.write(data[:rest])
                        if rest < piece_size:
                            return


    @staticmethod
    def _human_friendly_bytes_str(size: int) -> str:
        if size < 1e6:
            return f'{size // 1e3}KB'
        if size < 1e9:
            return f'{size / 1e6:.1f}MB'
        if size < 1e12:
            return f'{size / 1e9:.1f}GB'

        return 'wtf are you downloading?'
