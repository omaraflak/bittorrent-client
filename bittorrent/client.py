import os
import random
import logging
from threading import Lock
from typing import Optional
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from bittorrent.trackers import Trackers
from bittorrent.torrent import Torrent, Piece
from bittorrent.peer import Peer, PieceData, Bitfield


class Client:
    def __init__(
        self,
        torrent: Torrent,
        max_peer_workers: int = 1000,
        max_tracker_workers: int = 100,
        max_peers_per_tracker: int = 5000,
        max_peer_batch_requests: int = 5,
        piece_chunk_size: int = 2 ** 14,
        end_game_threashold: float = 0.9,
        end_game_peers_per_piece: int = 10
    ):
        self.torrent = torrent
        self.max_peer_workers = max_peer_workers
        self.max_peer_batch_requests = max_peer_batch_requests
        self.max_tracker_workers = max_tracker_workers
        self.max_peers_per_tracker = max_peers_per_tracker
        self.piece_chunk_size = piece_chunk_size
        self.end_game_threashold = end_game_threashold
        self.end_game_peers_per_piece = end_game_peers_per_piece
        self.peer_id = random.randbytes(20)
        self.work_queue: set[Piece] = set()
        self.work_result: dict[Piece, PieceData] = dict()
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
        self.work_queue.update(self.torrent.pieces)

        with ThreadPoolExecutor(max_workers=self.max_peer_workers) as executor:
            executor.map(
                Peer.start,
                [
                    Peer(
                        peer,
                        lambda bitfield: self._get_work(peer, bitfield),
                        lambda work: self._put_work(peer, work),
                        lambda result: self._put_result(peer, result),
                        self._has_finished,
                        self.torrent.info_hash,
                        self.peer_id,
                        len(self.work_queue),
                        self.piece_chunk_size,
                        self.max_peer_batch_requests
                    )
                    for peer in peers
                ]
            )

        if not self._has_finished():
            logging.warning('Could not download file.')
            return

        self._write_file(output_directory)


    def _get_work(self, peer: Peer, bitfield: Bitfield) -> Optional[Piece]:
        with self.lock:
            candidates = [
                work
                for work in self.work_queue
                if (
                    (bitfield.size == 0 or bitfield.has_piece(work.index)) and
                    len(self.workers_per_work[work]) < self.end_game_peers_per_piece
                )
            ]

            if not candidates:
                return None

            candidates.sort(key=lambda x: len(self.workers_per_work[x]))
            candidates = [
                work
                for work in candidates
                if len(self.workers_per_work[work]) == len(self.workers_per_work[candidates[0]])
            ]
            random.shuffle(candidates)

            work = candidates[0]
            self.workers_per_work[work].append(peer)

            if not self._end_game():
                self.work_queue.remove(work)

            return work


    def _end_game(self) -> bool:
        return len(self.work_result) >= self.end_game_threashold * self.torrent.piece_count


    def _put_work(self, peer: Peer, work: Piece):
        with self.lock:
            self.work_queue.add(work)
            self.workers_per_work[work].remove(peer)


    def _put_result(self, peer: Peer, result: PieceData):
        with self.lock:
            self.work_result[result.piece] = result
            if result.piece in self.work_queue:
                self.work_queue.remove(result.piece)

            if self._end_game():
                for worker in self.workers_per_work[result.piece]:
                    if worker != peer:
                        worker.cancel()

            self.workers_per_work[result.piece].clear()

            percent = int(100 * len(self.work_result) / self.torrent.piece_count)
            logging.info(f'Progress: {len(self.work_result)}/{self.torrent.piece_count} ({percent}%)')


    def _has_finished(self) -> bool:
        return len(self.work_result) == self.torrent.piece_count


    def _write_file(self, output_directory: str):
        logging.debug('Writing file ...')

        data = b''.join(
            result.data
            for result in sorted(self.work_result.values(), key=lambda x: x.piece.index)
        )

        for file in self.torrent.files:
            file_directoy = os.path.join(output_directory, *file.path[:-1])
            os.makedirs(file_directoy, exist_ok=True)
            file_path = os.path.join(file_directoy, file.path[-1])
            file_data = data[file.start : file.start + file.size]
            with open(file_path, 'wb') as fs:
                fs.write(file_data)

        logging.debug('File written to disk!')
