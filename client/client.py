import os
import random
import logging
from typing import Optional
from threading import Lock
from concurrent.futures import ThreadPoolExecutor
from client.trackers import Trackers
from client.torrent import Torrent, Piece
from client.peer import Peer, PieceData, Bitfield


class Client:
    def __init__(
        self,
        torrent: Torrent,
        max_peer_workers: int = 1000,
        max_tracker_workers: int = 100,
        max_peers_per_tracker: int = 5000,
        max_peer_batch_requests: int = 5,
        piece_chunk_size: int = 2 ** 14
    ):
        self.torrent = torrent
        self.max_peer_workers = max_peer_workers
        self.max_peer_batch_requests = max_peer_batch_requests
        self.max_tracker_workers = max_tracker_workers
        self.max_peers_per_tracker = max_peers_per_tracker
        self.piece_chunk_size = piece_chunk_size
        self.peer_id = random.randbytes(20)
        self.work_queue: set[Piece] = set()
        self.result_stack: list[PieceData] = list()
        self.lock = Lock()


    def download(self, output_directory: str):
        trackers = Trackers(
            self.torrent,
            self.peer_id,
            self.max_tracker_workers,
            self.max_peers_per_tracker,
        )
        peers = trackers.get_peers()

        self.work_queue.update(self.torrent.pieces)
        piece_count = len(self.work_queue)

        with ThreadPoolExecutor(max_workers=self.max_peer_workers) as executor:
            for peer in peers:
                worker = Peer(
                    peer,
                    self.work_queue,
                    self.result_stack,
                    self.torrent.info_hash,
                    self.peer_id,
                    piece_count,
                    self.piece_chunk_size,
                    self.max_peer_batch_requests
                )
                executor.submit(worker.start)

        if len(self.result_stack) != piece_count:
            logging.warning('Could not download file.')
            return

        self._write_file(output_directory)


    def _write_file(self, output_directory: str):
        logging.debug('Writing file ...')

        self.result_stack.sort(key=lambda x: x.piece.index)
        data_array = b''.join(result.data for result in self.result_stack)

        for file in self.torrent.files:
            file_directoy = os.path.join(output_directory, *file.path[:-1])
            os.makedirs(file_directoy, exist_ok=True)
            file_path = os.path.join(file_directoy, file.path[-1])
            file_data = data_array[file.start : file.start + file.size]
            with open(file_path, 'wb') as fs:
                fs.write(file_data)

        logging.debug('File written to disk!')