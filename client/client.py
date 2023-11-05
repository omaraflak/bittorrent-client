import os
import random
import logging
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from client.trackers import Trackers
from client.torrent import Torrent, Piece
from client.peer import Peer, PieceData


class Client:
    _PEERS_PER_TRACKER = 5000

    def __init__(
        self,
        torrent: Torrent,
        max_workers: int = 1000
    ):
        self.torrent = torrent
        self.max_workers = max_workers
        self.peer_id = random.randbytes(20)
        self.work_queue: deque[Piece] = deque()
        self.result_stack: list[PieceData] = list()


    def download(self, output_directory: str):
        trackers = Trackers(
            self.torrent,
            max_peers_per_tracker=Client._PEERS_PER_TRACKER,
            peer_id=self.peer_id
        )
        peers = trackers.get_peers()

        self.work_queue.extend(self.torrent.pieces)
        random.shuffle(self.work_queue)
        piece_count = len(self.work_queue)

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for peer in peers:
                worker = Peer(
                    peer,
                    self.work_queue,
                    self.result_stack,
                    self.torrent.info_hash,
                    self.peer_id,
                    piece_count
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