import os
import random
import logging
from collections import deque
from concurrent.futures import ThreadPoolExecutor, Future
from client.trackers import Trackers
from client.torrent import Torrent, Piece
from client.peer import Peer, PieceData


class Client:
    def __init__(
        self,
        torrent: Torrent,
        max_workers: int = 500
    ):
        self.torrent = torrent
        self.max_workers = max_workers
        self.peer_id = random.randbytes(20)
        self.work_queue: deque[Piece] = deque()
        self.result_stack: list[PieceData] = list()


    def download(self, output_directory: str):
        trackers = Trackers(self.torrent, max_peers_per_tracker=self.max_workers * 2, peer_id=self.peer_id)
        peers = trackers.get_peers()

        self.work_queue.extend(self.torrent.pieces())
        random.shuffle(self.work_queue)
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for peer in peers:
                worker = Peer(
                    peer,
                    self.work_queue,
                    self.result_stack,
                    self.torrent.info_hash,
                    self.peer_id,
                    self.torrent.piece_count
                )
                executor.submit(worker.start)

        if len(self.result_stack) != self.torrent.piece_count:
            logging.warning('Could not download file.')
            return

        logging.debug('Writing file ...')
        with open(os.path.join(output_directory, self.torrent.file_name), 'wb') as file:
            for result in sorted(self.result_stack, key=lambda x: x.piece.piece_index):
                file.write(result.data)

        logging.debug('File written to disk!')