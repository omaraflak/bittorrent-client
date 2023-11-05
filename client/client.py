import os
import random
import logging
from concurrent.futures import ThreadPoolExecutor
from client.trackers import Trackers
from client.torrent import Torrent
from client.peer import Peer, Work, Result


class Client:
    def __init__(
        self,
        torrent: Torrent,
        max_workers: int = 50
    ):
        self.torrent = torrent
        self.max_workers = max_workers
        self.peer_id = random.randbytes(20)
        self.work_queue: list[Work] = []
        self.result_queue: list[Result] = []


    def download(self, output_directory: str):
        trackers = Trackers(self.torrent, max_peers_per_tracker=10, peer_id=self.peer_id)
        peers = trackers.get_peers()

        for piece in self.torrent.pieces():
             self.work_queue.append(Work(piece.piece_index, piece.piece_size, piece.piece_hash))
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for peer in peers:
                worker = Peer(
                    peer,
                    self.work_queue,
                    self.result_queue,
                    self.torrent.info_hash,
                    self.peer_id,
                    self.torrent.piece_count
                )
                executor.submit(worker.start)

        if len(self.result_queue) != self.torrent.piece_count:
            logging.warning('Could not download file.')
            return

        logging.debug('Writing file ...')
        with open(os.path.join(output_directory, self.torrent.file_name), 'wb') as file:
            for result in sorted(self.result_queue, key=lambda x: x.index):
                file.write(result.data)

        logging.debug('File written to disk!')