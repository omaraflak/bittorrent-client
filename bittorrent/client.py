import os
import shutil
import random
import logging
import asyncio
from bittorrent.trackers import Trackers
from bittorrent.torrent import Torrent, File
from bittorrent.peer import AsyncPeer
from bittorrent.storage import Storage
from bittorrent.piece_manager import PieceManager


class Client:
    """
    BitTorrent client responsible for coordinating peer discovery, concurrent
    piece downloading, piece verification, and direct-to-disk file writing.
    """

    def __init__(
        self,
        torrent: Torrent,
        max_peer_workers: int = 60,
        max_tracker_workers: int = 20,
        max_peers_per_tracker: int = 200,
        max_peers_per_piece: int = 2,
        max_peer_batch_requests: int = 16,
        piece_chunk_size: int = 16384
    ):
        """
        Initialize a BitTorrent client instance.

        :param torrent: Parsed Torrent metadata object containing file and tracker details.
        :param max_peer_workers: Maximum number of concurrent peer connection workers (default: 60).
        :param max_tracker_workers: Maximum number of concurrent tracker query threads (default: 20).
        :param max_peers_per_tracker: Maximum number of peers to request per tracker announce (default: 200).
        :param max_peers_per_piece: Maximum number of concurrent workers allowed per piece during endgame mode (default: 2).
        :param max_peer_batch_requests: Initial pipeline depth for concurrent in-flight chunk requests per peer (default: 16).
        :param piece_chunk_size: Size in bytes of each chunk requested from a peer (default: 16384 / 16 KiB).
        """
        self.torrent = torrent
        self.max_peer_workers = max_peer_workers
        self.max_tracker_workers = max_tracker_workers
        self.max_peers_per_tracker = max_peers_per_tracker
        self.max_peers_per_piece = max_peers_per_piece
        self.max_peer_batch_requests = max_peer_batch_requests
        self.piece_chunk_size = piece_chunk_size

        self.peer_id = b'-AG0001-' + random.randbytes(12)
        self.stopped = False
        self.tmp = ''

    def download(self, output_directory: str):
        """
        Download all pieces of the torrent into the target directory.

        Discovers peers from configured trackers (both UDP and HTTP/HTTPS), connects
        to peers concurrently with adaptive pipelining, schedules chunks using
        block-level sharing and Rarest-First, and writes verified piece data directly
        to target files on disk. Resumes from existing verified files without overhead.

        :param output_directory: File system directory path where downloaded files will be saved.
        """
        if self.torrent.size == 0 or self.torrent.piece_count == 0:
            logging.info('Empty torrent, creating empty files.')
            Storage(self.torrent, output_directory)
            return

        try:
            asyncio.run(self._async_download(output_directory))
        except KeyboardInterrupt:
            logging.warning('Download interrupted by user.')

    async def _async_download(self, output_directory: str):
        # 1. Direct-to-Disk Storage (pre-allocates sparse files, eliminates .tmp copy)
        storage = Storage(self.torrent, output_directory)

        # 2. Check for already completed pieces on disk (instant resume)
        piece_manager = PieceManager(self.torrent, storage)
        verified = storage.verify_existing_pieces()
        for p_idx in verified:
            piece_manager.mark_piece_completed(p_idx)

        if piece_manager.is_finished():
            logging.info(
                'All pieces are already downloaded and verified on disk.')
            return

        if verified:
            logging.info(
                f'Resuming download: {len(verified)}/{len(self.torrent.pieces)} pieces already verified on disk.')

        # 3. Discover peers from trackers
        trackers = Trackers(
            self.torrent,
            self.peer_id,
            self.max_tracker_workers,
            self.max_peers_per_tracker
        )
        peers = trackers.get_peers()
        if not peers:
            logging.error('No peers discovered from trackers.')
            return

        logging.info(
            f'Starting high-speed async download with {len(peers)} discovered peers...')

        # 4. Spawn AsyncPeers with adaptive pipelining and block-level scheduling
        peer_tasks = []
        peers_to_connect = list(peers)[:self.max_peer_workers]
        for endpoint in peers_to_connect:
            ap = AsyncPeer(
                endpoint,
                self.torrent.info_hash,
                self.peer_id,
                piece_manager,
                initial_pipeline=self.max_peer_batch_requests,
                max_pipeline=64,
                min_pipeline=4
            )
            peer_tasks.append(asyncio.create_task(ap.run()))

        # 5. Monitor download progress
        total_pieces = len(self.torrent.pieces)
        last_logged_count = len(piece_manager.completed_pieces)

        try:
            while not piece_manager.is_finished():
                if all(t.done() for t in peer_tasks):
                    break

                await asyncio.sleep(1.0)
                current_count = len(piece_manager.completed_pieces)
                if current_count != last_logged_count:
                    last_logged_count = current_count
                    percent = int(100 * current_count /
                                  total_pieces) if total_pieces > 0 else 100
                    downloaded_bytes = sum(
                        self.torrent.pieces[i].size for i in piece_manager.completed_pieces)
                    human_done = self._human_friendly_bytes_str(
                        downloaded_bytes)
                    human_total = self._human_friendly_bytes_str(
                        self.torrent.size)
                    logging.info(
                        f'Progress: {current_count}/{total_pieces} pieces ({percent}%) [{human_done} / {human_total}]')

        finally:
            # Clean up all peer tasks
            for t in peer_tasks:
                t.cancel()
            await asyncio.gather(*peer_tasks, return_exceptions=True)

        if not piece_manager.is_finished():
            logging.error('Download did not finish completely.')
            return

        logging.info(
            'Download complete! All pieces verified directly on disk.')

    def _assemble_files(self, output_directory: str):
        """Assembles downloaded pieces from .tmp if present (backward compatibility)."""
        if self.torrent.is_multi_file:
            base_dir = os.path.join(output_directory, self.torrent.name)
            for file_info in self.torrent.files:
                file_dir = os.path.join(base_dir, *file_info.path[:-1])
                os.makedirs(file_dir, exist_ok=True)
                target_path = os.path.join(file_dir, file_info.path[-1])
                self._assemble_file(file_info, target_path)
        else:
            os.makedirs(output_directory, exist_ok=True)
            if self.torrent.files:
                file_info = self.torrent.files[0]
                target_path = os.path.join(
                    output_directory, file_info.path[-1])
                self._assemble_file(file_info, target_path)

        if self.tmp and os.path.isdir(self.tmp):
            try:
                shutil.rmtree(self.tmp)
            except Exception:
                pass

    def _assemble_file(self, file_info: File, output_path: str):
        """Assembles a single file from .tmp part files (backward compatibility)."""
        if file_info.size == 0:
            with open(output_path, 'wb'):
                pass
            return

        piece_size = self.torrent.piece_size
        file_start = file_info.start
        file_end = file_start + file_info.size
        start_piece_index = file_start // piece_size if piece_size > 0 else 0

        with open(output_path, 'wb') as dest:
            for piece in self.torrent.pieces[start_piece_index:]:
                piece_start = piece.index * piece_size
                piece_end = piece_start + piece.size

                if piece_start >= file_end:
                    break

                overlap_start = max(piece_start, file_start)
                overlap_end = min(piece_end, file_end)

                if overlap_end > overlap_start:
                    part_path = os.path.join(
                        self.tmp, f'piece_{piece.index}.part')
                    if os.path.exists(part_path):
                        with open(part_path, 'rb') as part_file:
                            part_file.seek(overlap_start - piece_start)
                            data = part_file.read(overlap_end - overlap_start)
                            dest.write(data)

    @staticmethod
    def _human_friendly_bytes_str(size: int) -> str:
        units = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB']
        val = float(size)
        for unit in units:
            if val < 1024.0 or unit == units[-1]:
                return f'{val:.1f} {unit}' if unit != 'B' else f'{int(val)} B'
            val /= 1024.0
        return f'{val:.1f} PiB'
