import os
import shutil
import random
import logging
import hashlib
from threading import Lock
from typing import Optional
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from bittorrent.trackers import Trackers
from bittorrent.torrent import Torrent, Piece, File
from bittorrent.peer import Peer, Bitfield


class Client:
    """
    BitTorrent client responsible for coordinating peer discovery, concurrent
    piece downloading, piece verification, and final file assembly.
    """

    def __init__(
        self,
        torrent: Torrent,
        max_peer_workers: int = 40,
        max_tracker_workers: int = 20,
        max_peers_per_tracker: int = 200,
        max_peers_per_piece: int = 2,
        max_peer_batch_requests: int = 16,
        piece_chunk_size: int = 16384
    ):
        """
        Initialize a BitTorrent client instance.

        :param torrent: Parsed Torrent metadata object containing file and tracker details.
        :param max_peer_workers: Maximum number of concurrent peer connection threads (default: 40).
        :param max_tracker_workers: Maximum number of concurrent tracker query threads (default: 20).
        :param max_peers_per_tracker: Maximum number of peers to request per tracker announce (default: 200).
        :param max_peers_per_piece: Maximum number of concurrent workers allowed per piece during endgame mode (default: 2).
        :param max_peer_batch_requests: Pipeline depth for concurrent in-flight chunk requests per peer (default: 16).
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
        self.work_queue: set[Piece] = set()
        self.work_done: set[Piece] = set()
        self.workers_per_work: dict[Piece, list[Peer]] = defaultdict(list)
        self.active_peers: list[Peer] = []
        self.lock = Lock()
        self.stopped = False
        self.tmp = ''

    def download(self, output_directory: str):
        """
        Download all pieces of the torrent into the target directory.

        Discovers peers from configured trackers (both UDP and HTTP/HTTPS), connects
        to peers concurrently, downloads and validates piece chunks against their SHA-1
        hashes, and assembles the verified pieces into target files on disk. Resumes
        from partially downloaded pieces if available in the temporary cache directory.

        :param output_directory: File system directory path where downloaded files will be saved.
        """
        if self.torrent.size == 0 or self.torrent.piece_count == 0:
            logging.info('Empty torrent, creating empty files.')
            self._assemble_files(output_directory)
            return

        # Setup temporary piece directory
        if self.torrent.is_multi_file:
            files_directory = os.path.join(output_directory, self.torrent.name)
        else:
            files_directory = output_directory
        os.makedirs(files_directory, exist_ok=True)

        self.tmp = os.path.join(
            files_directory, f'.tmp_{self.torrent.info_hash[:8].hex()}')
        os.makedirs(self.tmp, exist_ok=True)

        # Check existing downloaded pieces for resuming
        self._read_downloaded_parts()
        if self._has_finished():
            logging.info('All pieces already downloaded.')
            self._assemble_files(output_directory)
            return

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
            f'Starting download with {len(peers)} discovered peers...')

        peer_instances = [
            Peer(
                peer_endpoint,
                self._get_work,
                self._put_work,
                self._put_result,
                self._has_finished,
                self.torrent.info_hash,
                self.peer_id,
                self.torrent.piece_count,
                self.piece_chunk_size,
                self.max_peer_batch_requests
            )
            for peer_endpoint in peers
        ]
        with self.lock:
            self.active_peers = list(peer_instances)

        try:
            with ThreadPoolExecutor(max_workers=min(self.max_peer_workers, len(peers))) as executor:
                futures = [executor.submit(p.start) for p in peer_instances]
                # Wait for completion or all futures to end
                for f in futures:
                    try:
                        f.result()
                    except Exception as e:
                        logging.debug(f'Peer future exited: {e}')
        except KeyboardInterrupt:
            logging.warning('Download interrupted by user.')
            self._stop()
            raise
        finally:
            self._stop()

        if not self._has_finished():
            logging.error('Download did not finish completely.')
            return

        self._assemble_files(output_directory)

    def _stop(self):
        with self.lock:
            self.stopped = True
            for p in self.active_peers:
                p.cancel_work()

    def _get_work(self, peer: Peer, bitfield: Bitfield) -> Optional[Piece]:
        with self.lock:
            if self.stopped or self._has_finished():
                return None

            # Primary: Pieces not yet assigned to any worker that this peer has
            candidates = [
                work
                for work in self.work_queue
                if (bitfield.size == 0 or bitfield.has_piece(work.index))
            ]

            if candidates:
                work = random.choice(candidates)
                self.work_queue.remove(work)
                self.workers_per_work[work].append(peer)
                return work

            # Endgame: Duplicate remaining in-progress pieces, ensuring peer has it
            endgame_candidates = [
                work
                for work, workers in self.workers_per_work.items()
                if work not in self.work_done
                and peer not in workers
                and len(workers) < self.max_peers_per_piece
                and (bitfield.size == 0 or bitfield.has_piece(work.index))
            ]

            if endgame_candidates:
                work = random.choice(endgame_candidates)
                self.workers_per_work[work].append(peer)
                return work

            return None

    def _put_work(self, peer: Peer, work: Piece):
        with self.lock:
            if work in self.workers_per_work and peer in self.workers_per_work[work]:
                self.workers_per_work[work].remove(peer)

            # Re-queue piece only if it hasn't been completed and no other peer is on it
            if work not in self.work_done and len(self.workers_per_work[work]) == 0:
                self.work_queue.add(work)

    def _put_result(self, peer: Peer, piece: Piece, data: bytes):
        with self.lock:
            if piece in self.work_done:
                return

            if piece in self.work_queue:
                self.work_queue.remove(piece)

            # Cancel any other workers on this piece
            for worker in self.workers_per_work.get(piece, []):
                if worker != peer:
                    worker.cancel_work()

            if piece in self.workers_per_work:
                del self.workers_per_work[piece]

            self.work_done.add(piece)

            # Save piece part to disk
            part_path = os.path.join(self.tmp, f'piece_{piece.index}.part')
            with open(part_path, 'wb') as f:
                f.write(data)

            # Check if all pieces finished
            if self._has_finished():
                for workers in self.workers_per_work.values():
                    for worker in workers:
                        worker.cancel_work()

            done_count = len(self.work_done)
            total_count = self.torrent.piece_count
            percent = int(100 * done_count /
                          total_count) if total_count > 0 else 100
            downloaded_bytes = sum(p.size for p in self.work_done)
            human_done = Client._human_friendly_bytes_str(downloaded_bytes)
            human_total = Client._human_friendly_bytes_str(self.torrent.size)

            logging.info(
                f'Progress: {done_count}/{total_count} pieces ({percent}%) [{human_done} / {human_total}]')

    def _has_finished(self) -> bool:
        return len(self.work_done) >= self.torrent.piece_count

    def _read_downloaded_parts(self):
        for piece in self.torrent.pieces:
            part_path = os.path.join(self.tmp, f'piece_{piece.index}.part')
            if os.path.isfile(part_path):
                # Verify size and hash before trusting existing part
                try:
                    if os.path.getsize(part_path) == piece.size:
                        with open(part_path, 'rb') as f:
                            data = f.read()
                        if hashlib.sha1(data).digest() == piece.sha1:
                            self.work_done.add(piece)
                            continue
                except Exception:
                    pass
                # Corrupted part file, remove it
                try:
                    os.remove(part_path)
                except Exception:
                    pass
            self.work_queue.add(piece)

    def _assemble_files(self, output_directory: str):
        logging.info('Assembling downloaded pieces into destination files...')

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

        logging.info('All files written to disk successfully!')

        # Clean up temporary part files
        if self.tmp and os.path.isdir(self.tmp):
            try:
                shutil.rmtree(self.tmp)
                logging.debug('Temporary directory cleaned up.')
            except Exception as e:
                logging.debug(f'Failed to remove temporary directory: {e}')

    def _assemble_file(self, file_info: File, output_path: str):
        logging.info(
            f'Assembling {os.path.basename(output_path)} ({Client._human_friendly_bytes_str(file_info.size)})...')
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
