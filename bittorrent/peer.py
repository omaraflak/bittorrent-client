import time
import socket
import struct
import select
import asyncio
import hashlib
import logging
from typing import Optional, Callable
from dataclasses import dataclass, field
from bittorrent.ip import IpAndPort
from bittorrent.torrent import Piece

CHUNK_SIZE = 16384


class _OperationCancelled(Exception):
    pass


@dataclass
class _Cancelable:
    cancel: bool = False


@dataclass
class PeerMessage:
    KEEP_ALIVE = -1
    CHOKE = 0
    UNCHOKE = 1
    INTERESTED = 2
    NOT_INTERESTED = 3
    HAVE = 4
    BITFIELD = 5
    REQUEST = 6
    PIECE = 7
    CANCEL = 8

    message_id: int
    payload: bytes = b''

    def write(self, sock: socket.socket):
        if self.message_id == PeerMessage.KEEP_ALIVE:
            PeerMessage._write_bytes(sock, b'\x00\x00\x00\x00')
        else:
            data = self.message_id.to_bytes(1, 'big') + self.payload
            data = len(data).to_bytes(4, 'big') + data
            PeerMessage._write_bytes(sock, data)

    async def async_write(self, writer: asyncio.StreamWriter):
        if self.message_id == PeerMessage.KEEP_ALIVE:
            writer.write(b'\x00\x00\x00\x00')
        else:
            data = self.message_id.to_bytes(1, 'big') + self.payload
            data = len(data).to_bytes(4, 'big') + data
            writer.write(data)
        await writer.drain()

    @classmethod
    def read(cls, sock: socket.socket, cancelable: Optional[_Cancelable] = None, timeout: float = 15.0) -> 'PeerMessage':
        message_size_bytes = PeerMessage._read_bytes(
            sock, 4, cancelable, timeout)
        message_size = int.from_bytes(message_size_bytes, 'big')
        if message_size == 0:
            return PeerMessage(PeerMessage.KEEP_ALIVE)
        if message_size > 2 * 1024 * 1024:
            raise ValueError(f'Message size too large: {message_size} bytes')
        data = PeerMessage._read_bytes(sock, message_size, cancelable, timeout)
        return PeerMessage(data[0], data[1:])

    @classmethod
    async def async_read(cls, reader: asyncio.StreamReader) -> 'PeerMessage':
        size_bytes = await reader.readexactly(4)
        message_size = int.from_bytes(size_bytes, 'big')
        if message_size == 0:
            return PeerMessage(PeerMessage.KEEP_ALIVE)
        if message_size > 2 * 1024 * 1024:
            raise ValueError(f'Message size too large: {message_size} bytes')
        data = await reader.readexactly(message_size)
        return PeerMessage(data[0], data[1:])

    @staticmethod
    def _read_bytes(sock: socket.socket, size: int, cancelable: Optional[_Cancelable] = None, timeout: float = 15.0) -> bytes:
        data = bytearray()
        start_time = time.time()
        while len(data) < size:
            if cancelable and cancelable.cancel:
                raise _OperationCancelled('Operation cancelled')
            if time.time() - start_time > timeout:
                raise TimeoutError('Socket read timed out')

            # Select with short timeout to allow responsive cancellation and timeouts
            r, _, _ = select.select([sock], [], [], 0.5)
            if not r:
                continue

            length_needed = size - len(data)
            tmp = sock.recv(min(length_needed, 65536))
            if not tmp:
                raise ConnectionResetError('Socket connection closed by peer')
            data.extend(tmp)
        return bytes(data)

    @staticmethod
    def _write_bytes(sock: socket.socket, data: bytes):
        sock.sendall(data)


@dataclass
class Bitfield:
    value: bytearray = field(default_factory=bytearray)

    def set_piece(self, index: int):
        q = index // 8
        r = index % 8
        if q >= len(self.value):
            diff = q - len(self.value) + 1
            self.value.extend(bytes(diff))

        byte = self.value[q]
        mask = 1 << (7 - r)
        # Bitwise OR to ensure bit is set without inverting
        self.value[q] = byte | mask

    def has_piece(self, index: int) -> bool:
        q = index // 8
        r = index % 8
        if q >= len(self.value):
            return False

        byte = self.value[q]
        mask = 1 << (7 - r)
        return (mask & byte) > 0

    @property
    def size(self) -> int:
        return len(self.value)


class AsyncPeer:
    """
    High-performance asynchronous peer connection handler.
    Features adaptive pipelining (scaling from 16 to 64 chunks for gigabit throughput)
    and block-level cooperative scheduling with PieceManager.
    """

    def __init__(
        self,
        peer: IpAndPort,
        info_hash: bytes,
        peer_id: bytes,
        piece_manager,  # Type: PieceManager
        initial_pipeline: int = 16,
        max_pipeline: int = 64,
        min_pipeline: int = 4
    ):
        self.peer = peer
        self.info_hash = info_hash
        self.peer_id = peer_id
        self.piece_manager = piece_manager
        self.bitfield = Bitfield()
        self.choked = True
        self.pipeline_depth = initial_pipeline
        self.max_pipeline = max_pipeline
        self.min_pipeline = min_pipeline
        self.consecutive_fast_chunks = 0
        self.in_flight_chunks: set[tuple[int, int]
                                   ] = set()  # (piece_idx, chunk_idx)
        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None
        self.peer_id_str = f'{peer.ip}:{peer.port}'
        self.is_running = True

    async def run(self):
        try:
            # Connect with short timeout
            self.reader, self.writer = await asyncio.wait_for(
                asyncio.open_connection(self.peer.ip, self.peer.port),
                timeout=4.0
            )

            if not await self._async_handshake():
                return

            # Announce interest
            await PeerMessage(PeerMessage.INTERESTED).async_write(self.writer)

            while self.is_running and not self.piece_manager.is_finished():
                # Adaptive Pipelining: fill pipeline up to self.pipeline_depth
                if not self.choked:
                    while len(self.in_flight_chunks) < self.pipeline_depth:
                        chunk = self.piece_manager.get_next_chunk_request(
                            self.peer_id_str, self.bitfield)
                        if not chunk:
                            break
                        payload = struct.pack(
                            '!III', chunk.piece_index, chunk.begin, chunk.length)
                        await PeerMessage(PeerMessage.REQUEST, payload).async_write(self.writer)
                        self.in_flight_chunks.add(
                            (chunk.piece_index, chunk.chunk_index))

                # Read next message with timeout
                msg = await asyncio.wait_for(PeerMessage.async_read(self.reader), timeout=15.0)
                await self._handle_message(msg)

        except (asyncio.CancelledError, asyncio.TimeoutError, ConnectionResetError, OSError):
            pass
        except Exception as e:
            logging.debug(f'AsyncPeer {self.peer_id_str} error: {e}')
        finally:
            await self._cleanup()

    async def _async_handshake(self) -> bool:
        pstr = b'BitTorrent protocol'
        header = struct.pack('!B', len(pstr)) + pstr
        handshake = header + \
            struct.pack('!8s20s20s', bytes(8), self.info_hash, self.peer_id)

        self.writer.write(handshake)
        await self.writer.drain()

        data = await asyncio.wait_for(self.reader.readexactly(len(header) + 48), timeout=5.0)
        resp_header = data[:len(header)]
        resp_info_hash = data[len(header) + 8: len(header) + 28]

        return resp_header == header and resp_info_hash == self.info_hash

    async def _handle_message(self, message: PeerMessage):
        if message.message_id == PeerMessage.CHOKE:
            self.choked = True
            # Halve pipeline on choke and release in-flight chunks
            self.pipeline_depth = max(
                self.min_pipeline, self.pipeline_depth // 2)
            self.consecutive_fast_chunks = 0
            self.piece_manager.remove_peer(self.peer_id_str)
            self.in_flight_chunks.clear()

        elif message.message_id == PeerMessage.UNCHOKE:
            self.choked = False

        elif message.message_id == PeerMessage.HAVE:
            if len(message.payload) >= 4:
                idx = int.from_bytes(message.payload[:4], 'big')
                self.bitfield.set_piece(idx)
                self.piece_manager.update_peer_have(self.peer_id_str, idx)

        elif message.message_id == PeerMessage.BITFIELD:
            self.bitfield.value = bytearray(message.payload)
            self.piece_manager.update_peer_bitfield(
                self.peer_id_str, self.bitfield)

        elif message.message_id == PeerMessage.PIECE:
            if len(message.payload) < 8:
                return
            p_index = int.from_bytes(message.payload[:4], 'big')
            begin = int.from_bytes(message.payload[4:8], 'big')
            data = message.payload[8:]

            chunk_idx = begin // CHUNK_SIZE
            self.in_flight_chunks.discard((p_index, chunk_idx))

            # Adaptive Pipelining: steadily increase pipeline on sustained fast chunk delivery
            self.consecutive_fast_chunks += 1
            if self.consecutive_fast_chunks >= 8:
                self.pipeline_depth = min(
                    self.max_pipeline, self.pipeline_depth + 4)
                self.consecutive_fast_chunks = 0

            self.piece_manager.on_chunk_received(p_index, begin, data)

    async def _cleanup(self):
        self.is_running = False
        self.piece_manager.remove_peer(self.peer_id_str)
        self.in_flight_chunks.clear()
        if self.writer:
            try:
                self.writer.close()
                await self.writer.wait_closed()
            except Exception:
                pass


class Peer:
    """Synchronous Peer worker class kept for backward compatibility."""

    def __init__(
        self,
        peer: IpAndPort,
        get_work: Callable[['Peer', Bitfield], Optional[Piece]],
        put_work: Callable[['Peer', Piece], None],
        put_result: Callable[['Peer', Piece, bytes], None],
        has_finished: Callable[[], bool],
        info_hash: bytes,
        peer_id: bytes,
        piece_count: int,
        chunk_size: int = CHUNK_SIZE,
        max_batch_requests: int = 16
    ):
        self.peer = peer
        self.get_work = get_work
        self.put_work = put_work
        self.put_result = put_result
        self.has_finished = has_finished
        self.info_hash = info_hash
        self.peer_id = peer_id
        self.piece_count = piece_count
        self.chunk_size = chunk_size
        self.max_batch_requests = max_batch_requests
        self.bitfield = Bitfield()
        self.cancelable = _Cancelable()
        self.choked = True
        self.sock: Optional[socket.socket] = None

    def start(self):
        if self.has_finished():
            return

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(5.0)

        try:
            if not self._connect_and_handshake():
                return

            PeerMessage(PeerMessage.INTERESTED).write(self.sock)

            start_wait = time.time()
            while time.time() - start_wait < 2.0:
                if self.has_finished() or self.cancelable.cancel:
                    return
                r, _, _ = select.select([self.sock], [], [], 0.3)
                if not r:
                    break
                try:
                    msg = PeerMessage.read(
                        self.sock, self.cancelable, timeout=3.0)
                    self._handle_message_state(msg)
                    if not self.choked or self.bitfield.size > 0:
                        break
                except Exception:
                    break

            while not self.has_finished() and not self.cancelable.cancel:
                work = self.get_work(self, self.bitfield)
                if not work:
                    time.sleep(0.5)
                    continue

                try:
                    self._download(work)
                except _OperationCancelled:
                    self.put_work(self, work)
                    break
                except Exception as e:
                    logging.debug(
                        f'Peer {self.peer.ip}:{self.peer.port} error during piece #{work.index}: {e}')
                    self.put_work(self, work)
                    break
        except Exception as e:
            logging.debug(
                f'Peer {self.peer.ip}:{self.peer.port} connection error: {e}')
        finally:
            self._close_socket()

    def cancel_work(self):
        self.cancelable.cancel = True

    def _close_socket(self):
        if self.sock:
            try:
                self.sock.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            try:
                self.sock.close()
            except Exception:
                pass
            self.sock = None

    def _connect_and_handshake(self) -> bool:
        try:
            self.sock.connect((self.peer.ip, self.peer.port))
            if self._handshake():
                return True
        except Exception as e:
            logging.debug(
                f'Connection failed to {self.peer.ip}:{self.peer.port}: {e}')
        return False

    def _handshake(self) -> bool:
        pstr = b'BitTorrent protocol'
        header = struct.pack('!B', len(pstr)) + pstr
        handshake = header + \
            struct.pack('!8s20s20s', bytes(8), self.info_hash, self.peer_id)

        PeerMessage._write_bytes(self.sock, handshake)
        data = PeerMessage._read_bytes(self.sock, len(
            header) + 48, self.cancelable, timeout=6.0)
        if len(data) < len(header) + 48:
            return False

        resp_header = data[:len(header)]
        resp_info_hash = data[len(header) + 8: len(header) + 28]

        return resp_header == header and resp_info_hash == self.info_hash

    def _handle_message_state(self, message: PeerMessage):
        if message.message_id == PeerMessage.CHOKE:
            self.choked = True
        elif message.message_id == PeerMessage.UNCHOKE:
            self.choked = False
        elif message.message_id == PeerMessage.HAVE:
            if len(message.payload) >= 4:
                idx = int.from_bytes(message.payload[:4], 'big')
                self.bitfield.set_piece(idx)
        elif message.message_id == PeerMessage.BITFIELD:
            self.bitfield.value = bytearray(message.payload)

    def _download(self, work: Piece):
        chunk_size = self.chunk_size
        num_chunks = (work.size + chunk_size - 1) // chunk_size
        chunks_info = [
            (i, i * chunk_size, min(chunk_size, work.size - i * chunk_size))
            for i in range(num_chunks)
        ]

        downloaded_data = bytearray(work.size)
        received_chunks: set[int] = set()
        requested_chunks: set[int] = set()
        pending_chunks: list[int] = list(range(num_chunks))
        max_in_flight = min(self.max_batch_requests, 16)

        self.cancelable.cancel = False
        last_activity = time.time()

        while len(received_chunks) < num_chunks:
            if self.cancelable.cancel or self.has_finished():
                raise _OperationCancelled('Download cancelled')

            if time.time() - last_activity > 20.0:
                raise TimeoutError(
                    f'Peer {self.peer.ip}:{self.peer.port} timed out during piece #{work.index}')

            if not self.choked:
                while len(requested_chunks) < max_in_flight and pending_chunks:
                    chunk_idx = pending_chunks.pop(0)
                    _, begin, length = chunks_info[chunk_idx]
                    payload = struct.pack('!III', work.index, begin, length)
                    PeerMessage(PeerMessage.REQUEST, payload).write(self.sock)
                    requested_chunks.add(chunk_idx)

            r, _, _ = select.select([self.sock], [], [], 0.5)
            if not r:
                continue

            message = PeerMessage.read(
                self.sock, self.cancelable, timeout=10.0)
            last_activity = time.time()

            if message.message_id == PeerMessage.KEEP_ALIVE:
                continue
            elif message.message_id == PeerMessage.CHOKE:
                self.choked = True
                pending_chunks = sorted(
                    list(requested_chunks | set(pending_chunks)))
                requested_chunks.clear()
            elif message.message_id == PeerMessage.UNCHOKE:
                self.choked = False
            elif message.message_id == PeerMessage.HAVE:
                if len(message.payload) >= 4:
                    idx = int.from_bytes(message.payload[:4], 'big')
                    self.bitfield.set_piece(idx)
            elif message.message_id == PeerMessage.BITFIELD:
                self.bitfield.value = bytearray(message.payload)
                if not self.bitfield.has_piece(work.index):
                    self.put_work(self, work)
                    return
            elif message.message_id == PeerMessage.PIECE:
                if len(message.payload) < 8:
                    continue
                p_index = int.from_bytes(message.payload[0:4], 'big')
                begin = int.from_bytes(message.payload[4:8], 'big')
                data = message.payload[8:]

                if p_index != work.index or begin + len(data) > work.size:
                    continue

                chunk_idx = begin // chunk_size
                if chunk_idx in requested_chunks:
                    requested_chunks.remove(chunk_idx)

                if chunk_idx not in received_chunks:
                    downloaded_data[begin: begin + len(data)] = data
                    received_chunks.add(chunk_idx)

        if hashlib.sha1(downloaded_data).digest() == work.sha1:
            self.put_result(self, work, bytes(downloaded_data))
        else:
            logging.warning(
                f'Piece #{work.index} corrupted (hash mismatch). Re-queueing.')
            self.put_work(self, work)
