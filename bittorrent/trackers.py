import socket
import struct
import random
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional
from urllib.parse import urlencode, quote_from_bytes
import urllib.request
from dataclasses import dataclass
from bittorrent.ip import IpAndPort
from bittorrent.torrent import Torrent
from bittorrent.bencode import decode_bencode


@dataclass
class _ConnectRequest:
    _PROTOCOL_ID = 0x41727101980
    _ACTION = 0

    transaction_id: int

    def to_bytes(self) -> bytes:
        return struct.pack(
            '!QII',
            _ConnectRequest._PROTOCOL_ID,
            _ConnectRequest._ACTION,
            self.transaction_id
        )


@dataclass
class _ConnectResponse:
    transaction_id: int
    connection_id: int

    @classmethod
    def from_bytes(cls, data: bytes) -> Optional['_ConnectResponse']:
        if len(data) < 16:
            return None
        action, transaction_id, connection_id = struct.unpack_from(
            '!IIQ', data, 0)
        if action != _ConnectRequest._ACTION:
            return None
        return _ConnectResponse(transaction_id, connection_id)


@dataclass
class _AnnounceRequest:
    _ACTION = 1
    EVENT_NONE = 0
    EVENT_COMPLETE = 1
    EVENT_START = 2
    EVENT_STOP = 3

    connection_id: int
    transaction_id: int
    info_hash: bytes
    peer_id: bytes
    downloaded: int = 0
    left: int = 0
    uploaded: int = 0
    event: int = 0
    ip_address: int = 0
    key: int = 0
    num_want: int = -1
    port: int = 6881

    def to_bytes(self) -> bytes:
        return struct.pack(
            '!QII20s20sQQQIIIiH',
            self.connection_id,
            _AnnounceRequest._ACTION,
            self.transaction_id,
            self.info_hash,
            self.peer_id,
            self.downloaded,
            self.left,
            self.uploaded,
            self.event,
            self.ip_address,
            self.key,
            self.num_want,
            self.port
        )


@dataclass
class _AnnounceResponse:
    transaction_id: int
    interval: int
    leechers: int
    seeders: int
    peers: list[IpAndPort]

    @classmethod
    def from_bytes(cls, data: bytes) -> Optional['_AnnounceResponse']:
        if len(data) < 20:
            return None

        action, transaction_id = struct.unpack_from('!II', data, 0)
        if action == 3:  # Error response in BEP 15
            error_message = data[8:].decode('utf-8', errors='replace')
            logging.debug(f'Tracker returned error: {error_message}')
            return None

        if action != _AnnounceRequest._ACTION:
            return None

        peers_bytes_len = len(data) - 20
        if peers_bytes_len % 6 != 0:
            peers_bytes_len -= peers_bytes_len % 6

        peers_count = peers_bytes_len // 6
        unpack_format = '!IIIII' + (peers_count * 'IH')
        unpacked = struct.unpack_from(unpack_format, data, 0)

        action = unpacked[0]
        transaction_id = unpacked[1]
        interval = unpacked[2]
        leechers = unpacked[3]
        seeders = unpacked[4]
        ips_and_ports = unpacked[5:]

        peers: list[IpAndPort] = []
        for i in range(0, len(ips_and_ports), 2):
            raw_ip = ips_and_ports[i]
            port = ips_and_ports[i + 1]
            if port != 0:
                ip_str = socket.inet_ntoa(struct.pack('!I', raw_ip))
                if ip_str != '0.0.0.0':
                    peers.append(IpAndPort(ip_str, port))

        return _AnnounceResponse(transaction_id, interval, leechers, seeders, peers)


class Trackers:
    def __init__(
        self,
        torrent: Torrent,
        peer_id: bytes,
        max_workers: int = 20,
        max_peers_per_tracker: int = 200,
    ):
        self.torrent = torrent
        self.peer_id = peer_id
        self.max_workers = max_workers
        self.max_peers_per_tracker = max_peers_per_tracker

    def get_peers(self) -> set[IpAndPort]:
        peers: set[IpAndPort] = set()

        # Gather UDP tracker endpoints
        udp_trackers = self.torrent.trackers_by_protocol('udp')

        # Gather HTTP/HTTPS tracker URLs
        http_trackers = [
            url for url in self.torrent.trackers
            if url.startswith('http://') or url.startswith('https://')
        ]

        tasks = []
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            for tracker in udp_trackers:
                tasks.append(executor.submit(
                    self._get_peers_from_udp_tracker, tracker))
            for url in http_trackers:
                tasks.append(executor.submit(
                    self._get_peers_from_http_tracker, url))

            for future in as_completed(tasks):
                try:
                    res = future.result()
                    if res:
                        peers.update(res)
                except Exception as e:
                    logging.debug(f'Tracker query exception: {e}')

        logging.info(f'Discovered {len(peers)} unique peers from trackers.')
        return peers

    def _get_peers_from_udp_tracker(self, tracker: IpAndPort) -> set[IpAndPort]:
        response = self._announce_udp(tracker, _AnnounceRequest.EVENT_START)
        if not response:
            return set()
        return {p for p in response.peers if p.port != 0 and p.ip != '0.0.0.0'}

    def _announce_udp(self, tracker: IpAndPort, event: int) -> Optional[_AnnounceResponse]:
        logging.debug(
            f'Announcing to UDP tracker {tracker.ip}:{tracker.port}...')

        # Resolve hostname to IP first to avoid repeated DNS timeouts
        try:
            target_addr = (socket.gethostbyname(tracker.ip), tracker.port)
        except (socket.gaierror, OSError) as e:
            logging.debug(f'DNS resolution failed for {tracker.ip}: {e}')
            return None

        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.settimeout(2.5)

        try:
            # Step 1: Connect Request with retry
            connect_transaction_id = random.getrandbits(32)
            connect_request = _ConnectRequest(connect_transaction_id)
            connect_response: Optional[_ConnectResponse] = None

            for _ in range(2):
                try:
                    sock.sendto(connect_request.to_bytes(), target_addr)
                    raw_data = sock.recv(256)
                    resp = _ConnectResponse.from_bytes(raw_data)
                    if resp and resp.transaction_id == connect_transaction_id:
                        connect_response = resp
                        break
                except (socket.timeout, OSError):
                    continue

            if not connect_response:
                return None

            # Step 2: Announce Request with retry (BEP 15 requires new transaction_id)
            announce_transaction_id = random.getrandbits(32)
            announce_request = _AnnounceRequest(
                connection_id=connect_response.connection_id,
                transaction_id=announce_transaction_id,
                info_hash=self.torrent.info_hash,
                peer_id=self.peer_id,
                downloaded=0,
                left=self.torrent.size,
                uploaded=0,
                event=event
            )

            for _ in range(2):
                try:
                    sock.sendto(announce_request.to_bytes(), target_addr)
                    # Maximum UDP tracker response size for ~200 peers is ~1220 bytes (fits MTU)
                    raw_data = sock.recv(2048)
                    announce_response = _AnnounceResponse.from_bytes(raw_data)
                    if announce_response and announce_response.transaction_id == announce_transaction_id:
                        logging.debug(
                            f'Received {len(announce_response.peers)} peers from UDP {tracker.ip}:{tracker.port}')
                        return announce_response
                except (socket.timeout, OSError):
                    continue

            return None
        except Exception as e:
            logging.debug(
                f'UDP announce error with {tracker.ip}:{tracker.port}: {e}')
            return None
        finally:
            sock.close()

    def _get_peers_from_http_tracker(self, url: str) -> set[IpAndPort]:
        logging.debug(f'Announcing to HTTP tracker {url}...')
        try:
            params = {
                'info_hash': quote_from_bytes(self.torrent.info_hash),
                'peer_id': quote_from_bytes(self.peer_id),
                'port': 6881,
                'uploaded': 0,
                'downloaded': 0,
                'left': self.torrent.size,
                'compact': 1,
                'event': 'started'
            }
            # urlencode with safe='%' so quote_from_bytes percent-encoded values aren't double escaped
            delimiter = '&' if '?' in url else '?'
            query_string = delimiter + \
                '&'.join(f'{k}={v}' for k, v in params.items())
            full_url = url + query_string

            req = urllib.request.Request(
                full_url,
                headers={'User-Agent': 'BitTorrent/1.0.0'}
            )

            with urllib.request.urlopen(req, timeout=4) as response:
                content = response.read()

            decoded = decode_bencode(content)
            if not isinstance(decoded, dict):
                return set()

            if 'failure reason' in decoded:
                reason = decoded['failure reason']
                logging.debug(f'HTTP tracker failure reason: {reason}')
                return set()

            peers_data = decoded.get('peers')
            result_peers: set[IpAndPort] = set()

            # Compact peers representation (BEP 23): 6 bytes per peer (4 bytes IP, 2 bytes port)
            if isinstance(peers_data, bytes):
                for i in range(0, len(peers_data) - 5, 6):
                    ip_bytes = peers_data[i:i + 4]
                    port = struct.unpack('!H', peers_data[i + 4:i + 6])[0]
                    ip_str = socket.inet_ntoa(ip_bytes)
                    if port != 0 and ip_str != '0.0.0.0':
                        result_peers.add(IpAndPort(ip_str, port))
            elif isinstance(peers_data, list):
                # Dictionary model (BEP 3)
                for p in peers_data:
                    if isinstance(p, dict):
                        ip = p.get('ip')
                        if isinstance(ip, bytes):
                            ip = ip.decode('utf-8', errors='replace')
                        port = p.get('port')
                        if ip and port and isinstance(port, int) and port != 0:
                            result_peers.add(IpAndPort(str(ip), port))

            logging.debug(
                f'Received {len(result_peers)} peers from HTTP tracker {url}')
            return result_peers
        except Exception as e:
            logging.debug(f'HTTP announce failed for {url}: {e}')
            return set()
