import sys
import logging
import argparse
from bittorrent.torrent import Torrent
from bittorrent.client import Client


def _format_bytes(size: int) -> str:
    units = ['B', 'KiB', 'MiB', 'GiB', 'TiB', 'PiB']
    val = float(size)
    for unit in units:
        if val < 1024.0 or unit == units[-1]:
            return f'{val:.1f} {unit}' if unit != 'B' else f'{int(val)} B'
        val /= 1024.0
    return f'{val:.1f} PiB'


def main():
    parser = argparse.ArgumentParser(
        description='A simple, robust BitTorrent client.')
    parser.add_argument('file', help='Path to .torrent file')
    parser.add_argument('-o', '--output', default='./',
                        help='Output directory for downloaded files (default: ./)')
    parser.add_argument('-w', '--workers', type=int, default=60,
                        help='Maximum peer worker threads (default: 60)')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Enable verbose debug logging')

    args = parser.parse_args()

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%H:%M:%S'
    )

    try:
        torrent = Torrent.from_file(args.file)
        logging.info(
            f'Loaded torrent: "{torrent.name}" ({_format_bytes(torrent.size)})')
        logging.info(
            f'Piece count: {torrent.piece_count} (piece size: {_format_bytes(torrent.piece_size)})')
        logging.info(f'Trackers: {len(torrent.trackers)} found')

        client = Client(torrent, max_peer_workers=args.workers)
        client.download(args.output)
    except KeyboardInterrupt:
        logging.info('\nDownload cancelled by user.')
        sys.exit(0)
    except Exception as e:
        logging.error(f'Error: {e}')
        sys.exit(1)


if __name__ == '__main__':
    main()
