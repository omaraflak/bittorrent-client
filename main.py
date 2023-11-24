import fire
import logging
from bittorrent.torrent import Torrent
from bittorrent.client import Client


def main(file: str, output: str = './'):
    logging.basicConfig(level=logging.INFO)
    torrent = Torrent.from_file(file)
    client = Client(torrent)
    client.download(output)


if __name__ == '__main__':
    fire.Fire(main)
