# BitTorrent Client

A lightweight, robust BitTorrent client built with pure Python standard library (zero external dependencies).

## Installation

```bash
pip install bittorrent_client
```

## CLI Usage

Run directly via the command-line entry point:

```bash
bittorrent file.torrent -o ./downloads
```

Or run as a module:

```bash
python3 -m bittorrent file.torrent -o ./downloads
```

Options:
- `-o, --output`: Destination directory (default: `./`)
- `-w, --workers`: Maximum concurrent peer workers (default: `60`)
- `-v, --verbose`: Enable debug logging

## Python API

```python
import logging
from bittorrent.torrent import Torrent
from bittorrent.client import Client

logging.basicConfig(level=logging.INFO)

torrent = Torrent.from_file('file.torrent')
client = Client(torrent)
client.download('./downloads')
```

## Running Tests

```bash
python3 -m unittest discover test
```
