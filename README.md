# BitTorrent Client

A lightweight and fast BitTorrent client.

## Installation

```bash
pip install bittorrent-client
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
from bittorrent import Torrent, Client

logging.basicConfig(level=logging.INFO)

torrent = Torrent.from_file('file.torrent')
client = Client(torrent)
client.download('./downloads')
```

## Running Tests

```bash
python3 -m unittest discover test
```
