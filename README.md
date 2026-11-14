# BitTorrent Client

A lightweight BitTorrent client built with Python standard libraries.

## CLI Usage

```bash
python3 main.py file.torrent -o ./downloads
```

Options:
- `-o, --output`: Destination directory (default: `./`)
- `-w, --workers`: Maximum peer worker threads (default: `40`)
- `-v, --verbose`: Enable debug logging

## Python API

```python
import logging
from bittorrent.torrent import Torrent
from bittorrent.client import Client

logging.basicConfig(level=logging.INFO)

torrent = Torrent.from_file('file.torrent')
client = Client(torrent)
client.download('./')
```

## Running Tests

```bash
python3 -m unittest discover test
```
