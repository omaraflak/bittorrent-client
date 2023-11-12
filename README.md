# Bittorrent Client

This is a very simplistic bittorrent client.


```python
import logging
from client.torrent import Torrent
from client.client import Client

logging.basicConfig(level=logging.INFO)

torrent = Torrent.from_file('file.torrent')
client = Client(torrent)
client.download('./')
```
