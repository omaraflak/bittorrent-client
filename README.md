# BitTorrent Client

This is a very simple BitTorrent client.


```python
import logging
from bittorrent.torrent import Torrent
from bittorrent.client import Client

logging.basicConfig(level=logging.INFO)

torrent = Torrent.from_file('file.torrent')
client = Client(torrent)
client.download('./')
```
