from pathlib import Path
from . import server as _server
_server.Path = Path
app = _server.app

