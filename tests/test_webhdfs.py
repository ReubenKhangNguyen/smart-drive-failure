from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.parse import parse_qs, urlsplit

import pytest

from pipeline import webhdfs


class _Fake(BaseHTTPRequestHandler):
    """A tiny stand-in for the NameNode (answers 307 with a 'DataNode' address) and the DataNode (accepts the bytes)."""
    store = {}  # path -> bytes
    dirs = set()
    log = []

    def log_message(self, *args):
        pass

    def _reply(self, code, payload=None, headers=None):
        body = json.dumps(payload).encode("utf-8") if payload is not None else b""
        self.send_response(code)
        for key, value in (headers or {}).items():
            self.send_header(key, value)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _split(self):
        parts = urlsplit(self.path)
        return parts.path.replace("/webhdfs/v1", "", 1), {k: v[0] for k, v in parse_qs(parts.query).items()}

    def do_PUT(self):
        path, query = self._split()
        _Fake.log.append((self.command, path, dict(query)))
        if query["op"] == "MKDIRS":
            _Fake.dirs.add(path)
            return self._reply(200, {"boolean": True})
        if query["op"] == "CREATE" and "datanode" not in query:
            port = self.server.server_address[1]
            return self._reply(307, None, {"Location": "http://127.0.0.1:{}{}?op=CREATE&datanode=1".format(port, path)})
        if query["op"] == "CREATE":
            _Fake.store[path] = self.rfile.read(int(self.headers["Content-Length"]))
            return self._reply(201)
        self._reply(400)

    def do_GET(self):
        if urlsplit(self.path).path == "/jmx":
            return self._reply(200, {"beans": [{"NumLiveDataNodes": 3, "CapacityTotal": 1000, "CapacityRemaining": 400}]})
        path, query = self._split()
        if query["op"] == "LISTSTATUS":
            entries = [{"pathSuffix": p.rsplit("/", 1)[1], "type": "FILE", "length": len(b)} for p, b in _Fake.store.items() if p.rsplit("/", 1)[0] == path]
            entries += [{"pathSuffix": d.rsplit("/", 1)[1], "type": "DIRECTORY", "length": 0} for d in _Fake.dirs if d.rsplit("/", 1)[0] == path and d != path]
            if not entries and path not in _Fake.dirs:
                return self._reply(404, {"RemoteException": {}})
            return self._reply(200, {"FileStatuses": {"FileStatus": entries}})
        if query["op"] == "GETCONTENTSUMMARY":
            under = [b for p, b in _Fake.store.items() if p.startswith(path)]
            return self._reply(200, {"ContentSummary": {"length": sum(map(len, under)), "fileCount": len(under), "directoryCount": 1}})
        self._reply(400)


@pytest.fixture()
def server():
    _Fake.store, _Fake.dirs, _Fake.log = {}, set(), []
    httpd = HTTPServer(("127.0.0.1", 0), _Fake)
    threading.Thread(target=httpd.serve_forever, daemon=True).start()
    yield "http://127.0.0.1:{}".format(httpd.server_address[1])
    httpd.shutdown()


def test_mkdirs_sends_permission_and_user(server):
    assert webhdfs.mkdirs(server, "/smart-drive-demo/bronze") is True
    method, path, query = _Fake.log[-1]
    assert (method, path) == ("PUT", "/smart-drive-demo/bronze")
    assert query["permission"] == "777" and query["user.name"] == "root"


def test_put_file_follows_the_two_step_write_and_streams_the_whole_file(server, tmp_path):
    local = tmp_path / "2026-01-01.csv"
    local.write_bytes(b"date,serial\n" * 200_000)  # ~2.4 MB: more than one chunk
    seen = []

    sent = webhdfs.put_file(server, "/smart-drive-demo/bronze/2026-01-01.csv", local, replication=1, on_progress=lambda s, t: seen.append((s, t)))

    assert sent == local.stat().st_size
    assert _Fake.store["/smart-drive-demo/bronze/2026-01-01.csv"] == local.read_bytes()
    create = [q for _m, _p, q in _Fake.log if q["op"] == "CREATE" and "datanode" not in q][0]
    assert create["replication"] == "1" and create["overwrite"] == "true"
    assert len(seen) >= 2 and seen[-1] == (sent, sent)  # progress was reported while streaming


def test_file_sizes_lists_only_files_and_a_missing_directory_is_empty(server, tmp_path):
    local = tmp_path / "a.csv"
    local.write_bytes(b"abc")
    webhdfs.mkdirs(server, "/d")
    webhdfs.put_file(server, "/d/a.csv", local)
    webhdfs.mkdirs(server, "/d/sub")

    assert webhdfs.file_sizes(server, "/d") == {"a.csv": 3}
    assert webhdfs.list_status(server, "/missing") == []


def test_content_summary_and_namenode_state(server, tmp_path):
    local = tmp_path / "a.csv"
    local.write_bytes(b"12345")
    webhdfs.put_file(server, "/d/a.csv", local)

    summary = webhdfs.content_summary(server, "/d")
    assert summary["length"] == 5 and summary["fileCount"] == 1
    assert webhdfs.namenode_state(server) == {"live_datanodes": 3, "capacity_total": 1000, "capacity_remaining": 400}
