"""Minimal WebHDFS client (standard library only) for the demo steps that run where there is no `hdfs` command.

The Airflow container has no docker CLI and no Hadoop client, but it can reach the NameNode web port, so preparing
HDFS (step 1) and loading the CSV files (step 2) go through the WebHDFS REST API. A write is two requests: the
NameNode answers 307 with the DataNode address, then the data is sent there.
"""
from __future__ import annotations

import http.client
import json
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

DEFAULT_USER = "root"  # the HDFS superuser of this cluster; WebHDFS uses simple (unauthenticated) identity
CHUNK = 1024 * 1024


def _url(web_url: str, path: str, op: str, **params: Any) -> str:
    query = {"op": op, "user.name": DEFAULT_USER}
    query.update({k: v for k, v in params.items() if v is not None})
    return "{}/webhdfs/v1{}?{}".format(web_url.rstrip("/"), urllib.parse.quote(path), urllib.parse.urlencode(query))


def _json(url: str, method: str = "GET", timeout: float = 30.0) -> Dict[str, Any]:
    request = urllib.request.Request(url, method=method)
    with urllib.request.urlopen(request, timeout=timeout) as response:
        body = response.read().decode("utf-8")
    return json.loads(body) if body else {}


def mkdirs(web_url: str, path: str, permission: str = "777") -> bool:
    return bool(_json(_url(web_url, path, "MKDIRS", permission=permission), method="PUT").get("boolean"))


def list_status(web_url: str, path: str) -> List[Dict[str, Any]]:
    """The entries of a directory; a missing directory gives []."""
    try:
        return _json(_url(web_url, path, "LISTSTATUS")).get("FileStatuses", {}).get("FileStatus", [])
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return []
        raise


def file_sizes(web_url: str, path: str) -> Dict[str, int]:
    """{file name: size in bytes} of the files directly under `path` (directories skipped)."""
    return {e["pathSuffix"]: int(e["length"]) for e in list_status(web_url, path) if e.get("type") == "FILE"}


def content_summary(web_url: str, path: str) -> Dict[str, int]:
    """length (bytes before replication), fileCount, directoryCount of everything under `path`; zeros if missing."""
    try:
        summary = _json(_url(web_url, path, "GETCONTENTSUMMARY")).get("ContentSummary", {})
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return {"length": 0, "fileCount": 0, "directoryCount": 0}
        raise
    return {"length": int(summary.get("length", 0)), "fileCount": int(summary.get("fileCount", 0)),
            "directoryCount": int(summary.get("directoryCount", 0))}


def namenode_state(web_url: str) -> Dict[str, int]:
    """Live DataNodes and capacity straight from the NameNode: the check that HDFS is ready before anything is written."""
    url = "{}/jmx?qry=Hadoop:service=NameNode,name=FSNamesystemState".format(web_url.rstrip("/"))
    with urllib.request.urlopen(url, timeout=15) as response:
        bean = json.loads(response.read().decode("utf-8"))["beans"][0]
    return {"live_datanodes": int(bean["NumLiveDataNodes"]), "capacity_total": int(bean["CapacityTotal"]),
            "capacity_remaining": int(bean["CapacityRemaining"])}


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args: Any, **kwargs: Any) -> None:  # we need the 307 itself, to read the DataNode address
        return None


def put_file(web_url: str, hdfs_path: str, local_path: Path, replication: int = 1,
             on_progress: Optional[Callable[[int, int], None]] = None) -> int:
    """Create/overwrite one HDFS file from a local file, streaming it (the CSV files are ~135 MB each).
    Returns the number of bytes sent."""
    opener = urllib.request.build_opener(_NoRedirect)
    first = urllib.request.Request(_url(web_url, hdfs_path, "CREATE", overwrite="true", replication=replication), method="PUT")
    try:
        opener.open(first, timeout=30)
        raise RuntimeError("NameNode did not redirect the CREATE of {}".format(hdfs_path))
    except urllib.error.HTTPError as exc:
        if exc.code != 307:
            raise
        location = exc.headers["Location"]
    target = urllib.parse.urlsplit(location)
    size = Path(local_path).stat().st_size
    connection = http.client.HTTPConnection(target.hostname, target.port, timeout=600)
    try:
        connection.putrequest("PUT", "{}?{}".format(target.path, target.query))
        connection.putheader("Content-Length", str(size))
        connection.putheader("Content-Type", "application/octet-stream")
        connection.endheaders()
        sent = 0
        with open(str(local_path), "rb") as handle:
            while True:
                block = handle.read(CHUNK)
                if not block:
                    break
                connection.send(block)
                sent += len(block)
                if on_progress:
                    on_progress(sent, size)
        response = connection.getresponse()
        response.read()
        if response.status != 201:
            raise RuntimeError("DataNode answered HTTP {} for {}".format(response.status, hdfs_path))
    finally:
        connection.close()
    return sent
