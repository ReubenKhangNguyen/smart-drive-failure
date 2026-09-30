from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List


@dataclass
class UploadPlan:
    to_upload: List[Path]
    skipped_same_size: List[Path] = field(default_factory=list)


def plan_uploads(local_files: List[Path], existing_sizes: Dict[str, int]) -> UploadPlan:
    """Idempotent plan: skip files already present in HDFS with the same byte size."""
    to_upload: List[Path] = []
    skipped: List[Path] = []
    for f in local_files:
        existing_size = existing_sizes.get(f.name)
        if existing_size is not None and existing_size == f.stat().st_size:
            skipped.append(f)
        else:
            to_upload.append(f)
    return UploadPlan(to_upload=to_upload, skipped_same_size=skipped)


def to_container_path(local_file: Path, host_root: Path, container_root: str) -> str:
    """Map a host file path to the equivalent path inside a container that bind-mounts
    host_root at container_root (e.g. namenode's /external_data mount)."""
    relative = local_file.relative_to(host_root)
    return "{}/{}".format(container_root.rstrip("/"), relative.as_posix())


def hdfs_put_command(container_source_path: str, hdfs_target_dir: str, filename: str, replication: int) -> List[str]:
    return [
        "hdfs",
        "dfs",
        "-D",
        "dfs.replication={}".format(replication),
        "-put",
        "-f",
        container_source_path,
        "{}/{}".format(hdfs_target_dir.rstrip("/"), filename),
    ]


def parse_hdfs_ls_sizes(ls_output: str) -> Dict[str, int]:
    """Parse `hdfs dfs -ls <dir>` stdout into {filename: size_bytes}, skipping directories."""
    sizes: Dict[str, int] = {}
    for line in ls_output.splitlines():
        parts = line.split()
        if len(parts) < 8 or parts[0].startswith("Found") or parts[0].startswith("d"):
            continue
        try:
            size = int(parts[4])
        except ValueError:
            continue
        sizes[Path(parts[-1]).name] = size
    return sizes
