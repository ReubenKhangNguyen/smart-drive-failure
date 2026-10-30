"""Collect host/Docker facts for the benchmark (HD9 `benchmark_environment`). Run on the HOST right before measuring:

    python scripts/benchmark_env.py

Standard library only (a container only sees the WSL VM, not the real host RAM/CPU). Windows PowerShell is used for
host RAM/CPU; on other systems those items say they could not be determined.
"""
from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path
from typing import Dict, List

OUT = Path(__file__).resolve().parent.parent / "artifacts" / "reports" / "benchmark_raw" / "environment_host.json"
UNKNOWN = "không xác định"

POWERSHELL = (
    "$c=Get-CimInstance Win32_ComputerSystem; $p=Get-CimInstance Win32_Processor | Select-Object -First 1; "
    "$os=Get-CimInstance Win32_OperatingSystem; "
    "'host_ram_gb=' + [math]::Round($c.TotalPhysicalMemory/1GB,1); "
    "'host_ram_free_gb_at_start=' + [math]::Round($os.FreePhysicalMemory/1MB,1); "
    "'host_cpu=' + $p.Name.Trim(); 'host_cores=' + $p.NumberOfCores; 'host_logical_cpus=' + $p.NumberOfLogicalProcessors"
)


def parse_key_values(text: str) -> Dict[str, str]:
    """`key=value` lines -> dict (other lines ignored)."""
    out = {}  # type: Dict[str, str]
    for line in text.splitlines():
        if "=" in line:
            key, _, value = line.partition("=")
            if key.strip():
                out[key.strip()] = value.strip()
    return out


def _run(command: List[str], cwd: Path) -> str:
    try:
        done = subprocess.run(command, cwd=str(cwd), stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, universal_newlines=True, timeout=60)
        return done.stdout.strip() if done.returncode == 0 else ""
    except (OSError, subprocess.TimeoutExpired):
        return ""


def collect(root: Path) -> Dict[str, str]:
    items = {}  # type: Dict[str, str]
    if sys.platform.startswith("win"):
        items.update(parse_key_values(_run(["powershell", "-NoProfile", "-Command", POWERSHELL], root)))
    for key in ("host_ram_gb", "host_ram_free_gb_at_start", "host_cpu", "host_cores", "host_logical_cpus"):
        items.setdefault(key, UNKNOWN)

    docker = _run(["docker", "info", "--format", "{{.MemTotal}}|{{.NCPU}}"], root).split("|")
    items["docker_mem_bytes"] = docker[0] if len(docker) == 2 and docker[0] else UNKNOWN
    items["docker_ncpu"] = docker[1] if len(docker) == 2 and docker[1] else UNKNOWN
    for key, conf in (("hdfs_replication", "dfs.replication"), ("hdfs_blocksize", "dfs.blocksize")):
        value = _run(["docker", "compose", "exec", "-T", "namenode", "hdfs", "getconf", "-confKey", conf], root)
        items[key] = value.splitlines()[-1] if value else UNKNOWN
    items["git_commit"] = _run(["git", "rev-parse", "--short", "HEAD"], root) or UNKNOWN
    return items


def main() -> int:
    items = collect(Path(__file__).resolve().parent.parent)
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(items, ensure_ascii=False, indent=2))
    print("wrote", OUT)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
