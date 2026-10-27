"""Box Mount ticker: one line per file crossing between sandboxes through Box.

    python scripts/ticker.py sandbox-booth-extractor sandbox-booth-reviewer ...

Follows each sandbox's `docker logs` (the supervisor already filters Box Mount's sync log down to
uploads and downloads) and prints, merged:

    20:51:03.412  ↑ sandbox-booth-extractor   uploaded    Work/HALVORSEN-MSA/run-…/extraction.json
    20:51:07.530  ↓ sandbox-booth-reviewer    downloaded  Work/HALVORSEN-MSA/run-…/extraction.json   +4.1 s via Box
"""

import re
import subprocess
import sys
import threading
from datetime import datetime

LINE = re.compile(r"^\[(?P<host>[\w-]+) box-mount\] (?P<ts>\d\d:\d\d:\d\d\.\d+) (?P<msg>.*)$")
UPLOAD = re.compile(r"(?:CREATE\.create_file|UPDATE\.update_file\.start): .*local_path='/mnt/box/(?P<path>[^']+)'")
DOWNLOAD = re.compile(r"CREATE_OR_UPDATE\.write_file: target='/mnt/box/(?P<path>[^']+)'")
SKIP = ("_sync/", "_latency/")

UP, DOWN, DIM, BOLD, RESET = "\033[38;5;39m", "\033[38;5;78m", "\033[2m", "\033[1m", "\033[0m"
uploaded: dict[str, tuple[datetime, str]] = {}
lock = threading.Lock()


def _time(ts: str) -> datetime:
    return datetime.strptime(ts[:12], "%H:%M:%S.%f")


def follow(container: str) -> None:
    proc = subprocess.Popen(["docker", "logs", "-f", "--since", "1s", container],
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    for raw in proc.stdout:
        m = LINE.match(raw.rstrip())
        if not m:
            continue
        host, ts, msg = m["host"], m["ts"], m["msg"]
        if (u := UPLOAD.search(msg)) and not u["path"].startswith(SKIP):
            with lock:
                uploaded[u["path"]] = (_time(ts), host)
                print(f"{ts[:12]}  {UP}↑ {host:<26}{RESET} uploaded    {BOLD}{u['path']}{RESET}", flush=True)
        elif (d := DOWNLOAD.search(msg)) and not d["path"].startswith(SKIP):
            with lock:
                note = f"   {DIM}from Box (edited outside these sandboxes){RESET}"
                if d["path"] in uploaded and uploaded[d["path"]][1] == host:
                    note = ""
                elif d["path"] in uploaded:
                    delta = (_time(ts) - uploaded[d["path"]][0]).total_seconds()
                    note = f"   {DIM}+{delta:.1f} s via Box from {uploaded[d['path']][1]}{RESET}"
                print(f"{ts[:12]}  {DOWN}↓ {host:<26}{RESET} downloaded  {BOLD}{d['path']}{RESET}{note}", flush=True)


if __name__ == "__main__":
    containers = sys.argv[1:]
    if not containers:
        sys.exit("usage: ticker.py <container> [<container> ...]")
    print(f"{DIM}Box Mount ticker: {', '.join(containers)} (Ctrl-C to stop){RESET}", flush=True)
    threads = [threading.Thread(target=follow, args=(c,), daemon=True) for c in containers]
    for t in threads:
        t.start()
    try:
        for t in threads:
            t.join()
    except KeyboardInterrupt:
        pass
