"""Box Mount ticker: one line per file crossing between sandboxes through Box.

    python scripts/ticker.py sandbox-extractor sandbox-reviewer sandbox-coordinator

Follows each sandbox's `docker logs` (the supervisor already filters Box Mount's sync log down to
uploads and downloads; the coordinator logs its Box API actions) and prints, merged:

    20:51:03.412  ↑ sandbox-extractor     uploaded    Extraction/Work/HALVORSEN-MSA/run-…/extraction.json
    20:51:05.020  ⇢ sandbox-coordinator   COPY        Extraction/…/extraction.json v2101 -> Review/…/extraction.json
    20:51:09.530  ↓ sandbox-reviewer      downloaded  Review/Work/HALVORSEN-MSA/run-…/extraction.json   +4.5 s via Box

Each agent mounts its own folder, so paths are shown from the demo folder down
(Extraction/… or Review/…).
"""

import re
import subprocess
import sys
import threading
from datetime import datetime

LINE = re.compile(r"^\[(?P<host>[\w-]+) box-mount\] (?P<ts>\d\d:\d\d:\d\d\.\d+) (?P<msg>.*)$")
UPLOAD = re.compile(r"(?:CREATE\.create_file|UPDATE\.update_file\.start): .*local_path='/mnt/box/(?P<path>[^']+)'")
DOWNLOAD = re.compile(r"CREATE_OR_UPDATE\.write_file: target='/mnt/box/(?P<path>[^']+)'")
API = re.compile(r"^\[(?P<host>[\w-]+) box-api\] (?P<ts>\d\d:\d\d:\d\d\.\d+) (?P<action>[A-Z]+) (?P<text>.*)$")
COPY = re.compile(r"(?P<src>\S+) v\S+ -> (?P<dest>\S+)")
SKIP = ("_sync/",)
# The agents mount their own folders; show their paths from the demo folder down.
AREA = {"sandbox-extractor": "Extraction/", "sandbox-reviewer": "Review/"}
API_COLOR = "\033[38;5;177m"

UP, DOWN, DIM, BOLD, RESET = "\033[38;5;39m", "\033[38;5;78m", "\033[2m", "\033[1m", "\033[0m"
uploaded: dict[str, tuple[datetime, str]] = {}
lock = threading.Lock()


def _time(ts: str) -> datetime:
    return datetime.strptime(ts[:12], "%H:%M:%S.%f")


def follow(container: str) -> None:
    proc = subprocess.Popen(["docker", "logs", "-f", "--since", "1s", container],
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    for raw in proc.stdout:
        if a := API.match(raw.rstrip()):
            with lock:
                if c := COPY.search(a["text"]):
                    uploaded[c["dest"]] = (_time(a["ts"]), f"{a['host']} (Box API copy)")
                print(f"{a['ts'][:12]}  {API_COLOR}⇢ {a['host']:<26}{RESET} {a['action']:<11} {BOLD}{a['text']}{RESET}",
                      flush=True)
            continue
        m = LINE.match(raw.rstrip())
        if not m:
            continue
        host, ts, msg = m["host"], m["ts"], m["msg"]
        area = AREA.get(host, "")
        if (u := UPLOAD.search(msg)) and not u["path"].startswith(SKIP):
            path = area + u["path"]
            with lock:
                uploaded[path] = (_time(ts), host)
                print(f"{ts[:12]}  {UP}↑ {host:<26}{RESET} uploaded    {BOLD}{path}{RESET}", flush=True)
        elif (d := DOWNLOAD.search(msg)) and not d["path"].startswith(SKIP):
            path = area + d["path"]
            with lock:
                note = f"   {DIM}from Box (edited outside these sandboxes){RESET}"
                if path in uploaded and uploaded[path][1] == host:
                    note = ""
                elif path in uploaded:
                    delta = (_time(ts) - uploaded[path][0]).total_seconds()
                    note = f"   {DIM}+{delta:.1f} s via Box from {uploaded[path][1]}{RESET}"
                print(f"{ts[:12]}  {DOWN}↓ {host:<26}{RESET} downloaded  {BOLD}{path}{RESET}{note}", flush=True)


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
