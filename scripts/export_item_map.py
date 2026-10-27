"""Print Box Mount's local-file -> Box ID map as JSON (run as the daemon user by the supervisor).

The daemon's sync database is private to boxsvc. The supervisor runs this every few seconds and
writes the output to /run/boxmount/item_map.json, so the agent can turn a file it can see into a
Box web link without any Box credentials. Only IDs leave the database.
"""

import json
import sqlite3
import sys

DB = "/home/boxsvc/.box-mount/data/sync.sqlite3"

try:
    con = sqlite3.connect(f"file:{DB}?mode=ro", uri=True, timeout=1)
    rows = con.execute("select inode_id, box_id, box_type from item_map").fetchall()
except sqlite3.Error:
    rows = []
json.dump({inode: {"box_id": box_id, "type": box_type} for inode, box_id, box_type in rows}, sys.stdout)
