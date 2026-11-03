#!/bin/sh
# Sandbox supervisor (root):
#   1. mount the Box folder at /mnt/box as boxsvc;
#   2. publish the daemon's status every 2s to /run/boxmount/status, and its file -> Box ID map to
#      /run/boxmount/item_map.json (the agent can't read the daemon's state: it's private to boxsvc),
#      and remount the folder if the daemon stops;
#   3. run this sandbox's agent worker as `agent`, without the BOX_* credentials, and restart it
#      whenever it exits. While /run/demo/hold exists the worker stays down (`just kill`).
set -eu

: "${BOX_FOLDER_ID:?Set BOX_FOLDER_ID in .env (the number at the end of the Box folder URL)}"
DATA=/home/boxsvc/.box-mount
BM="box-mount --data-path $DATA"
log() { echo "[$(hostname) supervisor] $*"; }
# The Box key must be readable by boxsvc only. `docker cp` leaves it owned by the host user.
own_key() { [ -f "$DATA/box-jwt-config.json" ] && chown boxsvc:boxmount "$DATA/box-jwt-config.json" && chmod 600 "$DATA/box-jwt-config.json"; true; }
remount() {
  own_key
  # A mount record left by a previous daemon has a stale pid (often reused by this container),
  # and box-mount 0.6.0 refuses to mount over it. Plain `unmount` (no --reset-force) keeps unsynced
  # local edits but clears the sync database, so the mount re-downloads from Box (~8s).
  if [ -f "$DATA/mount.json" ]; then
    su boxsvc -s /bin/sh -c "$BM unmount /mnt/box" >/dev/null 2>&1 || true
  fi
  su boxsvc -s /bin/sh -c "$BM mount /mnt/box '$BOX_FOLDER_ID'"
}

if [ "${BOX_AUTH_TYPE:-}" = "jwt" ] && [ ! -r "${BOX_JWT_CONFIG_PATH:-/nonexistent}" ]; then
  log "BOX_AUTH_TYPE=jwt but ${BOX_JWT_CONFIG_PATH:-BOX_JWT_CONFIG_PATH} is missing: start sandboxes with 'just up'" >&2
  sleep 30; exit 1
fi

mkdir -p "$DATA" /run/boxmount /run/demo /run/boxmount/requests
chown -R boxsvc:boxmount "$DATA" && chmod 700 "$DATA"
own_key
chmod 755 /run/boxmount /run/demo
chmod 1777 /run/boxmount/requests   # the agent drops requests here (e.g. a sync pulse)
chown boxsvc:boxmount /mnt/box
# The daemon creates folders as 755; a default ACL keeps everything under /mnt/box group-writable
# so the agent user can write into folders that sync down from Box (Box's recommended setup).
setfacl -R -m g:boxmount:rwX /mnt/box
find /mnt/box -type d -exec setfacl -m d:g:boxmount:rwX {} +

remount

# 2. status heartbeat, and remount after three checks in a row (~6 s) without a running daemon
(
  down=0
  while true; do
    su boxsvc -s /bin/sh -c "$BM status" > /run/boxmount/status.tmp 2>&1 || true
    mv /run/boxmount/status.tmp /run/boxmount/status
    if grep -q "status=running" /run/boxmount/status; then down=0; else down=$((down + 1)); fi
    if [ "$down" -ge 3 ]; then
      log "Box Mount is not running ($(cat /run/boxmount/status)): remounting"
      remount || log "remount failed; trying again shortly"
      down=0
    fi
    # Sync pulse requested by an agent: rewrite this sandbox's pulse file in place, as its owner.
    # The upload echoes back and makes the daemon re-list remote changes (catches a missed
    # notification). The agent can't write it itself: files Box Mount downloads are read-only to
    # it, and replacing the file (delete + create) risks Box Mount's folder-identity bug.
    if [ -e /run/boxmount/requests/pulse ]; then
      rm -f /run/boxmount/requests/pulse
      su boxsvc -s /bin/sh -c "mkdir -p /mnt/box/_sync && date +%s.%N > /mnt/box/_sync/$(hostname).txt" || true
    fi
    # Local file -> Box ID map (IDs only), so agents can link to files in the Box web app.
    if su boxsvc -s /bin/sh -c "/app/.venv/bin/python /app/scripts/export_item_map.py" > /run/boxmount/item_map.tmp 2>/dev/null; then
      mv /run/boxmount/item_map.tmp /run/boxmount/item_map.json
    fi
    sleep 2
  done
) &

# Box Mount's sync events, one line each, for `docker logs` (the full log stays in $DATA/logs).
SYNC_LOG="$DATA/logs/box-mount-sync.stderr.log"
touch "$SYNC_LOG"
(
  tail -n0 -F "$SYNC_LOG" 2>/dev/null \
    | grep --line-buffered -E "CREATE\.create_file|UPDATE\.|write_file|DELETE\.|Bootstrap cycle|ERROR|conflict" \
    | sed -u -E "s#^[0-9-]+ ([0-9:]+),([0-9]+) \[[^]]*\] [A-Z]+: #[$(hostname) box-mount] \1.\2 #; s#/tmp/bdlc-sync-remote[^']*##"
) &

# 3. the agent worker
if [ -z "${DEMO_ROLE:-}" ]; then
  log "no DEMO_ROLE: Box Mount only"
  wait
fi
while true; do
  if [ -e /run/demo/hold ]; then sleep 1; continue; fi
  log "starting $DEMO_ROLE worker"
  (
    for v in $(env | sed -n 's/^\(BOX_[A-Z_]*\)=.*/\1/p'); do unset "$v"; done
    cd /app && exec runuser -u agent -- /app/.venv/bin/python -m demo.worker
  ) || true
  log "$DEMO_ROLE worker exited"
  sleep 1
done
