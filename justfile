# Box Mount + Temporal Agent Harness demo. `just` lists recipes.
#
# Host:      just temporal (:7233, UI :8233) | just session-manager | just server        (three terminals)
# Sandboxes: just up, then just seed
# Demo:      just status | just logs <role> | just tree <role> | just flaky on|off
#            just kill <sandbox> | just revive <sandbox> | just reset

set dotenv-path := ".env.local"
set dotenv-load := true
export PATH := env_var("HOME") + "/.docker/bin:" + env_var("PATH")
# Host processes and the sandboxes all use the demo's Temporal dev server on the default port.
# Only one dev server may listen on 7233: stop any other before `just temporal`.
export TEMPORAL_ADDRESS := "127.0.0.1:7233"

sandboxes := "coordinator extractor compliance reviewer"
booth_sandboxes := "booth-coordinator booth-extractor booth-reviewer"

default:
    @just --list

# Local Temporal dev server on :7233, reachable from the sandboxes (Web UI: http://localhost:8233).
temporal:
    mkdir -p .temporal && temporal server start-dev --ip 0.0.0.0 --db-filename .temporal/dev.db

# The harness's packaged session-manager worker.
session-manager:
    uv run python scripts/harness_cli.py session-manager

# Harness UI + API on http://localhost:8000.
server:
    uv run python scripts/harness_cli.py serve agents.toml --host 127.0.0.1 --port 8000

# Build the sandbox image and start all five sandboxes. The Box key is copied into each sandbox's
# private volume (not bind-mounted, which would let the agent user read it).
up:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p /tmp/temporal-large-payloads
    docker compose --profile full up --no-start --build
    for s in {{sandboxes}}; do
      docker cp -q secrets/box-jwt-config.json sandbox-$s:/home/boxsvc/.box-mount/box-jwt-config.json
    done
    docker compose --profile full up -d

# Stop every sandbox (both stacks). Box Mount state is kept in the volumes.
down:
    docker compose --profile full --profile booth down

# After editing agents.toml: the session-manager workflow keeps the registry it started with, so
# end it; restart `just server` afterwards and it starts a fresh one. Ends open UI sessions.
registry-reload:
    temporal workflow terminate -w session-manager --reason "agents.toml changed" || true
    @echo "now restart 'just server' (and 'just session-manager')"

# Restart the agent workers only (after editing demo/*.py); Box Mount keeps running.
reload:
    for c in $(docker ps --format '{{{{.Names}}' | grep '^sandbox-'); do docker exec $c pkill -f "[d]emo.worker" || true; done

# Box Mount and worker state in every sandbox.
status:
    #!/usr/bin/env bash
    for c in $(docker ps --format '{{{{.Names}}' | grep '^sandbox-' | sort); do
      bm=$(docker exec $c cat /run/boxmount/status 2>/dev/null | sed 's/.*| status=/box-mount=/')
      w=$(docker exec $c pgrep -f demo.worker >/dev/null 2>&1 && echo up || echo DOWN)
      hold=$(docker exec $c test -e /run/demo/hold 2>/dev/null && echo " (held)" || true)
      printf "%-26s %-18s worker=%s%s\n" "$c" "${bm:-unreachable}" "$w" "$hold"
    done

# Follow one sandbox's supervisor, Box Mount sync events and worker log.
logs sandbox:
    docker logs -f --tail 30 sandbox-{{sandbox}}

# Show the Box folder as Box Mount has synced it into one sandbox.
tree sandbox="coordinator":
    docker exec sandbox-{{sandbox}} tree -a -I _sync /mnt/box

# Generate the vendor packet and playbook, and put them in Box (via the coordinator's mount).
seed:
    uv run -q --group seed python scripts/make_seed.py
    docker exec -u boxsvc sandbox-coordinator /app/.venv/bin/python /app/scripts/box_admin.py seed

# Clean slate between rehearsals (~1 min). Never deletes anything while sandboxes are running:
# Box Mount 0.6.0 can map a re-created folder (reused inode) to the trashed Box folder, after which
# its files fail to sync or land at the top of the Box folder. So: wipe every sandbox's local copy,
# delete old outputs in Box from one fresh sandbox, then bring every sandbox up fresh.
reset:
    #!/usr/bin/env bash
    set -euo pipefail
    just _end-sessions box
    just _wipe full "{{sandboxes}}"
    docker compose --profile full up --no-start coordinator
    docker cp -q secrets/box-jwt-config.json sandbox-coordinator:/home/boxsvc/.box-mount/box-jwt-config.json
    docker compose --profile full start coordinator
    until docker exec sandbox-coordinator grep -q status=running /run/boxmount/status 2>/dev/null; do sleep 1; done
    sleep 5
    docker exec -u boxsvc sandbox-coordinator /app/.venv/bin/python /app/scripts/box_admin.py reset
    echo "waiting for the deletes to reach Box..."; sleep 15
    just _wipe full coordinator
    just up
    echo "clean slate: run 'just status'"

# End every open session and agent of one stack (prefix box or booth) by its task queues. The
# session-manager workflows are on their own queue and keep running. A session left open across
# a code change can't continue anyway (its history no longer matches the code), so resets start clean.
_end-sessions prefix:
    #!/usr/bin/env bash
    if ! nc -z 127.0.0.1 7233 2>/dev/null; then echo "Temporal not running: no sessions to end"; exit 0; fi
    q="ExecutionStatus='Running' AND TaskQueue IN ('{{prefix}}-coordinator','{{prefix}}-extractor','{{prefix}}-compliance','{{prefix}}-reviewer')"
    n=$(temporal workflow count --query "$q" -o json | python3 -c "import json,sys; print(json.load(sys.stdin).get('count', 0))")
    if [ "${n:-0}" = 0 ]; then echo "no open {{prefix}} sessions"; exit 0; fi
    echo "ending $n open {{prefix}} session(s)/agent(s):"
    temporal workflow list --query "$q" -o json | python3 -c "import json,sys; [print('  ', w['execution']['workflowId']) for w in json.load(sys.stdin)]"
    temporal workflow terminate --query "$q" --reason "just {{prefix}} reset" --yes >/dev/null
    for i in $(seq 1 15); do
      [ "$(temporal workflow count --query "$q" -o json | python3 -c "import json,sys; print(json.load(sys.stdin).get('count', 0))")" = 0 ] && break
      sleep 1
    done

# Remove a stack's containers and their Box Mount volumes (local copies only; Box is untouched).
_wipe profile services:
    #!/usr/bin/env bash
    project=$(basename "$PWD")
    for s in {{services}}; do
      docker compose --profile {{profile}} rm -sf $s >/dev/null 2>&1 || true
      docker volume rm -f ${project}_$s-data ${project}_$s-files >/dev/null 2>&1 || true
    done

# Make the simulated ERP time out twice per lookup in the compliance sandboxes (on|off).
flaky state:
    #!/usr/bin/env bash
    for s in compliance; do
      if [ "{{state}}" = on ]; then docker exec sandbox-$s touch /run/demo/flaky; else docker exec sandbox-$s rm -f /run/demo/flaky; fi
    done
    echo "ERP flakiness {{state}}"

# The crash moment: wait until <sandbox>'s agent has a model call in flight, then kill -9 that
# worker. The sandbox restarts it in ~1 s and Temporal resumes the same step there.
# e.g. `just crash booth-reviewer` or `just crash compliance`.
crash sandbox:
    uv run -q python scripts/crash.py {{sandbox}}

# Cheat sheet for the Crash tab that booth-launch opens.
crash-help:
    @echo "Crash moment (add-on B). Type go in the Booth UI, then within ~20 s:"
    @echo ""
    @echo "  just crash booth-reviewer     kill -9 mid model call; restarts and resumes as attempt 2"
    @echo "  just crash booth-extractor    same, for the Extractor (run right after go)"
    @echo "  just kill booth-reviewer      kill and keep it down: the step waits in Temporal"
    @echo "  just revive booth-reviewer    let it come back and pick up where it was"
    @echo "  just status                   worker and box-mount state per sandbox"
    @echo ""

# Crash a sandbox's agent worker (kill -9) and keep it down. Box Mount keeps running.
kill sandbox:
    docker exec sandbox-{{sandbox}} sh -c 'touch /run/demo/hold; pkill -9 -f "[d]emo.worker"; echo "killed worker in $(hostname)"'

# Let a killed worker restart.
revive sandbox:
    docker exec sandbox-{{sandbox}} rm -f /run/demo/hold
    @echo "worker in sandbox-{{sandbox}} restarting"

# ---------------------------------------------------------------------------------------------
# Booth demo: Box Relay (Extractor -> Reviewer through Box, plus a second reviewer for failover)
# ---------------------------------------------------------------------------------------------

# Start the four booth sandboxes on the booth folder (BOOTH_BOX_FOLDER_ID).
booth-up:
    #!/usr/bin/env bash
    set -euo pipefail
    mkdir -p /tmp/temporal-large-payloads
    docker compose --profile booth up --no-start --build
    for s in {{booth_sandboxes}}; do
      docker cp -q secrets/box-jwt-config.json sandbox-$s:/home/boxsvc/.box-mount/box-jwt-config.json
    done
    docker compose --profile booth up -d

# First time only: put the contract and playbook into the booth folder.
booth-seed:
    uv run -q --group seed python scripts/make_seed.py
    docker exec -u boxsvc sandbox-booth-coordinator /app/.venv/bin/python /app/scripts/box_admin.py seed --booth

# Clean slate for the booth (~1 min): ends any open booth sessions, clears old runs and published
# memos, and restores the original playbook so add-on A (editing it in Box) starts fresh.
booth-reset:
    #!/usr/bin/env bash
    set -euo pipefail
    just _end-sessions booth
    just _wipe booth "{{booth_sandboxes}}"
    docker compose --profile booth up --no-start booth-coordinator
    docker cp -q secrets/box-jwt-config.json sandbox-booth-coordinator:/home/boxsvc/.box-mount/box-jwt-config.json
    docker compose --profile booth start booth-coordinator
    until docker exec sandbox-booth-coordinator grep -q status=running /run/boxmount/status 2>/dev/null; do sleep 1; done
    sleep 5
    docker exec -u boxsvc sandbox-booth-coordinator /app/.venv/bin/python /app/scripts/box_admin.py reset --booth
    # Put the contract and the original playbook back, so add-on A (editing the playbook) starts fresh.
    uv run -q --group seed python scripts/make_seed.py >/dev/null
    docker exec -u boxsvc sandbox-booth-coordinator /app/.venv/bin/python /app/scripts/box_admin.py seed --booth
    echo "waiting for the changes to reach Box..."; sleep 15
    just _wipe booth booth-coordinator
    just booth-up
    just _wait-sandboxes
    just _verify-playbook
    echo "booth clean slate: run 'just status'"

# Between visitors (a few seconds): put the original playbook back after add-on A, without a full
# reset. It overwrites the file in place, so Box keeps the edited one in version history.
booth-playbook-reset:
    uv run -q --group seed python scripts/make_seed.py >/dev/null
    docker exec -u boxsvc sandbox-booth-coordinator /app/.venv/bin/python /app/scripts/box_admin.py seed --booth
    just _verify-playbook

# Wait until the agents' sandboxes have the seed playbook from Box. Box refuses uploads to a
# locked file (e.g. left open in Box Edit after add-on A), and the local copy alone proves nothing.
_verify-playbook:
    #!/usr/bin/env bash
    want=$(shasum -a 256 seed/Playbook/playbook.md | cut -d' ' -f1)
    since=$(date -u +%Y-%m-%dT%H:%M:%SZ)
    for i in $(seq 1 30); do
      ok=1
      for s in booth-extractor booth-reviewer; do
        got=$(docker exec sandbox-$s sha256sum /mnt/box/Playbook/playbook.md 2>/dev/null | cut -d' ' -f1)
        [ "$got" = "$want" ] || { ok=0; docker exec sandbox-$s touch /run/boxmount/requests/pulse 2>/dev/null; }
      done
      [ $ok = 1 ] && { echo "playbook restored (Net 45) in Box and in both agents' sandboxes"; exit 0; }
      if docker logs --since "$since" sandbox-booth-coordinator 2>&1 | grep -q "playbook.md.*item locked"; then
        echo "Box refused the upload: Playbook/playbook.md is LOCKED in Box."
        echo "Unlock it in the Box web UI (… > Unlock, or close it in Box Edit), then rerun: just booth-playbook-reset"
        exit 1
      fi
      sleep 3
    done
    echo "playbook not synced to the agents' sandboxes after 90 s; check 'just status'"; exit 1

# The on-screen strip: every file crossing between booth sandboxes through Box, with timing.
ticker:
    python3 scripts/ticker.py sandbox-booth-extractor sandbox-booth-reviewer sandbox-booth-coordinator

# The booth's Harness UI on http://localhost:8000: only Box Relay, with its own session-manager
# workflow (booth-session-manager). Run instead of `just server` (both use port 8000).
booth-server:
    DEMO_SESSION_MANAGER_ID=booth-session-manager DEMO_STREAM_MODE=${BOOTH_STREAM_MODE:-minimal} uv run python scripts/harness_cli.py serve agents.booth.toml --host 127.0.0.1 --port 8000

# Stop the booth sandboxes.
booth-down:
    docker compose --profile booth down

# One command for the whole booth: a Ghostty window with a titled tab per process (Temporal,
# session manager, sandboxes, booth UI, ticker), started in the right order, then the browser
# pages. `just booth-launch dry` opens the tabs but only echoes the commands.
booth-launch mode="":
    osascript scripts/booth-ghostty.applescript "{{justfile_directory()}}" {{mode}}

# Stop everything booth-launch started (the tabs stay open at a prompt) and the booth sandboxes.
booth-stop:
    -pkill -f "scripts/ticker.py"
    -pkill -f "harness_cli.py"
    -pkill -f "temporal server start-dev --ip 0.0.0.0"
    docker compose --profile booth down

# Helpers for booth-launch: wait for Temporal, for the sandboxes, and open the pages once the UI is up.
_wait-temporal:
    @until nc -z 127.0.0.1 7233 2>/dev/null; do sleep 1; done

_wait-sandboxes:
    @until [ "$(docker ps --format '{{{{.Names}}' | grep -c '^sandbox-booth-')" -ge 3 ]; do sleep 2; done

_open-booth-pages-when-up:
    @until nc -z 127.0.0.1 8000 2>/dev/null; do sleep 1; done; sleep 2
    @open "http://localhost:8000"; open "http://localhost:8233"; open "https://app.box.com/folder/${BOOTH_BOX_FOLDER_ID:-427527786148}"
