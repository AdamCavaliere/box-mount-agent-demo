# Box Relay: Box Mount + Temporal Agent Kit. Run `just` to list the recipes.
#
# First time:  just setup, then just databricks seed (optional)
# Every time:  just launch ... just stop

set dotenv-path := ".env.local"
set dotenv-load := true
export PATH := env_var("HOME") + "/.docker/bin:" + env_var("PATH")
# Host processes and the sandboxes all use the demo's Temporal dev server on the default port.
# Only one dev server may listen on 7233: stop any other before `just temporal`.
export TEMPORAL_ADDRESS := "127.0.0.1:7233"

sandboxes := "coordinator extractor reviewer"

[private]
default:
    @just --list --unsorted

# --- setup (once) -----------------------------------------------------------------------------

# Gives each agent its own Box folder inside DEMO_BOX_FOLDER_ID (Extraction/ for the Extractor,
# Review/ for the Reviewer), puts the contract and playbook in them, and writes the two folder IDs
# to .env. Anything else at the top of that folder goes to the Box trash.
[group("setup")]
[doc("Create the agents' Box folders and the seed files (first time only)")]
setup:
    uv run -q --group seed python scripts/make_seed.py
    docker compose rm -sf extractor reviewer
    uv run python scripts/box_folders.py setup

# Settings in .env.databricks; without them the coordinator skips its Databricks steps.
# stop-warehouse makes the next run's spend query wait through a cold start.
[group("setup")]
[doc("Databricks tables: check | seed | findings | stop-warehouse")]
databricks command="check":
    uv run python scripts/databricks_admin.py {{command}}

# --- run --------------------------------------------------------------------------------------

# A Ghostty window with a titled tab per process (Temporal, session manager, sandboxes, UI, ticker,
# crash), started in order, then the browser pages. `just launch dry` only echoes the commands.
[group("run")]
[doc("Start everything in one Ghostty window and open the browser pages")]
launch mode="":
    @osascript scripts/ghostty-launch.applescript "{{justfile_directory()}}" {{mode}} > .launch-window
    @echo "opened the demo window (Ghostty window $(cat .launch-window))"

# Terminates every running workflow first, while Temporal is up. The window closes a second after
# this recipe exits, so it also works when run from one of those tabs.
[group("run")]
[doc("Stop everything, end every workflow, close the Ghostty window")]
stop:
    -just _end-workflows
    -pkill -f "scripts/ticker.py"
    -pkill -f "agent_kit_cli.py"
    -pkill -f "temporal server start-dev --ip 0.0.0.0"
    docker compose down
    -@id=$(cat .launch-window 2>/dev/null); rm -f .launch-window; nohup sh -c "sleep 1; osascript scripts/ghostty-close.applescript '$id'" >/dev/null 2>&1 &

[group("run")]
[doc("Temporal dev server on :7233 (Web UI http://localhost:8233)")]
temporal:
    mkdir -p .temporal && temporal server start-dev --ip 0.0.0.0 --db-filename .temporal/dev.db

[group("run")]
[doc("The Agent Kit's session-manager worker")]
session-manager:
    uv run python scripts/agent_kit_cli.py session-manager

[group("run")]
[doc("Agent Kit UI on http://localhost:8000")]
server:
    uv run python scripts/agent_kit_cli.py serve agents.toml --host 127.0.0.1 --port 8000

# The Box key is copied into each sandbox's private volume, not bind-mounted (Docker Desktop's
# macOS file sharing ignores Linux permissions, so the agent user could read a bind-mounted key).
[group("run")]
[doc("Build the image and start the three sandboxes")]
up:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ ! -x private/box-mount-0.8.0/box-mount ]; then
      echo "Box Mount isn't here: put the Linux arm64 box-mount binary at private/box-mount-0.8.0/box-mount"
      echo "(Box Mount is a Box preview; ask Box for it. See private/README.md.)"
      exit 1
    fi
    mkdir -p /tmp/temporal-large-payloads
    docker compose up --no-start --build
    for s in {{sandboxes}}; do
      # Only into a sandbox that hasn't started: docker cp makes the key yours (mode 600), and a
      # running Box Mount daemon that reloads it then can't read it. The entrypoint fixes it at start.
      [ "$(docker inspect -f '{{{{.State.Running}}' sandbox-$s)" = true ] && continue
      docker cp -q secrets/box-jwt-config.json sandbox-$s:/home/boxsvc/.box-mount/box-jwt-config.json
    done
    docker compose up -d

[group("run")]
[doc("Stop the sandboxes (their Box Mount state is kept)")]
down:
    docker compose down

[group("run")]
[doc("Show every file crossing between sandboxes through Box, with timing")]
ticker:
    python3 scripts/ticker.py sandbox-extractor sandbox-reviewer sandbox-coordinator

# --- demo moments -----------------------------------------------------------------------------

[group("demo")]
[doc("Make the simulated ERP time out twice per lookup: on | off")]
flaky state="on":
    @if [ "{{state}}" = on ]; then docker exec sandbox-coordinator touch /run/demo/flaky; else docker exec sandbox-coordinator rm -f /run/demo/flaky; fi
    @echo "ERP flakiness {{state}}"

# Waits until the agent has a model call in flight, then kill -9s its worker. The sandbox restarts
# the worker in about a second and Temporal resumes the same step there.
[group("demo")]
[doc("Kill an agent's worker mid model call, and watch it resume: extractor | reviewer")]
crash agent="reviewer":
    uv run -q python scripts/crash.py {{agent}}

[group("demo")]
[doc("Kill a sandbox's worker and keep it down (its steps wait in Temporal)")]
kill sandbox="reviewer":
    docker exec sandbox-{{sandbox}} sh -c 'touch /run/demo/hold; pkill -9 -f "[d]emo.worker"; echo "killed worker in $(hostname)"'

[group("demo")]
[doc("Let a killed worker restart and pick up where it was")]
revive sandbox="reviewer":
    docker exec sandbox-{{sandbox}} rm -f /run/demo/hold
    @echo "worker in sandbox-{{sandbox}} restarting"

# The Crash tab that `just launch` opens prints this.
[private]
crash-help:
    @echo "Crash moment (add-on B). Type go in the Agent Kit UI, then within ~20 s:"
    @echo ""
    @echo "  just crash reviewer     kill -9 mid model call; restarts and resumes as attempt 2"
    @echo "  just crash extractor    same, for the Extractor (run right after go)"
    @echo "  just kill reviewer      kill and keep it down: the step waits in Temporal"
    @echo "  just revive reviewer    let it come back and pick up where it was"
    @echo "  just status             worker and Box Mount state per sandbox"
    @echo ""

# --- look inside ------------------------------------------------------------------------------

[group("inspect")]
[doc("Box Mount and worker state in each sandbox")]
status:
    #!/usr/bin/env bash
    for s in {{sandboxes}}; do
      c=sandbox-$s
      bm=$(docker exec $c cat /run/boxmount/status 2>/dev/null | sed 's/.*| status=/box-mount=/')
      w=$(docker exec $c pgrep -f demo.worker >/dev/null 2>&1 && echo up || echo DOWN)
      hold=$(docker exec $c test -e /run/demo/hold 2>/dev/null && echo " (held)" || true)
      printf "%-20s %-26s worker=%s%s\n" "$c" "${bm:-unreachable}" "$w" "$hold"
    done

[group("inspect")]
[doc("Follow a sandbox's log: supervisor, Box Mount sync and worker")]
logs sandbox="coordinator":
    docker logs -f --tail 30 sandbox-{{sandbox}}

[group("inspect")]
[doc("An agent's Box folder as synced into its sandbox: extractor | reviewer")]
files agent="reviewer":
    docker exec sandbox-{{agent}} tree -a -I _sync /mnt/box

[group("inspect")]
[doc("What is in Box now, with each file's version and who changed it")]
box:
    uv run python scripts/box_folders.py tree

# --- between runs -----------------------------------------------------------------------------

# Writes a new version, so Box keeps the edited one in its version history.
[group("between runs")]
[doc("Put the original playbook back after editing it in Box (seconds)")]
playbook-reset:
    uv run -q --group seed python scripts/make_seed.py >/dev/null
    uv run python scripts/box_folders.py playbook-reset
    just _verify-playbook

# Ends open sessions, wipes the sandboxes' local copies, then clears runs and published memos in
# Box and restores the contract and playbook, and starts the sandboxes fresh.
[group("between runs")]
[doc("Clean slate: clear old runs and memos, restore the seed files (~1 min)")]
reset:
    #!/usr/bin/env bash
    set -euo pipefail
    just _end-sessions
    just _wipe
    uv run -q --group seed python scripts/make_seed.py >/dev/null
    uv run python scripts/box_folders.py reset
    just up
    echo "clean slate: run 'just status'"

[group("between runs")]
[doc("Restart the workers after editing demo/*.py (Box Mount keeps running)")]
reload:
    for s in {{sandboxes}}; do docker exec sandbox-$s pkill -f "[d]emo.worker" || true; done

# --- helpers ----------------------------------------------------------------------------------

# End every open session and agent by its Task Queues. The session-manager workflow is on its own
# queue and keeps running. A session left open across a code change can't continue anyway (its
# history no longer matches the code), so resets start clean.
_end-sessions:
    #!/usr/bin/env bash
    if ! nc -z 127.0.0.1 7233 2>/dev/null; then echo "Temporal not running: no sessions to end"; exit 0; fi
    q="ExecutionStatus='Running' AND TaskQueue IN ('box-coordinator','box-extractor','box-reviewer')"
    n=$(temporal workflow count --query "$q" -o json | python3 -c "import json,sys; print(json.load(sys.stdin).get('count', 0))")
    if [ "${n:-0}" = 0 ]; then echo "no open sessions"; exit 0; fi
    echo "ending $n open session(s)/agent(s):"
    temporal workflow list --query "$q" -o json | python3 -c "import json,sys; [print('  ', w['execution']['workflowId']) for w in json.load(sys.stdin)]"
    temporal workflow terminate --query "$q" --reason "just reset" --yes >/dev/null
    for i in $(seq 1 15); do
      [ "$(temporal workflow count --query "$q" -o json | python3 -c "import json,sys; print(json.load(sys.stdin).get('count', 0))")" = 0 ] && break
      sleep 1
    done

# Remove the sandboxes and their volumes (local copies and Box Mount state only; Box is untouched).
_wipe:
    #!/usr/bin/env bash
    project=$(basename "$PWD")
    docker compose rm -sf {{sandboxes}} >/dev/null 2>&1 || true
    for v in coordinator-state extractor-state extractor-box reviewer-state reviewer-box; do
      docker volume rm -f ${project}_$v >/dev/null 2>&1 || true
    done

# Terminate every running workflow on the local Temporal dev server, and wait for them to close.
_end-workflows:
    #!/usr/bin/env bash
    if ! nc -z 127.0.0.1 7233 2>/dev/null; then echo "Temporal not running: no workflows to end"; exit 0; fi
    q="ExecutionStatus='Running'"
    count() { temporal workflow count --query "$q" -o json | python3 -c "import json,sys; print(json.load(sys.stdin).get('count', 0))"; }
    n=$(count)
    if [ "${n:-0}" = 0 ]; then echo "no running workflows"; exit 0; fi
    echo "terminating $n running workflow(s):"
    temporal workflow list --query "$q" -o json | python3 -c "import json,sys; [print('  ', w['execution']['workflowId']) for w in json.load(sys.stdin)]"
    temporal workflow terminate --query "$q" --reason "just stop" --yes >/dev/null
    for i in $(seq 1 15); do [ "$(count)" = 0 ] && exit 0; sleep 1; done
    echo "some workflows were still closing after 15 s: check the Temporal UI"

# Wait until the Reviewer's sandbox has the seed playbook from Box.
_verify-playbook:
    #!/usr/bin/env bash
    want=$(shasum -a 256 seed/Playbook/playbook.md | cut -d' ' -f1)
    for i in $(seq 1 30); do
      got=$(docker exec sandbox-reviewer sha256sum /mnt/box/Playbook/playbook.md 2>/dev/null | cut -d' ' -f1)
      [ "$got" = "$want" ] && { echo "playbook restored in Box and in the Reviewer's sandbox"; exit 0; }
      docker exec sandbox-reviewer touch /run/boxmount/requests/pulse 2>/dev/null
      sleep 3
    done
    echo "playbook not synced to the Reviewer's sandbox after 90 s; check 'just status'"; exit 1

# Helpers for launch: wait for Temporal, for the sandboxes, and open the pages once the UI is up.
_wait-temporal:
    @until nc -z 127.0.0.1 7233 2>/dev/null; do sleep 1; done

# Wait (up to 2 minutes) until a worker polls each queue, so the pages open only when a new
# session will be picked up straight away.
_wait-workers:
    #!/usr/bin/env bash
    pollers() { temporal task-queue describe --task-queue "$1" -o json 2>/dev/null | python3 -c "import json,sys; print(len(json.load(sys.stdin).get('pollers') or []))" 2>/dev/null || echo 0; }
    for i in $(seq 1 60); do
      ready=0
      for q in box-coordinator box-extractor box-reviewer; do [ "$(pollers $q)" -gt 0 ] && ready=$((ready + 1)); done
      [ "$ready" = 3 ] && { echo "workers ready"; exit 0; }
      sleep 2
    done
    echo "workers not all polling after 2 minutes: check 'just status'"

_wait-sandboxes:
    @until [ "$(docker ps --format '{{{{.Names}}' | grep -cE '^sandbox-(coordinator|extractor|reviewer)$')" -ge 3 ]; do sleep 2; done

_open-pages-when-up:
    @until nc -z 127.0.0.1 8000 2>/dev/null; do sleep 1; done; sleep 2
    @just _wait-workers
    @open "http://localhost:8000"; open "http://localhost:8233"; open "https://app.box.com/folder/$(sed -n 's/^DEMO_BOX_FOLDER_ID=//p' .env)"
    @sleep 1; open "{{justfile_directory()}}/docs/architecture.html"
