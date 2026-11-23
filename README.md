# Box Relay: Box Mount + Temporal Agent Kit

Two AI agents review a supplier contract, each in its own Linux sandbox with its own Box folder.
The **Extractor** turns the contract PDF into `extraction.json`; the **Contract reviewer** checks it
against Legal's playbook and writes a memo. They never talk to each other: files cross between
them only through Box, synced into each sandbox by **Box Mount**. A **coordinator** (a plain
Temporal Workflow, no model of its own) runs them in order, pins every file version through the
Box API and checks between steps that nothing changed, runs fixed checks in code (ERP and
sanctions, Databricks spend), and asks a person to approve publishing. You drive it from the
Agent Kit UI; Temporal makes every wait, approval and crash durable.

Two terms used throughout: a **harness** is the loop that decides what an agent does next. The
Extractor and Reviewer are **inner harnesses** (a model loop with tools, each in its own
sandbox). The coordinator is the **outer harness**: plain code that starts the inner ones and
runs the case. All three are Temporal Workflows built with the
[Temporal Agent Kit](https://pypi.org/project/temporal-agent-kit/).

```
                          Temporal (localhost:7233)  ◄──  Agent Kit UI (localhost:8000)
          ┌──────────────────────────┼──────────────────────────┐
  sandbox-coordinator         sandbox-extractor           sandbox-reviewer
  worker, no Box Mount        worker + Box Mount          worker + Box Mount
          │ Box API                  │ Extraction/               │ Review/
          └──────────────────────────┴───── Box (DEMO_BOX_FOLDER_ID) ┘
```

| Box folder | Mounted by | Holds |
|---|---|---|
| `Extraction/` | the Extractor (`sandbox-extractor`) | `Incoming/` contract PDF, `Work/…/extraction.json` |
| `Review/` | the Reviewer (`sandbox-reviewer`) | `Playbook/playbook.md`, `Work/…/` the handed-off extraction, memo and findings |
| `Reviewed/` | nobody | approved memos, copied there by the coordinator |

| Harness | Task Queue | Runs in | Tools |
|---|---|---|---|
| Coordinator (outer, scripted) | `box-coordinator` | `sandbox-coordinator` | Box API checks and copies, `erp_find_vendor_by_name`, `screen_vendor_against_sanctions_lists`, Databricks, `continue_with_changed_file`*, `publish_memo_to_reviewed_folder`* |
| Extractor (inner, model) | `box-extractor` | `sandbox-extractor` | `read_pdf_text_from_box`, `run_python_on_box_folder`, `check_sandbox_and_box_mount` |
| Contract reviewer (inner, model) | `box-reviewer` | `sandbox-reviewer` | `read_handed_off_file_from_box`, `get_approved_clause_language`, `score_contract_risk`, `run_python_on_box_folder`, `check_sandbox_and_box_mount` |

\* waits for a person in the UI. The ERP, sanctions lists and clause library are simulated
(`demo/fixtures.py`); the document tools really open the PDF in the Extractor's mount. Every
tool's Python function has the same name as the tool, so a name you see in the UI or in
Temporal's event history is what to search the code for.

## How the pieces work

- **Each sandbox sees only what its job needs.** Box Mount syncs one Box folder per sandbox, so
  the PDF never reaches the Reviewer. The coordinator mounts nothing: it reads file IDs,
  versions and SHA-1s through the Box API (`demo/box_api.py`) and never holds file content.
- **Chain of custody** (`demo/custody_activities.py`, `demo/agents/coordinator_workflow.py`). The
  coordinator pins the contract and playbook versions, waits until each agent's output has reached
  Box with exactly the content the agent wrote, and hands `extraction.json` to the Reviewer by
  copying that exact version into `Review/` on the Box side. After every step it asks Box for every
  pinned file's current version. If one changed (someone edited the contract mid-extraction, say),
  it stops and asks a person on a card: approve to continue with the new version (the coordinator
  redoes the steps that depend on it), reject to stop. Approving publication copies the exact memo
  version the person saw. The supplier check and the Databricks write-back need a few facts from
  the agents' files; fixed-code activities read those in the sandbox that holds the file, on its
  Task Queue, and only the facts cross to the coordinator.
- **`BoxMountFS`** (`demo/boxfs.py`) is an agent's `/box`: an Agent Kit `ActivityFileSystem` over
  `/mnt/box`. Every `open()` in an agent's Code Mode script becomes an activity
  (`vfs.box-mount.read`, `.write`, ...) on that agent's Task Queue, so in its sandbox. Each agent
  may write only its own output files, and writes pause while Box Mount isn't syncing.
- **Handoffs by fingerprint** (`demo/handoff_activities.py`). An agent's first step,
  `wait_for_file_to_sync_from_box`, waits until the exact version it was handed (by SHA-1, the hash
  Box reports) has synced into *its* sandbox, retrying durably.
- **Credentials.** In the agents' sandboxes the box-mount daemon runs as `boxsvc` and the worker as
  `agent`, which never sees `BOX_*` variables or the JWT key (`check_sandbox_and_box_mount` reports
  this). The coordinator's worker holds the key for the Box API; no model-written code runs there.
  Keys are copied into private Docker volumes by `just up`, never bind-mounted: Docker Desktop's
  macOS file sharing ignores Linux permissions. Databricks credentials (`.env.databricks`) go only
  to the coordinator's sandbox.
- **One sandbox per agent.** Every activity of an agent, including each Box file operation, runs on
  its Task Queue. A second sandbox polling the same queue would split one agent's writes across two
  Box Mounts, which produced a conflict-copy loop. So the crash moment kills and restarts the
  worker inside its own sandbox rather than failing over.

## Code tour

Read in this order; each file opens with a docstring that explains its part.

| File | What it is |
|---|---|
| [`demo/agents/coordinator_workflow.py`](demo/agents/coordinator_workflow.py) | **Start here.** The outer harness: a Temporal Workflow with no model. The steps of a run, the version checks between them, the supplier check, the approval, and questions. |
| [`demo/agents/specialist_workflows.py`](demo/agents/specialist_workflows.py) | The two inner harnesses (Extractor, Contract reviewer): their prompts, tools and the files each may write. |
| [`demo/agents/common.py`](demo/agents/common.py) | One model turn with the OpenAI Agents SDK, and the deterministic loop around an agent's turn (wait for inputs, prefetch, run, check outputs, fingerprint). |
| [`demo/custody_activities.py`](demo/custody_activities.py), [`demo/box_api.py`](demo/box_api.py) | The coordinator's Box API activities (pin, wait for upload, copy an exact version, wait for an edit, publish) and the activities that read facts inside an agent's sandbox. |
| [`demo/handoff_activities.py`](demo/handoff_activities.py) | How an agent waits, durably, for an exact file version to sync into its own Box Mount. |
| [`demo/boxfs.py`](demo/boxfs.py) | `/box` for an agent's Code Mode scripts: every file operation is an activity in its sandbox, limited to the files it may write. |
| [`demo/activities.py`](demo/activities.py) | The agents' tools (read a PDF, the clause library, risk score) and the simulated ERP and sanctions services. |
| [`demo/databricks_activities.py`](demo/databricks_activities.py), [`demo/databricks_sql.py`](demo/databricks_sql.py) | The spend query and the findings write-back: submit, then poll with a heartbeat, so a retry resumes the same statement. |
| [`demo/worker.py`](demo/worker.py) | One worker per sandbox: which workflow it hosts, the Agent Kit and OpenAI plugins, and the activities it registers. |
| [`demo/agents/idle.py`](demo/agents/idle.py) | A session closes itself after 10 minutes without messages, so old ones don't pile up in the Temporal UI. |
| [`demo/models.py`](demo/models.py), [`demo/config.py`](demo/config.py), [`demo/fixtures.py`](demo/fixtures.py) | Shared data types, Task Queue names, and the demo's contract, playbook and simulated records. |
| [`docker/entrypoint.sh`](docker/entrypoint.sh), [`docker-compose.yml`](docker-compose.yml) | The sandboxes: Box Mount as its own user, a supervisor that remounts a stuck daemon, the worker without Box credentials. |
| [`scripts/`](scripts/) | `box_folders.py` (set up and reset the Box folders), `databricks_admin.py`, `ticker.py`, `crash.py`, `make_seed.py`, `agent_kit_cli.py` (readable session IDs), and the Ghostty launch scripts. |

## Run it

Requirements:

- **A Mac with Apple Silicon.** The sandboxes are `linux/arm64` containers, and `just launch` /
  `just stop` drive Ghostty (1.3 or later) with AppleScript.
- Docker Desktop, [`uv`](https://docs.astral.sh/uv/), [`just`](https://just.systems/) and the
  [`temporal` CLI](https://docs.temporal.io/cli) (its dev server is the Temporal Service here).
- **Box Mount, which you provide.** It's a Box preview and isn't in this repository: ask Box for
  access, then put the Linux arm64 `box-mount` binary at `private/box-mount-0.8.0/box-mount`
  (see [`private/README.md`](private/README.md)). It's git-ignored, and `just up` stops with a
  message if it's missing.
- **A Box JWT app** whose service account can edit a **new, empty Box folder**: its config at
  `secrets/box-jwt-config.json`, the folder's ID as `DEMO_BOX_FOLDER_ID` in `.env` (copy
  `.env.example`). `just setup` creates `Extraction/`, `Review/` and `Reviewed/` in it and moves
  anything else at its top level to the Box trash, so don't point it at a folder in use.
- An OpenAI API key with access to `gpt-5.1` in `.env.local` (copy `.env.local.example`).
  `DEMO_MODEL` and `DEMO_REASONING_EFFORT` (default `low`) change the model and its effort.
- Optional: a Databricks workspace (see below).

```bash
just setup     # first time only: Extraction/ and Review/ in Box, seed files, folder IDs into .env
just launch    # Temporal, session manager, sandboxes, UI, ticker, crash tab; then the browser pages
just stop      # ends every workflow, stops it all, closes the window
```

`just launch` opens a Ghostty window with a titled tab per process, starts them in order, and
opens the Agent Kit UI, the Temporal UI, the Box folder and, last, the architecture diagram
(`docs/architecture.html`). `just launch dry` only echoes. By hand, the same thing is
`just temporal`, `just session-manager`, `just server`, `just up` and `just ticker`, each in its
own terminal. `just status` shows each sandbox's Box Mount and worker, and `just` on its own
lists every recipe, grouped: setup, run, demo, inspect, between runs.

Only one Temporal dev server can listen on 7233: stop any other local dev server first, or the UI
and the sandboxes can end up talking to different servers.

**Databricks (optional).** Copy `.env.databricks.example` to `.env.databricks` and fill in the
workspace host, a token (or service principal) and a SQL warehouse ID, then run
`just databricks seed` once (creates `ap_invoices` with synthetic history and `review_findings`)
and `just up`. Without the file, the coordinator skips both Databricks steps. Newer workspaces
name the default catalog after the workspace (e.g. `box_relay_demo`); set `DATABRICKS_CATALOG` to
match. A service principal needs, as an admin grants in SQL:

```sql
CREATE SCHEMA IF NOT EXISTS box_relay_demo.box_relay;
GRANT USE CATALOG ON CATALOG box_relay_demo TO `<service principal application ID>`;
GRANT USE SCHEMA, CREATE TABLE, SELECT, MODIFY ON SCHEMA box_relay_demo.box_relay TO `<service principal application ID>`;
```

## The demo (~90 s, plus add-ons)

Screen: Agent Kit UI on the left, the Box web app on the right, `just ticker` along the bottom; the
Temporal UI (http://localhost:8233) in another tab.

1. **Run**: New session, choose **Box Relay**, type `go`. The ticker shows
   `↑ sandbox-extractor uploaded Extraction/…/extraction.json`, then
   `⇢ sandbox-coordinator COPY … -> Review/…/extraction.json` (the Box API copy), then
   `↓ sandbox-reviewer downloaded Review/… +5 s via Box`, then the memo going up. The Agent Kit
   UI's state pane lists every pinned version and whether its last check passed.
   **Supplier check (code, not a model):** after extraction the coordinator looks the supplier up
   in the ERP vendor master and screens it against sanctions lists; a supplier that is missing,
   inactive or flagged stops the run. With `just flaky on`, the first two ERP attempts time out
   and the Temporal UI shows the retries.
   **Spend profile (Databricks):** next it reads the supplier's last 12 months of accounts payable
   from `ap_invoices` and works out what the contract's Net 15 means against the Net 45 on file
   (about $200K more working capital tied up). The query is submitted, then polled with a
   heartbeat carrying its statement ID, so a stopped SQL warehouse is just a longer wait
   (`just databricks stop-warehouse` before a run shows it) and a crash resumes the same query.
   **Human in the loop:** when the review is done the chat links the memo in Box, and an approval
   card for `publish_memo_to_reviewed_folder` lists every version checked. **Approve** copies that
   exact memo version to `Reviewed/` (an edit made after the card appeared isn't published) and
   merges the findings into the `review_findings` Delta table (`just databricks findings` lists
   them). **Reject** leaves it in the Reviewer's `Work/`.
2. **A file changes mid-run** (~10 s): while the Extractor works, save a new version of the
   contract (`Extraction/Incoming/`) or playbook (`Review/Playbook/`) in Box. After the step the
   coordinator stops with a card naming the file, both versions and who changed it.
3. **Add-on A, a person edits in Box** (~40 s): after a run, type `re-review`. The coordinator waits
   (the Temporal UI shows `wait_for_edit_in_box` retrying). Edit `Review/Playbook/playbook.md` in the
   Box web app, e.g. change the payment-terms row to
   `Net 45; Net 15 is approved for Halvorsen | Shorter than Net 15`. The coordinator sees the new
   version and pins it, the ticker shows it syncing into the Reviewer's sandbox, and the Reviewer
   writes `review_memo_v2.md`, which marks payment terms OK.
4. **Add-on B, crash and resume** (+6 s): during a review, run `just crash reviewer`. It waits for
   the Reviewer's model call, kills the worker with `kill -9`, the sandbox restarts it, and about
   6 s later the same call resumes as attempt 2 (new worker PID in the Temporal UI). The memo still
   lands once.
5. **Ask anything**: any message that isn't `go` or `re-review` is a question, answered by a model
   that reads each file in the sandbox that holds it.

Each `go` writes a new `Work/HALVORSEN-MSA/<run>/` folder in each agent's folder, so runs never
collide and nothing is deleted. **Between runs**, `just playbook-reset` (a few seconds) puts the
original playbook back after add-on A; Box keeps the edited one in its version history.
**When the folders get long**, `just reset` (~1 min) clears old runs and published memos and
restores the contract and playbook. `just box` lists what is in Box, with every version, and `just files reviewer` what has synced
into the Reviewer's sandbox.

## In the Temporal UI

Sessions are named after the agent and start time (`box-relay-1120-143012-a3f9`, set by
`scripts/agent_kit_cli.py`), subagents after their role (`reviewer-subagent-…`). Each agent workflow
keeps a *Current Details* line up to date (e.g. "Box Relay · paused: playbook changed in Box;
waiting for a person"). Don't terminate the `session-manager` workflow: that ends every session,
and only restarting `just server` creates a new one. The session manager keeps the agent list it
started with, so after editing `agents.toml`, run `just stop` and `just launch` again.

The agents use the Agent Kit's streaming as it ships: model text, thoughts and tool calls reach the
UI as they happen. Each flush is a Signal into the agent's Workflow and each UI poll is an Update,
so an agent's history shows its whole conversation with the UI, not just its activities. Tools are
named for what they do (`wait_for_file_to_sync_from_box`, `get_box_file_version`,
`copy_box_file_version`, `continue_with_changed_file`, ...); file operations appear as
`vfs.box-mount.read` / `.write` and model calls as `invoke_model_activity_streaming`.

## Box Mount behaviour this demo works around

Found on Box Mount 0.6.0. The workarounds stay in place on 0.8.0, which the demo now ships with;
not every issue has been re-checked there.

1. **Inode reuse after a remote delete.** When a folder is deleted in Box, other sandboxes remove
   it locally but keep the mapping from its inode to the trashed Box id
   (`delete_local_item ... Unable to resolve local item`). A folder created later that reuses
   the inode is treated as the trashed folder: creating children fails with `Not Found`, and files
   are uploaded to the **top of the mounted Box folder** instead. A local update to a file whose
   Box copy was trashed retries `Not Found` forever. *Workaround:* nothing in the mount is deleted
   during a run, each run writes a fresh `Work/<case>/<run>/` folder, and `just reset` wipes the
   sandboxes' local state before deleting old outputs in Box.
2. **Restart in a new container.** `mount` refuses an existing mount record whose PID is dead (and
   often reused in the new container). Plain `unmount` is a reset: it deletes the sync database
   and re-downloads. The entrypoint does unmount + mount (about a minute on 0.8.0, whose mount
   and unmount each run a full sync).
3. **Occasional missed change notification.** A remote change can sit unsynced until the next
   event in the folder. `wait_for_file_to_sync_from_box` writes a small pulse file under `_sync/` from its second
   check on, which makes the daemon re-list remote changes.
4. **Conflict-copy loop.** Two sandboxes wrote the same new file within milliseconds (an empty
   write, then the content). One daemon created `review_memo.conflict.local.<timestamp>.md` and then
   kept creating and uploading a new conflict copy every 2-3 s (78 in 4 minutes) until it was
   stopped. *Workaround:* one writer sandbox per agent.
5. Downloaded files are made group read-only, so `BoxMountFS.write` falls back to
   write-then-rename for a file synced in from another sandbox (Box records a new file, not a
   version).
6. The OAuth access token is logged in plain text at INFO by the bundled SDK.

## Agent Kit notes (`temporal-agent-kit` 0.12.1)

- `openai`, `openai-agents` and `pydantic` are pinned to the Agent Kit's own lockfile; newer
  `openai` breaks its streaming observer.
- Code Mode scripts can import only `asyncio` and `pathlib`: JSON goes through the `parse_json`
  and `to_json` host functions.
- No `from __future__ import annotations` in agent or tool modules; `Injected[...]` parameters go
  last and keyword-only (the OpenAI adapter passes model arguments positionally); build
  `WorkflowStream()` in `@agent.init` itself.
