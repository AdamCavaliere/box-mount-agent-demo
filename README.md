# Box Mount + Temporal Agent Harness demo

Four AI agents process a vendor packet (an invoice plus a supplier contract). Each agent runs in
its own Linux sandbox. **The only thing the agents share is a Box folder, mounted into every
sandbox by Box Mount.** Temporal orchestrates them, durably, and you drive the whole thing from
the Agent Harness UI.

```
                     ┌──────────── Box folder (Incoming/ Playbook/ Work/ Reviewed/) ────────────┐
                     ▲               ▲                  ▲                ▲                 ▲
          sandbox-coordinator  sandbox-extractor  sandbox-compliance  sandbox-reviewer
          box-mount + worker   box-mount + worker box-mount + worker  box-mount + worker
                     └──────────────────── Temporal (localhost:7233) ─────────────────────────┘
                                                   ▲
                                 Agent Harness UI (localhost:8000)
```

| Agent | Task queue | Reads (via its own Box Mount) | Tools (run in its sandbox) | Writes |
|---|---|---|---|---|
| Coordinator (you chat with it) | `box-coordinator` | everything | `ask_extractor_agent`, `ask_compliance_agent`, `ask_contract_reviewer_agent`, `write_decision_to_box`, `record_accounts_payable_decision`*, `route_contract_to_legal_or_signature`* | `Reviewed/<case>/<run>/decision.md` |
| Extractor | `box-extractor` | the invoice and contract PDFs (writes every clause verbatim + key terms) | `classify_document_type`, `read_pdf_text_from_box`, `extract_invoice_fields_from_pdf` | `Work/<case>/<run>/extraction.json` |
| Compliance | `box-compliance` | extraction.json, playbook | `erp_lookup_vendor_record` (ERP), `erp_match_invoice_to_purchase_order`, `screen_vendor_against_sanctions_lists` | `compliance.json` |
| Contract reviewer | `box-reviewer` | extraction.json (clauses verbatim + key terms) and the playbook; never the PDF | `get_approved_clause_language`, `score_contract_risk` | `review_memo.md` |

\* needs a human to approve it in the UI. Enterprise systems are simulated (`demo/fixtures.py`);
the document tools really open the PDFs in that sandbox's mount.

## How the pieces work

- **`BoxMountFS`** (`demo/boxfs.py`) is the agents' `/box`: a harness `ActivityFileSystem` over
  `/mnt/box`. Every `open()` in an agent's Code Mode script becomes an activity
  (`vfs.box-mount.read`, `.write`, ...) that runs on that agent's task queue, so in its sandbox.
  Each agent may write only its own output file, and writes pause while Box Mount isn't syncing.
- **Handoffs by fingerprint** (`demo/handoff.py`). The coordinator passes only a `FileRef`
  (path, sha256, which sandbox wrote it). The next agent's first step, `wait_for_file_to_sync_from_box`, waits
  until that exact version has synced into *its* sandbox, retrying durably. Source PDFs and the
  playbook are handed off the same way.
- **Credentials.** In each sandbox the box-mount daemon runs as `boxsvc` and the agent worker
  as `agent`. The worker never sees `BOX_*` variables or the JWT key (`check_sandbox_and_box_mount` reports
  this). The key is copied into a private Docker volume by `just up`, never bind-mounted:
  Docker Desktop's macOS file sharing ignores Linux permissions.

**One worker sandbox per agent role.** Every activity of an agent, including each Box file
operation, runs on its role's task queue. A second sandbox polling the same queue would split one
agent's writes across two Box Mounts: a Code Mode save is two writes (create empty, then the
content), and when those landed in different sandboxes Box Mount produced conflict copies, then
looped, uploading a new one every 2-3 s. So the crash moment kills and restarts the worker inside
its own sandbox rather than failing over to a second sandbox.

## Run it

Prereqs: Docker Desktop, `uv`, `just`, the `temporal` CLI, the box-mount binary in
`private/box-mount-0.6.0/`, `secrets/box-jwt-config.json`, `.env` (Box folder id), `.env.local`
(`OPENAI_API_KEY`).

```bash
just temporal          # terminal 1: Temporal dev server on :7233 (Web UI http://localhost:8233)
just session-manager   # terminal 2
just server            # terminal 3: Harness UI on http://localhost:8000
just up                # build + start the four sandboxes (copies the Box key into each)
just seed              # first time only: put the vendor packet and playbook into Box
just status            # every sandbox: box-mount=running worker=up
```

Only one Temporal dev server can listen on 7233: stop any other local dev server before
`just temporal`, or the UI and the sandboxes can end up talking to different servers.

### Keeping the Temporal UI readable

`DEMO_STREAM_MODE` in `.env.local` (and `BOOTH_STREAM_MODE` for the booth, default `minimal`) sets how much of the Harness's live stream lands in workflow
history (`demo/stream_settings.py`). Measured on the Compliance agent's history for one case:

| Mode | Total events | Stream signals | Stream polls | UI feel |
|---|---|---|---|---|
| `live` (harness defaults) | 1,596 | 97 | 224 | token-by-token |
| `quiet` (default) | 428 | 18 | 36 | updates every 1-2 s |
| `off` (no model streaming) | 393 | 8 | 28 | each reply appears whole |
| `minimal` (booth default: no model streaming, 5 s polls) | booth reviewer: 320 (was 463 in `quiet`) | 14 | 14 | updates in 5 s steps |

Changing it needs `just up` / `just booth-up` and a restart of `just server` and `just session-manager`. Switch modes only between sessions: a session started in one mode can't be replayed by workers in another (`off` and the streaming modes schedule different model activities).

**Finding things in the Temporal UI.** Sessions are named after the demo and start time
(`contract-review-1015-143012-a3f9`, `vendor-intake-…`; set by `scripts/harness_cli.py`), subagents after
their role (`reviewer-subagent-…`). Each agent workflow keeps a *Current Details* line up to date
(e.g. "Contract reviewer · waiting for 3 handed-off file(s) to sync into this sandbox through Box").
Don't terminate the `session-manager` workflow: that ends every session under it, and only
restarting `just server` creates a new one.

**Tool names.** Tools are named for what they do, and those names are what the Harness UI and
Temporal's event history show: `wait_for_file_to_sync_from_box`, `record_file_version_for_handoff`,
`read_pdf_text_from_box`, `erp_lookup_vendor_record`, `publish_memo_to_reviewed_folder`,
`run_python_on_box_folder`, and so on. File operations appear as `vfs.box-mount.read` / `.write`,
and model calls as `invoke_model_activity_streaming` (both named by the harness).

## Run of show (~10 minutes)

1. **Set the scene** (Box web UI): `Incoming/` has the invoice and MSA, `Playbook/playbook.md`
   has Legal and AP's rules. Run `just status` to show four sandboxes, each with its own Box Mount.
2. **Start**: in the Harness UI, open a new *Vendor Packet Intake* session and send
   *"Process the new vendor packet in Incoming."*
3. **Execution inside the sandboxes**: the coordinator calls `check_sandbox_and_box_mount` (hostname, aarch64,
   box-mount status, no Box credentials visible). Each subagent appears on its own runtime card.
   Its input lists the files it was handed: checksums and the sandbox that wrote each one.
   `just logs extractor` shows Box Mount uploading `extraction.json`; `just logs compliance`
   shows it downloading.
4. **Box as the shared workspace**: switch to the Box web UI and watch
   `Work/INV-20417/<run>/` fill in as each agent writes.
5. **Parallel agents**: Compliance and Contract review start in the same instant, in different
   sandboxes.
6. **Durability, part 1 (flaky ERP)**: before step 2, `just flaky on`. The vendor-master lookup
   times out twice; Temporal retries it and the agent never notices.
7. **Durability, part 2 (crash and resume)**: while Compliance is working, run
   `just crash compliance`. It waits for the agent's model call to be in flight, then kills the
   worker with `kill -9`. The sandbox restarts it in about a second, and about 5 s later the Temporal
   UI shows the same model call resuming as attempt 2 on the new worker process. Nothing already
   written to Box is redone.
8. **Human in the loop**: the coordinator writes `decision.md` and pauses on two approvals (AP
   hold, route to legal). The workflow waits durably for as long as it takes. Approve both.
9. **People edit in Box** (optional): edit `Playbook/playbook.md` in the Box web UI (e.g. allow
   Nevada governing law), wait for `just logs coordinator` to show the download, then ask the
   coordinator to *"Re-review the contract; the playbook changed."* (not yet rehearsed)

The packet is designed to trip a bank-detail mismatch (payment-fraud signal), an 8.33% PO overage
and eight playbook violations in the MSA. Expected outcome: **AP hold, contract to legal review.**

Between rehearsals: `just reset` (about a minute; see below). Other recipes: `just tree <sandbox>`,
`just reload` (restart workers after editing `demo/*.py`), `just flaky off`.

**Where the agents work.** Each booth sandbox's Box Mount maps the `Booth/` folder to `/mnt/box`,
and the agents' scripts see it as `/box/…` (the full demo's sandboxes mount the parent demo folder
at the same path). So an agent never knows which Box folder it is in: it can't see or touch
anything outside `Booth/` (least privilege), the same agent code runs against any mounted folder
(the folder is a deployment setting, `BOOTH_BOX_FOLDER_ID`), and Box permissions on that folder
govern what it can do, with no Box credentials in the agent.

## Booth demo: Box Relay (~60 s, plus two optional add-ons)

A short version for a booth: an **Extractor** and a **Contract reviewer**, each in its own sandbox,
relay a contract review through Box. A scripted relay workflow (no model of its own) runs them in
order and passes only file fingerprints, so each agent waits for the exact file version to sync
into its own sandbox. It runs on its own stack: the `Booth/` subfolder of the demo folder
(`BOOTH_BOX_FOLDER_ID`), `booth-*` task queues, and three sandboxes (the relay, extractor and reviewer). Booth model calls use low reasoning effort (`BOOTH_REASONING_EFFORT`).

```bash
just temporal; just session-manager; just server   # as for the full demo
just booth-up          # three booth sandboxes
just booth-seed        # first time only: contract + playbook into Booth/
just ticker            # the on-screen strip of files crossing between sandboxes
```

**One command:** `just booth-launch` opens a Ghostty window with a titled tab per process (Temporal,
session manager, sandboxes, booth UI, ticker, crash), starts them in order, and opens the Harness UI,
Temporal UI, Box folder and, last, the architecture diagram (`docs/booth-architecture.html`) in the
browser; `just booth-stop` stops it all and closes the window. `just booth-launch dry` only echoes.

Screen: Harness UI on the left, the Box web app open at `Booth/` on the right, `just ticker` along
the bottom; the Temporal UI (http://localhost:8233) in another tab.

1. **Run** (~60 s, then your approval): New session, choose **Box Relay (booth)**, type `go`. The ticker shows
   `↑ sandbox-booth-extractor uploaded …/extraction.json`, then `↓ sandbox-booth-reviewer
   downloaded … +3 s via Box`, then the memo going back up. The files appear in the Box web app as
   they're written; the Harness UI shows each subagent with the fingerprints it was handed.
   **Human in the loop:** when the review is done the chat shows *Review ready for approval* with a
   clickable **Open review_memo.md in Box** link, and an approval card for `publish_memo_to_reviewed_folder`. Open the
   memo in Box, then **Approve**: the relay copies it to `Booth/Reviewed/…`, it appears in Box, and
   the chat links the published copy. **Reject** leaves it in `Work/`. While the card waits, the
   Temporal UI shows the relay paused on a person (its *Current Details* line says so, with the link).
2. **Add-on A, a person edits in Box** (~40 s): type `re-review` first. The relay waits (the
   Temporal UI shows `wait_for_edit_in_box` retrying: "waiting for an edit in Box to sync"). Edit
   `Booth/Playbook/playbook.md` in the Box web app, e.g. change the payment-terms row to
   `Net 45; Net 15 is approved for Halvorsen | Shorter than Net 15`. Within seconds the ticker shows
   the playbook syncing into every sandbox ("edited outside these sandboxes"), and the reviewer
   writes `review_memo_v2.md`, which marks payment terms OK.
3. **Add-on B, crash and resume** (+6 s): during a review, run `just crash booth-reviewer`. It
   waits for the reviewer's model call, kills the worker with `kill -9`, the sandbox restarts it,
   and about 6 s later the same call resumes as attempt 2 (new worker PID in the Temporal UI). The
   memo still lands once, with no duplicate writes.

Each `go` writes a new `Work/HALVORSEN-MSA/<run>/` folder, so runs never collide and nothing is
deleted. **Between visitors**, `just booth-playbook-reset` (a few seconds) puts the original
playbook back after add-on A; Box keeps the edited one in its version history. **Before an event**
(or when the folder gets long), `just booth-reset` (~1 min) clears old runs and published memos and
restores the original playbook.
After editing `agents.toml`, run `just registry-reload` and restart `just server`.

## Box Mount 0.6.0 behaviour this demo works around (report to Box)

1. **Inode reuse after a remote delete.** When a folder is deleted in Box, other sandboxes remove
   it locally but keep the mapping from its inode to the trashed Box id
   (`delete_local_item ... Unable to resolve local item`). A folder created later that reuses
   the inode is treated as the trashed folder: creating children fails with `Not Found`, and files
   are uploaded to the **top of the mounted Box folder** instead. A local update to a file whose
   Box copy was trashed retries `Not Found` forever. *Workaround:* nothing in the mount is deleted
   during a run, each run writes a fresh `Work/<case>/<run>/` folder, and `just reset` wipes every
   sandbox's local state before and after deleting old outputs.
2. **Restart in a new container.** `mount` refuses an existing mount record whose PID is dead (and
   often reused in the new container). Plain `unmount` is a reset: it deletes the sync database
   and re-downloads. The entrypoint does unmount + mount (~8 s).
3. **Occasional missed change notification.** A remote change can sit unsynced until the next
   event in the folder. `wait_for_file_to_sync_from_box` writes a small pulse file under `_sync/` from its second
   check on, which makes the daemon re-list remote changes.
5. **Conflict-copy loop.** Two sandboxes wrote the same new file within milliseconds (an empty
   write, then the content). One daemon created `review_memo.conflict.local.<timestamp>.md` and then
   kept creating and uploading a new conflict copy every 2-3 s (78 in 4 minutes) until it was
   stopped. *Workaround:* one writer sandbox per agent.
6. Downloaded files are made group read-only, so `BoxMountFS.write` falls back to
   write-then-rename for a file synced in from another sandbox (Box records a new file, not a
   version).
7. The OAuth access token is logged in plain text at INFO by the bundled SDK.

## Harness notes (commit `091c875`)

- The VFS (`ActivityFileSystem`) is on `main`, not in the 0.6.0 tag.
- `openai`, `openai-agents` and `pydantic` are pinned to the harness's own lockfile; newer
  `openai` breaks its streaming observer.
- Code Mode scripts can import only `asyncio` and `pathlib`: JSON goes through the `parse_json`
  and `to_json` host functions.
- No `from __future__ import annotations` in agent or tool modules; `Injected[...]` parameters go
  last and keyword-only (the OpenAI adapter passes model arguments positionally); build
  `WorkflowStream()` in `@agent.init` itself.
