# Standalone `notify` CLI — Extraction Design

- **Date:** 2026-07-11
- **Status:** Approved design, pending spec review
- **Context:** The notification code currently lives inside the skannonser repo (`main/notify/`). The `mbp` box will host other projects (including a Node/TS Spotify station), so the server-level pieces should become a shared, standalone tool.

## Goal

Extract the server-level notification pieces out of skannonser into a standalone, reusable **`notify` CLI** (its own git repo, deployed on the box), so any project — Python, Node/TS, bash, cron — can send a push with one command, sharing a single credential store. skannonser keeps only its listing-specific logic and delegates delivery to `notify`.

## Non-goals (YAGNI)

- No new notification features — this is a refactor/extraction; behavior is preserved.
- No channels beyond Pushover + the Healthchecks heartbeat.
- No packaging to PyPI / no pip install — it's a local box tool.
- No rewrite of skannonser's listing logic — only its *delivery* call changes.

## Split boundary

**Moves into the new `notify` repo (server-level):**
- Pushover sender, battery/power monitor (+ its history logging), the Healthchecks heartbeat, and the shared secrets (Pushover app token + user key, Healthchecks URL).

**Stays in skannonser (listing-specific):**
- `listing_metrics.py`, `daily_summary.py`, `weekly_summary.py`, and the `daily_listing_snapshot` / `daily_metrics` tables + their DB methods.

## Decisions (locked)

- **Interface:** a CLI command `notify` (language-agnostic; shelled out to).
- **Location:** its own git repo, deployed at `~/kode/notify` on the box (+ GitHub).
- **Dependencies:** stdlib-only — Pushover send and the heartbeat use `urllib` instead of `requests`, so the tool needs **no venv** and runs on system `python3`.
- **Name:** repo and command both `notify`.
- **State dir:** `~/notify-state/` (box-level), replacing `~/skannonser-notify-state/`.

## The new `notify` repo

Layout:
```
~/kode/notify/
  notify/
    __init__.py
    config.py        # loads secrets.py or env
    secrets.py       # gitignored: PUSHOVER_APP_TOKEN, PUSHOVER_USER_KEY, HEALTHCHECKS_URL
    pushover.py      # send(title, message, priority) via urllib
    battery.py       # read_battery, decide_alerts, run (+ history log), heartbeat helper
    cli.py           # argparse entrypoint
  bin/notify         # wrapper: PYTHONPATH=<repo> exec python3 -m notify.cli "$@"
  tests/
    test_pushover.py
    test_battery.py
    test_cli.py
  .gitignore         # notify/secrets.py
  README.md
```

### Components

1. **`notify/pushover.py`** — `send(title, message, priority=0, *, app_token=None, user_key=None, timeout=10) -> bool`. POSTs form-encoded data to `https://api.pushover.net/1/messages.json` via `urllib.request`. Reads creds from `config` when not passed. Returns success; logs + returns False on error. (Same contract as today's version, minus `requests`.)

2. **`notify/battery.py`** — moved verbatim in behavior: `BatteryState`, `read_battery`, `_seed_ceiling`, `decide_alerts`, history logging (`format_reading_line`, `_append_history`), state load/save, and `run(state_path, power_supply_dir, send, history_path)`. Plus a `heartbeat(url, timeout=10) -> bool` using `urllib` (replacing the old cron one-liner). Default state/history paths become `~/notify-state/battery.json` and `~/notify-state/battery-history.log`.

3. **`notify/config.py`** — loads `PUSHOVER_APP_TOKEN`, `PUSHOVER_USER_KEY`, `HEALTHCHECKS_URL` from `notify/secrets.py` (gitignored) or environment.

4. **`notify/cli.py`** — `argparse` with subcommands:
   - `notify send "<title>" "<message>" [--priority N]` → `pushover.send(...)`; exit 0/1.
   - `notify battery` → `battery.run(~/notify-state/battery.json, history_path=~/notify-state/battery-history.log)`; exit 0/1.
   - `notify heartbeat` → `battery.heartbeat(config.HEALTHCHECKS_URL)`; exit 0/1.

5. **`bin/notify`** — a small bash wrapper that runs `python3 -m notify.cli "$@"` with the repo on `PYTHONPATH`. Symlinked to `~/.local/bin/notify` so it's on PATH.

## Changes in skannonser

- **Delete:** `main/notify/pushover.py`, `main/notify/battery.py`, `main/notify/config.py`, `tests/test_notify_pushover.py`, `tests/test_battery.py`, and `main/config/notify_secrets.py`. Remove the now-moot `main/config/notify_secrets.py` line from `.gitignore`.
- **Keep:** `main/notify/listing_metrics.py`, `daily_summary.py`, `weekly_summary.py`, and the DB tables/methods.
- **Add** `main/notify/send.py`:
  ```python
  import os, subprocess
  def send(title, message, priority=0) -> bool:
      bin = os.environ.get("NOTIFY_BIN", "notify")
      try:
          return subprocess.run([bin, "send", title, message, "--priority", str(priority)],
                                timeout=15).returncode == 0
      except Exception:
          return False
  ```
- **Rewire:** `daily_summary.py` and `weekly_summary.py` change their default from `from main.notify import pushover` (`send=pushover.send`) to `from main.notify.send import send` (`send=send`). Their `run(send=…)` injection is unchanged, so existing tests keep passing by injecting a fake `send`.

## Cron migration on the box (crontab)

Add a `PATH` line so cron finds `~/.local/bin/notify`, then swap the two server-level jobs:
```
PATH=/home/mbp2016/.local/bin:/usr/bin:/bin
*/10 * * * * notify battery   >> /home/mbp2016/notify-state/notify.log 2>&1
*/10 * * * * notify heartbeat >/dev/null 2>&1
```
skannonser's daily (07:00) and weekly (Sun 08:00) jobs stay as `python -m main.notify.daily_summary` / `weekly_summary` (now delegating to `notify`). The old `python -m main.notify.battery` and python heartbeat lines are removed.

## Migration safety (cutover order)

1. Build + fully test the new `notify` repo; deploy to `~/kode/notify`; symlink `~/.local/bin/notify`.
2. Create `~/notify-state/`; write the real `secrets.py` (copy the three values from skannonser's `notify_secrets.py`).
3. Verify `notify send`, `notify battery`, `notify heartbeat` all work live (real push received; Healthchecks pinged).
4. **Only then**: swap the cron entries and merge the skannonser delegation change.
5. Remove the moved files + old state dir from skannonser last.

This keeps the live alerts working throughout — the old jobs stay until the new ones are proven.

## Testing

- New repo (stdlib `unittest`): `pushover.send` with mocked `urllib.request.urlopen`; `battery.decide_alerts` (the state machine, ported tests); `read_battery` parsing from a temp dir; `cli` argument dispatch (each subcommand calls the right function, with the sender injected/mocked).
- skannonser: `daily_summary` / `weekly_summary` tests stay green via fake `send` injection; delete the moved `test_pushover`/`test_battery`.

## Open items

- Confirm `~/.local/bin` is acceptable for the symlink (no sudo needed; on PATH via the crontab line and interactive shells).
- The new repo needs a GitHub remote (create `notify` repo under the same account); the box's existing GitHub deploy key already grants push (account-level auth key).
