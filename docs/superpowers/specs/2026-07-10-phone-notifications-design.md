# Phone Notifications for the `mbp` Box — Design

- **Date:** 2026-07-10
- **Status:** Approved design, pending spec review
- **Target:** the `mbp` Ubuntu box (always-on home server) running the skannonser pipeline

## Goal

Send push notifications to the owner's phone for:
1. **Server offline** — Wi-Fi loss or power outage.
2. **Daily listing churn** — how many listings were added / removed each day.
3. **Weekly summary** — listings added and sold over the past week.
4. **Battery / power** — low-battery warnings and power-lost/restored events.

## Non-goals (YAGNI)

- No web dashboard, no interactive controls, no historical charts (the `daily_metrics`
  table we add makes charts possible later, but they are out of scope now).
- No alerting logic for the offline case in our own code — Healthchecks.io owns that.
- No notification channels other than Pushover.

## Delivery channels

- **Pushover** (chosen): the box sends alerts via a single HTTPS POST to the Pushover API.
  Reliable, supports priority for critical alerts.
- **Healthchecks.io** (chosen): external dead-man's-switch for offline detection. The box
  cannot report its own outage, so it pings a Healthchecks URL every 10 minutes; when pings
  stop, Healthchecks alerts the phone (via its Pushover integration).

### Two delivery paths

- **Box → Pushover** (direct): daily summary, weekly summary, battery alerts. Sent while the
  box is alive and online.
- **Box → Healthchecks.io → Pushover** (heartbeat): offline detection. Covers Wi-Fi loss and
  full power outages, where the box can send nothing itself.

## Architecture

New Python package `main/notify/`, driven by cron. Consistent with the existing
Python + cron + Makefile codebase. Each unit has one purpose, a clear interface, and is
testable in isolation (real sqlite for DB logic; mocked HTTP for the sender).

### Components

1. **`main/notify/pushover.py`** — the single send choke-point.
   - `send(title: str, message: str, priority: int = 0) -> bool`
   - Reads `PUSHOVER_APP_TOKEN` / `PUSHOVER_USER_KEY` from config (below).
   - POSTs to `https://api.pushover.net/1/messages.json`. `priority`: 0 normal, 1 high
     (used for power-lost and ≤10% battery). Returns success; logs and returns False on error.
   - Depends on: `requests`, config. Nothing else depends on its internals.

2. **`main/notify/config.py`** — loads secrets from a gitignored file (below) or env vars.
   Exposes `PUSHOVER_APP_TOKEN`, `PUSHOVER_USER_KEY`, `HEALTHCHECKS_URL`.

3. **`main/notify/listing_metrics.py`** — the added/removed engine, decoupled from scrape
   internals via a **daily active-set diff**.
   - "Active tracked listings" = `eiendom.active = 1` AND passing the existing sheet filters
     (`SHEETS_MAX_PRICE`, `MIN_BRA_I`) — i.e. the same set the owner sees on the map/sheet.
     (If raw/unfiltered is preferred later, this is a one-line change.)
   - `current_active_set(db) -> set[str]`
   - `previous_active_set(db) -> set[str]` — reads `daily_listing_snapshot`.
   - `save_active_snapshot(db, active: set[str])` — replaces `daily_listing_snapshot`.
   - `compute_daily_metrics(current: set, previous: set, db) -> DailyMetrics`
     - `added = current - previous`
     - `removed = previous - current`; each removed finnkode classified **sold** (current
       `tilgjengelighet == 'Solgt'`) vs **delisted** (otherwise).
     - Returns counts + the finnkode sets.
   - `record_daily_metrics(db, date, metrics)` — inserts one row into `daily_metrics`.

4. **`main/notify/daily_summary.py`** — orchestrates the daily run:
   load previous set → compute metrics → send Pushover → record metrics → save new snapshot.
   - Message e.g.: `🏠 Today: +7 added, −5 removed (3 sold, 2 delisted). Active: 1125.`

5. **`main/notify/weekly_summary.py`** — Sunday rollup of the past 7 days:
   - `added` = sum of `daily_metrics.added` over the week.
   - `sold` = count of `eiendom_status_history` rows with `new_status = 'Solgt'` and
     `observed_at` in the week (authoritative sold signal from the table we already added).
   - Message e.g.: `📅 This week: +48 added, 19 sold.`

6. **`main/notify/battery.py`** — battery/power monitor.
   - `read_battery() -> BatteryState(percent: int, on_ac: bool, status: str)` — auto-detects
     the `BAT*` and AC-adapter entries under `/sys/class/power_supply/`.
   - `decide_alerts(current: BatteryState, prev: dict, thresholds=(50, 20, 10)) -> (alerts, new_state)`
     — **pure function**, the core testable logic (see state machine below).
   - `run()` — read battery → load prev state (JSON file) → decide → send alerts → save state.

7. **Heartbeat** — no code. A cron `curl -fsS -m 10 --retry 3 "$HEALTHCHECKS_URL"` every
   10 minutes. Healthchecks.io owns the missed-ping alerting and its Pushover integration.

## Battery state machine (anti-spam)

State persisted in a small JSON file (e.g. `~/skannonser-notify-state/battery.json`):
`{ "power": "ac" | "battery", "last_threshold_alerted": int | null }`.

On each 10-minute check:
- **AC → battery** (unplugged / power lost): send `⚡ Power lost — on battery (X%)`, **priority 1**.
  Set `power = battery`; set `last_threshold_alerted` to the **lowest threshold still above `X`**
  (this suppresses already-passed higher thresholds while still allowing lower ones to fire as the
  battery keeps draining — e.g. unplug at 15% won't re-fire 50/20, but will still fire 10%).
  First-ever run with no saved state seeds state the same way without sending alerts.
- **battery → AC** (restored): send `🔌 Power restored (X%)`, priority 0. Set `power = ac`,
  `last_threshold_alerted = null`.
- **On battery, crossing a new lower threshold:** for thresholds `50, 20, 10` descending, if
  `percent ≤ threshold` and this threshold is lower than `last_threshold_alerted` (not yet
  alerted), send `🔋 Battery low: X%` (priority 1 at ≤10%, else 0). Update
  `last_threshold_alerted = threshold`. Guarantees **one alert per crossing**, no repeats.
- **On AC:** no threshold alerts.

Note: in a *full* power outage the Wi-Fi router also dies, so these battery alerts may not get
out — that case is covered by the Healthchecks heartbeat instead. Battery alerts are most useful
when the box is unplugged but Wi-Fi is still up (e.g. tripped cord, failed charger).

## Data model (additive, in `properties.db`)

- `daily_listing_snapshot(finnkode TEXT PRIMARY KEY)` — the previous run's active set;
  fully replaced each daily run.
- `daily_metrics(metric_date TEXT PRIMARY KEY, added INT, removed_sold INT,
  removed_delisted INT, total_active INT)` — one row per day; source for the weekly rollup
  and any future charts.

Both created with `CREATE TABLE IF NOT EXISTS` in `PropertyDatabase._init_db` (same additive
migration pattern already used).

## Scheduling (cron on the box)

| Job | Schedule | Command (via venv) |
|---|---|---|
| daily summary + snapshot | `0 7 * * *` | `notify.daily_summary` |
| weekly summary | `0 8 * * 0` (Sunday 08:00) | `notify.weekly_summary` |
| battery check | `*/10 * * * *` | `notify.battery` |
| heartbeat ping | `*/10 * * * *` | `curl … "$HEALTHCHECKS_URL"` |

Invoked either directly (`.venv/bin/python -m main.notify.daily_summary`) or via thin `make`
targets (`notify-daily`, `notify-weekly`, `notify-battery`) for consistency with the repo.
All output logged under `~/skannonser-logs/`.

## Config / secrets

A gitignored `main/config/notify_secrets.py` (or `.env`) on the box holds
`PUSHOVER_APP_TOKEN`, `PUSHOVER_USER_KEY`, and `HEALTHCHECKS_URL`. Never committed — matches
how Google credentials are handled. `.gitignore` gets a `notify_secrets.py` entry.

## One-time external setup (owner)

- **Pushover:** create account → create an Application (gives `APP_TOKEN`); copy `USER_KEY`
  from the dashboard; install the Pushover app on the phone (~$5 one-time after trial).
- **Healthchecks.io:** create a check with period 10m / grace ~20m → copy its ping URL; add
  the Pushover integration so missed-ping alerts arrive on the same channel.
- Provide the three values; they go into `notify_secrets.py` on the box.

## Testing strategy (TDD, stdlib `unittest`)

- `listing_metrics`: real temp sqlite — added/removed math, sold vs delisted classification,
  snapshot save/replace, `daily_metrics` recording.
- `battery.decide_alerts`: pure-function unit tests — each transition and threshold crossing,
  anti-spam (no repeat within a level), reset on AC restore.
- summary formatting: exact message strings for representative inputs.
- `pushover.send`: mock `requests.post` — asserts payload/priority and success/failure handling.
- Battery `read_battery` parsing: feed sample `/sys/class/power_supply` contents from a temp dir.

## Open prerequisites

- Verify the box actually exposes a readable battery under `/sys/class/power_supply/BAT*`
  (checked during implementation; the 2016 MBP should).
- Owner completes the Pushover + Healthchecks setup and supplies the three secret values.
