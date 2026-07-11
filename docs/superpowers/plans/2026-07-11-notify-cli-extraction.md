# Notify CLI Extraction Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans or subagent-driven-development. Steps use checkbox (`- [ ]`) syntax.

**Goal:** Extract the server-level notification pieces from skannonser into a standalone, stdlib-only `notify` CLI (its own repo `~/kode/notify`, GitHub `git@github.com:tehBaer/notify.git`), and rewire skannonser to delegate delivery — without live alerts going dark during cutover.

**Architecture:** New `notify` git repo (already cloned on the Mac, branch `main`), pure stdlib Python (urllib for HTTP), CLI with `send`/`battery`/`heartbeat`, installed at `~/.local/bin/notify`. skannonser calls `notify send` via subprocess.

**Tech Stack:** Python 3.12 stdlib only (`urllib`, `argparse`, `unittest`), cron, bash wrapper.

## Global Constraints

- **Stdlib only** in the `notify` repo — no third-party imports; HTTP via `urllib.request`.
- Tests: stdlib `unittest`. New repo: `python3 -m unittest discover -s tests` from `~/kode/notify`. skannonser: `.venv/bin/python -m unittest ...`.
- Tests put repo root on `sys.path`: `REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))`.
- Secrets only in gitignored `notify/secrets.py` on the box.
- Names: repo + command `notify`; state dir `~/notify-state/`.
- **Cutover order:** prove new tool live (Task 4) BEFORE swapping cron/rewiring skannonser (5–6); delete moved files last.
- Commits end with: `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.

---

## File Structure

**`~/kode/notify` (already cloned; add these):** `notify/{__init__,config,secrets,pushover,battery,cli}.py`, `bin/notify`, `tests/test_{pushover,battery,cli}.py`, `.gitignore`, `README.md`.

**skannonser:** add `main/notify/send.py` + `tests/test_notify_send.py`; modify `main/notify/{daily_summary,weekly_summary}.py`; delete `main/notify/{pushover,battery,config}.py`, `tests/test_{notify_pushover,battery}.py`, `main/config/notify_secrets.py` (+ its `.gitignore` line).

---

### Task 1: notify repo — scaffold + config + Pushover sender

**Files:** Create `notify/__init__.py`, `notify/config.py`, `notify/pushover.py`, `.gitignore`, `README.md`; Test `tests/test_pushover.py`.

**Interfaces produced:** `pushover.send(title, message, priority=0, *, app_token=None, user_key=None, timeout=10) -> bool`; `config.PUSHOVER_APP_TOKEN`, `PUSHOVER_USER_KEY`, `HEALTHCHECKS_URL`.

- [ ] **Step 1: Scaffold inside the existing clone**

```bash
cd ~/kode/notify
mkdir -p notify tests bin
touch notify/__init__.py
printf 'notify/secrets.py\n__pycache__/\n*.pyc\n' > .gitignore
printf '# notify\n\nStandalone Pushover notifier + battery/power monitor for the mbp box.\nStdlib-only. CLI: `notify send "title" "msg"`, `notify battery`, `notify heartbeat`.\n' > README.md
```

- [ ] **Step 2: Write the failing test** — `tests/test_pushover.py`:

```python
import os, sys, unittest
from unittest import mock

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from notify import pushover


class PushoverTests(unittest.TestCase):
    def _urlopen_mock(self, status=200):
        m = mock.MagicMock()
        m.return_value.__enter__.return_value.status = status
        return m

    def test_send_success_posts_expected_payload(self):
        u = self._urlopen_mock(200)
        with mock.patch.object(pushover.urllib.request, "urlopen", u):
            ok = pushover.send("Title", "Body", priority=1, app_token="A", user_key="U")
        self.assertTrue(ok)
        args, kwargs = u.call_args
        self.assertEqual(args[0], "https://api.pushover.net/1/messages.json")
        body = kwargs["data"].decode()
        self.assertIn("token=A", body)
        self.assertIn("user=U", body)
        self.assertIn("priority=1", body)

    def test_send_returns_false_when_credentials_missing(self):
        u = self._urlopen_mock(200)
        with mock.patch.object(pushover.urllib.request, "urlopen", u):
            ok = pushover.send("T", "B", app_token="", user_key="")
        self.assertFalse(ok)
        u.assert_not_called()

    def test_send_returns_false_on_error(self):
        with mock.patch.object(pushover.urllib.request, "urlopen", side_effect=Exception("boom")):
            self.assertFalse(pushover.send("T", "B", app_token="A", user_key="U"))


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 3: Run — expect FAIL** — `cd ~/kode/notify && python3 -m unittest tests.test_pushover -v` → `ModuleNotFoundError: No module named 'notify.pushover'`.

- [ ] **Step 4: Implement** — `notify/config.py`:

```python
"""Loads notifier secrets from a gitignored module or environment variables."""
import os

try:
    from notify import secrets as _secrets
except Exception:
    _secrets = None


def _get(name, default=""):
    if _secrets is not None and hasattr(_secrets, name):
        return getattr(_secrets, name)
    return os.environ.get(name, default)


PUSHOVER_APP_TOKEN = _get("PUSHOVER_APP_TOKEN")
PUSHOVER_USER_KEY = _get("PUSHOVER_USER_KEY")
HEALTHCHECKS_URL = _get("HEALTHCHECKS_URL")
```

`notify/pushover.py`:

```python
"""Send Pushover notifications (stdlib only)."""
import urllib.parse
import urllib.request

from notify import config

API_URL = "https://api.pushover.net/1/messages.json"


def send(title, message, priority=0, *, app_token=None, user_key=None, timeout=10) -> bool:
    token = app_token if app_token is not None else config.PUSHOVER_APP_TOKEN
    user = user_key if user_key is not None else config.PUSHOVER_USER_KEY
    if not token or not user:
        print("[pushover] missing credentials; not sending")
        return False
    data = urllib.parse.urlencode({
        "token": token, "user": user, "title": title,
        "message": message, "priority": priority,
    }).encode()
    try:
        with urllib.request.urlopen(API_URL, data=data, timeout=timeout) as resp:
            return 200 <= resp.status < 300
    except Exception as exc:
        print(f"[pushover] send failed: {exc}")
        return False
```

- [ ] **Step 5: Run — expect PASS (3).** `cd ~/kode/notify && python3 -m unittest tests.test_pushover -v`

- [ ] **Step 6: Commit**

```bash
cd ~/kode/notify && git add -A
git commit -m "notify: scaffold config + stdlib Pushover sender" -m "Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 2: notify repo — battery monitor + heartbeat

**Files:** Create `notify/battery.py`; Test `tests/test_battery.py`.

**Interfaces produced:** `BatteryState(percent, on_ac, status)`, `Alert(title, message, priority)`, `read_battery(power_supply_dir)`, `decide_alerts(current, prev, thresholds=(50,20,10)) -> (list, dict)`, `format_reading_line(when, state)`, `run(state_path, power_supply_dir, send, history_path)`, `heartbeat(url, timeout=10) -> bool`.

- [ ] **Step 1: Write the failing test** — `tests/test_battery.py`:

```python
import os, sys, tempfile, unittest
from unittest import mock

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from notify import battery
from notify.battery import BatteryState, decide_alerts


class DecideAlertsTests(unittest.TestCase):
    def test_first_observation_seeds_state_without_alert(self):
        alerts, state = decide_alerts(BatteryState(95, True, "Charging"), {})
        self.assertEqual(alerts, [])
        self.assertEqual(state, {"power": "ac", "last_threshold_alerted": None})

    def test_unplug_sends_power_lost_high_priority(self):
        alerts, state = decide_alerts(BatteryState(80, False, "Discharging"),
                                      {"power": "ac", "last_threshold_alerted": None})
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0].title, "Power lost")
        self.assertEqual(alerts[0].priority, 1)
        self.assertEqual(state["power"], "battery")

    def test_threshold_50_fires_once_then_suppressed(self):
        alerts, state = decide_alerts(BatteryState(50, False, "Discharging"),
                                      {"power": "battery", "last_threshold_alerted": None})
        self.assertEqual(len(alerts), 1)
        self.assertIn("50%", alerts[0].message)
        alerts2, _ = decide_alerts(BatteryState(48, False, "Discharging"), state)
        self.assertEqual(alerts2, [])

    def test_ten_percent_high_priority(self):
        alerts, _ = decide_alerts(BatteryState(9, False, "Discharging"),
                                  {"power": "battery", "last_threshold_alerted": 20})
        self.assertEqual(alerts[0].priority, 1)

    def test_unplug_at_15_defers_to_10(self):
        alerts, state = decide_alerts(BatteryState(15, False, "Discharging"),
                                      {"power": "ac", "last_threshold_alerted": None})
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0].title, "Power lost")
        alerts2, _ = decide_alerts(BatteryState(9, False, "Discharging"), state)
        self.assertEqual(len(alerts2), 1)
        self.assertIn("9%", alerts2[0].message)

    def test_restore_resets(self):
        alerts, state = decide_alerts(BatteryState(30, True, "Charging"),
                                      {"power": "battery", "last_threshold_alerted": 20})
        self.assertEqual(alerts[0].title, "Power restored")
        self.assertEqual(state, {"power": "ac", "last_threshold_alerted": None})


class ReadAndHistoryTests(unittest.TestCase):
    def test_read_battery_parses_percent_and_ac(self):
        with tempfile.TemporaryDirectory() as tmp:
            os.makedirs(os.path.join(tmp, "BAT0")); os.makedirs(os.path.join(tmp, "AC"))
            open(os.path.join(tmp, "BAT0", "type"), "w").write("Battery\n")
            open(os.path.join(tmp, "BAT0", "capacity"), "w").write("42\n")
            open(os.path.join(tmp, "BAT0", "status"), "w").write("Discharging\n")
            open(os.path.join(tmp, "AC", "type"), "w").write("Mains\n")
            open(os.path.join(tmp, "AC", "online"), "w").write("0\n")
            st = battery.read_battery(tmp)
            self.assertEqual(st.percent, 42)
            self.assertFalse(st.on_ac)

    def test_format_reading_line(self):
        self.assertEqual(
            battery.format_reading_line("2026-07-11T14:30:00", BatteryState(52, True, "Charging")),
            "2026-07-11T14:30:00 percent=52 status=Charging on_ac=True")


class HeartbeatTests(unittest.TestCase):
    def test_heartbeat_get_success(self):
        u = mock.MagicMock()
        u.return_value.__enter__.return_value.status = 200
        with mock.patch.object(battery.urllib.request, "urlopen", u):
            self.assertTrue(battery.heartbeat("https://hc-ping.com/abc"))
        self.assertEqual(u.call_args[0][0], "https://hc-ping.com/abc")

    def test_heartbeat_empty_url_false(self):
        self.assertFalse(battery.heartbeat(""))


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run — expect FAIL.** `cd ~/kode/notify && python3 -m unittest tests.test_battery -v`

- [ ] **Step 3: Implement** — `notify/battery.py`:

```python
"""Battery/power monitor with anti-spam state machine + Healthchecks heartbeat."""
import json
import os
import urllib.request
from collections import namedtuple
from datetime import datetime

from notify import pushover

BatteryState = namedtuple("BatteryState", ["percent", "on_ac", "status"])
Alert = namedtuple("Alert", ["title", "message", "priority"])


def read_battery(power_supply_dir="/sys/class/power_supply") -> BatteryState:
    percent, status, on_ac = None, "Unknown", None
    try:
        names = sorted(os.listdir(power_supply_dir))
    except OSError:
        return BatteryState(percent=None, on_ac=True, status="Unknown")
    for name in names:
        path = os.path.join(power_supply_dir, name)
        try:
            with open(os.path.join(path, "type")) as f:
                dev_type = f.read().strip()
        except OSError:
            continue
        if dev_type == "Battery":
            try:
                with open(os.path.join(path, "capacity")) as f:
                    percent = int(f.read().strip())
                with open(os.path.join(path, "status")) as f:
                    status = f.read().strip()
            except OSError:
                continue
        elif dev_type == "Mains":
            try:
                with open(os.path.join(path, "online")) as f:
                    on_ac = f.read().strip() == "1"
            except OSError:
                pass
    if on_ac is None:
        on_ac = status != "Discharging"
    return BatteryState(percent=percent, on_ac=on_ac, status=status)


def _seed_ceiling(pct, thresholds):
    at_or_above = [t for t in thresholds if t >= pct]
    return min(at_or_above) if at_or_above else None


def decide_alerts(current: BatteryState, prev: dict, thresholds=(50, 20, 10)):
    thresholds = sorted(thresholds, reverse=True)
    pct = current.percent if current.percent is not None else 100
    new_power = "ac" if current.on_ac else "battery"
    prev_power = prev.get("power")
    last = prev.get("last_threshold_alerted")
    alerts = []

    if prev_power is None:
        last = _seed_ceiling(pct, thresholds) if new_power == "battery" else None
        return alerts, {"power": new_power, "last_threshold_alerted": last}

    if prev_power == "ac" and new_power == "battery":
        alerts.append(Alert("Power lost", f"⚡ Power lost - on battery ({pct}%)", 1))
        last = _seed_ceiling(pct, thresholds)
    elif prev_power == "battery" and new_power == "ac":
        alerts.append(Alert("Power restored", f"\U0001F50C Power restored ({pct}%)", 0))
        last = None
    elif new_power == "battery":
        ceiling = last if last is not None else 101
        for t in thresholds:
            if pct <= t < ceiling:
                alerts.append(Alert("Battery low", f"\U0001F50B Battery low: {pct}%", 1 if t <= 10 else 0))
                last = t
                ceiling = t
    return alerts, {"power": new_power, "last_threshold_alerted": last}


def format_reading_line(when: str, state: BatteryState) -> str:
    return f"{when} percent={state.percent} status={state.status} on_ac={state.on_ac}"


def _append_history(history_path, line):
    os.makedirs(os.path.dirname(history_path) or ".", exist_ok=True)
    with open(history_path, "a") as f:
        f.write(line + "\n")


def _load_state(state_path):
    try:
        with open(state_path) as f:
            return json.load(f)
    except (OSError, ValueError):
        return {}


def _save_state(state_path, state):
    os.makedirs(os.path.dirname(state_path) or ".", exist_ok=True)
    with open(state_path, "w") as f:
        json.dump(state, f)


def run(state_path, power_supply_dir="/sys/class/power_supply", send=pushover.send,
        history_path=None) -> bool:
    current = read_battery(power_supply_dir)
    if history_path:
        _append_history(history_path,
                        format_reading_line(datetime.now().isoformat(timespec="seconds"), current))
    prev = _load_state(state_path)
    alerts, new_state = decide_alerts(current, prev)
    for a in alerts:
        send(a.title, a.message, a.priority)
    _save_state(state_path, new_state)
    return True


def heartbeat(url, timeout=10) -> bool:
    if not url:
        return False
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return 200 <= resp.status < 300
    except Exception:
        return False
```

- [ ] **Step 4: Run — expect PASS (10).** `cd ~/kode/notify && python3 -m unittest tests.test_battery -v`

- [ ] **Step 5: Commit**

```bash
cd ~/kode/notify && git add notify/battery.py tests/test_battery.py
git commit -m "notify: add battery monitor and Healthchecks heartbeat" -m "Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 3: notify repo — CLI + wrapper

**Files:** Create `notify/cli.py`, `bin/notify`; Test `tests/test_cli.py`.

**Interfaces produced:** `cli.main(argv=None) -> int`.

- [ ] **Step 1: Write the failing test** — `tests/test_cli.py`:

```python
import os, sys, unittest
from unittest import mock

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from notify import cli


class CliTests(unittest.TestCase):
    def test_send_dispatch_and_exit0(self):
        with mock.patch.object(cli.pushover, "send", return_value=True) as s:
            rc = cli.main(["send", "T", "M", "--priority", "1"])
        s.assert_called_once_with("T", "M", 1)
        self.assertEqual(rc, 0)

    def test_send_failure_exit1(self):
        with mock.patch.object(cli.pushover, "send", return_value=False):
            self.assertEqual(cli.main(["send", "T", "M"]), 1)

    def test_battery_dispatch(self):
        with mock.patch.object(cli.battery, "run", return_value=True) as r:
            rc = cli.main(["battery"])
        r.assert_called_once()
        self.assertEqual(rc, 0)

    def test_heartbeat_dispatch(self):
        with mock.patch.object(cli.battery, "heartbeat", return_value=True) as h:
            rc = cli.main(["heartbeat"])
        h.assert_called_once()
        self.assertEqual(rc, 0)


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run — expect FAIL.** `cd ~/kode/notify && python3 -m unittest tests.test_cli -v`

- [ ] **Step 3: Implement** — `notify/cli.py`:

```python
"""notify CLI: send / battery / heartbeat."""
import argparse
import os
import sys

from notify import battery, config, pushover

STATE_DIR = os.path.expanduser("~/notify-state")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(prog="notify")
    sub = parser.add_subparsers(dest="command", required=True)
    p_send = sub.add_parser("send")
    p_send.add_argument("title")
    p_send.add_argument("message")
    p_send.add_argument("--priority", type=int, default=0)
    sub.add_parser("battery")
    sub.add_parser("heartbeat")
    args = parser.parse_args(argv)

    if args.command == "send":
        ok = pushover.send(args.title, args.message, args.priority)
    elif args.command == "battery":
        ok = battery.run(os.path.join(STATE_DIR, "battery.json"),
                         history_path=os.path.join(STATE_DIR, "battery-history.log"))
    elif args.command == "heartbeat":
        ok = battery.heartbeat(config.HEALTHCHECKS_URL)
    else:
        parser.error("unknown command")
        return 2
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
```

`bin/notify`:

```bash
#!/usr/bin/env bash
REPO="$(cd "$(dirname "$(readlink -f "$0")")/.." && pwd)"
exec env PYTHONPATH="$REPO" python3 -m notify.cli "$@"
```

`chmod +x ~/kode/notify/bin/notify`

- [ ] **Step 4: Run — expect PASS (4).** `cd ~/kode/notify && python3 -m unittest tests.test_cli -v`

- [ ] **Step 5: Full suite + commit + push**

```bash
cd ~/kode/notify
python3 -m unittest discover -s tests -v   # expect 17 pass (3+10+4)
git add notify/cli.py bin/notify tests/test_cli.py
git commit -m "notify: add CLI entrypoint and bin wrapper" -m "Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
git push origin main
```

---

### Task 4: Deploy new repo to box + live verification (needs secrets)

- [ ] **Step 1: Pull the repo on the box**

```bash
ssh mbp 'git clone git@github.com:tehBaer/notify.git ~/kode/notify 2>/dev/null || (cd ~/kode/notify && git pull --ff-only); ls ~/kode/notify/notify'
```

- [ ] **Step 2: Migrate secrets + create state dir (reuse the working values)**

```bash
ssh mbp 'mkdir -p ~/notify-state && cp /home/mbp2016/kode/skannonser/main/config/notify_secrets.py /home/mbp2016/kode/notify/notify/secrets.py && echo copied'
```

- [ ] **Step 3: Symlink onto PATH**

```bash
ssh mbp 'mkdir -p ~/.local/bin && ln -sf ~/kode/notify/bin/notify ~/.local/bin/notify && echo linked'
```

- [ ] **Step 4: Live-verify all three subcommands**

```bash
ssh mbp 'export PATH=$HOME/.local/bin:$PATH
notify send "notify CLI test" "Hello from the new notify tool" ; echo "send: $?"
notify heartbeat ; echo "hb: $?"
notify battery ; echo "bat: $?"
cat ~/notify-state/battery.json; echo; tail -1 ~/notify-state/battery-history.log'
```
Expected: real push arrives; all exit 0; state + history under `~/notify-state/`. **STOP and do not proceed to cutover if the push does not arrive.**

---

### Task 5: Rewire skannonser to delegate

**Files:** Create `main/notify/send.py`, `tests/test_notify_send.py`; Modify `main/notify/daily_summary.py`, `weekly_summary.py`; Delete moved modules. Branch `notify-delegate` in `~/kode/skannonser`.

- [ ] **Step 1: Branch + failing test** — `cd ~/kode/skannonser && git checkout -b notify-delegate`; create `tests/test_notify_send.py`:

```python
import os, sys, unittest
from unittest import mock

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from main.notify import send as send_mod


class SendTests(unittest.TestCase):
    def test_shells_out_to_notify_send(self):
        with mock.patch.object(send_mod.subprocess, "run") as run:
            run.return_value.returncode = 0
            ok = send_mod.send("Title", "Body", 1)
        self.assertTrue(ok)
        self.assertEqual(run.call_args[0][0],
                         ["notify", "send", "Title", "Body", "--priority", "1"])

    def test_returns_false_on_nonzero(self):
        with mock.patch.object(send_mod.subprocess, "run") as run:
            run.return_value.returncode = 1
            self.assertFalse(send_mod.send("T", "B"))

    def test_returns_false_on_exception(self):
        with mock.patch.object(send_mod.subprocess, "run", side_effect=OSError("no notify")):
            self.assertFalse(send_mod.send("T", "B"))

    def test_respects_NOTIFY_BIN_env(self):
        with mock.patch.dict(os.environ, {"NOTIFY_BIN": "/opt/notify"}), \
             mock.patch.object(send_mod.subprocess, "run") as run:
            run.return_value.returncode = 0
            send_mod.send("T", "B")
        self.assertEqual(run.call_args[0][0][0], "/opt/notify")


if __name__ == "__main__":
    unittest.main()
```

- [ ] **Step 2: Run — expect FAIL.** `.venv/bin/python -m unittest tests.test_notify_send -v`

- [ ] **Step 3: Implement** — `main/notify/send.py`:

```python
"""Delegate a notification to the standalone `notify` CLI."""
import os
import subprocess


def send(title, message, priority=0) -> bool:
    binary = os.environ.get("NOTIFY_BIN", "notify")
    try:
        return subprocess.run(
            [binary, "send", title, message, "--priority", str(priority)],
            timeout=15,
        ).returncode == 0
    except Exception:
        return False
```

- [ ] **Step 4: Run — expect PASS (4).** `.venv/bin/python -m unittest tests.test_notify_send -v`

- [ ] **Step 5: Repoint daily + weekly.** In `main/notify/daily_summary.py`: replace `from main.notify import pushover` with `from main.notify.send import send as default_send`, and change `def run(db_path=None, today=None, send=pushover.send) -> bool:` → `def run(db_path=None, today=None, send=default_send) -> bool:`. Do the identical change in `main/notify/weekly_summary.py`.

- [ ] **Step 6: Delete moved files**

```bash
cd ~/kode/skannonser
git rm main/notify/pushover.py main/notify/battery.py main/notify/config.py \
       tests/test_notify_pushover.py tests/test_battery.py
```

- [ ] **Step 7: Full suite — expect OK.** `.venv/bin/python -m unittest discover -s tests 2>&1 | grep -E "^(OK|FAILED|Ran)"`

- [ ] **Step 8: Commit, merge, push**

```bash
cd ~/kode/skannonser
git add -A
git commit -m "notify: delegate delivery to standalone notify CLI; drop moved modules" -m "Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
git checkout master && git merge --no-ff notify-delegate -m "Merge notify-delegate: delegate to notify CLI" -m "Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
.venv/bin/python -m unittest discover -s tests 2>&1 | grep -E "^(OK|FAILED|Ran)"
git branch -d notify-delegate && git push origin master
```

---

### Task 6: Cron cutover + cleanup

- [ ] **Step 1: Deploy skannonser change.** `ssh mbp 'cd ~/kode/skannonser && git pull --ff-only'`

- [ ] **Step 2: Swap cron (remove old battery/heartbeat, add PATH + notify jobs)**

```bash
ssh mbp '
NOTIFYLOG=/home/mbp2016/notify-state/notify.log
mkdir -p /home/mbp2016/notify-state
( echo "PATH=/home/mbp2016/.local/bin:/usr/bin:/bin";
  crontab -l 2>/dev/null | grep -vF "main.notify.battery" | grep -vF "config.HEALTHCHECKS_URL" | grep -v "^PATH=";
  echo "*/10 * * * * notify battery >> $NOTIFYLOG 2>&1";
  echo "*/10 * * * * notify heartbeat >/dev/null 2>&1"
) | crontab -
crontab -l'
```
Expected: midnight pipeline + daily 07:00 + weekly Sun 08:00 unchanged; two new `notify` lines; `PATH=` header; old battery/heartbeat lines gone.

- [ ] **Step 3: Verify skannonser daily still delivers via the CLI**

```bash
ssh mbp 'cd /home/mbp2016/kode/skannonser && PATH=$HOME/.local/bin:$PATH .venv/bin/python -m main.notify.daily_summary; echo "exit: $?"'
```
Expected: exit 0 and a daily push arrives (proves skannonser → `notify send`).

- [ ] **Step 4: Verify notify cron commands standalone**

```bash
ssh mbp 'export PATH=$HOME/.local/bin:$PATH; notify battery; echo "bat:$?"; notify heartbeat; echo "hb:$?"; tail -2 ~/notify-state/battery-history.log'
```

- [ ] **Step 5: Remove dead skannonser secrets + old state dir**

```bash
ssh mbp 'rm -f /home/mbp2016/kode/skannonser/main/config/notify_secrets.py; rm -rf /home/mbp2016/skannonser-notify-state; echo cleaned'
```

- [ ] **Step 6: Drop stale `.gitignore` line.** In `~/kode/skannonser/.gitignore` remove `main/config/notify_secrets.py`; then:

```bash
cd ~/kode/skannonser && git add .gitignore
git commit -m "notify: drop stale notify_secrets gitignore entry" -m "Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
git push origin master
```

---

## Self-Review

- **Spec coverage:** split boundary → Tasks 1–3 build, 5 rewires+deletes ✓; CLI 3 subcommands → Task 3 ✓; stdlib urllib → Tasks 1–2 ✓; own repo + push + clone → 1/3/4 ✓; `~/.local/bin` symlink → 4 ✓; secrets → gitignored + migrated (1/4) ✓; delegation w/ NOTIFY_BIN → 5 ✓; `~/notify-state/` → 3/6 ✓; cron cutover w/ PATH → 6 ✓; prove-before-cutover ordering (4 gate → 5 → 6) ✓.
- **Placeholders:** none; secrets copied programmatically from the working file.
- **Type consistency:** `send(title, message, priority)` uniform across pushover/battery/skannonser-send/cli/tests; `battery.run(...)`/`heartbeat(url)` match cli+tests; `cli.main(argv)->int` matches tests; daily/weekly keep `send=` injection so their tests stay valid.
