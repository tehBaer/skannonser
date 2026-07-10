"""Battery/power monitor with an anti-spam alert state machine."""
import json
import os
from collections import namedtuple

from main.notify import pushover

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


def run(state_path, power_supply_dir="/sys/class/power_supply", send=pushover.send) -> bool:
    current = read_battery(power_supply_dir)
    prev = _load_state(state_path)
    alerts, new_state = decide_alerts(current, prev)
    for a in alerts:
        send(a.title, a.message, a.priority)
    _save_state(state_path, new_state)
    return True


if __name__ == "__main__":
    import sys
    default_state = os.path.expanduser("~/skannonser-notify-state/battery.json")
    sys.exit(0 if run(default_state) else 1)
