import os, sys, tempfile, unittest

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from main.notify import battery
from main.notify.battery import BatteryState, decide_alerts


class DecideAlertsTests(unittest.TestCase):
    def test_first_observation_seeds_state_without_alert(self):
        alerts, state = decide_alerts(BatteryState(95, True, "Charging"), {})
        self.assertEqual(alerts, [])
        self.assertEqual(state, {"power": "ac", "last_threshold_alerted": None})

    def test_unplug_sends_power_lost_high_priority(self):
        prev = {"power": "ac", "last_threshold_alerted": None}
        alerts, state = decide_alerts(BatteryState(80, False, "Discharging"), prev)
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0].title, "Power lost")
        self.assertEqual(alerts[0].priority, 1)
        self.assertEqual(state["power"], "battery")

    def test_threshold_50_fires_once_then_suppressed(self):
        prev = {"power": "battery", "last_threshold_alerted": None}
        alerts, state = decide_alerts(BatteryState(50, False, "Discharging"), prev)
        self.assertEqual(len(alerts), 1)
        self.assertIn("50%", alerts[0].message)
        self.assertEqual(state["last_threshold_alerted"], 50)
        alerts2, _ = decide_alerts(BatteryState(48, False, "Discharging"), state)
        self.assertEqual(alerts2, [])

    def test_ten_percent_is_high_priority(self):
        prev = {"power": "battery", "last_threshold_alerted": 20}
        alerts, _ = decide_alerts(BatteryState(9, False, "Discharging"), prev)
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0].priority, 1)

    def test_unplug_at_15_does_not_refire_50_or_20_but_fires_10_later(self):
        prev = {"power": "ac", "last_threshold_alerted": None}
        alerts, state = decide_alerts(BatteryState(15, False, "Discharging"), prev)
        self.assertEqual(len(alerts), 1)               # only power-lost
        self.assertEqual(alerts[0].title, "Power lost")
        alerts2, state2 = decide_alerts(BatteryState(9, False, "Discharging"), state)
        self.assertEqual(len(alerts2), 1)
        self.assertIn("9%", alerts2[0].message)

    def test_restore_sends_power_restored_and_resets(self):
        prev = {"power": "battery", "last_threshold_alerted": 20}
        alerts, state = decide_alerts(BatteryState(30, True, "Charging"), prev)
        self.assertEqual(len(alerts), 1)
        self.assertEqual(alerts[0].title, "Power restored")
        self.assertEqual(state, {"power": "ac", "last_threshold_alerted": None})


class ReadBatteryTests(unittest.TestCase):
    def _make(self, tmp, bat_pct, bat_status, ac_online):
        os.makedirs(os.path.join(tmp, "BAT0"))
        os.makedirs(os.path.join(tmp, "AC"))
        with open(os.path.join(tmp, "BAT0", "type"), "w") as f: f.write("Battery\n")
        with open(os.path.join(tmp, "BAT0", "capacity"), "w") as f: f.write(f"{bat_pct}\n")
        with open(os.path.join(tmp, "BAT0", "status"), "w") as f: f.write(f"{bat_status}\n")
        with open(os.path.join(tmp, "AC", "type"), "w") as f: f.write("Mains\n")
        with open(os.path.join(tmp, "AC", "online"), "w") as f: f.write(f"{ac_online}\n")

    def test_reads_percent_and_ac(self):
        with tempfile.TemporaryDirectory() as tmp:
            self._make(tmp, 42, "Discharging", 0)
            st = battery.read_battery(tmp)
            self.assertEqual(st.percent, 42)
            self.assertFalse(st.on_ac)


if __name__ == "__main__":
    unittest.main()
