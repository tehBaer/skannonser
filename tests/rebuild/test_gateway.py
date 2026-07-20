from datetime import datetime

import pytest

from skannonser.config.domain import Budget
from skannonser.gateway import BudgetExceeded, Gateway
from skannonser.store import connection, migrations

CURRENT_MONTH = datetime.now().strftime("%Y-%m")


def fixed_clock() -> str:
    return CURRENT_MONTH


@pytest.fixture
def conn(tmp_path):
    c = connection.connect(tmp_path / "gateway.db")
    migrations.migrate(c)
    return c


def make_budget(**overrides) -> Budget:
    defaults = dict(
        routes_monthly_cap=9000,
        geocode_monthly_cap=9000,
        warn_pcts=[50, 80],
        routes_rpm=60,
        geocode_rpm=60,
    )
    defaults.update(overrides)
    return Budget(**defaults)


def seed_rows(conn, api, count, outcome="ok", month=CURRENT_MONTH):
    """Seed api_usage rows directly (bypassing the gateway) with an explicit
    called_at inside `month`, so month_usage's strftime filter picks them up
    regardless of what the real wall-clock date happens to be."""
    called_at = f"{month}-01 00:00:00"
    conn.executemany(
        "INSERT INTO api_usage (api, outcome, called_at) VALUES (?, ?, ?)",
        [(api, outcome, called_at) for _ in range(count)],
    )
    conn.commit()


def test_gateway_records_and_counts(conn):
    budget = make_budget(routes_monthly_cap=100, routes_rpm=6000)
    gw = Gateway(conn, budget, notify=lambda m: None, sleeper=lambda s: None, clock=fixed_clock)

    gw.call("routes", lambda: "value")

    def boom():
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        gw.call("routes", boom)

    # bookkeeping rows seeded directly must NOT count toward usage
    seed_rows(conn, "routes", 1, outcome="blocked")
    seed_rows(conn, "routes", 1, outcome="warn:50")

    assert gw.month_usage("routes") == 2  # only the ok + error calls above


def test_gateway_rate_limits_per_api(conn):
    budget = make_budget(routes_monthly_cap=9000, routes_rpm=60)  # min interval = 1.0s
    sleeps = []
    gw = Gateway(conn, budget, notify=lambda m: None, sleeper=sleeps.append, clock=fixed_clock)

    gw.call("routes", lambda: None)  # first call never sleeps
    gw.call("routes", lambda: None)  # second call sleeps ~60/rpm

    assert len(sleeps) == 1
    assert sleeps[0] == pytest.approx(1.0, abs=0.1)


def test_gateway_hard_stop_at_cap(conn):
    cap = 5
    budget = make_budget(routes_monthly_cap=cap, routes_rpm=6000)
    gw = Gateway(conn, budget, notify=lambda m: None, sleeper=lambda s: None, clock=fixed_clock)

    seed_rows(conn, "routes", cap - 1)  # one call away from the cap

    result = gw.call("routes", lambda: "ok")  # brings usage to cap, still allowed
    assert result == "ok"
    assert gw.month_usage("routes") == cap

    calls = []

    def should_not_run():
        calls.append(1)
        return "never"

    with pytest.raises(BudgetExceeded) as exc_info:
        gw.call("routes", should_not_run)

    assert calls == []  # fn was never invoked
    exc = exc_info.value
    assert exc.api == "routes"
    assert exc.usage == cap
    assert exc.cap == cap

    last_outcome = conn.execute(
        "SELECT outcome FROM api_usage WHERE api='routes' ORDER BY id DESC LIMIT 1"
    ).fetchone()["outcome"]
    assert last_outcome == "blocked"


def test_gateway_warns_once_per_threshold(conn):
    cap = 10
    budget = make_budget(routes_monthly_cap=cap, warn_pcts=[50, 80], routes_rpm=6000)
    notifications = []
    gw = Gateway(
        conn, budget, notify=notifications.append, sleeper=lambda s: None, clock=fixed_clock
    )

    seed_rows(conn, "routes", 5)  # exactly 50% of cap already used this month

    gw.call("routes", lambda: "ok")  # usage_before=5 -> crosses the 50% threshold
    assert len(notifications) == 1
    assert "50" in notifications[0]

    gw.call("routes", lambda: "ok")  # usage_before=6 -> still below 80%, 50% already warned
    assert len(notifications) == 1


def test_gateway_notify_failure_never_raises(conn):
    cap = 10
    budget = make_budget(routes_monthly_cap=cap, warn_pcts=[50], routes_rpm=6000)

    def bad_notify(message):
        raise RuntimeError("notify service down")

    gw = Gateway(conn, budget, notify=bad_notify, sleeper=lambda s: None, clock=fixed_clock)
    seed_rows(conn, "routes", 5)  # 50% threshold already met, notify will be attempted

    result = gw.call("routes", lambda: "success")

    assert result == "success"
    assert gw.month_usage("routes") == 6


def test_gateway_unknown_api_raises_value_error(conn):
    budget = make_budget()
    gw = Gateway(conn, budget, notify=lambda m: None, sleeper=lambda s: None, clock=fixed_clock)

    with pytest.raises(ValueError):
        gw.call("weather", lambda: None)

    with pytest.raises(ValueError):
        gw.month_usage("weather")
