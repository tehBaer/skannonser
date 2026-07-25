import sqlite3

import pytest

from skannonser.store import connection, migrations

EXPECTED_TABLES = {
    "eiendom", "eiendom_processed", "dnbeiendom", "manual_overrides",
    "listing_comments", "stations", "station_lines", "station_travel",
    "annotations", "sold_prices", "sold_sweep_state", "sold_price_attempts",
    "listing_details", "listing_facilities",
}

ALL_MIGRATIONS = [
    "001_adopt_live_schema", "002_notify_tables", "003_api_usage",
    "004_dnb_travel", "005_annotations", "006_sold_prices",
    "007_sold_sweep_state", "008_postnummer_pad", "009_sold_attempts",
    "010_listing_details", "011_neighbour_sold", "012_neighbour_sold_index",
]


def _tables(conn):
    rows = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
    )
    return {r["name"] for r in rows}


def test_migrate_fresh_db_creates_full_schema(tmp_path):
    conn = connection.connect(tmp_path / "fresh.db")
    ran = migrations.migrate(conn)
    assert ran == ALL_MIGRATIONS
    assert EXPECTED_TABLES <= _tables(conn)
    assert "schema_migrations" in _tables(conn)


def test_migrate_is_idempotent(tmp_path):
    conn = connection.connect(tmp_path / "fresh.db")
    migrations.migrate(conn)
    assert migrations.migrate(conn) == []
    assert migrations.pending(conn) == []


def test_migrate_adopts_preexisting_schema(tmp_path):
    """Simulates the live DB: schema already exists, migration must no-op cleanly."""
    conn = connection.connect(tmp_path / "live.db")
    sql = (migrations.MIGRATIONS_DIR / "001_adopt_live_schema.sql").read_text(encoding="utf-8")
    conn.executescript(sql)  # pre-existing schema, no migration bookkeeping
    ran = migrations.migrate(conn)
    assert ran == ALL_MIGRATIONS
    assert EXPECTED_TABLES <= _tables(conn)


def test_connection_settings(tmp_path):
    conn = connection.connect(tmp_path / "x.db")
    assert conn.execute("PRAGMA journal_mode").fetchone()[0] == "wal"
    assert conn.execute("PRAGMA foreign_keys").fetchone()[0] == 1


def test_migrate_cli_fails_loud_when_db_missing(tmp_path, monkeypatch):
    from typer.testing import CliRunner

    from skannonser.cli import app

    missing = tmp_path / "does-not-exist.db"
    monkeypatch.setenv("SKANNONSER_DB_PATH", str(missing))
    result = CliRunner().invoke(app, ["db", "migrate"])
    assert result.exit_code == 1
    assert not missing.exists()


def test_pending_fails_loud_when_migrations_dir_missing(tmp_path, monkeypatch):
    monkeypatch.setattr(migrations, "MIGRATIONS_DIR", tmp_path / "nope")
    conn = connection.connect(tmp_path / "x.db")
    with pytest.raises(FileNotFoundError):
        migrations.pending(conn)


def test_failed_migration_rolls_back_and_is_not_recorded(tmp_path, monkeypatch):
    mig_dir = tmp_path / "migs"
    mig_dir.mkdir()
    (mig_dir / "001_good.sql").write_text("CREATE TABLE a (x INTEGER);")
    (mig_dir / "002_bad.sql").write_text(
        "CREATE TABLE b (x INTEGER);\nINSERT INTO nope VALUES (1);"
    )
    monkeypatch.setattr(migrations, "MIGRATIONS_DIR", mig_dir)
    conn = connection.connect(tmp_path / "x.db")

    with pytest.raises(sqlite3.OperationalError):
        migrations.migrate(conn)

    tables = {r["name"] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")}
    assert "a" in tables          # 001 fully applied and recorded
    assert "b" not in tables      # 002 rolled back entirely, no partial DDL
    applied = {r["id"] for r in conn.execute("SELECT id FROM schema_migrations")}
    assert applied == {"001_good"}


def test_migration_002_creates_notify_tables(tmp_path):
    conn = connection.connect(tmp_path / "fresh.db")
    ran = migrations.migrate(conn)
    assert ran == ALL_MIGRATIONS
    tables = {r["name"] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    assert {"eiendom_status_history", "daily_listing_snapshot", "daily_metrics"} <= tables


def test_migration_003_creates_api_usage_table(tmp_path):
    conn = connection.connect(tmp_path / "fresh.db")
    ran = migrations.migrate(conn)
    assert "003_api_usage" in ran
    tables = {r["name"] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    assert "api_usage" in tables
    # Verify table structure
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(api_usage)")}
    assert cols == {"id", "called_at", "api", "outcome", "finnkode"}
    # Verify index exists
    indexes = {r["name"] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='api_usage'")}
    assert "idx_api_usage_called_at" in indexes


def test_migration_004_adds_dnb_travel_columns(tmp_path):
    conn = connection.connect(tmp_path / "fresh.db")
    ran = migrations.migrate(conn)
    assert "004_dnb_travel" in ran
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(dnbeiendom)")}
    assert {"pendl_rush_brj", "pendl_rush_mvv"} <= cols


def test_migration_011_adds_neighbour_sold_columns(tmp_path):
    conn = connection.connect(tmp_path / "fresh.db")
    ran = migrations.migrate(conn)
    assert "011_neighbour_sold" in ran
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(sold_prices)")}
    assert {
        "size", "property_type", "bedrooms", "collective_debt",
        "ownership_type", "discovered_near_finnkode"
    } <= cols


def test_migration_005_creates_annotations_table(tmp_path):
    conn = connection.connect(tmp_path / "fresh.db")
    ran = migrations.migrate(conn)
    assert "005_annotations" in ran
    tables = {r["name"] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table'")}
    assert "annotations" in tables
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(annotations)")}
    assert cols == {"finnkode", "kommentar", "tag", "imported_at", "updated_at"}
    pk_cols = [r["name"] for r in conn.execute("PRAGMA table_info(annotations)") if r["pk"]]
    assert pk_cols == ["finnkode"]


def test_migration_008_pads_legacy_stripped_postnummer(tmp_path):
    conn = connection.connect(tmp_path / "fresh.db")
    migrations.migrate(conn)
    # Simulate a pre-008 database: un-record 008, then seed rows in the mixed
    # legacy/new forms the backfill must (and must not) touch.
    conn.execute("DELETE FROM schema_migrations WHERE id = '008_postnummer_pad'")
    conn.executemany(
        "INSERT INTO eiendom (finnkode, postnummer) VALUES (?, ?)",
        [
            ("legacy-3", "581"),      # stripped -> pad
            ("legacy-2", "57"),       # stripped -> pad
            ("modern", "0581"),       # already 4-digit -> untouched
            ("weird", "N/A"),         # non-numeric -> untouched
            ("empty", ""),            # blank -> untouched
            ("nullpc", None),         # NULL -> untouched
        ],
    )
    conn.execute(
        "INSERT INTO dnbeiendom (url, postnummer, active) VALUES (?, ?, 1)",
        ("https://dnb/legacy", "172"),
    )
    conn.commit()

    assert migrations.migrate(conn) == ["008_postnummer_pad"]

    got = {
        r["finnkode"]: r["postnummer"]
        for r in conn.execute("SELECT finnkode, postnummer FROM eiendom")
    }
    assert got == {
        "legacy-3": "0581", "legacy-2": "0057", "modern": "0581",
        "weird": "N/A", "empty": "", "nullpc": None,
    }
    dnb_pc = conn.execute("SELECT postnummer FROM dnbeiendom").fetchone()["postnummer"]
    assert dnb_pc == "0172"


def test_migration_011_adds_columns_to_populated_sold_prices_table(tmp_path):
    conn = connection.connect(tmp_path / "fresh.db")
    # Run migrations up to 010 only (excluding 011).
    ran = migrations.migrate(conn)
    assert "011_neighbour_sold" in ran
    # Remove 011 from the record to simulate a pre-011 state.
    conn.execute("DELETE FROM schema_migrations WHERE id = '011_neighbour_sold'")
    # Simulate dropping the columns that 011 added by recreating the table
    # with just the pre-011 columns. Use a temporary table approach.
    conn.executescript("""
        CREATE TABLE sold_prices_pre011 (
            finnkode TEXT PRIMARY KEY,
            sold_price INTEGER,
            sold_date TEXT,
            cadastral_sold_date TEXT,
            price_suggestion INTEGER,
            address TEXT,
            source TEXT NOT NULL DEFAULT 'finn_map',
            fetched_at TEXT NOT NULL DEFAULT (datetime('now')),
            updated_at TEXT NOT NULL DEFAULT (datetime('now'))
        );
        INSERT INTO sold_prices_pre011
            SELECT finnkode, sold_price, sold_date, cadastral_sold_date,
                   price_suggestion, address, source, fetched_at, updated_at
            FROM sold_prices;
        DROP TABLE sold_prices;
        ALTER TABLE sold_prices_pre011 RENAME TO sold_prices;
    """)
    # Seed pre-011 rows into the now-simplified sold_prices table.
    conn.executemany(
        "INSERT INTO sold_prices (finnkode, sold_price, cadastral_sold_date) VALUES (?, ?, ?)",
        [
            ("123456789", 5000000, "2026-01-15"),
            ("987654321", 4500000, "2026-02-20"),
        ],
    )
    conn.commit()

    assert migrations.migrate(conn) == ["011_neighbour_sold"]

    # Pre-existing rows survive with their original values intact
    rows = {
        r["finnkode"]: (r["sold_price"], r["cadastral_sold_date"])
        for r in conn.execute(
            "SELECT finnkode, sold_price, cadastral_sold_date FROM sold_prices"
        )
    }
    assert rows == {
        "123456789": (5000000, "2026-01-15"),
        "987654321": (4500000, "2026-02-20"),
    }

    # New columns are present and NULL for existing rows
    for row in conn.execute("SELECT * FROM sold_prices"):
        assert row["size"] is None
        assert row["property_type"] is None
        assert row["bedrooms"] is None
        assert row["collective_debt"] is None
        assert row["ownership_type"] is None
        assert row["discovered_near_finnkode"] is None


def test_migration_012_indexes_discovered_near_finnkode(tmp_path):
    conn = connection.connect(tmp_path / "fresh.db")
    ran = migrations.migrate(conn)
    assert "012_neighbour_sold_index" in ran
    indexes = {r["name"] for r in conn.execute(
        "SELECT name FROM sqlite_master WHERE type='index' AND tbl_name='sold_prices'")}
    assert "idx_sold_prices_discovered_near" in indexes


def test_statements_keeps_trigger_block_intact():
    sql = (
        "CREATE TABLE t (x INTEGER);\n"
        "CREATE TRIGGER trg AFTER INSERT ON t BEGIN\n"
        "  UPDATE t SET x = 1; UPDATE t SET x = 2;\n"
        "END;\n"
        "CREATE TABLE u (y INTEGER);\n"
    )
    stmts = migrations._statements(sql)
    assert len(stmts) == 3
    assert stmts[1].startswith("CREATE TRIGGER") and stmts[1].rstrip().endswith("END;")
