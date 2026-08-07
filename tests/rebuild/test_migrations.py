import sqlite3

import pytest

from skannonser.store import connection, migrations

EXPECTED_TABLES = {
    "eiendom", "eiendom_processed", "dnbeiendom", "manual_overrides",
    "listing_comments", "stations", "station_lines", "station_travel",
    "annotations", "sold_prices", "sold_sweep_state", "sold_price_attempts",
    "listing_details", "listing_facilities",
    "listing_salgsoppgave", "listing_tg_findings", "listing_egenerklaering",
    "listing_tilstand",
    "salgsoppgave_llm_cache",
}

ALL_MIGRATIONS = [
    "001_adopt_live_schema", "002_notify_tables", "003_api_usage",
    "004_dnb_travel", "005_annotations", "006_sold_prices",
    "007_sold_sweep_state", "008_postnummer_pad", "009_sold_attempts",
    "010_listing_details", "011_neighbour_sold", "012_neighbour_sold_index",
    "013_gjovikbanen_missing_stations", "014_r31_north_of_jaren",
    "015_salgsoppgave", "016_tilstand", "017_classification_provenance",
]


def _migrate_through(conn, last_stem: str) -> None:
    """Apply migrations in order up to and including `last_stem`, leaving the
    rest pending -- so a data-fixing migration can be tested against the state
    that actually preceded it."""
    for path in migrations.pending(conn):
        for stmt in migrations._statements(path.read_text(encoding="utf-8")):
            conn.execute(stmt)
        conn.execute("INSERT INTO schema_migrations (id) VALUES (?)", (path.stem,))
        if path.stem == last_stem:
            break
    conn.commit()


def test_migration_017_adds_provenance_columns(tmp_path):
    conn = connection.connect(tmp_path / "t.db")
    migrations.migrate(conn)
    cache_cols = {r["name"] for r in conn.execute(
        "PRAGMA table_info(salgsoppgave_llm_cache)")}
    assert "effort" in cache_cols
    tilstand_cols = {r["name"] for r in conn.execute(
        "PRAGMA table_info(listing_tilstand)")}
    assert "content_sha256" in tilstand_cols


def test_migration_017_relabels_only_the_interactive_session_rows(tmp_path):
    """The 150 rows loaded 2026-08-06 were produced in-session, not by the API
    seam, but carry cache_put's default label. Relabel exactly those: scoped by
    date so a genuine API run on a later date keeps its own label."""
    conn = connection.connect(tmp_path / "t.db")
    _migrate_through(conn, "016_tilstand")
    rows = [
        ("mislabelled", "claude-opus-5", "2026-08-06 20:00:00"),
        ("already_honest", "claude-opus-5 (interactive session)", "2026-08-06 09:00:00"),
        ("later_api_run", "claude-opus-5", "2026-08-09 12:00:00"),
    ]
    for sha, model, created in rows:
        conn.execute(
            "INSERT INTO salgsoppgave_llm_cache "
            "(content_sha256, response_json, model, created_at) VALUES (?, '{}', ?, ?)",
            (sha, model, created),
        )
    conn.commit()
    migrations.migrate(conn)
    got = {r[0]: r[1] for r in conn.execute(
        "SELECT content_sha256, model FROM salgsoppgave_llm_cache")}
    assert got["mislabelled"] == "claude-opus-5 (interactive session)"
    assert got["already_honest"] == "claude-opus-5 (interactive session)"
    assert got["later_api_run"] == "claude-opus-5"


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


def _apply_013(conn):
    sql = (migrations.MIGRATIONS_DIR / "013_gjovikbanen_missing_stations.sql").read_text(
        encoding="utf-8")
    for stmt in migrations._statements(sql):
        conn.execute(stmt)
    conn.commit()


def _seed_legacy_stations(conn):
    """The legacy-seeded neighbours 013 repairs around (see its guard)."""
    conn.executemany(
        "INSERT INTO stations (name, lat, lng) VALUES (?, ?, ?)",
        [("Snippen", 60.0240121, 10.8099853),
         ("Nittedal", 60.0583068, 10.8648814),
         ("Varingskollen", 60.105965, 10.847299)])
    conn.commit()


def test_migration_013_seeds_movatn_and_aneby(tmp_path):
    conn = connection.connect(tmp_path / "fresh.db")
    ran = migrations.migrate(conn)
    assert "013_gjovikbanen_missing_stations" in ran
    # Fresh DB: no legacy stations, so the guard holds and nothing is seeded.
    assert list(conn.execute("SELECT COUNT(*) AS n FROM stations"))[0]["n"] == 0

    _seed_legacy_stations(conn)
    _apply_013(conn)
    rows = list(conn.execute(
        "SELECT s.name, s.lat, s.lng, sl.line, st.destination, st.minutes "
        "FROM stations s "
        "JOIN station_lines sl ON sl.station_id = s.id "
        "JOIN station_travel st ON st.station_line_id = sl.id "
        "WHERE s.name IN ('Movatn', 'Åneby') ORDER BY s.name, sl.line"))
    assert [(r["name"], r["line"], r["destination"], r["minutes"]) for r in rows] == [
        ("Movatn", "R31", "Oslo S", 25),
        ("Movatn", "RE30", "Oslo S", 25),
        ("Åneby", "R31", "Oslo S", 34),
        ("Åneby", "RE30", "Oslo S", 34),
    ]
    # Line order: Movatn sits between Snippen (60.0240) and Nittedal (60.0583),
    # Åneby between Nittedal and Varingskollen (60.1060).
    coords = {r["name"]: (r["lat"], r["lng"]) for r in rows}
    assert 60.0240 < coords["Movatn"][0] < 60.0583 < coords["Åneby"][0] < 60.1060


def test_migration_013_is_reapplyable_over_existing_rows(tmp_path):
    """Re-running 013 against a DB that already has the rows is a no-op.

    The live DB is written by the server, not by this repo; a hand-applied or
    partially-applied 013 must not produce duplicate lines/travel rows.
    """
    conn = connection.connect(tmp_path / "fresh.db")
    migrations.migrate(conn)
    _seed_legacy_stations(conn)
    _apply_013(conn)
    before = list(conn.execute("SELECT COUNT(*) AS n FROM station_lines"))[0]["n"]
    _apply_013(conn)
    assert list(conn.execute("SELECT COUNT(*) AS n FROM station_lines"))[0]["n"] == before
    assert list(conn.execute(
        "SELECT COUNT(*) AS n FROM stations WHERE name = 'Movatn'"))[0]["n"] == 1


def test_migration_014_drops_r31_north_of_jaren(tmp_path):
    conn = connection.connect(tmp_path / "fresh.db")
    migrations.migrate(conn)
    conn.execute("INSERT INTO stations (name, lat, lng) VALUES ('Gjøvik', 60.79, 10.69)")
    sid = conn.execute("SELECT id FROM stations WHERE name = 'Gjøvik'").fetchone()["id"]
    for line in ("R31", "RE30"):
        conn.execute("INSERT INTO station_lines (station_id, line) VALUES (?, ?)", (sid, line))
        lid = conn.execute(
            "SELECT id FROM station_lines WHERE station_id = ? AND line = ?", (sid, line)
        ).fetchone()["id"]
        conn.execute(
            "INSERT INTO station_travel (station_line_id, destination, minutes) "
            "VALUES (?, 'Oslo S', 57)", (lid,))
    conn.commit()

    sql = (migrations.MIGRATIONS_DIR / "014_r31_north_of_jaren.sql").read_text(encoding="utf-8")
    for _ in range(2):  # idempotent
        for stmt in migrations._statements(sql):
            conn.execute(stmt)
        conn.commit()

    rows = list(conn.execute(
        "SELECT sl.line, st.minutes FROM station_lines sl "
        "LEFT JOIN station_travel st ON st.station_line_id = sl.id "
        "WHERE sl.station_id = ?", (sid,)))
    # R31 gone with its travel row; RE30 keeps the identical minutes.
    assert [(r["line"], r["minutes"]) for r in rows] == [("RE30", 57)]
    assert list(conn.execute("SELECT COUNT(*) AS n FROM station_travel"))[0]["n"] == 1


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


def test_migration_015_adds_dl_columns_to_listing_details(tmp_path):
    """The two pricing-<dl> labels parse_details was dropping need columns."""
    conn = connection.connect(tmp_path / "fresh.db")
    migrations.migrate(conn)
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(listing_details)")}
    assert {"eiendomsskatt_kr", "verditakst"} <= cols


def test_016_reshapes_phase2_tables(tmp_path):
    conn = connection.connect(tmp_path / "fresh.db")
    migrations.migrate(conn)
    tg_cols = {r["name"] for r in conn.execute("PRAGMA table_info(listing_tg_findings)")}
    assert {"id", "alvorlighet", "kostnad_lav", "kostnad_hoy", "kostnad_kilde"} <= tg_cols
    # the UNIQUE collapse is gone: two TG3 vatrom rows must coexist
    conn.execute("INSERT INTO eiendom (finnkode) VALUES ('1')")
    for _ in range(2):
        conn.execute(
            "INSERT INTO listing_tg_findings (finnkode, tg, bygningsdel, alvorlighet) "
            "VALUES ('1', 3, 'vatrom', 'alvorlig')"
        )
    n = conn.execute("SELECT COUNT(*) FROM listing_tg_findings").fetchone()[0]
    assert n == 2
    # phase-2 columns left listing_salgsoppgave
    so_cols = {r["name"] for r in conn.execute("PRAGMA table_info(listing_salgsoppgave)")}
    assert not ({"tg2_count", "tg3_count", "tilstandsrapport_dato",
                 "tilstandsrapport_utsteder", "egenerklaering_antall"} & so_cols)
    rollup_cols = {r["name"] for r in conn.execute("PRAGMA table_info(listing_tilstand)")}
    assert {"finnkode", "tg2_count", "tg3_count", "reparasjon_lav", "reparasjon_hoy",
            "reparasjon_est", "alvorlighet", "verste_bygningsdel", "reparasjon_kilde",
            "tilstandsrapport_dato", "tilstandsrapport_utsteder",
            "egenerklaering_antall", "classified_at"} <= rollup_cols
