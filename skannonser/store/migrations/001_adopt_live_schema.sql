CREATE TABLE IF NOT EXISTS "eiendom" (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                finnkode TEXT UNIQUE NOT NULL,
                tilgjengelighet TEXT,
                adresse TEXT,
                postnummer TEXT,
                pris INTEGER,
                url TEXT,
                areal INTEGER,
                pris_kvm INTEGER,
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                search_hit BOOLEAN DEFAULT 1,
                exported_to_sheets BOOLEAN DEFAULT 0
            , info_usable_area INTEGER, info_usable_i_area INTEGER, info_primary_area INTEGER, info_gross_area INTEGER, info_usable_e_area INTEGER, info_open_area INTEGER, info_usable_b_area INTEGER, info_plot_area INTEGER, info_construction_year INTEGER, info_plot_ownership TEXT, info_property_type TEXT, image_url TEXT, image_hosted_url TEXT, active BOOLEAN DEFAULT 0);
CREATE INDEX IF NOT EXISTS idx_eiendom_finnkode ON eiendom(finnkode);
CREATE INDEX IF NOT EXISTS idx_eiendom_exported ON eiendom(exported_to_sheets);
CREATE TABLE IF NOT EXISTS "eiendom_processed" (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                finnkode TEXT UNIQUE NOT NULL,
                adresse_cleaned TEXT,
                pendl_morn_brj INTEGER,
                bil_morn_brj INTEGER,
                pendl_dag_brj INTEGER,
                bil_dag_brj INTEGER,
                google_maps_url TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, pendl_morn_mvv INTEGER, bil_morn_mvv INTEGER, pendl_dag_mvv INTEGER, bil_dag_mvv INTEGER, areal INTEGER, walk_grocery_m INTEGER, walk_bus_min INTEGER, walk_tram_min INTEGER, walk_train_min INTEGER, lat REAL, lng REAL, travel_copy_from_finnkode TEXT, pendl_morn_cntr INTEGER, bil_morn_cntr INTEGER, pendl_dag_cntr INTEGER, bil_dag_cntr INTEGER, geocode_failed INTEGER, pendl_rush_brj INTEGER, pendl_rush_mvv INTEGER, pendl_rush_mvv_uni_rush INTEGER,
                FOREIGN KEY (finnkode) REFERENCES eiendom(finnkode)
            );
CREATE INDEX IF NOT EXISTS idx_eiendom_processed_finnkode ON eiendom_processed(finnkode);
CREATE TABLE IF NOT EXISTS manual_overrides (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                finnkode TEXT UNIQUE NOT NULL,
                areal INTEGER,
                pris INTEGER,
                override_reason TEXT,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            , adresse TEXT, postnummer TEXT);
CREATE INDEX IF NOT EXISTS idx_manual_overrides_finnkode ON manual_overrides(finnkode);
CREATE INDEX IF NOT EXISTS idx_eiendom_search_hit ON eiendom(search_hit);
CREATE TABLE IF NOT EXISTS dnbeiendom (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                dnb_id TEXT,
                url TEXT UNIQUE,
                adresse TEXT,
                postnummer TEXT,
                pris INTEGER,
                lat REAL,
                lng REAL,
                duplicate_of_finnkode TEXT,
                scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                stale BOOLEAN DEFAULT 1, exported_to_sheets BOOLEAN DEFAULT 0, active BOOLEAN, property_type TEXT,
                FOREIGN KEY (duplicate_of_finnkode) REFERENCES eiendom(finnkode)
            );
CREATE INDEX IF NOT EXISTS idx_dnbeiendom_duplicate_finnkode ON dnbeiendom(duplicate_of_finnkode);
CREATE INDEX IF NOT EXISTS idx_dnbeiendom_url ON dnbeiendom(url);
CREATE TABLE IF NOT EXISTS listing_comments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                finnkode TEXT NOT NULL,
                comment_type TEXT NOT NULL,
                user_id TEXT,
                text TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
CREATE INDEX IF NOT EXISTS idx_listing_comments_finnkode ON listing_comments(finnkode);
CREATE INDEX IF NOT EXISTS idx_listing_comments_type ON listing_comments(comment_type);
CREATE INDEX IF NOT EXISTS idx_dnbeiendom_active ON dnbeiendom(active);
CREATE INDEX IF NOT EXISTS idx_dnbeiendom_exported ON dnbeiendom(exported_to_sheets);
CREATE INDEX IF NOT EXISTS idx_eiendom_active ON eiendom(active);
CREATE TABLE IF NOT EXISTS stations (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                name        TEXT    NOT NULL UNIQUE,
                lat         REAL,
                lng         REAL,
                radius_m    REAL,
                to_skoyen_min INTEGER,
                updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
CREATE TABLE IF NOT EXISTS station_lines (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                station_id  INTEGER NOT NULL REFERENCES stations(id) ON DELETE CASCADE,
                line        TEXT    NOT NULL,
                updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(station_id, line)
            );
CREATE TABLE IF NOT EXISTS station_travel (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                station_line_id  INTEGER NOT NULL REFERENCES station_lines(id) ON DELETE CASCADE,
                destination      TEXT    NOT NULL,
                minutes          INTEGER,
                updated_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(station_line_id, destination)
            );
CREATE INDEX IF NOT EXISTS idx_station_lines_station_id ON station_lines(station_id);
CREATE INDEX IF NOT EXISTS idx_station_travel_line_id   ON station_travel(station_line_id);
