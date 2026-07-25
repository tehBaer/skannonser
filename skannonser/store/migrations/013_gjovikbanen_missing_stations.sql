-- 013_gjovikbanen_missing_stations.sql
-- Seed the two Gjøvikbanen stops the legacy station table never had: Movatn
-- and Åneby. Every other R31 halt between Kjelsås and Jaren is present
-- (Snippen, Nittedal, Varingskollen, Hakadal, Stryken, Harestua, ...), so
-- listings around Movatn/Åneby simply had no station ring and no nearest-
-- station distance. The stations tables were seeded by the legacy system
-- deleted in phase 6 (e2b6dc6) and nothing in the current codebase writes
-- them, so a data migration is the only maintenance path left.
--
-- Coordinates: no.wikipedia.org station articles (decimal degrees), which put
-- Movatn between Snippen (60.0240) and Nittedal (60.0583) and Åneby between
-- Nittedal and Varingskollen (60.1060) -- the correct line order.
--
-- Lines: both R31 and RE30, matching every neighbouring row -- and verified
-- against the timetable rather than merely copied. RE30 is the Oslo S-Gjøvik
-- service and R31 the Oslo S-Jaren one, but RE30 is not an express that skips
-- the small halts: three through-trains to Gjøvik call at Movatn and Åneby
-- (e.g. Movatn 1827 -> Nittedal 1832 -> Åneby 1836 -> ... -> Gjøvik 2007,
-- and the same pattern at 2027 and 2231). Both lines genuinely stop here.
--
-- Minutes: minimum scheduled southbound time to Oslo S in the 14.12.2025-
-- 28.06.2026 weekday table (rutetabeller tog/RE30-R31.pdf). NOTE the legacy
-- values were NOT produced by this rule -- against that timetable they land
-- on the minimum for some stations (Nittedal 26, Kjelsås 13) and the maximum
-- for others (Hakadal 44, Varingskollen 42), and Snippen 24 / Nittedal 26 sit
-- 2 min apart where the timetable says 7. Their true provenance (likely the
-- legacy Google Directions budget) is lost, so a stated, reproducible rule is
-- preferred over guessing at the old one. Low stakes either way: the map's
-- commute filter reads only the "Sandvika"/"Sandvika Transfer" destinations
-- (stations.js), which no Gjøvikbanen row has.
--
-- Idempotent: `stations.name`, `station_lines(station_id, line)` and
-- `station_travel(station_line_id, destination)` are all UNIQUE, so every
-- statement is INSERT OR IGNORE and re-running is a no-op.
--
-- Gated on Nittedal existing, because this is a REPAIR of the legacy seed
-- rather than a seed of its own: it is meaningful only on a database that
-- already carries the other 136 stations. Ungated it would also fire on every
-- fresh database -- leaving one with exactly two stations and the rest of
-- Norway missing, which is worse than none -- and would silently poison the
-- fixture suite, where several tests build a migrated DB and then assert over
-- the stations they insert themselves (test_db_stats, test_export,
-- test_web_api). The follow-on statements key off the rows this one inserts,
-- so they no-op on their own when the guard does not pass.

INSERT OR IGNORE INTO stations (name, lat, lng)
SELECT * FROM (
              SELECT 'Movatn' AS name, 60.0375    AS lat, 10.8127778 AS lng
    UNION ALL SELECT 'Åneby',          60.086436,        10.862294
)
WHERE EXISTS (SELECT 1 FROM stations WHERE name = 'Nittedal');

INSERT OR IGNORE INTO station_lines (station_id, line)
SELECT s.id, l.line
FROM stations s
CROSS JOIN (SELECT 'R31' AS line UNION ALL SELECT 'RE30') l
WHERE s.name IN ('Movatn', 'Åneby');

INSERT OR IGNORE INTO station_travel (station_line_id, destination, minutes)
SELECT sl.id, 'Oslo S', CASE s.name WHEN 'Movatn' THEN 25 ELSE 34 END
FROM station_lines sl
JOIN stations s ON s.id = sl.station_id
WHERE s.name IN ('Movatn', 'Åneby');
