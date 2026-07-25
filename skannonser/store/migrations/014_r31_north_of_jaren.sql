-- 014_r31_north_of_jaren.sql
-- Drop the R31 line rows on Gjøvik, Raufoss and Reinsvoll: R31 runs Oslo S
-- -Jaren and terminates there, so it does not serve any station north of
-- Jaren. All three sit on the Gjøvik-only stretch (Jaren - Bleiken - Eina -
-- Reinsvoll - Raufoss - Gjøvik) that only RE30 covers.
--
-- Evidence: in rutetabeller tog/RE30-R31.pdf, every column with a time at
-- Gjøvik/Raufoss/Reinsvoll/Eina also runs through to Gjøvik; no Jaren-
-- terminating service appears in those rows. This was checked the other way
-- round too -- the audit that found this also flagged R14/RE11/R12/R13/RE10/
-- RE20 rows, and every one of those turned out to be a PDF-parsing miss with
-- the DB correct (Bodung, Svingen and Tuen are real R14 request stops with
-- one call per table; Dal, Lillehammer, Göteborg C, Oslo Lufthavn et al. are
-- squarely in their timetables). These three are the only survivors.
--
-- Nothing is lost: each station keeps its RE30 line row carrying an identical
-- `Oslo S` travel value (Gjøvik 57, Raufoss 107, Reinsvoll 102), so only the
-- duplicate R31 attribution goes. station_travel rows hanging off the deleted
-- station_lines rows are removed explicitly -- connection.connect() does set
-- PRAGMA foreign_keys=ON, but the migration should not depend on the caller's
-- pragma state for a delete.
--
-- Idempotent: both statements are no-ops once the rows are gone.

DELETE FROM station_travel
WHERE station_line_id IN (
    SELECT sl.id FROM station_lines sl
    JOIN stations s ON s.id = sl.station_id
    WHERE sl.line = 'R31' AND s.name IN ('Gjøvik', 'Raufoss', 'Reinsvoll')
);

DELETE FROM station_lines
WHERE line = 'R31'
  AND station_id IN (
      SELECT id FROM stations WHERE name IN ('Gjøvik', 'Raufoss', 'Reinsvoll')
  );
