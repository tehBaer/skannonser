-- Backfill: zero-pad legacy-stripped postnummer values. The legacy pipeline's
-- CSV round-trip stripped leading zeros ("0581" -> "581"); both current ingest
-- paths already write 4-digit values (finn/parse.py's \d{4} regex, DnbRepo's
-- _to_postnummer zfill), so rows scraped before the rebuild are the only ones
-- carrying the stripped form -- mixing "581"/"0581" in one column, which reads
-- wrong in the UI and breaks text sort/filter and DNB address matching.
-- Norwegian postcodes are always 4 digits: pad every purely-numeric 1-3 digit
-- value in both listing tables. Non-numeric or already-4-digit values are
-- untouched.
UPDATE eiendom
SET postnummer = printf('%04d', CAST(TRIM(postnummer) AS INTEGER))
WHERE postnummer IS NOT NULL
  AND LENGTH(TRIM(postnummer)) BETWEEN 1 AND 3
  AND TRIM(postnummer) <> ''
  AND NOT TRIM(postnummer) GLOB '*[^0-9]*';

UPDATE dnbeiendom
SET postnummer = printf('%04d', CAST(TRIM(postnummer) AS INTEGER))
WHERE postnummer IS NOT NULL
  AND LENGTH(TRIM(postnummer)) BETWEEN 1 AND 3
  AND TRIM(postnummer) <> ''
  AND NOT TRIM(postnummer) GLOB '*[^0-9]*';
