# Tilstand classifier — working brief

Dispatch brief for one classification batch. Hand this to each agent doing
interactive classification, along with a directory of ad extracts.

The operational loop around it — selecting ads, dumping `classify_input()`,
loading responses, the quality gates — lives in
[`tilstand-runbook.md`](tilstand-runbook.md). Read that first if you are
running the batch rather than classifying in it.

---

You are acting as the tilstand classifier for a Norwegian property-listing
pipeline. You read salgsoppgave (sales prospectus) condition extracts and emit
one strict-JSON classification per ad.

## Read the contract first

Read `skannonser/enrich/tilstand.py`. It defines everything binding on you:

- `_SYSTEM_PROMPT` — the classification rules. **Follow it exactly; it is your
  instruction set, not background reading.**
- `TILSTAND_SCHEMA` and the `TilstandResponse` / `TgFinding` pydantic models —
  your output shape.
- The controlled vocabularies: `BYGNINGSDEL`, `TILTAK`, `ALVORLIGHET`,
  `FORHOLD`, `UTSTEDER`, `TG_GRADES`, `KOSTNAD_KILDE`, `RADON_STATUS`,
  `RADONSPERRE`, and `GRID` (the only legal cost values).

Any value outside those vocabularies fails validation and your work is
discarded, so check each one.

## Output

Write one JSON file per ad, named `<finnkode>.json`, to the `resp/` directory
given in your dispatch. Each file contains ONLY the JSON object — no markdown
fences, no commentary.

## Calibration from the ads already classified in this corpus

These are real patterns from this exact data. They are guidance for applying
`_SYSTEM_PROMPT`, not a replacement for it.

**Shape of the source text**

- **Bare TG lists are the common case.** Many ads list building parts under
  "Følgende har fått tilstandsgrad 2:" with no defect body and no cost. Each
  listed line is a finding. Assign `bygningsdel` from the line's leaf term,
  `alvorlighet` from what that defect type normally implies, and estimate a
  Norwegian repair band snapped to `GRID`, with `kostnad_kilde: "estimat"`.
- **A heading's TG grade can contradict the body.** One ad listed an item under
  "Følgende tiltak har fått TG3" whose body read "TG2 vurderes". The body wins.
- **Verbatim duplicate lines** (same building part, same page reference, listed
  twice) are a formatting artifact — count once. Genuinely distinct bullets
  under one heading are separate findings.
- **Lost line breaks merge two entries onto one line.** If a line names two
  unrelated rooms/parts back to back, split it into two findings.
- **Where bullets describe one cascading defect** (deformation caused the loose
  tiles, which caused the cracked grout), that is one finding, not three —
  otherwise its cost is counted three times in the rollup.
- **A TG-listed item whose body describes no defect** — scope or method
  boilerplate under an "Elektrisk anlegg" or "Krypkjeller" heading — is still a
  finding, but with all three cost fields null. Do not drop it, and do not
  price it.

**Costs**

- Stated ranges snap OUTWARD to grid values: "10.000 - 25.000" →
  `10000` / `50000`.
- "Over 300 000" → `300000` / `1000000` (1000000 is the "1M+" marker).
- "Under 20 000" → `0` / `20000`.
- A cost folded into another item ("Kostnadsestimat: Er medtatt under
  Takkonstruksjon") → all three cost fields null, so it isn't double-counted.
- A defect whose body never describes the defect → all three cost fields null.
- Stated costs get `kostnad_kilde: "takst"`; your own estimates get
  `"estimat"`. Never mix.
- **One room, several rows.** Reports routinely list gulv, vegger, sluk/membran
  and rør as separate rows for the same bathroom. Keep them as separate
  findings, but load the renovation cost onto the substantive row and keep its
  siblings low — otherwise one bathroom is charged two or three times in the
  rollup.

**What is NOT a finding**

- **TGIU** (tilstandsgrad ikke undersøkt) is outside the TG2/TG3 vocabulary.
  Ads where snow or a bathroom cabin blocked inspection legitimately produce
  few findings; that is not an extraction miss.
- **"Helse, miljø og sikkerhet" blocks.** HMS items (railing openings, escape
  routes, radon advice, el-kontroll recommendations) carry no TG grade and
  often sit outside both TG lists. Some ads say so explicitly: "skal ikke
  forstås som en tilstandsgrad". Exclude them — including radon lines. Radon is
  a finding ONLY when it appears inside a TG2/TG3 list, in which case
  `bygningsdel: "radon"`.
- **Some extracts contain no condition content at all** — pure boilerplate about
  avhendingsloven, boligselgerforsikring, vedlegg. That is a legitimate empty
  result: `findings: []`. Do not invent findings.

**Radon fields**

The radon fields are separate from findings: they describe the property even
when the radon text is nowhere near a TG list. `_SYSTEM_PROMPT` has the base
rules; these three cases come from ads that have actually been misread.

- **A bare `Radonmåling` line under a TGIU heading is NOT `ikke_malt`.** TGIU
  means the surveyor did not investigate it, which says nothing about whether a
  measurement exists. All three radon fields stay null. Contrast statements
  that *are* about the property — "Det er ukjent om det er foretatt
  radonmåling i boligen", "Takstmannen har ikke kjennskap til radonmåling" —
  where `ikke_malt` is the closest legal value.
- **"Det antas at radonduk er montert … ettersom det foreligger ferdigattest"
  is regulatory inference, not observation** → `radonsperre: null`. It is the
  TEK17 boilerplate wearing a property-specific sentence. Compare two that
  legitimately earn `finnes`: "utført med forskriftsmessig radonsperre og
  dokumentasjon foreligger", and a radonduk the seller admits installing as
  ufaglært egeninnsats — one observed it, the other built it.
- **The HMS block and the seller's egenerklæring can contradict each other.**
  Ads exist whose HMS/Radon section says no measurement was taken while the
  seller reports an actual one ("målt 2020, 40-50", "målt 2003, under godkjent
  verdi"). The seller's concrete statement wins → `malt_under_grense`, with
  `radon_bq` null, because a range or a remembered verdict is not a value.

**egenerklaering**

- `egenerklaering_present` is true only for the seller's own disclosures. A
  seller who discloses nothing ("Ingen spesielle ting å nevne", or has never
  lived in the property) still gets `true` with an empty list. An ad that merely
  lists "Egenerklæringsskjema" among attachments gets `false`.
- **Record a FORHOLD only for an actual adverse condition.** Disclosures that
  are entirely positive — work done by a named qualified firm, documentation
  exists, ferdigattest foreligger — yield an EMPTY list even though
  `present` is true. But an adverse detail inside an otherwise positive
  disclosure does count: "ny flis, smøremembran, **ikke nytt mansjett til
  sluk**" → `["annet"]`.
- **Norwegian negation, as the prompt warns.** "Selger har ikke tegnet
  boligselgerforsikring" contains "har tegnet". One ad answered "Ja" to a
  question about påbud/heftelser and then explained it meant nothing beyond an
  ordinary mortgage — that is not `palegg_offentlig`. Read the sentence.

**Report metadata**

- `tilstandsrapport_utsteder` takes an enum value only when the named firm
  matches one (`anticimex`, `norsk_takst`, `takstinstituttet`, `nito_takst`).
  A named individual surveyor, or any other firm, is `annet`. Nothing stated is
  `null`.
- A placeholder like "datert (dato)" means `tilstandsrapport_dato: null`.

## Verify before you finish

For each file you wrote, run this from the repo root and confirm it prints OK:

```bash
PYTHONPATH=. ./.venv/bin/python -c "
from skannonser.enrich.tilstand import TilstandResponse
import pathlib,sys
p=pathlib.Path(sys.argv[1]); TilstandResponse.model_validate_json(p.read_text()); print('OK', p.name)
" <path-to-your-json>
```

Fix anything that fails and re-verify. Do not report success on a file you have
not seen validate.

## Report back (under 15 lines)

- **Status:** DONE | DONE_WITH_CONCERNS | BLOCKED
- One line per ad: finnkode, TG2 count, TG3 count, anything odd about it
- Confirmation that every file validated
- Any ad where you were genuinely unsure, and why

**Flagging uncertainty is load-bearing, not politeness.** Every correction
made before loading a batch so far has come from an agent saying it was
unsure. A confident wrong answer is written to the cache and never revisited;
a flagged one gets checked against the source. When a call is close, say so.

Do not write to the database, do not run the classifier CLI, and do not touch
any file outside your own output paths.
