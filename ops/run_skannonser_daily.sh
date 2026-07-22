#!/usr/bin/env bash
#
# Polite-access nightly wrapper.
#
# Deploy to the server (the ops runbook's `~/run_skannonser_daily.sh`) and
# point the server crontab at it. Two jobs:
#
#   1. Sleep a random offset before running, so the pipeline does NOT hit
#      FINN at the same wall-clock minute every day. Combined with the paced,
#      browser-UA crawl (config [crawl], skannonser.http), this keeps the
#      footprint gentle and human-shaped.
#   2. Invoke `skannonser run nightly` (the paced ingest+enrich+publish run),
#      logging to the standard log dir.
#
# Cron fires this at a fixed early time (e.g. `0 1 * * *`); the jitter below
# spreads the actual start across the following window. A run takes a few
# minutes to ~15 min with the polite delays, well under the jitter window, so
# runs never overlap.
#
# Tunables (env, all optional):
#   SKANNONSER_REPO           repo path            (default: $HOME/kode/skannonser)
#   SKANNONSER_LOG_DIR        log dir              (default: $HOME/skannonser-logs)
#   SKANNONSER_START_JITTER_S max random delay, s  (default: 21600 = 6h; 0 disables)
#
set -euo pipefail

REPO="${SKANNONSER_REPO:-$HOME/kode/skannonser}"
LOG_DIR="${SKANNONSER_LOG_DIR:-$HOME/skannonser-logs}"
MAX_JITTER_S="${SKANNONSER_START_JITTER_S:-21600}"

mkdir -p "$LOG_DIR"
log="$LOG_DIR/nightly-$(date +%Y%m%d-%H%M%S).log"

if [ "$MAX_JITTER_S" -gt 0 ]; then
  # $RANDOM is 0..32767; combine two draws for a range wider than that.
  jitter=$(( (RANDOM * 32768 + RANDOM) % (MAX_JITTER_S + 1) ))
  echo "[$(date -Is)] sleeping ${jitter}s (max ${MAX_JITTER_S}s) before nightly run" | tee -a "$log"
  sleep "$jitter"
fi

cd "$REPO"
echo "[$(date -Is)] starting nightly" | tee -a "$log"

# Prefer the project venv's console script; fall back to PATH.
if [ -x "$REPO/.venv/bin/skannonser" ]; then
  SKANNONSER_BIN="$REPO/.venv/bin/skannonser"
else
  SKANNONSER_BIN="skannonser"
fi

set +e
"$SKANNONSER_BIN" run nightly >>"$log" 2>&1
rc=$?
set -e
echo "[$(date -Is)] nightly finished (exit $rc)" | tee -a "$log"
exit "$rc"
