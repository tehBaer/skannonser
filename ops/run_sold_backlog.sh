#!/usr/bin/env bash
#
# FINN sold-price backlog wrapper. Run this a few times a day, SPACED OUT, to
# slowly fill the sold-price backlog while keeping the footprint tiny.
#
# Each invocation makes at most SKANNONSER_SOLD_REQUESTS FINN requests (default
# 1), densest-cells-first. On throttle it suspends itself and pings Pushover;
# it then stays idle (every later run is a no-op) until you clear it:
#     skannonser run enrich-sold --resume
# Check state any time without hitting FINN:
#     skannonser run enrich-sold --status
#
# Suggested crontab on the server (4 spaced runs/day, ~1 request each = up to
# ~60 listings/day given the 15-per-request cap). A small random start-jitter
# per entry avoids hitting FINN at the same minute daily:
#
#   9  0    * * *  sleep $((RANDOM \% 1800)); ~/run_sold_backlog.sh
#   37 8    * * *  sleep $((RANDOM \% 1800)); ~/run_sold_backlog.sh
#   3  14   * * *  sleep $((RANDOM \% 1800)); ~/run_sold_backlog.sh
#   51 20   * * *  sleep $((RANDOM \% 1800)); ~/run_sold_backlog.sh
#
# NOT part of the nightly pipeline -- deliberately separate, low, and spaced.
#
set -euo pipefail

REPO="${SKANNONSER_REPO:-$HOME/kode/skannonser}"
LOG_DIR="${SKANNONSER_LOG_DIR:-$HOME/skannonser-logs}"
REQUESTS="${SKANNONSER_SOLD_REQUESTS:-1}"

mkdir -p "$LOG_DIR"
log="$LOG_DIR/sold-$(date +%Y%m%d).log"

cd "$REPO"
if [ -x "$REPO/.venv/bin/skannonser" ]; then
  BIN="$REPO/.venv/bin/skannonser"
else
  BIN="skannonser"
fi

echo "[$(date -Is)] sold backlog run (requests=$REQUESTS)" >>"$log"
"$BIN" run enrich-sold --requests "$REQUESTS" >>"$log" 2>&1
echo "[$(date -Is)] done" >>"$log"
