#!/usr/bin/env bash
#
# FINN sold-price backlog wrapper. Run this a few times a day, SPACED OUT, to
# slowly fill the sold-price backlog while keeping the footprint tiny.
#
# Each invocation makes at most SKANNONSER_SOLD_REQUESTS FINN requests (default
# 1), fewest-prior-attempts first and densest-cluster first within that tier.
# On throttle it suspends itself and pings Pushover; it then stays idle (every
# later run is a no-op) until a HUMAN clears it:
#     skannonser run enrich-sold --resume
# Check state any time without hitting FINN:
#     skannonser run enrich-sold --status
#
# NEVER put --resume in the crontab. It is not a "run anyway" flag: it clears
# the suspension and RETURNS WITHOUT SWEEPING, so a scheduled --resume would
# both disarm the throttle guard daily and stop the sweep from ever running.
# The wrapper below deliberately passes only --requests.
#
# Crontab actually deployed on the server -- ONE session per day at a random
# time inside a 7-hour window, 13-17 requests in that session (the jitter and
# the odd request count keep the daily footprint from looking metronomic):
#
#   0 9 * * * /bin/bash -c 'sleep $((RANDOM \% 25200)); \
#       SKANNONSER_SOLD_REQUESTS=$((13 + RANDOM \% 5)) ~/kode/skannonser/ops/run_sold_backlog.sh'
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
