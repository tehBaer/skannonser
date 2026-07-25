#!/usr/bin/env bash
#
# Bootstrap a fresh git worktree of this repo so its test suite runs clean.
#
# A worktree checkout omits every gitignored path, and three of those are load-
# bearing here: the virtualenv, a crawled HTML test fixture, and the live DB.
# Without them `pytest` fails in ways that look like real breakage but are not.
# Run this once from inside a new worktree:
#
#     ./ops/setup-worktree.sh [--with-db]
#
# Idempotent: re-running only repairs whatever is missing.
#
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
WT="$(pwd -P)"

GIT_DIR="$(cd "$(git rev-parse --git-dir)" && pwd -P)"
GIT_COMMON="$(cd "$(git rev-parse --git-common-dir)" && pwd -P)"
if [ "$GIT_DIR" = "$GIT_COMMON" ]; then
    echo "Not in a linked worktree -- this is the main clone. Nothing to do." >&2
    exit 1
fi
MAIN="$(dirname "$GIT_COMMON")"
echo "worktree: $WT"
echo "main    : $MAIN"

link() {  # link <target> <linkname>
    if [ ! -e "$1" ]; then
        echo "  SKIP  $2 (source missing in main clone: $1)"
        return
    fi
    mkdir -p "$(dirname "$2")"
    ln -sfn "$1" "$2"
    echo "  ok    $2"
}

# 1. Virtualenv. Symlinked, not rebuilt: the dependency set (pandas, the google
#    api client) makes a per-worktree venv slow and pointless when the packages
#    are identical. `.venv` is gitignored, so the symlink never shows in status.
echo "venv:"
link "$MAIN/.venv" "$WT/.venv"

# 2. Crawled fixture. tests/rebuild/test_dnb.py reads this at a path relative to
#    the repo root; `*.html` is gitignored so it is absent in a fresh worktree
#    and two tests fail with FileNotFoundError.
echo "fixtures:"
link "$MAIN/data/dnbeiendom/html_crawled" "$WT/data/dnbeiendom/html_crawled"

# 3. Live DB, opt-in. COPIED rather than symlinked on purpose: the server is the
#    sole writer and a symlink would let two parallel sessions write one SQLite
#    file. A copy is a safe read-only snapshot for ad-hoc inspection; the test
#    suite never touches it (tests build their own DBs under tmp_path).
if [ "${1:-}" = "--with-db" ]; then
    echo "database:"
    if [ -f "$MAIN/main/database/properties.db" ]; then
        mkdir -p "$WT/main/database"
        cp "$MAIN/main/database/properties.db" "$WT/main/database/properties.db"
        echo "  ok    main/database/properties.db (snapshot copy, not a symlink)"
    else
        echo "  SKIP  no properties.db in main clone"
    fi
fi

echo
echo "baseline:"
"$WT/.venv/bin/python" -m pytest tests/ -q 2>&1 | tail -3

cat <<'NOTE'

Note on which source runs:
  pytest        -> the WORKTREE's code (cwd goes on sys.path first). Correct.
  skannonser    -> the MAIN CLONE's code. The console script is an editable
                   install pointing at the main checkout, and its sys.path does
                   not include your cwd. To exercise worktree code through the
                   CLI, run it as a module with PYTHONPATH set:

                     PYTHONPATH=. ./.venv/bin/python -m skannonser.cli <args>
NOTE
