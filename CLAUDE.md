# Working in this repo

## Use a worktree — several sessions run here at once

This repo is worked on by more than one Claude session at a time. A clone has
**one** working tree and **one** HEAD, so two sessions in `~/kode/skannonser`
share a single branch pointer: a `git checkout` in either one drags the other's
uncommitted files onto the new branch. That is not hypothetical — it happened on
2026-07-25, when station-data work started on `master` and surfaced on
`feat/ui-polish-round-2` mid-task.

**So: start work by entering a worktree** (`EnterWorktree`, which creates one
under `.claude/worktrees/`). Then run:

```bash
./ops/setup-worktree.sh
```

A worktree checkout omits every gitignored path, and three matter here — the
venv, a crawled HTML fixture (two `test_dnb.py` tests fail without it), and the
live DB. The script restores all three and runs a baseline `pytest`. Expect
**659 passed**; anything less was already broken before you arrived.

Worktrees share one object store, so commits are visible across them instantly —
no push or fetch needed. Git refuses to check out the same branch in two
worktrees, which is the guardrail that catches this class of collision.

One caveat the script prints and it is worth repeating: `pytest` runs the
worktree's code, but the installed `skannonser` console script runs the **main
clone's** code. To exercise worktree code through the CLI:

```bash
PYTHONPATH=. ./.venv/bin/python -m skannonser.cli <args>
```

## Adding a migration? Check for a number collision first

`skannonser/store/migrations/` is numbered sequentially, and the runner records
applied migrations by filename stem. Two sessions on separate branches will
happily both create an `013_*.sql`; after a merge both survive with distinct
stems and run in lexical order, which is not necessarily the order either author
intended. Before naming one:

```bash
git fetch && ls skannonser/store/migrations/ && git log --oneline origin/master -5
```

`ALL_MIGRATIONS` in `tests/rebuild/test_migrations.py` is an ordered list that
every migration appends to, so it conflicts on merge whenever two branches add
one. Expect to resolve it by keeping **both** lines in numeric order.

## Worktrees isolate code, not shared state

`main/database/properties.db`, the dev-server ports, and the FINN crawl budget
are single shared resources no matter how many worktrees exist. Two sessions
running pipeline commands at once contend for the same SQLite file.

The live DB on the server (`mbp2016@100.77.139.22`, tailnet) is written **only**
by the server. Committing a migration does not deploy it: that needs a merge, a
pull on the server, `skannonser db migrate`, and a container restart.
