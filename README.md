# PushshiftIngest

Temporary replacement for CommentStreamer's sequential ID walk. Polls the
Pushshift comment search API for RemindMeBot and UpdateMeBot trigger terms and
writes hits into an ingest database the bots consume unchanged.

Design: `docs/superpowers/specs/2026-09-14-pushshift-ingest-design.md`.

## Setup

    pipenv install
    echo "<bearer token>" > pushshift_token.txt

The token file is gitignored. The poller refreshes the token itself on a
401 or 403 and writes the new one back to the file.

## Running

Shadow mode (default): records what Pushshift finds, writes nothing for the bots.

    pipenv run python src/main.py Watchful1 --streamer_db ../CommentStreamer/database.db

Active mode: also writes `ingest_comments` rows for the bots.

    pipenv run python src/main.py Watchful1 --active

Flags: `--db` (default `database.db`), `--streamer_db`, `--token_file`
(default `pushshift_token.txt`), `--interval` (default 30), `--port`
(default 8006), `--debug`, `--once`.

## The audit trigger in the streamer database

The comparison needs a durable record of what CommentStreamer queues, and the
bots drain `ingest_comments` within seconds. So a SQLite trigger is installed
once into the streamer's database file. It copies every insert into an
`ingest_audit` table. There is no change to the streamer's code and no restart.

    pipenv run python scripts/install_audit_trigger.py --db ../CommentStreamer/database.db

Remove it after cutover with `--uninstall`. If you are reading the streamer's
code and wondering why `ingest_audit` fills up, this is why.

## Comparison

With `--streamer_db`, every cycle diffs the two pipelines over comments
created between 24 hours and 15 minutes ago. Results go to the
`pushshift_comparison` gauge and `pushshift_misses_total` counter, and every
miss is stored in `comparison_misses` with its side. A Discord warning fires
when more than 5 new unresolved misses appear in an hour, at most once an hour.

## Cutover

1. Stop CommentStreamer.
2. Restart PushshiftIngest with `--active` and without `--streamer_db`.
   Use the same `--db` file as the shadow run, since the no-replay guarantee
   comes from the `seen_comments` rows already in it.
3. Restart RemindMeBot and UpdateMeBot with `--ingest_db` pointing at this
   project's database file.

Nothing seen before activation is queued, so the bots do not replay history.

## Tests

    pipenv run pytest
