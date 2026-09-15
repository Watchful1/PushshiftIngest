# PushshiftIngest design

Date: 2026-09-14
Status: approved in discussion, pending written review

## Background

RemindMeBot and UpdateMeBot find trigger comments through a shared ingest
pipeline. CommentStreamer walks Reddit's sequential comment ID space, does a
lowercase substring match against a fixed set of trigger terms, and writes hits
as `IngestComment` rows into a SQLite file. Each bot drains its own client's
rows from that file every loop.

Reddit is making comment IDs non-sequential in roughly two weeks, which breaks
the ID walk. Pushshift is receiving Reddit's firehose and will keep working, and
we have authorised access to its search API. The bots themselves are unaffected.

This is a temporary bridge. Both bots are being migrated to a Reddit hosted
service with native triggers over the next few months.

## Goals

- Replace the ID walk with a Pushshift keyword poller that produces the same
  `IngestComment` rows the bots consume today.
- Leave RemindMeBot, UpdateMeBot, and CommentStreamer source untouched.
- Survive an unstable Pushshift with retries, backoff, and token refresh, and
  never crash the process on a request failure.
- Tolerate out of order arrival. Never stop looking back on the first seen
  comment.
- Run side by side with the existing pipeline and report, in real time, how
  many comments each side misses.
- Manual cutover. No automatic detection.

## Non-goals

- Replacing the full firehose dumps that CommentStreamer writes for
  PushshiftDumps. Keyword search cannot produce those.
- Automatic cutover detection.
- Long term maintainability beyond a few months.

## Components

A new project `PushshiftIngest` next to CommentStreamer, using the same
dependency style: `discord-logging`, `requests`, `sqlalchemy`,
`prometheus-client`, and `praw-wrapper` for `IngestDatabase` and
`IngestComment`.

| Module | Responsibility |
|---|---|
| `src/main.py` | Argument parsing, startup, the main loop, signal handling. |
| `src/pushshift.py` | `PushshiftClient`: one HTTP request with timeout, retry classification, bearer auth, token refresh. |
| `src/matching.py` | The term table and the per-client substring match. Pure function. |
| `src/store.py` | The poller's own tables (`seen_comments`, `comparison_misses`) and their queries, bound to the same engine as the `IngestDatabase`. |
| `src/comparison.py` | Reads the streamer's audit table and the poller's `seen_comments`, computes the diff, updates gauges and the misses table. |
| `src/counters.py` | Prometheus gauges and counters. |
| `scripts/install_audit_trigger.py` | Creates the audit table and trigger in the streamer's database. Idempotent. Has an `--uninstall` flag. |

## Data flow

```
Pushshift search API
        |  one page every 30 s
        v
PushshiftClient.search()  ->  list of comment dicts
        |
        v
matching.match_clients(body)  ->  [(client_name, term)]
        |
        v
store.upsert_seen()   -> seen_comments (poller db)      always
        |
        +-- if --active and row is new -> IngestComment (poller db, ingest_comments)
                                                |
                                                v
                                     RemindMeBot / UpdateMeBot drain as today

CommentStreamer -> ingest_comments (streamer db) --trigger--> ingest_audit (streamer db)
                                                                    |
comparison.run() reads ingest_audit + seen_comments  -> gauges, comparison_misses, logs
```

## Term matching

The term table is a constant in `matching.py`, identical to the one in
CommentStreamer's `main.py`:

| Client | Terms |
|---|---|
| `remindme` | `remindme`, `remind me`, `remindmerepeat`, `cakeday` |
| `updateme` | `updateme`, `subscribeme`, `subscribeall` |

`match_clients(body)` lowercases the body once and returns one entry per
client whose term list has any substring hit, with the first matching term.
This is the same logic as `Ingest.check_process_request` in CommentStreamer, so
the bots receive identical input from both pipelines. Comments whose `author`
is missing or `[deleted]` are skipped, matching the streamer.

The search query sent to Pushshift is a single `q` parameter joining all seven
terms with the OR syntax the old `TRIGGER_COMBINED` constant used, with the
two word phrase quoted:
`remindme|"remind me"|remindmerepeat|cakeday|updateme|subscribeme|subscribeall`.
The exact string is a constant in `pushshift.py`. It is verified against the
Pushshift guide during implementation and adjusted there if the syntax differs.
Because the poller matches locally on the body, an over broad query only costs
bandwidth and an under broad query is caught by the comparison.

## The poll loop

Every cycle, default 30 seconds:

1. Request one page from `https://api.pushshift.io/reddit/comment/search` with
   `q`, `limit=250`, `order=desc`, no `before` or `after`. Trigger traffic is
   roughly one comment every couple of minutes, so one page covers hours.
2. For every returned comment, run `match_clients`. For each match, upsert into
   `seen_comments` keyed on `(id, client)`. Existing rows are left alone, so
   late and out of order arrivals are handled by the table, never by stopping
   at the first seen comment.
3. If `--active`, and the row was newly inserted this cycle, also insert an
   `IngestComment` into `ingest_comments` for that client and mark the row
   `queued`. Rows that existed before activation are never queued, so the bots
   do not replay history at cutover.
4. Run the comparison (section below).
5. Prune `seen_comments` older than 7 days and `comparison_misses` older than
   30 days. Runs once an hour.
6. Update gauges and sleep.

### Catch up after an outage

`last_success_utc` is stored in the poller's key value table. When a request
succeeds and `now - last_success_utc` exceeds 60 minutes, the poller pages
backward: it repeats the request with `before=<oldest created_utc on the
previous page>` until the oldest comment on a page is older than
`last_success_utc - 15 min`, a page comes back empty, or 10 pages have been
fetched. Every page goes through the same upsert. Then it returns to single
page mode. On a fresh database `last_success_utc` is set to the start time,
so the first run does not backfill.

### Failure handling

`PushshiftClient.search()` returns either a result list or a failure reason.
It never raises out of the loop.

- Timeout is 20 seconds per request.
- Any exception, non-200 status, or unparseable body is a failure. Consecutive
  failures double the sleep from 30 s up to a 300 s cap. A success resets it.
- A 401 or 403 triggers a token refresh before the next attempt.
- After 5 consecutive failures a single Discord warning is logged. Further
  failures log at info until the next success, which logs an info recovery
  line with the outage length.
- `sys.exit` is only used at startup: missing token, unwritable database, or
  unreadable streamer database when `--streamer_db` is given.

### Token handling

The bearer token lives in `pushshift_token.txt` in the project directory,
gitignored, following the pattern in PushshiftDumps. The initial token is
pasted in by hand. Refresh follows `merge_and_backfill.py`:

- `POST https://auth.pushshift.io/refresh?access_token=<current>`.
- Response with `access_token`: save it to the file and use it.
- Response with detail `Access token is still active and can not be
  refreshed.`: keep the current token.
- Anything else: log a warning, keep the current token, and let the normal
  backoff retry. Never exit.

Token values are never written to the log.

## Storage

The poller owns one SQLite file, default `database.db` in the project
directory, overridable with `--db`. It is opened with `praw_wrapper.IngestDatabase`
so the standard `clients`, `search`, `ingest_comments`, and `key_value` tables
exist and the bots can be pointed at it unchanged. The poller registers both
clients and all seven searches on startup, exactly as the bots do.

The poller's own tables are declared in `store.py` on a separate declarative
base and created against the same engine.

`seen_comments`

| Column | Notes |
|---|---|
| `id` | Reddit comment ID. Primary key with `client`. |
| `client` | Client name, `remindme` or `updateme`. |
| `matched_term` | The term that hit. |
| `author`, `subreddit`, `created_utc`, `permalink`, `link_id`, `body` | As returned by Pushshift. |
| `retrieved_utc` | Pushshift's own ingest timestamp, nullable. |
| `first_seen_utc` | When the poller first stored it. |
| `queued` | 1 if an `IngestComment` was written for it. |

`comparison_misses`

| Column | Notes |
|---|---|
| `id`, `client` | Primary key. |
| `side` | `streamer_only` or `pushshift_only`. |
| `created_utc` | The comment's creation time. |
| `flagged_utc` | When first flagged. |
| `resolved_utc` | Set if the other side later saw it within the window. Nullable. |

Key value entries: `last_success_utc`, `comparison_start_utc`.

## The audit trigger in the streamer database

`scripts/install_audit_trigger.py --db <path>` runs, with `IF NOT EXISTS`:

```sql
CREATE TABLE IF NOT EXISTS ingest_audit (
    id TEXT NOT NULL,
    client_id INTEGER NOT NULL,
    author TEXT, subreddit TEXT, created_utc INTEGER,
    permalink TEXT, link_id TEXT, body TEXT,
    inserted_utc INTEGER NOT NULL
);
CREATE TRIGGER IF NOT EXISTS ingest_audit_insert
AFTER INSERT ON ingest_comments
BEGIN
    INSERT INTO ingest_audit
    SELECT NEW.id, NEW.client_id, NEW.author, NEW.subreddit, NEW.created_utc,
           NEW.permalink, NEW.link_id, NEW.body, strftime('%s', 'now');
END;
```

There is deliberately no unique constraint on `ingest_audit`, so nothing the
trigger does can make the streamer's commit fail beyond what
`ingest_comments` itself would. SQLAlchemy's `create_all` does not drop
triggers, and the streamer's `--clear` only deletes from `ingest_comments`, so
both survive a streamer restart. `--uninstall` drops the trigger and the
table. The trigger must be installed before the poller starts so the
comparison window is valid from the poller's first cycle.

The audit table grows by well under a thousand rows a day and is never pruned.

## Real time comparison

Runs once per cycle when `--streamer_db` is given. It considers comments with
`created_utc` between `now - 24 h` and `now - 15 min`, and not earlier than
`comparison_start_utc`, which is written on the poller's first run. The
15 minute floor absorbs Pushshift lag so a comment is not flagged before both
sides have had a chance to see it.

Streamer side: read `ingest_audit` in the window, and map `client_id` to a
client name through the streamer database's own `clients` table, since IDs
can differ between the two files. Pushshift side: read `seen_comments` in the
window. Both reads are read-only.

Per client it computes `both`, `streamer_only`, and `pushshift_only` and sets
the gauge `pushshift_comparison{client, result}`. Each newly appearing miss is
inserted into `comparison_misses`, logged at info with its permalink, and
counted in `pushshift_misses_total{client, side}`. A miss that later appears
on the other side inside the window gets `resolved_utc` set and is logged at
info. If more than 5 new unresolved misses appear within an hour, one Discord
warning is logged, rate limited to once per hour.

This gives running totals in Grafana for how many each pipeline misses, and a
durable list of the specific comments, without any on-demand script.

## Metrics

Prometheus on `--port`, default 8006.

| Metric | Type | Labels |
|---|---|---|
| `pushshift_lag_seconds` | gauge | none. `now - max(created_utc)` on the last successful page. |
| `pushshift_request_results_total` | counter | `result` in `success`, `timeout`, `status_<code>`, `error`, `refresh` |
| `pushshift_consecutive_failures` | gauge | none |
| `pushshift_mode` | gauge | `mode` in `shadow`, `active`, value 1 for the current mode |
| `pushshift_seen_total` | counter | `client` |
| `pushshift_queued_total` | counter | `client` |
| `pushshift_comparison` | gauge | `client`, `result` in `both`, `streamer_only`, `pushshift_only` |
| `pushshift_misses_total` | counter | `client`, `side` |
| `pushshift_ingest_pending` | gauge | `client`. Rows waiting in `ingest_comments`. |

## Command line

```
main.py user [--db PATH] [--streamer_db PATH] [--active] [--interval SECONDS]
             [--port PORT] [--debug] [--once]
```

- `user` is the praw.ini section used for Discord logging, matching the
  sibling projects.
- Shadow mode is the default. `--active` writes `IngestComment` rows.
- `--streamer_db` enables the comparison. Omit it after cutover.
- `--once` runs one cycle and exits, for testing.

## Cutover runbook

Before the deadline, in order:

1. Stop CommentStreamer.
2. Restart PushshiftIngest with `--active` and without `--streamer_db`.
3. Restart RemindMeBot and UpdateMeBot with `--ingest_db` pointing at the
   PushshiftIngest database file.

The gap is seconds. Comments that Pushshift surfaces after activation but
that were created before it are queued a second time. That window is
Pushshift's lag, and the bots' existing thread level dedupe turns those into
message replies rather than duplicate comments, so it is accepted.

## Testing

Pytest with in-memory databases. `IngestDatabase(debug=True)` already gives
an in-memory engine. The streamer audit table is exercised against a
temporary file so the real trigger SQL runs.

- `matching`: each term hits its client, case insensitivity, a body hitting
  both clients yields two entries, deleted author skipped.
- `store`: upsert is idempotent, out of order timestamps do not affect
  presence, queued flag set only in active mode and only for new rows,
  pruning removes only old rows.
- `pushshift`: a fake session injected into the client. Success path, timeout
  path, non-200 path, 403 then refresh then success, refresh returning
  still-active, refresh returning an error. Backoff sequence and reset.
- Catch up: with `last_success_utc` two hours old, paging continues until the
  age condition, stops on an empty page, and stops at the 10 page cap.
- `install_audit_trigger`: install on a fresh file, insert into
  `ingest_comments`, assert the audit row. Install twice is a no-op.
  Uninstall removes both.
- `comparison`: seeded audit and seen tables produce the expected
  `both`, `streamer_only`, and `pushshift_only` sets, respect the 15 minute
  floor, record and then resolve a miss, and honour `comparison_start_utc`.
- `--once` end to end with a fake session: one cycle populates
  `seen_comments`, and in active mode also `ingest_comments`, and a bot
  style `get_comments` on the same `IngestDatabase` returns the row.

## Deployment

Pipenv, same Python as CommentStreamer on the server. Runs as a long lived
process beside the streamer during the comparison period. The token file and
database are gitignored. The project README records the trigger's existence
in the streamer database and the runbook above.
