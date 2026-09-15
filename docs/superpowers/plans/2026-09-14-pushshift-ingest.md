# PushshiftIngest Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A standalone poller that finds RemindMeBot and UpdateMeBot trigger comments through the Pushshift search API, writes them into an ingest database the bots can be re-pointed at, and compares its results in real time against the existing CommentStreamer pipeline.

**Architecture:** One long-lived Python process. Every 30 seconds it fetches one page of Pushshift search results, matches bodies against the per-client term lists, and upserts hits into a `seen_comments` table that is never drained. In `--active` mode newly seen comments are also written as `IngestComment` rows for the bots. A SQLite trigger installed once into the streamer's database records everything the old pipeline queues, and a comparison step diffs the two sides.

**Tech Stack:** Python 3.9, pipenv, requests, SQLAlchemy 2.x, prometheus-client, discord-logging (Watchful1 fork on GitHub), praw-wrapper (Watchful1 fork on GitHub, provides `IngestDatabase` and `IngestComment`), pytest.

**Spec:** `docs/superpowers/specs/2026-09-14-pushshift-ingest-design.md`. Read it first.

**Conventions used by the sibling projects and followed here:**
- Tabs for indentation in Python files.
- `log = discord_logging.get_logger()` in modules, `discord_logging.init_logging()` once in `main.py` and `conftest.py`.
- Tests live in `test/`, import from `src/` via `sys.path.append("src")` in `conftest.py`.
- All timestamps are integer Unix seconds in UTC. `int(time.time())` in production, explicit integers in tests.
- All commands below are run from the project root `C:\Users\greg\Desktop\PyCharm\PushshiftIngest` unless stated otherwise. Tests run with `pipenv run pytest`.

---

## File structure

| Path | Responsibility |
|---|---|
| `Pipfile` | Dependencies, Python 3.9. |
| `.gitignore` | Token file, databases, logs, caches. |
| `pytest.ini` | Warning filter, same as siblings. |
| `src/matching.py` | `SEARCH_TERMS` table, `match_clients(body)`, `is_valid_author(author)`. Pure functions. |
| `src/store.py` | `SeenComment` and `ComparisonMiss` models, `Store` class wrapping the poller's own queries on top of an `IngestDatabase`. |
| `src/pushshift.py` | `TokenStore`, `PushshiftClient` with `search()`, `refresh_token()`, `sleep_seconds()`. HTTP only, no database. |
| `src/comparison.py` | `read_streamer_audit()` and `Comparison` class. |
| `src/counters.py` | Prometheus metrics. |
| `src/main.py` | Argument parsing, `process_page()`, `catch_up()`, `run_cycle()`, main loop. |
| `scripts/install_audit_trigger.py` | `install(path)`, `uninstall(path)`, CLI. |
| `test/conftest.py` | Logging init, path setup, `ingest_db` and `store` fixtures. |
| `test/fakes.py` | `FakeResponse`, `FakeSession` for HTTP tests. |
| `test/matching_test.py`, `test/store_test.py`, `test/pushshift_test.py`, `test/trigger_test.py`, `test/comparison_test.py`, `test/main_test.py` | One test module per source module. |
| `README.md` | What it is, how to run, the trigger note, the cutover runbook. |

---

### Task 1: Project scaffold

**Files:**
- Create: `Pipfile`
- Create: `.gitignore`
- Create: `pytest.ini`
- Create: `src/__init__.py`
- Create: `test/conftest.py`
- Create: `test/fakes.py`

- [ ] **Step 1: Create `Pipfile`**

```toml
[[source]]
name = "pypi"
url = "https://pypi.org/simple"
verify_ssl = true

[dev-packages]
pytest = "*"

[packages]
discord-logging = {git = "https://github.com/Watchful1/DiscordLogging.git"}
requests = "*"
sqlalchemy = "*"
prometheus-client = "*"
praw-wrapper = {editable = true, git = "https://github.com/Watchful1/PrawWrapper.git"}

[requires]
python_version = "3.9"
```

- [ ] **Step 2: Create `.gitignore`**

```
pushshift_token.txt
*.db
*.db-journal
logs/
__pycache__/
.pytest_cache/
.idea/
```

- [ ] **Step 3: Create `pytest.ini`**

```ini
[pytest]
filterwarnings =
    ignore::DeprecationWarning
```

- [ ] **Step 4: Create `src/__init__.py`** as an empty file.

- [ ] **Step 5: Create `test/conftest.py`**

```python
import sys
import pytest
import discord_logging

log = discord_logging.init_logging(debug=True)

sys.path.append("src")

from praw_wrapper import IngestDatabase
from store import Store


@pytest.fixture
def ingest_db():
	database = IngestDatabase(debug=True)
	database.set_default_client("remindme")
	database.get_or_add_client("updateme")
	return database


@pytest.fixture
def store(ingest_db):
	return Store(ingest_db)
```

Note: `Store` does not exist until Task 3. Until then the import will fail, so Task 1 verifies only that dependencies install. Task 3 makes the fixture importable.

- [ ] **Step 6: Create `test/fakes.py`**

```python
import requests


class FakeResponse:
	def __init__(self, status_code, payload=None, bad_json=False):
		self.status_code = status_code
		self.payload = payload
		self.bad_json = bad_json

	def json(self):
		if self.bad_json:
			raise ValueError("not json")
		return self.payload


class FakeSession:
	"""Returns queued responses in order. An Exception instance in the queue is raised instead of returned."""
	def __init__(self, responses):
		self.responses = list(responses)
		self.calls = []

	def _next(self, method, url, params):
		self.calls.append((method, url, params))
		if not self.responses:
			raise AssertionError("FakeSession ran out of responses")
		item = self.responses.pop(0)
		if isinstance(item, Exception):
			raise item
		return item

	def get(self, url, params=None, headers=None, timeout=None):
		return self._next("get", url, params)

	def post(self, url, params=None, headers=None, timeout=None):
		return self._next("post", url, params)


def timeout_error():
	return requests.exceptions.Timeout("timed out")
```

- [ ] **Step 7: Install dependencies**

Run: `pipenv install --dev`
Expected: completes without error and creates `Pipfile.lock`.

- [ ] **Step 8: Verify imports**

Run: `pipenv run python -c "import praw_wrapper, discord_logging, sqlalchemy, prometheus_client, requests; print('ok')"`
Expected: `ok`

- [ ] **Step 9: Commit**

```bash
git add Pipfile Pipfile.lock .gitignore pytest.ini src/__init__.py test/conftest.py test/fakes.py
git commit -m "Scaffold PushshiftIngest project"
```

---

### Task 2: Term matching

**Files:**
- Create: `src/matching.py`
- Create: `test/matching_test.py`

- [ ] **Step 1: Write the failing tests**

`test/matching_test.py`:

```python
import matching


def test_each_remindme_term_matches_remindme():
	for term in ["remindme", "remind me", "remindmerepeat", "cakeday"]:
		assert matching.match_clients(f"hello {term} 1 day") == [("remindme", term)]


def test_each_updateme_term_matches_updateme():
	for term in ["updateme", "subscribeme", "subscribeall"]:
		assert matching.match_clients(f"{term}!") == [("updateme", term)]


def test_case_insensitive():
	assert matching.match_clients("RemindMe! 2 weeks") == [("remindme", "remindme")]


def test_first_matching_term_reported():
	# "remindmerepeat" contains "remindme", and "remindme" is listed first
	assert matching.match_clients("remindmerepeat! 1 day") == [("remindme", "remindme")]


def test_both_clients_match():
	result = matching.match_clients("RemindMe! 1 day and UpdateMe!")
	assert result == [("remindme", "remindme"), ("updateme", "updateme")]


def test_no_match():
	assert matching.match_clients("just a normal comment") == []


def test_valid_author():
	assert matching.is_valid_author("Watchful1")
	assert not matching.is_valid_author(None)
	assert not matching.is_valid_author("[deleted]")
```

- [ ] **Step 2: Run to verify failure**

Run: `pipenv run pytest test/matching_test.py -v`
Expected: import error on `store` from conftest. That is expected until Task 3. To run this task's tests in isolation, temporarily run with `pipenv run pytest test/matching_test.py -v -p no:cacheprovider --noconftest` after adding `sys.path.append("src")` at the top of the test file. Simpler: create a minimal `src/store.py` placeholder now containing only `class Store:\n\tdef __init__(self, ingest_database):\n\t\tself.db = ingest_database` so conftest imports. Task 3 replaces it entirely.
Expected after placeholder: `ModuleNotFoundError: No module named 'matching'`.

- [ ] **Step 3: Implement `src/matching.py`**

```python
SEARCH_TERMS = {
	"remindme": ["remindme", "remind me", "remindmerepeat", "cakeday"],
	"updateme": ["updateme", "subscribeme", "subscribeall"],
}

CLIENT_NAMES = list(SEARCH_TERMS.keys())


def match_clients(body):
	"""Return [(client_name, term)] for every client with at least one substring hit.

	Same logic as CommentStreamer's Ingest.check_process_request so both pipelines
	feed the bots identical comments.
	"""
	body_lower = body.lower()
	matches = []
	for client_name, terms in SEARCH_TERMS.items():
		for term in terms:
			if term in body_lower:
				matches.append((client_name, term))
				break
	return matches


def is_valid_author(author):
	return author is not None and author != "[deleted]"
```

- [ ] **Step 4: Run to verify pass**

Run: `pipenv run pytest test/matching_test.py -v`
Expected: 7 passed.

- [ ] **Step 5: Commit**

```bash
git add src/matching.py src/store.py test/matching_test.py
git commit -m "Add trigger term matching"
```

---

### Task 3: Store

**Files:**
- Replace: `src/store.py`
- Create: `test/store_test.py`

The `Store` wraps the `IngestDatabase` from praw-wrapper. It reuses that object's `session` and `engine` so the poller's tables live in the same file and the same transactions as `ingest_comments`.

Reference for what `IngestDatabase` offers (in `praw_wrapper/ingest.py`): `session`, `engine`, `commit()`, `get_or_add_client(name)` returning an object with `.id` and `.name`, `register_search(search_term, client_name)`, `add_comment(IngestComment)`, `get_comments(client=None, limit=100)`, `save_keystore(key, value)`, `get_keystore(key)`. `IngestComment(id, author, subreddit, created_utc, permalink, link_id, body, client_id)`.

- [ ] **Step 1: Write the failing tests**

`test/store_test.py`:

```python
from store import SeenComment, ComparisonMiss


def make_comment(comment_id="abc123", created_utc=1000, author="Watchful1", retrieved_utc=1010):
	return {
		"id": comment_id,
		"author": author,
		"subreddit": "test",
		"created_utc": created_utc,
		"retrieved_utc": retrieved_utc,
		"permalink": f"/r/test/comments/thread1/_/{comment_id}/",
		"link_id": "t3_thread1",
		"body": "RemindMe! 1 day",
	}


def test_upsert_new_returns_row(store):
	row = store.upsert_seen(make_comment(), "remindme", "remindme", now_utc=2000)
	assert row is not None
	assert row.id == "abc123"
	assert row.client == "remindme"
	assert row.matched_term == "remindme"
	assert row.first_seen_utc == 2000
	assert row.queued is False


def test_upsert_existing_returns_none(store):
	assert store.upsert_seen(make_comment(), "remindme", "remindme", now_utc=2000) is not None
	assert store.upsert_seen(make_comment(), "remindme", "remindme", now_utc=3000) is None
	assert store.count_seen() == 1


def test_same_id_different_client_is_separate_row(store):
	assert store.upsert_seen(make_comment(), "remindme", "remindme", now_utc=2000) is not None
	assert store.upsert_seen(make_comment(), "updateme", "updateme", now_utc=2000) is not None
	assert store.count_seen() == 2


def test_out_of_order_arrival_is_stored(store):
	store.upsert_seen(make_comment("new1", created_utc=5000), "remindme", "remindme", now_utc=6000)
	assert store.upsert_seen(make_comment("old1", created_utc=1000), "remindme", "remindme", now_utc=6001) is not None
	assert store.count_seen() == 2


def test_missing_retrieved_utc_is_allowed(store):
	comment = make_comment()
	del comment["retrieved_utc"]
	row = store.upsert_seen(comment, "remindme", "remindme", now_utc=2000)
	assert row.retrieved_utc is None


def test_queue_writes_ingest_comment(store, ingest_db):
	row = store.upsert_seen(make_comment(), "remindme", "remindme", now_utc=2000)
	store.queue(row)
	store.commit()
	assert row.queued is True
	pending = ingest_db.get_comments(client=ingest_db.get_or_add_client("remindme"))
	assert len(pending) == 1
	assert pending[0].id == "abc123"
	assert pending[0].body == "RemindMe! 1 day"
	assert store.count_pending("remindme") == 1
	assert store.count_pending("updateme") == 0


def test_prune_removes_only_old_rows(store):
	week = 7 * 24 * 3600
	now = 10 * week
	store.upsert_seen(make_comment("old", created_utc=now - week - 1), "remindme", "remindme", now_utc=now)
	store.upsert_seen(make_comment("new", created_utc=now - week + 1), "remindme", "remindme", now_utc=now)
	store.add_miss("m_old", "remindme", "streamer_only", created_utc=1, flagged_utc=now - 31 * 24 * 3600)
	store.add_miss("m_new", "remindme", "streamer_only", created_utc=1, flagged_utc=now - 29 * 24 * 3600)
	store.prune(now_utc=now)
	store.commit()
	assert [row.id for row in store.get_seen_between(0, now)] == ["new"]
	assert store.get_miss("m_old", "remindme") is None
	assert store.get_miss("m_new", "remindme") is not None


def test_get_seen_between_is_inclusive(store):
	store.upsert_seen(make_comment("a", created_utc=100), "remindme", "remindme", now_utc=1)
	store.upsert_seen(make_comment("b", created_utc=200), "remindme", "remindme", now_utc=1)
	store.upsert_seen(make_comment("c", created_utc=300), "remindme", "remindme", now_utc=1)
	assert sorted(row.id for row in store.get_seen_between(100, 200)) == ["a", "b"]


def test_misses(store):
	assert store.get_miss("x", "remindme") is None
	miss = store.add_miss("x", "remindme", "pushshift_only", created_utc=50, flagged_utc=100)
	assert miss.resolved_utc is None
	store.resolve_miss(miss, resolved_utc=150)
	assert store.get_miss("x", "remindme").resolved_utc == 150
	assert store.count_unresolved_misses_since(flagged_after=0) == 0
	store.add_miss("y", "updateme", "streamer_only", created_utc=50, flagged_utc=120)
	assert store.count_unresolved_misses_since(flagged_after=110) == 1
	assert store.count_unresolved_misses_since(flagged_after=121) == 0


def test_int_keystore(store):
	assert store.get_int_key("last_success_utc") is None
	store.set_int_key("last_success_utc", 12345)
	store.commit()
	assert store.get_int_key("last_success_utc") == 12345
```

- [ ] **Step 2: Run to verify failure**

Run: `pipenv run pytest test/store_test.py -v`
Expected: `ImportError: cannot import name 'SeenComment'`.

- [ ] **Step 3: Implement `src/store.py`**

```python
import discord_logging
from sqlalchemy import Column, Integer, String, Boolean
from sqlalchemy.orm import declarative_base
from praw_wrapper import IngestComment

log = discord_logging.get_logger()

Base = declarative_base()

SEEN_RETENTION_SECONDS = 7 * 24 * 3600
MISS_RETENTION_SECONDS = 30 * 24 * 3600


class SeenComment(Base):
	__tablename__ = 'seen_comments'

	id = Column(String(12), primary_key=True)
	client = Column(String(20), primary_key=True)
	matched_term = Column(String(40), nullable=False)
	author = Column(String(80), nullable=False)
	subreddit = Column(String(80), nullable=False)
	created_utc = Column(Integer, nullable=False, index=True)
	retrieved_utc = Column(Integer, nullable=True)
	permalink = Column(String(400), nullable=False)
	link_id = Column(String(12), nullable=False)
	body = Column(String(10000), nullable=False)
	first_seen_utc = Column(Integer, nullable=False)
	queued = Column(Boolean, nullable=False, default=False)


class ComparisonMiss(Base):
	__tablename__ = 'comparison_misses'

	id = Column(String(12), primary_key=True)
	client = Column(String(20), primary_key=True)
	side = Column(String(20), nullable=False)
	created_utc = Column(Integer, nullable=False)
	flagged_utc = Column(Integer, nullable=False, index=True)
	resolved_utc = Column(Integer, nullable=True)


class Store:
	def __init__(self, ingest_database):
		self.db = ingest_database
		self.session = ingest_database.session
		Base.metadata.create_all(ingest_database.engine)

	def commit(self):
		self.session.commit()

	# seen comments

	def upsert_seen(self, comment, client, term, now_utc):
		"""Insert a seen row if absent. Returns the new row, or None if it already existed."""
		existing = self.session.get(SeenComment, {"id": comment["id"], "client": client})
		if existing is not None:
			return None
		row = SeenComment(
			id=comment["id"],
			client=client,
			matched_term=term,
			author=comment["author"],
			subreddit=comment["subreddit"],
			created_utc=int(comment["created_utc"]),
			retrieved_utc=int(comment["retrieved_utc"]) if comment.get("retrieved_utc") is not None else None,
			permalink=comment["permalink"],
			link_id=comment["link_id"],
			body=comment["body"],
			first_seen_utc=now_utc,
			queued=False,
		)
		self.session.add(row)
		return row

	def queue(self, row):
		client = self.db.get_or_add_client(row.client)
		self.db.add_comment(IngestComment(
			id=row.id,
			author=row.author,
			subreddit=row.subreddit,
			created_utc=row.created_utc,
			permalink=row.permalink,
			link_id=row.link_id,
			body=row.body,
			client_id=client.id,
		))
		row.queued = True

	def count_seen(self):
		return self.session.query(SeenComment).count()

	def get_seen_between(self, start_utc, end_utc):
		return self.session.query(SeenComment) \
			.filter(SeenComment.created_utc >= start_utc) \
			.filter(SeenComment.created_utc <= end_utc) \
			.all()

	def count_pending(self, client_name):
		client = self.db.get_or_add_client(client_name)
		return self.db.get_count_comments(client)

	# comparison misses

	def get_miss(self, comment_id, client):
		return self.session.get(ComparisonMiss, {"id": comment_id, "client": client})

	def add_miss(self, comment_id, client, side, created_utc, flagged_utc):
		miss = ComparisonMiss(
			id=comment_id, client=client, side=side, created_utc=created_utc, flagged_utc=flagged_utc
		)
		self.session.add(miss)
		return miss

	def resolve_miss(self, miss, resolved_utc):
		miss.resolved_utc = resolved_utc

	def count_unresolved_misses_since(self, flagged_after):
		return self.session.query(ComparisonMiss) \
			.filter(ComparisonMiss.flagged_utc > flagged_after) \
			.filter(ComparisonMiss.resolved_utc.is_(None)) \
			.count()

	# retention

	def prune(self, now_utc):
		# default synchronize_session so deleted objects leave the identity map
		seen_deleted = self.session.query(SeenComment) \
			.filter(SeenComment.created_utc < now_utc - SEEN_RETENTION_SECONDS) \
			.delete()
		miss_deleted = self.session.query(ComparisonMiss) \
			.filter(ComparisonMiss.flagged_utc < now_utc - MISS_RETENTION_SECONDS) \
			.delete()
		if seen_deleted or miss_deleted:
			log.info(f"Pruned {seen_deleted} seen comments and {miss_deleted} misses")

	# integer key value

	def get_int_key(self, key):
		value = self.db.get_keystore(key)
		return int(value) if value is not None else None

	def set_int_key(self, key, value):
		self.db.save_keystore(key, str(int(value)))
```

- [ ] **Step 4: Run to verify pass**

Run: `pipenv run pytest test/store_test.py test/matching_test.py -v`
Expected: all passed.

- [ ] **Step 5: Commit**

```bash
git add src/store.py test/store_test.py
git commit -m "Add seen comment and comparison miss store"
```

---

### Task 4: Pushshift client

**Files:**
- Create: `src/pushshift.py`
- Create: `test/pushshift_test.py`

- [ ] **Step 1: Write the failing tests**

`test/pushshift_test.py`:

```python
import pytest
import pushshift
from fakes import FakeResponse, FakeSession, timeout_error


@pytest.fixture
def token_file(tmp_path):
	path = tmp_path / "pushshift_token.txt"
	path.write_text("token-one\n")
	return path


def make_client(token_file, responses):
	session = FakeSession(responses)
	client = pushshift.PushshiftClient(pushshift.TokenStore(token_file), session=session)
	return client, session


def test_token_store_round_trip(token_file):
	store = pushshift.TokenStore(token_file)
	assert store.load() == "token-one"
	store.save("token-two")
	assert store.load() == "token-two"


def test_token_store_missing_file(tmp_path):
	store = pushshift.TokenStore(tmp_path / "missing.txt")
	assert store.load() is None


def test_search_success(token_file):
	page = [{"id": "a", "created_utc": 100}]
	client, session = make_client(token_file, [FakeResponse(200, {"data": page})])
	comments, reason = client.search()
	assert comments == page
	assert reason is None
	assert client.consecutive_failures == 0
	method, url, params = session.calls[0]
	assert url == pushshift.SEARCH_URL
	assert params["q"] == pushshift.QUERY
	assert params["limit"] == 250
	assert params["order"] == "desc"
	assert "before" not in params


def test_search_with_before(token_file):
	client, session = make_client(token_file, [FakeResponse(200, {"data": []})])
	client.search(before=12345)
	assert session.calls[0][2]["before"] == 12345


def test_search_timeout(token_file):
	client, _ = make_client(token_file, [timeout_error()])
	comments, reason = client.search()
	assert comments is None
	assert reason == "timeout"
	assert client.consecutive_failures == 1


def test_search_non_200(token_file):
	client, _ = make_client(token_file, [FakeResponse(502)])
	comments, reason = client.search()
	assert comments is None
	assert reason == "status_502"


def test_search_bad_json(token_file):
	client, _ = make_client(token_file, [FakeResponse(200, bad_json=True)])
	comments, reason = client.search()
	assert comments is None
	assert reason == "parse"


def test_search_missing_data_key(token_file):
	client, _ = make_client(token_file, [FakeResponse(200, {"nope": []})])
	comments, reason = client.search()
	assert reason == "parse"


def test_search_generic_exception(token_file):
	client, _ = make_client(token_file, [RuntimeError("boom")])
	comments, reason = client.search()
	assert reason == "error"


def test_failures_accumulate_and_reset(token_file):
	client, _ = make_client(token_file, [FakeResponse(500), FakeResponse(500), FakeResponse(200, {"data": []})])
	client.search()
	client.search()
	assert client.consecutive_failures == 2
	client.search()
	assert client.consecutive_failures == 0


def test_backoff_sequence(token_file):
	client, _ = make_client(token_file, [])
	assert client.sleep_seconds(30) == 30
	client.consecutive_failures = 1
	assert client.sleep_seconds(30) == 30
	client.consecutive_failures = 2
	assert client.sleep_seconds(30) == 60
	client.consecutive_failures = 3
	assert client.sleep_seconds(30) == 120
	client.consecutive_failures = 4
	assert client.sleep_seconds(30) == 240
	client.consecutive_failures = 5
	assert client.sleep_seconds(30) == 300
	client.consecutive_failures = 20
	assert client.sleep_seconds(30) == 300


def test_403_refreshes_then_succeeds(token_file):
	client, session = make_client(token_file, [
		FakeResponse(403, {"detail": "expired"}),
		FakeResponse(200, {"access_token": "token-two"}),
		FakeResponse(200, {"data": []}),
	])
	comments, reason = client.search()
	assert comments is None
	assert reason == "status_403"
	assert client.token == "token-two"
	assert pushshift.TokenStore(token_file).load() == "token-two"
	assert session.calls[1][0] == "post"
	assert session.calls[1][1] == pushshift.REFRESH_URL
	assert session.calls[1][2] == {"access_token": "token-one"}
	comments, reason = client.search()
	assert reason is None


def test_401_also_refreshes(token_file):
	client, session = make_client(token_file, [
		FakeResponse(401),
		FakeResponse(200, {"access_token": "token-two"}),
	])
	client.search()
	assert client.token == "token-two"


def test_refresh_still_active_keeps_token(token_file):
	client, _ = make_client(token_file, [
		FakeResponse(200, {"detail": pushshift.STILL_ACTIVE_DETAIL}),
	])
	assert client.refresh_token() == "still_active"
	assert client.token == "token-one"


def test_refresh_rejected_keeps_token(token_file):
	client, _ = make_client(token_file, [FakeResponse(200, {"detail": "no"})])
	assert client.refresh_token() == "rejected"
	assert client.token == "token-one"


def test_refresh_exception_keeps_token(token_file):
	client, _ = make_client(token_file, [timeout_error()])
	assert client.refresh_token() == "error"
	assert client.token == "token-one"


def test_bearer_header_uses_current_token(token_file):
	class HeaderSession(FakeSession):
		def get(self, url, params=None, headers=None, timeout=None):
			self.headers = headers
			return super().get(url, params, headers, timeout)
	session = HeaderSession([FakeResponse(200, {"data": []})])
	client = pushshift.PushshiftClient(pushshift.TokenStore(token_file), session=session)
	client.search()
	assert session.headers["Authorization"] == "Bearer token-one"
	assert "User-Agent" in session.headers
```

- [ ] **Step 2: Run to verify failure**

Run: `pipenv run pytest test/pushshift_test.py -v`
Expected: `ModuleNotFoundError: No module named 'pushshift'`.

- [ ] **Step 3: Implement `src/pushshift.py`**

```python
import os
import requests
import discord_logging

log = discord_logging.get_logger()

SEARCH_URL = "https://api.pushshift.io/reddit/comment/search"
REFRESH_URL = "https://auth.pushshift.io/refresh"
# OR syntax as used by the old RemindMeBot TRIGGER_COMBINED constant. Verified against
# https://api.pushshift.io/guide during implementation; adjust here if the syntax differs.
QUERY = 'remindme|"remind me"|remindmerepeat|cakeday|updateme|subscribeme|subscribeall'
USER_AGENT = "PushshiftIngest by u/Watchful1"
PAGE_SIZE = 250
MAX_BACKOFF_SECONDS = 300
STILL_ACTIVE_DETAIL = "Access token is still active and can not be refreshed."


class TokenStore:
	def __init__(self, path):
		self.path = str(path)

	def load(self):
		if not os.path.exists(self.path):
			return None
		with open(self.path, 'r') as handle:
			token = handle.read().strip()
		return token if token else None

	def save(self, token):
		with open(self.path, 'w') as handle:
			handle.write(token)


class PushshiftClient:
	"""One HTTP call per method. Never raises out of search() or refresh_token()."""
	def __init__(self, token_store, session=None, timeout=20):
		self.token_store = token_store
		self.token = token_store.load()
		self.session = session if session is not None else requests.Session()
		self.timeout = timeout
		self.consecutive_failures = 0

	def _headers(self):
		return {
			'User-Agent': USER_AGENT,
			'Authorization': f"Bearer {self.token}",
		}

	def _fail(self, reason):
		self.consecutive_failures += 1
		return None, reason

	def search(self, before=None, limit=PAGE_SIZE):
		"""Returns (comments, None) on success or (None, reason) on failure.

		reason is one of: timeout, error, parse, status_<code>.
		A 401 or 403 triggers a token refresh before returning.
		"""
		params = {"q": QUERY, "limit": limit, "order": "desc"}
		if before is not None:
			params["before"] = before
		try:
			response = self.session.get(SEARCH_URL, params=params, headers=self._headers(), timeout=self.timeout)
		except requests.exceptions.Timeout:
			return self._fail("timeout")
		except Exception as err:
			log.info(f"Pushshift request error: {type(err).__name__}: {err}")
			return self._fail("error")

		if response.status_code in (401, 403):
			log.info(f"Pushshift returned {response.status_code}, refreshing token")
			self.refresh_token()
			return self._fail(f"status_{response.status_code}")
		if response.status_code != 200:
			return self._fail(f"status_{response.status_code}")

		try:
			comments = response.json()["data"]
		except Exception as err:
			log.info(f"Pushshift parse error: {type(err).__name__}: {err}")
			return self._fail("parse")

		self.consecutive_failures = 0
		return comments, None

	def sleep_seconds(self, base_seconds):
		if self.consecutive_failures <= 1:
			return base_seconds
		return min(base_seconds * (2 ** (self.consecutive_failures - 1)), MAX_BACKOFF_SECONDS)

	def refresh_token(self):
		"""Returns one of: refreshed, still_active, rejected, error. Never logs the token value."""
		try:
			response = self.session.post(
				REFRESH_URL, params={"access_token": self.token}, headers={'User-Agent': USER_AGENT}, timeout=self.timeout)
			result = response.json()
		except Exception as err:
			log.warning(f"Pushshift token refresh failed: {type(err).__name__}: {err}")
			return "error"

		if isinstance(result, dict) and 'access_token' in result:
			self.token = result['access_token']
			self.token_store.save(self.token)
			log.info("Refreshed pushshift token")
			return "refreshed"

		detail = result.get('detail') if isinstance(result, dict) else None
		if detail == STILL_ACTIVE_DETAIL:
			log.info("Pushshift token still active, keeping it")
			return "still_active"

		log.warning(f"Pushshift token refresh rejected: {detail}")
		return "rejected"
```

- [ ] **Step 4: Run to verify pass**

Run: `pipenv run pytest test/pushshift_test.py -v`
Expected: 18 passed.

- [ ] **Step 5: Commit**

```bash
git add src/pushshift.py test/pushshift_test.py
git commit -m "Add pushshift search client with token refresh"
```

---

### Task 5: Audit trigger installer

**Files:**
- Create: `scripts/install_audit_trigger.py`
- Create: `test/trigger_test.py`

- [ ] **Step 1: Write the failing tests**

`test/trigger_test.py`:

```python
import sqlite3
import sys
import os

sys.path.append("scripts")

import install_audit_trigger
from praw_wrapper import IngestDatabase, IngestComment


def make_streamer_db(tmp_path):
	path = str(tmp_path / "streamer.db")
	database = IngestDatabase(location=path)
	database.set_default_client("remindme")
	return path, database


def add_comment(database, comment_id):
	client = database.get_or_add_client("remindme")
	database.add_comment(IngestComment(
		id=comment_id, author="Watchful1", subreddit="test", created_utc=1000,
		permalink=f"/r/test/comments/t1/_/{comment_id}/", link_id="t3_t1", body="RemindMe! 1 day",
		client_id=client.id,
	))
	database.commit()


def audit_rows(path):
	connection = sqlite3.connect(path)
	rows = connection.execute("select id, client_id, author, body, inserted_utc from ingest_audit order by inserted_utc").fetchall()
	connection.close()
	return rows


def test_install_records_inserts(tmp_path):
	path, database = make_streamer_db(tmp_path)
	install_audit_trigger.install(path)
	add_comment(database, "c1")
	rows = audit_rows(path)
	assert len(rows) == 1
	assert rows[0][0] == "c1"
	assert rows[0][2] == "Watchful1"
	assert rows[0][3] == "RemindMe! 1 day"
	assert rows[0][4] > 0


def test_audit_survives_ingest_delete(tmp_path):
	path, database = make_streamer_db(tmp_path)
	install_audit_trigger.install(path)
	add_comment(database, "c1")
	comment = database.get_comments()[0]
	database.delete_comment(comment)
	database.commit()
	assert len(audit_rows(path)) == 1


def test_install_twice_is_noop(tmp_path):
	path, database = make_streamer_db(tmp_path)
	install_audit_trigger.install(path)
	install_audit_trigger.install(path)
	add_comment(database, "c1")
	assert len(audit_rows(path)) == 1


def test_uninstall_removes_trigger_and_table(tmp_path):
	path, database = make_streamer_db(tmp_path)
	install_audit_trigger.install(path)
	install_audit_trigger.uninstall(path)
	connection = sqlite3.connect(path)
	names = {row[0] for row in connection.execute("select name from sqlite_master").fetchall()}
	connection.close()
	assert "ingest_audit" not in names
	assert "ingest_audit_insert" not in names
	add_comment(database, "c1")  # must not error


def test_is_installed(tmp_path):
	path, database = make_streamer_db(tmp_path)
	assert install_audit_trigger.is_installed(path) is False
	install_audit_trigger.install(path)
	assert install_audit_trigger.is_installed(path) is True
```

- [ ] **Step 2: Run to verify failure**

Run: `pipenv run pytest test/trigger_test.py -v`
Expected: `ModuleNotFoundError: No module named 'install_audit_trigger'`.

- [ ] **Step 3: Implement `scripts/install_audit_trigger.py`**

```python
"""Install or remove the audit trigger in CommentStreamer's ingest database.

The trigger copies every row inserted into ingest_comments into ingest_audit so the
PushshiftIngest comparison can see what the streamer queued after the bots drain it.
Safe to run while the streamer is running. Idempotent.

    python scripts/install_audit_trigger.py --db /path/to/streamer/database.db
    python scripts/install_audit_trigger.py --db /path/to/streamer/database.db --uninstall
"""
import argparse
import sqlite3

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS ingest_audit (
	id TEXT NOT NULL,
	client_id INTEGER NOT NULL,
	author TEXT,
	subreddit TEXT,
	created_utc INTEGER,
	permalink TEXT,
	link_id TEXT,
	body TEXT,
	inserted_utc INTEGER NOT NULL
)
"""

CREATE_INDEX = "CREATE INDEX IF NOT EXISTS ingest_audit_created ON ingest_audit (created_utc)"

CREATE_TRIGGER = """
CREATE TRIGGER IF NOT EXISTS ingest_audit_insert
AFTER INSERT ON ingest_comments
BEGIN
	INSERT INTO ingest_audit (id, client_id, author, subreddit, created_utc, permalink, link_id, body, inserted_utc)
	VALUES (NEW.id, NEW.client_id, NEW.author, NEW.subreddit, NEW.created_utc, NEW.permalink, NEW.link_id, NEW.body, strftime('%s', 'now'));
END
"""


def install(path):
	connection = sqlite3.connect(path, timeout=20)
	try:
		connection.execute(CREATE_TABLE)
		connection.execute(CREATE_INDEX)
		connection.execute(CREATE_TRIGGER)
		connection.commit()
	finally:
		connection.close()


def uninstall(path):
	connection = sqlite3.connect(path, timeout=20)
	try:
		connection.execute("DROP TRIGGER IF EXISTS ingest_audit_insert")
		connection.execute("DROP TABLE IF EXISTS ingest_audit")
		connection.commit()
	finally:
		connection.close()


def is_installed(path):
	connection = sqlite3.connect(path, timeout=20)
	try:
		row = connection.execute(
			"SELECT count(*) FROM sqlite_master WHERE type = 'trigger' AND name = 'ingest_audit_insert'").fetchone()
		return row[0] == 1
	finally:
		connection.close()


if __name__ == "__main__":
	parser = argparse.ArgumentParser(description="Install the ingest audit trigger in the CommentStreamer database")
	parser.add_argument("--db", required=True, help="Path to the streamer's ingest database file")
	parser.add_argument("--uninstall", action='store_const', const=True, default=False, help="Remove the trigger and table")
	args = parser.parse_args()

	if args.uninstall:
		uninstall(args.db)
		print(f"Removed ingest_audit trigger and table from {args.db}")
	else:
		install(args.db)
		print(f"Installed ingest_audit trigger in {args.db}")
```

- [ ] **Step 4: Run to verify pass**

Run: `pipenv run pytest test/trigger_test.py -v`
Expected: 5 passed.

- [ ] **Step 5: Commit**

```bash
git add scripts/install_audit_trigger.py test/trigger_test.py
git commit -m "Add audit trigger installer for the streamer database"
```

---

### Task 6: Comparison

**Files:**
- Create: `src/comparison.py`
- Create: `test/comparison_test.py`

The comparison reads the streamer's database read-only with the stdlib `sqlite3` module using a `file:` URI so it never takes a write lock on the streamer's file.

- [ ] **Step 1: Write the failing tests**

`test/comparison_test.py`:

```python
import sys
import pytest

sys.path.append("scripts")

import comparison
import install_audit_trigger
from praw_wrapper import IngestDatabase, IngestComment

HOUR = 3600
DAY = 24 * HOUR
NOW = 100 * DAY


@pytest.fixture
def streamer(tmp_path):
	path = str(tmp_path / "streamer.db")
	database = IngestDatabase(location=path)
	database.get_or_add_client("updateme")  # id 1 here, so ids differ from the poller db
	database.get_or_add_client("remindme")
	install_audit_trigger.install(path)
	return path, database


def streamer_add(database, comment_id, client_name, created_utc):
	client = database.get_or_add_client(client_name)
	database.add_comment(IngestComment(
		id=comment_id, author="Watchful1", subreddit="test", created_utc=created_utc,
		permalink=f"/r/test/comments/t1/_/{comment_id}/", link_id="t3_t1", body="RemindMe! 1 day",
		client_id=client.id,
	))
	database.commit()


def poller_add(store, comment_id, client_name, created_utc):
	store.upsert_seen({
		"id": comment_id, "author": "Watchful1", "subreddit": "test", "created_utc": created_utc,
		"retrieved_utc": created_utc + 5, "permalink": f"/r/test/comments/t1/_/{comment_id}/",
		"link_id": "t3_t1", "body": "RemindMe! 1 day",
	}, client_name, "remindme", now_utc=created_utc + 10)
	store.commit()


def test_read_streamer_audit_maps_client_names(streamer):
	path, database = streamer
	streamer_add(database, "a", "remindme", NOW - HOUR)
	streamer_add(database, "b", "updateme", NOW - HOUR)
	streamer_add(database, "c", "remindme", NOW - 2 * DAY)
	rows = comparison.read_streamer_audit(path, NOW - DAY, NOW)
	assert rows == {("a", "remindme"), ("b", "updateme")}


def test_sets_per_client(store, streamer):
	path, database = streamer
	streamer_add(database, "both1", "remindme", NOW - HOUR)
	poller_add(store, "both1", "remindme", NOW - HOUR)
	streamer_add(database, "s_only", "remindme", NOW - HOUR)
	poller_add(store, "p_only", "updateme", NOW - HOUR)
	store.set_int_key("comparison_start_utc", NOW - DAY)
	comp = comparison.Comparison(store, path)
	result = comp.run(NOW)
	assert result["remindme"] == {"both": 1, "streamer_only": 1, "pushshift_only": 0}
	assert result["updateme"] == {"both": 0, "streamer_only": 0, "pushshift_only": 1}
	assert store.get_miss("s_only", "remindme").side == "streamer_only"
	assert store.get_miss("p_only", "updateme").side == "pushshift_only"
	assert store.get_miss("both1", "remindme") is None


def test_recent_comments_not_flagged(store, streamer):
	path, database = streamer
	streamer_add(database, "fresh", "remindme", NOW - 5 * 60)
	store.set_int_key("comparison_start_utc", NOW - DAY)
	result = comparison.Comparison(store, path).run(NOW)
	assert result["remindme"]["streamer_only"] == 0
	assert store.get_miss("fresh", "remindme") is None


def test_comparison_start_respected(store, streamer):
	path, database = streamer
	streamer_add(database, "before_start", "remindme", NOW - 2 * HOUR)
	store.set_int_key("comparison_start_utc", NOW - HOUR)
	result = comparison.Comparison(store, path).run(NOW)
	assert result["remindme"]["streamer_only"] == 0


def test_miss_is_flagged_once_then_resolved(store, streamer):
	path, database = streamer
	streamer_add(database, "late", "remindme", NOW - HOUR)
	store.set_int_key("comparison_start_utc", NOW - DAY)
	comp = comparison.Comparison(store, path)
	comp.run(NOW)
	miss = store.get_miss("late", "remindme")
	assert miss.flagged_utc == NOW
	assert miss.resolved_utc is None
	comp.run(NOW + 60)
	assert store.get_miss("late", "remindme").flagged_utc == NOW
	poller_add(store, "late", "remindme", NOW - HOUR)
	comp.run(NOW + 120)
	assert store.get_miss("late", "remindme").resolved_utc == NOW + 120


def test_warning_threshold_rate_limited(store, streamer, monkeypatch):
	path, database = streamer
	for i in range(6):
		streamer_add(database, f"m{i}", "remindme", NOW - HOUR)
	store.set_int_key("comparison_start_utc", NOW - DAY)
	warnings = []
	monkeypatch.setattr(comparison.log, "warning", lambda message: warnings.append(message))
	comp = comparison.Comparison(store, path)
	comp.run(NOW)
	assert len(warnings) == 1
	comp.run(NOW + 60)
	assert len(warnings) == 1
	# an hour later with no new misses: the old ones no longer count as "in the last hour"
	comp.run(NOW + comparison.WARN_INTERVAL_SECONDS + 1)
	assert len(warnings) == 1
	# six fresh misses after the interval: warn again
	for i in range(6, 12):
		streamer_add(database, f"m{i}", "remindme", NOW - HOUR)
	comp.run(NOW + comparison.WARN_INTERVAL_SECONDS + 2)
	assert len(warnings) == 2


def test_below_threshold_no_warning(store, streamer, monkeypatch):
	path, database = streamer
	for i in range(5):
		streamer_add(database, f"m{i}", "remindme", NOW - HOUR)
	store.set_int_key("comparison_start_utc", NOW - DAY)
	warnings = []
	monkeypatch.setattr(comparison.log, "warning", lambda message: warnings.append(message))
	comparison.Comparison(store, path).run(NOW)
	assert warnings == []
```

- [ ] **Step 2: Run to verify failure**

Run: `pipenv run pytest test/comparison_test.py -v`
Expected: `ModuleNotFoundError: No module named 'comparison'`.

- [ ] **Step 3: Implement `src/comparison.py`**

```python
import sqlite3
from pathlib import Path
import discord_logging

import counters
from matching import CLIENT_NAMES

log = discord_logging.get_logger()

WINDOW_SECONDS = 24 * 3600
FLOOR_SECONDS = 15 * 60
WARN_THRESHOLD = 5
WARN_INTERVAL_SECONDS = 3600
SIDES = ("streamer_only", "pushshift_only")


def read_streamer_audit(path, start_utc, end_utc):
	"""Read-only. Returns a set of (comment_id, client_name) from the streamer's audit table."""
	uri = f"{Path(path).resolve().as_uri()}?mode=ro"
	connection = sqlite3.connect(uri, uri=True, timeout=20)
	try:
		rows = connection.execute(
			"SELECT a.id, c.name FROM ingest_audit a JOIN clients c ON c.id = a.client_id "
			"WHERE a.created_utc >= ? AND a.created_utc <= ?",
			(start_utc, end_utc),
		).fetchall()
	finally:
		connection.close()
	return {(row[0], row[1]) for row in rows}


class Comparison:
	def __init__(self, store, streamer_db_path):
		self.store = store
		self.streamer_db_path = streamer_db_path
		self.last_warning_utc = None

	def run(self, now_utc):
		"""Diff both sides inside the window. Returns {client: {both, streamer_only, pushshift_only}}."""
		start_utc = now_utc - WINDOW_SECONDS
		comparison_start = self.store.get_int_key("comparison_start_utc")
		if comparison_start is not None:
			start_utc = max(start_utc, comparison_start)
		end_utc = now_utc - FLOOR_SECONDS

		streamer_side = read_streamer_audit(self.streamer_db_path, start_utc, end_utc)
		seen_rows = self.store.get_seen_between(start_utc, end_utc)
		pushshift_side = {(row.id, row.client) for row in seen_rows}
		created_by_key = {(row.id, row.client): row.created_utc for row in seen_rows}

		result = {client: {"both": 0, "streamer_only": 0, "pushshift_only": 0} for client in CLIENT_NAMES}
		for key in streamer_side & pushshift_side:
			result[key[1]]["both"] += 1
			self._resolve_if_flagged(key, now_utc)
		for key in streamer_side - pushshift_side:
			result[key[1]]["streamer_only"] += 1
			self._flag(key, "streamer_only", created_by_key.get(key, 0), now_utc)
		for key in pushshift_side - streamer_side:
			result[key[1]]["pushshift_only"] += 1
			self._flag(key, "pushshift_only", created_by_key.get(key, 0), now_utc)

		for client, counts in result.items():
			for name, value in counts.items():
				counters.comparison.labels(client=client, result=name).set(value)

		self._maybe_warn(now_utc)
		return result

	def _flag(self, key, side, created_utc, now_utc):
		comment_id, client = key
		if self.store.get_miss(comment_id, client) is not None:
			return
		self.store.add_miss(comment_id, client, side, created_utc=created_utc, flagged_utc=now_utc)
		counters.misses.labels(client=client, side=side).inc()
		log.info(f"Comparison miss {side} for {client}: https://www.reddit.com/comments/{comment_id}")

	def _resolve_if_flagged(self, key, now_utc):
		miss = self.store.get_miss(*key)
		if miss is not None and miss.resolved_utc is None:
			self.store.resolve_miss(miss, now_utc)
			log.info(f"Comparison miss resolved ({miss.side}) for {key[1]}: {key[0]}")

	def _maybe_warn(self, now_utc):
		if self.last_warning_utc is not None and now_utc - self.last_warning_utc < WARN_INTERVAL_SECONDS:
			return
		recent = self.store.count_unresolved_misses_since(now_utc - WARN_INTERVAL_SECONDS)
		if recent > WARN_THRESHOLD:
			log.warning(f"{recent} unresolved comparison misses in the last hour")
			self.last_warning_utc = now_utc
```

Note the miss log line uses `/comments/<id>` which Reddit redirects to the comment; the audit row's permalink is not loaded to keep the read query small.

- [ ] **Step 4: Create `src/counters.py`** (needed by comparison; the full set from the spec so later tasks do not touch it again)

```python
import prometheus_client

lag = prometheus_client.Gauge('pushshift_lag_seconds', "Seconds between now and the newest comment on the last successful page")
request_results = prometheus_client.Counter('pushshift_request_results_total', "Pushshift request outcomes", ['result'])
consecutive_failures = prometheus_client.Gauge('pushshift_consecutive_failures', "Consecutive failed pushshift requests")
mode = prometheus_client.Gauge('pushshift_mode', "1 for the current mode", ['mode'])
seen = prometheus_client.Counter('pushshift_seen_total', "Trigger comments first seen", ['client'])
queued = prometheus_client.Counter('pushshift_queued_total', "Trigger comments written for the bots", ['client'])
comparison = prometheus_client.Gauge('pushshift_comparison', "Comparison window counts", ['client', 'result'])
misses = prometheus_client.Counter('pushshift_misses_total', "Comparison misses flagged", ['client', 'side'])
ingest_pending = prometheus_client.Gauge('pushshift_ingest_pending', "Rows waiting in ingest_comments", ['client'])


def init(port):
	prometheus_client.start_http_server(port)
```

- [ ] **Step 5: Run to verify pass**

Run: `pipenv run pytest test/comparison_test.py -v`
Expected: 7 passed.

- [ ] **Step 6: Commit**

```bash
git add src/comparison.py src/counters.py test/comparison_test.py
git commit -m "Add real time comparison against the streamer audit table"
```

---

### Task 7: Main loop

**Files:**
- Create: `src/main.py`
- Create: `test/main_test.py`

`main.py` holds the cycle logic as plain functions so tests can drive one cycle with a fake session and an in-memory database.

- [ ] **Step 1: Write the failing tests**

`test/main_test.py`:

```python
import pytest
import main
import pushshift
from fakes import FakeResponse, FakeSession

HOUR = 3600
NOW = 1_000_000


def comment(comment_id, created_utc, body="RemindMe! 1 day", author="Watchful1"):
	return {
		"id": comment_id, "author": author, "subreddit": "test", "created_utc": created_utc,
		"retrieved_utc": created_utc + 5, "permalink": f"/r/test/comments/t1/_/{comment_id}/",
		"link_id": "t3_t1", "body": body,
	}


def make_client(tmp_path, responses):
	token = tmp_path / "token.txt"
	token.write_text("tok")
	return pushshift.PushshiftClient(pushshift.TokenStore(token), session=FakeSession(responses))


def test_process_page_stores_matches_only(store):
	page = [
		comment("a", NOW - 60),
		comment("b", NOW - 120, body="nothing here"),
		comment("c", NOW - 180, body="updateme! and remindme!"),
		comment("d", NOW - 240, author="[deleted]"),
	]
	new_count, oldest = main.process_page(page, store, active=False, now_utc=NOW)
	assert new_count == 3
	assert oldest == NOW - 240
	assert store.count_seen() == 3
	assert store.count_pending("remindme") == 0


def test_process_page_active_queues_new_only(store):
	main.process_page([comment("a", NOW - 60)], store, active=False, now_utc=NOW)
	store.commit()
	new_count, _ = main.process_page([comment("a", NOW - 60), comment("b", NOW - 30)], store, active=True, now_utc=NOW + 30)
	store.commit()
	assert new_count == 1
	assert store.count_pending("remindme") == 1


def test_process_page_empty(store):
	assert main.process_page([], store, active=False, now_utc=NOW) == (0, None)


def test_run_cycle_success(store, tmp_path):
	client = make_client(tmp_path, [FakeResponse(200, {"data": [comment("a", NOW - 60)]})])
	state = main.LoopState()
	main.run_cycle(client, store, comparison=None, active=False, state=state, now_utc=NOW)
	assert store.count_seen() == 1
	assert store.get_int_key("last_success_utc") == NOW
	assert store.get_int_key("comparison_start_utc") == NOW


def test_run_cycle_failure_keeps_last_success(store, tmp_path):
	client = make_client(tmp_path, [FakeResponse(500)])
	store.set_int_key("last_success_utc", NOW - 100)
	store.commit()
	state = main.LoopState()
	main.run_cycle(client, store, comparison=None, active=False, state=state, now_utc=NOW)
	assert store.get_int_key("last_success_utc") == NOW - 100
	assert state.first_failure_utc == NOW


def test_run_cycle_warns_once_after_threshold(store, tmp_path, monkeypatch):
	client = make_client(tmp_path, [FakeResponse(500)] * 7 + [FakeResponse(200, {"data": []})])
	warnings = []
	infos = []
	monkeypatch.setattr(main.log, "warning", lambda message: warnings.append(message))
	monkeypatch.setattr(main.log, "info", lambda message: infos.append(message))
	state = main.LoopState()
	for i in range(7):
		main.run_cycle(client, store, comparison=None, active=False, state=state, now_utc=NOW + i)
	assert len(warnings) == 1
	main.run_cycle(client, store, comparison=None, active=False, state=state, now_utc=NOW + 100)
	assert any("recovered" in message.lower() for message in infos)
	assert state.first_failure_utc is None
	assert state.warned is False


def test_catch_up_pages_until_last_success(store, tmp_path):
	last_success = NOW - 2 * HOUR
	client = make_client(tmp_path, [
		FakeResponse(200, {"data": [comment("p1a", NOW - 60), comment("p1b", NOW - HOUR)]}),
		FakeResponse(200, {"data": [comment("p2a", NOW - HOUR - 60), comment("p2b", last_success - 20 * 60)]}),
		FakeResponse(200, {"data": [comment("never", 1)]}),
	])
	store.set_int_key("last_success_utc", last_success)
	store.commit()
	main.run_cycle(client, store, comparison=None, active=False, state=main.LoopState(), now_utc=NOW)
	assert store.count_seen() == 4
	assert len(client.session.calls) == 2
	assert client.session.calls[1][2]["before"] == NOW - HOUR


def test_catch_up_stops_on_empty_page(store, tmp_path):
	client = make_client(tmp_path, [
		FakeResponse(200, {"data": [comment("p1a", NOW - 60)]}),
		FakeResponse(200, {"data": []}),
	])
	store.set_int_key("last_success_utc", NOW - 2 * HOUR)
	store.commit()
	main.run_cycle(client, store, comparison=None, active=False, state=main.LoopState(), now_utc=NOW)
	assert len(client.session.calls) == 2


def test_catch_up_stops_at_page_cap(store, tmp_path):
	pages = [FakeResponse(200, {"data": [comment(f"c{i}", NOW - 10 * (i + 1))]}) for i in range(20)]
	client = make_client(tmp_path, pages)
	store.set_int_key("last_success_utc", NOW - 2 * HOUR)
	store.commit()
	main.run_cycle(client, store, comparison=None, active=False, state=main.LoopState(), now_utc=NOW)
	assert len(client.session.calls) == main.MAX_CATCH_UP_PAGES


def test_no_catch_up_inside_one_hour(store, tmp_path):
	client = make_client(tmp_path, [FakeResponse(200, {"data": [comment("a", NOW - 60)]})])
	store.set_int_key("last_success_utc", NOW - 30 * 60)
	store.commit()
	main.run_cycle(client, store, comparison=None, active=False, state=main.LoopState(), now_utc=NOW)
	assert len(client.session.calls) == 1


def test_end_to_end_active_row_visible_to_bot(store, ingest_db, tmp_path):
	client = make_client(tmp_path, [FakeResponse(200, {"data": [comment("a", NOW - 60)]})])
	main.run_cycle(client, store, comparison=None, active=True, state=main.LoopState(), now_utc=NOW)
	ingest_db.set_default_client("remindme")
	rows = ingest_db.get_comments(limit=30)
	assert len(rows) == 1
	assert rows[0].id == "a"
	assert rows[0].body == "RemindMe! 1 day"


def test_prune_runs_hourly(store, tmp_path, monkeypatch):
	calls = []
	monkeypatch.setattr(store, "prune", lambda now_utc: calls.append(now_utc))
	client = make_client(tmp_path, [FakeResponse(200, {"data": []})] * 3)
	state = main.LoopState()
	main.run_cycle(client, store, comparison=None, active=False, state=state, now_utc=NOW)
	main.run_cycle(client, store, comparison=None, active=False, state=state, now_utc=NOW + 60)
	main.run_cycle(client, store, comparison=None, active=False, state=state, now_utc=NOW + HOUR + 1)
	assert calls == [NOW, NOW + HOUR + 1]
```

- [ ] **Step 2: Run to verify failure**

Run: `pipenv run pytest test/main_test.py -v`
Expected: `ModuleNotFoundError: No module named 'main'`.

- [ ] **Step 3: Implement `src/main.py`**

```python
#!/usr/bin/python3

import argparse
import logging.handlers
import os
import signal
import sys
import time
import traceback
import discord_logging

# get_logger(init=True) reuses the logger conftest already initialised in tests
log = discord_logging.get_logger(init=True)

import praw_wrapper

import counters
import matching
import pushshift
from comparison import Comparison
from store import Store

CATCH_UP_AFTER_SECONDS = 3600
CATCH_UP_MARGIN_SECONDS = 15 * 60
MAX_CATCH_UP_PAGES = 10
PRUNE_INTERVAL_SECONDS = 3600
FAILURE_WARN_THRESHOLD = 5

store = None
ingest_database = None


class LoopState:
	def __init__(self):
		self.first_failure_utc = None
		self.warned = False
		self.last_prune_utc = None


def process_page(comments, store, active, now_utc):
	"""Upsert every matching comment. Returns (new_row_count, oldest_created_utc)."""
	new_count = 0
	oldest = None
	for comment in comments:
		created_utc = int(comment["created_utc"])
		if oldest is None or created_utc < oldest:
			oldest = created_utc
		if not matching.is_valid_author(comment.get("author")):
			continue
		for client, term in matching.match_clients(comment["body"]):
			row = store.upsert_seen(comment, client, term, now_utc)
			if row is None:
				continue
			new_count += 1
			counters.seen.labels(client=client).inc()
			if active:
				store.queue(row)
				counters.queued.labels(client=client).inc()
				log.info(f"Queued {client} comment {row.id} from u/{row.author} in r/{row.subreddit}")
	return new_count, oldest


def catch_up(client, store, active, now_utc, last_success_utc, before):
	"""Page backward until we pass last_success_utc minus a margin, an empty page, or the page cap."""
	target = last_success_utc - CATCH_UP_MARGIN_SECONDS
	pages = 1  # the first page was already fetched by run_cycle
	while pages < MAX_CATCH_UP_PAGES and before is not None and before > target:
		comments, reason = client.search(before=before)
		pages += 1
		if comments is None:
			counters.request_results.labels(result=reason).inc()
			log.info(f"Catch up page failed: {reason}")
			return
		counters.request_results.labels(result="success").inc()
		if not comments:
			return
		new_count, oldest = process_page(comments, store, active, now_utc)
		log.info(f"Catch up page {pages}: {len(comments)} comments, {new_count} new, oldest {oldest}")
		before = oldest


def run_cycle(client, store, comparison, active, state, now_utc):
	last_success_utc = store.get_int_key("last_success_utc")
	if store.get_int_key("comparison_start_utc") is None:
		store.set_int_key("comparison_start_utc", now_utc)
	if last_success_utc is None:
		store.set_int_key("last_success_utc", now_utc)
		last_success_utc = now_utc

	comments, reason = client.search()
	counters.consecutive_failures.set(client.consecutive_failures)
	if comments is None:
		counters.request_results.labels(result=reason).inc()
		if state.first_failure_utc is None:
			state.first_failure_utc = now_utc
		if client.consecutive_failures >= FAILURE_WARN_THRESHOLD and not state.warned:
			log.warning(f"Pushshift failing: {client.consecutive_failures} consecutive failures, latest {reason}")
			state.warned = True
		else:
			log.info(f"Pushshift request failed: {reason}")
		store.commit()
		return

	counters.request_results.labels(result="success").inc()
	if state.first_failure_utc is not None:
		log.info(f"Pushshift recovered after {now_utc - state.first_failure_utc} seconds")
		state.first_failure_utc = None
		state.warned = False

	new_count, oldest = process_page(comments, store, active, now_utc)
	if comments:
		newest = max(int(comment["created_utc"]) for comment in comments)
		counters.lag.set(max(now_utc - newest, 0))
	log.debug(f"Page: {len(comments)} comments, {new_count} new")

	if now_utc - last_success_utc > CATCH_UP_AFTER_SECONDS and comments:
		log.info(f"Last success was {now_utc - last_success_utc} seconds ago, catching up")
		catch_up(client, store, active, now_utc, last_success_utc, oldest)

	store.set_int_key("last_success_utc", now_utc)

	if comparison is not None:
		comparison.run(now_utc)

	if state.last_prune_utc is None or now_utc - state.last_prune_utc >= PRUNE_INTERVAL_SECONDS:
		store.prune(now_utc)
		state.last_prune_utc = now_utc

	for client_name in matching.CLIENT_NAMES:
		counters.ingest_pending.labels(client=client_name).set(store.count_pending(client_name))

	store.commit()


def signal_handler(signal, frame):
	log.info("Handling interrupt")
	if store is not None:
		store.commit()
	if ingest_database is not None:
		ingest_database.close()
	discord_logging.flush_discord()
	sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)


if __name__ == "__main__":
	parser = argparse.ArgumentParser(description="Pushshift trigger comment poller for RemindMeBot and UpdateMeBot")
	parser.add_argument("user", help="The praw.ini section to use for discord logging")
	parser.add_argument("--db", help="Path to this poller's ingest database", default="database.db")
	parser.add_argument("--streamer_db", help="Path to CommentStreamer's database, enables the comparison", default=None)
	parser.add_argument("--token_file", help="Path to the pushshift bearer token file", default="pushshift_token.txt")
	parser.add_argument("--active", help="Write ingest rows for the bots. Default is shadow mode", action='store_const', const=True, default=False)
	parser.add_argument("--interval", help="Seconds between polls", type=int, default=30)
	parser.add_argument("--port", help="Prometheus port", type=int, default=8006)
	parser.add_argument("--debug", help="Set the log level to debug", action='store_const', const=True, default=False)
	parser.add_argument("--once", help="Run one cycle and exit", action='store_const', const=True, default=False)
	args = parser.parse_args()

	if args.debug:
		discord_logging.set_level(logging.DEBUG)
	discord_logging.init_discord_logging(args.user, logging.WARNING, 1)

	token_store = pushshift.TokenStore(args.token_file)
	if token_store.load() is None:
		log.error(f"No pushshift token in {args.token_file}")
		sys.exit(1)

	if args.streamer_db is not None and not os.path.exists(args.streamer_db):
		log.error(f"Streamer database not found: {args.streamer_db}")
		sys.exit(1)

	counters.init(args.port)
	counters.mode.labels(mode="active").set(1 if args.active else 0)
	counters.mode.labels(mode="shadow").set(0 if args.active else 1)

	ingest_database = praw_wrapper.IngestDatabase(location=args.db)
	for client_name, terms in matching.SEARCH_TERMS.items():
		for term in terms:
			ingest_database.register_search(search_term=term, client_name=client_name)
	ingest_database.commit()
	store = Store(ingest_database)

	comparison = Comparison(store, args.streamer_db) if args.streamer_db is not None else None
	client = pushshift.PushshiftClient(token_store)
	state = LoopState()

	log.info(f"Starting in {'active' if args.active else 'shadow'} mode, db {args.db}, comparison {'on' if comparison else 'off'}")

	while True:
		start_time = time.perf_counter()
		try:
			run_cycle(client, store, comparison, args.active, state, int(time.time()))
		except Exception as err:
			log.warning(f"Uncaught error in cycle: {err}")
			log.warning(traceback.format_exc())
			try:
				store.session.rollback()
			except Exception:
				pass
		discord_logging.flush_discord()

		if args.once:
			break

		sleep_seconds = client.sleep_seconds(args.interval)
		log.debug(f"Cycle took {time.perf_counter() - start_time:.2f}s, sleeping {sleep_seconds}")
		time.sleep(sleep_seconds)
```

- [ ] **Step 4: Run to verify pass**

Run: `pipenv run pytest test/main_test.py -v`
Expected: 12 passed.

- [ ] **Step 5: Run the whole suite**

Run: `pipenv run pytest -v`
Expected: all passed, no failures.

- [ ] **Step 6: Smoke the CLI without a token**

Run: `pipenv run python src/main.py Watchful1BotTest --once --db test_smoke.db --token_file does_not_exist.txt`
Expected: exits with code 1 and logs `No pushshift token in does_not_exist.txt`. Then delete `test_smoke.db` if it was created.

- [ ] **Step 7: Commit**

```bash
git add src/main.py test/main_test.py
git commit -m "Add poll loop with catch up, failure handling and comparison"
```

---

### Task 8: README

**Files:**
- Create: `README.md`

- [ ] **Step 1: Write `README.md`**

```markdown
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

Flags: `--db` (default `database.db`), `--streamer_db`, `--token_file`,
`--interval` (default 30), `--port` (default 8006), `--debug`, `--once`.

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
3. Restart RemindMeBot and UpdateMeBot with `--ingest_db` pointing at this
   project's database file.

Nothing seen before activation is queued, so the bots do not replay history.

## Tests

    pipenv run pytest
```

- [ ] **Step 2: Commit**

```bash
git add README.md
git commit -m "Add README with runbook and trigger note"
```

---

## Self-review

**Spec coverage.** Poll loop and single page: Task 7. Local matching identical to streamer: Task 2. Seen table never drained, upsert keyed on id and client: Task 3. Active versus shadow, new rows only queued: Tasks 3 and 7. Catch up after an hour with 15 minute margin, 10 page cap, empty page stop: Task 7. Retries, backoff 30 to 300, 401 and 403 refresh, warning after 5 failures, recovery line, no exit in the loop: Tasks 4 and 7. Token file and refresh semantics: Task 4. Storage on the `IngestDatabase` file, client registration: Tasks 3 and 7. Audit trigger, idempotent install and uninstall: Task 5. Comparison window, floor, comparison start, client name mapping, misses table with resolution, rate limited warning: Task 6. Metrics table: Task 6 step 4. Command line: Task 7. Runbook: Task 8. Tests per spec list: Tasks 2 to 7.

**Placeholders.** None. The `QUERY` constant is flagged in a code comment as something to confirm against the Pushshift guide, and the subagent for Task 4 should do that confirmation if the guide is reachable, otherwise leave the constant as written.

**Type consistency.** `Store.upsert_seen(comment_dict, client, term, now_utc)` returns a row or `None`; `process_page` relies on that. `Store.queue(row)`, `count_pending(client_name)`, `get_seen_between`, `get_miss`, `add_miss(..., created_utc, flagged_utc)`, `resolve_miss(miss, resolved_utc)`, `count_unresolved_misses_since(flagged_after)`, `get_int_key`, `set_int_key` are used with the same signatures in Tasks 6 and 7. `PushshiftClient.search(before=None, limit=PAGE_SIZE)` returns `(comments, reason)` in Tasks 4 and 7. `LoopState` fields `first_failure_utc`, `warned`, `last_prune_utc` match between tests and implementation. `counters` names match between Task 6 step 4 and their uses in Tasks 6 and 7.
