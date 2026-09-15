from store import SeenComment, ComparisonMiss
from praw_wrapper.ingest import Client


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


def test_count_pending_unknown_client_does_not_create_it(store, ingest_db):
	assert store.count_pending("nope") == 0
	assert ingest_db.session.query(Client).filter_by(name="nope").first() is None
