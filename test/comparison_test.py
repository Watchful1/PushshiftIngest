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
	assert set(rows.keys()) == {("a", "remindme"), ("b", "updateme")}
	assert rows[("a", "remindme")] == (NOW - HOUR, "/r/test/comments/t1/_/a/")


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


def test_miss_records_real_created_utc_and_permalink(store, streamer, monkeypatch):
	path, database = streamer
	streamer_add(database, "s_only", "remindme", NOW - HOUR)
	poller_add(store, "p_only", "updateme", NOW - HOUR)
	store.set_int_key("comparison_start_utc", NOW - DAY)
	messages = []
	monkeypatch.setattr(comparison.log, "info", lambda message: messages.append(message))
	comparison.Comparison(store, path).run(NOW)

	s_miss = store.get_miss("s_only", "remindme")
	p_miss = store.get_miss("p_only", "updateme")
	assert s_miss.created_utc == NOW - HOUR
	assert p_miss.created_utc == NOW - HOUR

	assert any(message.endswith("https://www.reddit.com/r/test/comments/t1/_/s_only/") for message in messages)
	assert any(message.endswith("https://www.reddit.com/r/test/comments/t1/_/p_only/") for message in messages)


def test_unknown_streamer_client_does_not_crash(store, streamer, monkeypatch):
	path, database = streamer
	database.get_or_add_client("other")
	streamer_add(database, "x", "other", NOW - HOUR)
	store.set_int_key("comparison_start_utc", NOW - DAY)
	warnings = []
	monkeypatch.setattr(comparison.log, "warning", lambda message: warnings.append(message))
	comp = comparison.Comparison(store, path)

	result = comp.run(NOW)
	assert result["other"]["streamer_only"] == 1
	assert result["remindme"] == {"both": 0, "streamer_only": 0, "pushshift_only": 0}
	assert len(warnings) == 1

	comp.run(NOW + 1)
	assert len(warnings) == 1


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
