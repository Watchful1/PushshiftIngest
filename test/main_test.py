import pytest
import counters
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


def test_process_page_skips_comment_missing_required_field(store, monkeypatch):
	bad = comment("a", NOW - 60)
	del bad["body"]
	page = [comment("b", NOW - 120), bad]
	warnings = []
	monkeypatch.setattr(main.log, "warning", lambda message: warnings.append(message))
	before = counters.malformed_comments._value.get()
	new_count, _ = main.process_page(page, store, active=False, now_utc=NOW)
	after = counters.malformed_comments._value.get()
	assert new_count == 1
	assert store.count_seen() == 1
	assert len(warnings) == 1
	assert "body" in warnings[0]
	assert after - before == 1


def test_process_page_builds_permalink_when_missing(store):
	bare = comment("a", NOW - 60)
	del bare["permalink"]
	main.process_page([bare], store, active=False, now_utc=NOW)
	rows = store.get_seen_between(NOW - 3600, NOW)
	assert len(rows) == 1
	assert rows[0].permalink == "/r/test/comments/t1/_/a/"


def test_process_page_rejects_bad_created_utc(store):
	bad = comment("a", NOW - 60)
	bad["created_utc"] = "nope"
	new_count, oldest = main.process_page([bad], store, active=False, now_utc=NOW)
	assert new_count == 0
	assert oldest is None
	assert store.count_seen() == 0


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


def test_catch_up_pages_until_last_success(store, tmp_path, monkeypatch):
	last_success = NOW - 2 * HOUR
	client = make_client(tmp_path, [
		FakeResponse(200, {"data": [comment("p1a", NOW - 60), comment("p1b", NOW - HOUR)]}),
		FakeResponse(200, {"data": [comment("p2a", NOW - HOUR - 60), comment("p2b", last_success - 20 * 60)]}),
		FakeResponse(200, {"data": [comment("never", 1)]}),
	])
	store.set_int_key("last_success_utc", last_success)
	store.commit()
	warnings = []
	monkeypatch.setattr(main.log, "warning", lambda message: warnings.append(message))
	commit_count = 0
	original_commit = store.commit

	def counting_commit():
		nonlocal commit_count
		commit_count += 1
		original_commit()

	monkeypatch.setattr(store, "commit", counting_commit)
	main.run_cycle(client, store, comparison=None, active=False, state=main.LoopState(), now_utc=NOW)
	assert store.count_seen() == 4
	assert len(client.session.calls) == 2
	assert client.session.calls[1][2]["until"] == NOW - HOUR
	assert warnings == []
	assert commit_count >= 2


def test_catch_up_stops_on_empty_page(store, tmp_path):
	client = make_client(tmp_path, [
		FakeResponse(200, {"data": [comment("p1a", NOW - 60)]}),
		FakeResponse(200, {"data": []}),
	])
	store.set_int_key("last_success_utc", NOW - 2 * HOUR)
	store.commit()
	main.run_cycle(client, store, comparison=None, active=False, state=main.LoopState(), now_utc=NOW)
	assert len(client.session.calls) == 2


def test_catch_up_stops_at_page_cap(store, tmp_path, monkeypatch):
	pages = [FakeResponse(200, {"data": [comment(f"c{i}", NOW - 10 * (i + 1))]}) for i in range(20)]
	client = make_client(tmp_path, pages)
	store.set_int_key("last_success_utc", NOW - 2 * HOUR)
	store.commit()
	warnings = []
	monkeypatch.setattr(main.log, "warning", lambda message: warnings.append(message))
	main.run_cycle(client, store, comparison=None, active=False, state=main.LoopState(), now_utc=NOW)
	assert len(client.session.calls) == main.MAX_CATCH_UP_PAGES
	assert len(warnings) == 1
	assert "page cap" in warnings[0]


def test_no_catch_up_inside_threshold(store, tmp_path):
	client = make_client(tmp_path, [FakeResponse(200, {"data": [comment("a", NOW - 60)]})])
	store.set_int_key("last_success_utc", NOW - 20 * 60)
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


def test_active_sets_activated_utc_once(store, tmp_path):
	client = make_client(tmp_path, [
		FakeResponse(200, {"data": [comment("a", NOW - 60)]}),
		FakeResponse(200, {"data": [comment("b", NOW - 30)]}),
	])
	state = main.LoopState()
	main.run_cycle(client, store, comparison=None, active=True, state=state, now_utc=NOW)
	main.run_cycle(client, store, comparison=None, active=True, state=state, now_utc=NOW + 100)
	assert store.get_int_key("activated_utc") == NOW


def test_active_does_not_queue_comments_created_before_activation(store, tmp_path):
	client = make_client(tmp_path, [
		FakeResponse(200, {"data": [comment("old", NOW - 3600), comment("new", NOW - 30)]}),
	])
	main.run_cycle(client, store, comparison=None, active=True, state=main.LoopState(), now_utc=NOW)
	assert store.count_seen() == 2
	assert store.count_pending("remindme") == 1


def test_prune_runs_hourly(store, tmp_path, monkeypatch):
	calls = []
	monkeypatch.setattr(store, "prune", lambda now_utc: calls.append(now_utc))
	client = make_client(tmp_path, [FakeResponse(200, {"data": []})] * 3)
	state = main.LoopState()
	main.run_cycle(client, store, comparison=None, active=False, state=state, now_utc=NOW)
	main.run_cycle(client, store, comparison=None, active=False, state=state, now_utc=NOW + 60)
	main.run_cycle(client, store, comparison=None, active=False, state=state, now_utc=NOW + HOUR + 1)
	assert calls == [NOW, NOW + HOUR + 1]


class RaisingStore:
	def commit(self):
		raise RuntimeError("commit failed")


class RaisingIngestDatabase:
	def close(self):
		raise RuntimeError("close failed")


def test_signal_handler_always_exits_zero(monkeypatch):
	monkeypatch.setattr(main, "store", RaisingStore())
	monkeypatch.setattr(main, "ingest_database", RaisingIngestDatabase())
	with pytest.raises(SystemExit) as excinfo:
		main.signal_handler(None, None)
	assert excinfo.value.code == 0


def test_summary_logged_on_first_success_then_every_interval(store, tmp_path, monkeypatch):
	infos = []
	monkeypatch.setattr(main.log, "info", lambda message: infos.append(message))
	client = make_client(tmp_path, [
		FakeResponse(200, {"data": [comment("a", NOW - 60)]}),
		FakeResponse(200, {"data": [comment("b", NOW - 30, body="updateme!")]}),
		FakeResponse(200, {"data": []}),
	])
	state = main.LoopState()
	main.run_cycle(client, store, comparison=None, active=False, state=state, now_utc=NOW)
	summaries = [m for m in infos if m.startswith("Summary:")]
	assert len(summaries) == 1
	assert "1 cycles, 1 new matches (remindme=1, updateme=0)" in summaries[0]
	assert "mode shadow" in summaries[0]
	assert "pushshift lag 60s" in summaries[0]
	main.run_cycle(client, store, comparison=None, active=False, state=state, now_utc=NOW + 60)
	assert len([m for m in infos if m.startswith("Summary:")]) == 1
	main.run_cycle(client, store, comparison=None, active=False, state=state, now_utc=NOW + main.SUMMARY_INTERVAL_SECONDS + 1)
	summaries = [m for m in infos if m.startswith("Summary:")]
	assert len(summaries) == 2
	assert "2 cycles, 1 new matches (remindme=0, updateme=1)" in summaries[1]


class FakePusher:
	def __init__(self):
		self.calls = []

	def push(self):
		self.calls.append(True)
		return "pushed"


def test_maybe_push_metrics_none_pusher_is_noop():
	state = main.LoopState()
	assert main.maybe_push_metrics(None, state, NOW) is None
	assert state.last_push_utc is None


def test_maybe_push_metrics_runs_once_per_interval():
	pusher = FakePusher()
	state = main.LoopState()
	main.maybe_push_metrics(pusher, state, NOW)
	main.maybe_push_metrics(pusher, state, NOW + 30)
	main.maybe_push_metrics(pusher, state, NOW + 60)
	assert len(pusher.calls) == 2
	assert state.last_push_utc == NOW + 60
