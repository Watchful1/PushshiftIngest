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
	assert client.session.calls[1][2]["until"] == NOW - HOUR


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
