import pytest
import counters
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


def test_token_store_save_is_atomic(token_file):
	store = pushshift.TokenStore(token_file)
	store.save("token-two")
	assert store.load() == "token-two"
	assert not (token_file.parent / (token_file.name + ".tmp")).exists()


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
	assert "until" not in params


def test_search_with_before(token_file):
	client, session = make_client(token_file, [FakeResponse(200, {"data": []})])
	client.search(before=12345)
	assert session.calls[0][2]["until"] == 12345


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


def test_refresh_increments_refresh_counter(token_file):
	client, _ = make_client(token_file, [FakeResponse(200, {"access_token": "token-two"})])
	before = counters.request_results.labels(result="refresh")._value.get()
	client.refresh_token()
	after = counters.request_results.labels(result="refresh")._value.get()
	assert after == before + 1


def test_refresh_post_headers_have_no_authorization(token_file):
	class HeaderSession(FakeSession):
		def post(self, url, params=None, headers=None, timeout=None):
			self.headers = headers
			return super().post(url, params, headers, timeout)
	session = HeaderSession([FakeResponse(200, {"access_token": "token-two"})])
	client = pushshift.PushshiftClient(pushshift.TokenStore(token_file), session=session)
	client.refresh_token()
	assert "User-Agent" in session.headers
	assert "Authorization" not in session.headers


def test_refresh_failure_log_never_contains_the_token(token_file, monkeypatch):
	import requests
	warnings = []
	monkeypatch.setattr(pushshift.log, "warning", lambda message: warnings.append(message))
	# requests includes the full URL, query string and all, in ConnectionError messages
	err = requests.exceptions.ConnectionError(
		"HTTPSConnectionPool(host='auth.pushshift.io', port=443): Max retries exceeded with url: "
		"/refresh?access_token=token-one (Caused by NewConnectionError)")
	client, _ = make_client(token_file, [err])
	assert client.refresh_token() == "error"
	assert len(warnings) == 1
	assert "token-one" not in warnings[0]


def test_query_covers_bot_name_tokens():
	"""Pushshift tokenises on word boundaries and case changes: `u/RemindMeBot` splits into
	remind/me/bot and is found by the "remind me" phrase, but lowercase `u/remindmebot` is one
	token that none of the trigger terms match, and `u/UpdateMeBot` never splits into `updateme`.
	Both are valid triggers for the bots, so the bot names must be query terms themselves."""
	terms = pushshift.QUERY.split("|")
	assert "remindmebot" in terms
	assert "updatemebot" in terms
