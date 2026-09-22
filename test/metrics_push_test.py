import counters
import metrics_push
from fakes import FakeResponse, FakeSession, timeout_error


def make_pusher(responses):
	session = FakeSession(responses)
	pusher = metrics_push.MetricsPusher("https://example.invalid/push", "secret", session=session)
	return pusher, session


def test_push_success_returns_pushed_and_posts_expected_request():
	pusher, session = make_pusher([FakeResponse(200)])
	result = pusher.push()
	assert result == "pushed"
	assert session.last_post["headers"]["Authorization"] == "Bearer secret"
	assert session.last_post["headers"]["Content-Type"] == "text/plain"
	assert b"pushshift_lag_seconds" in session.last_post["data"]


def test_push_204_returns_pushed():
	pusher, _ = make_pusher([FakeResponse(204)])
	assert pusher.push() == "pushed"


def test_push_401_returns_status_401_and_counts_failure():
	pusher, _ = make_pusher([FakeResponse(401)])
	assert pusher.push() == "status_401"
	assert pusher.consecutive_failures == 1


def test_push_timeout_returns_timeout_and_counts_failure():
	pusher, _ = make_pusher([timeout_error()])
	assert pusher.push() == "timeout"
	assert pusher.consecutive_failures == 1


def test_push_generic_exception_returns_error_and_counts_failure():
	pusher, _ = make_pusher([RuntimeError("boom")])
	assert pusher.push() == "error"
	assert pusher.consecutive_failures == 1


def test_warns_once_after_threshold_then_recovers(monkeypatch):
	pusher, _ = make_pusher([FakeResponse(500)] * 5 + [FakeResponse(200)])
	warnings = []
	monkeypatch.setattr(metrics_push.log, "warning", lambda message: warnings.append(message))
	for _ in range(5):
		pusher.push()
	assert len(warnings) == 1
	assert pusher.consecutive_failures == 5

	infos = []
	monkeypatch.setattr(metrics_push.log, "info", lambda message: infos.append(message))
	result = pusher.push()
	assert result == "pushed"
	assert any("recovered" in message.lower() for message in infos)
	assert pusher.consecutive_failures == 0
	assert pusher.warned is False


def test_metrics_push_counter_increments_on_success():
	pusher, _ = make_pusher([FakeResponse(200)])
	before = counters.metrics_push.labels(result="pushed")._value.get()
	pusher.push()
	after = counters.metrics_push.labels(result="pushed")._value.get()
	assert after == before + 1
