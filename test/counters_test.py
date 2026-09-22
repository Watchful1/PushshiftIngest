import counters
from prometheus_client import generate_latest


def test_generate_latest_has_no_created_series():
	counters.seen.labels(client="remindme", term="remindme", kind="command").inc()
	output = generate_latest()
	assert b"pushshift_seen_created" not in output
	assert b"pushshift_seen_total" in output
