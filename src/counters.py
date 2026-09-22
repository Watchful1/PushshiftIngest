import os

# Drop the *_created companion series prometheus_client emits for every counter;
# they add nothing on the dashboard and double the pushed series count.
os.environ.setdefault("PROMETHEUS_DISABLE_CREATED_SERIES", "True")

import prometheus_client

lag = prometheus_client.Gauge('pushshift_lag_seconds', "Seconds between now and the newest comment on the last successful page")
request_results = prometheus_client.Counter('pushshift_request_results_total', "Pushshift request outcomes", ['result'])
consecutive_failures = prometheus_client.Gauge('pushshift_consecutive_failures', "Consecutive failed pushshift requests")
mode = prometheus_client.Gauge('pushshift_mode', "1 for the current mode", ['mode'])
seen = prometheus_client.Counter('pushshift_seen_total', "Trigger comments first seen", ['client', 'term', 'kind'])
queued = prometheus_client.Counter('pushshift_queued_total', "Trigger comments written for the bots", ['client'])
comparison = prometheus_client.Gauge('pushshift_comparison', "Comparison window counts", ['client', 'result', 'kind'])
misses = prometheus_client.Counter('pushshift_misses_total', "Comparison misses flagged", ['client', 'side', 'kind', 'term'])
misses_window = prometheus_client.Gauge('pushshift_misses_window', "Misses currently inside the comparison window", ['client', 'side', 'kind', 'term'])
ingest_pending = prometheus_client.Gauge('pushshift_ingest_pending', "Rows waiting in ingest_comments", ['client'])
catch_up_truncated = prometheus_client.Counter('pushshift_catch_up_truncated_total', "Catch up runs that hit the page cap before reaching the last success time")
malformed_comments = prometheus_client.Counter('pushshift_malformed_comments_total', "Pushshift comments skipped because a required field was missing")
metrics_push = prometheus_client.Counter('pushshift_metrics_push_total', "Metric pushes to the remote store by outcome", ['result'])


def init(port):
	prometheus_client.start_http_server(port)
