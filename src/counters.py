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
