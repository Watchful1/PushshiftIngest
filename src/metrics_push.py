import requests
import discord_logging
from prometheus_client import generate_latest

import counters

log = discord_logging.get_logger()

PUSH_INTERVAL_SECONDS = 60
FAILURE_WARN_THRESHOLD = 5


class MetricsPusher:
	"""POSTs this process's metrics to the remote store's proxy. Never raises."""
	def __init__(self, url, key, session=None, timeout=10):
		self.url = url
		self.key = key
		self.session = session if session is not None else requests.Session()
		self.timeout = timeout
		self.consecutive_failures = 0
		self.warned = False

	def push(self):
		"""Returns one of: pushed, timeout, error, status_<code>."""
		try:
			response = self.session.post(
				self.url,
				data=generate_latest(),
				headers={'Authorization': f"Bearer {self.key}", 'Content-Type': 'text/plain'},
				timeout=self.timeout,
			)
		except requests.exceptions.Timeout:
			return self._fail("timeout")
		except Exception as err:
			log.info(f"Metrics push error: {type(err).__name__}: {err}")
			return self._fail("error")

		if response.status_code not in (200, 204):
			return self._fail(f"status_{response.status_code}")

		counters.metrics_push.labels(result="pushed").inc()
		if self.consecutive_failures:
			log.info(f"Metrics push recovered after {self.consecutive_failures} failures")
		self.consecutive_failures = 0
		self.warned = False
		return "pushed"

	def _fail(self, reason):
		counters.metrics_push.labels(result=reason).inc()
		self.consecutive_failures += 1
		if self.consecutive_failures >= FAILURE_WARN_THRESHOLD and not self.warned:
			log.warning(f"Metrics push failing: {self.consecutive_failures} consecutive failures, latest {reason}")
			self.warned = True
		else:
			log.info(f"Metrics push failed: {reason}")
		return reason
