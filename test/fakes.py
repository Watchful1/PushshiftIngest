import requests


class FakeResponse:
	def __init__(self, status_code, payload=None, bad_json=False):
		self.status_code = status_code
		self.payload = payload
		self.bad_json = bad_json

	def json(self):
		if self.bad_json:
			raise ValueError("not json")
		return self.payload


class FakeSession:
	"""Returns queued responses in order. An Exception instance in the queue is raised instead of returned."""
	def __init__(self, responses):
		self.responses = list(responses)
		self.calls = []

	def _next(self, method, url, params):
		self.calls.append((method, url, params))
		if not self.responses:
			raise AssertionError("FakeSession ran out of responses")
		item = self.responses.pop(0)
		if isinstance(item, Exception):
			raise item
		return item

	def get(self, url, params=None, headers=None, timeout=None):
		return self._next("get", url, params)

	def post(self, url, params=None, headers=None, timeout=None, data=None):
		self.last_post = {"url": url, "headers": headers, "data": data}
		return self._next("post", url, params)


def timeout_error():
	return requests.exceptions.Timeout("timed out")
