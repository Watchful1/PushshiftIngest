import os
import requests
import discord_logging

log = discord_logging.get_logger()

SEARCH_URL = "https://api.pushshift.io/reddit/comment/search"
REFRESH_URL = "https://auth.pushshift.io/refresh"
# OR syntax as used by the old RemindMeBot TRIGGER_COMBINED constant. Verified against
# https://api.pushshift.io/guide during implementation; adjust here if the syntax differs.
QUERY = 'remindme|"remind me"|remindmerepeat|cakeday|updateme|subscribeme|subscribeall'
USER_AGENT = "PushshiftIngest by u/Watchful1"
PAGE_SIZE = 250
MAX_BACKOFF_SECONDS = 300
STILL_ACTIVE_DETAIL = "Access token is still active and can not be refreshed."


class TokenStore:
	def __init__(self, path):
		self.path = str(path)

	def load(self):
		if not os.path.exists(self.path):
			return None
		with open(self.path, 'r') as handle:
			token = handle.read().strip()
		return token if token else None

	def save(self, token):
		with open(self.path, 'w') as handle:
			handle.write(token)


class PushshiftClient:
	"""One HTTP call per method. Never raises out of search() or refresh_token()."""
	def __init__(self, token_store, session=None, timeout=20):
		self.token_store = token_store
		self.token = token_store.load()
		self.session = session if session is not None else requests.Session()
		self.timeout = timeout
		self.consecutive_failures = 0

	def _headers(self):
		return {
			'User-Agent': USER_AGENT,
			'Authorization': f"Bearer {self.token}",
		}

	def _fail(self, reason):
		self.consecutive_failures += 1
		return None, reason

	def search(self, before=None, limit=PAGE_SIZE):
		"""Returns (comments, None) on success or (None, reason) on failure.

		reason is one of: timeout, error, parse, status_<code>.
		A 401 or 403 triggers a token refresh before returning.
		"""
		params = {"q": QUERY, "limit": limit, "order": "desc"}
		if before is not None:
			params["before"] = before
		try:
			response = self.session.get(SEARCH_URL, params=params, headers=self._headers(), timeout=self.timeout)
		except requests.exceptions.Timeout:
			return self._fail("timeout")
		except Exception as err:
			log.info(f"Pushshift request error: {type(err).__name__}: {err}")
			return self._fail("error")

		if response.status_code in (401, 403):
			log.info(f"Pushshift returned {response.status_code}, refreshing token")
			self.refresh_token()
			return self._fail(f"status_{response.status_code}")
		if response.status_code != 200:
			return self._fail(f"status_{response.status_code}")

		try:
			comments = response.json()["data"]
		except Exception as err:
			log.info(f"Pushshift parse error: {type(err).__name__}: {err}")
			return self._fail("parse")

		self.consecutive_failures = 0
		return comments, None

	def sleep_seconds(self, base_seconds):
		if self.consecutive_failures <= 1:
			return base_seconds
		return min(base_seconds * (2 ** (self.consecutive_failures - 1)), MAX_BACKOFF_SECONDS)

	def refresh_token(self):
		"""Returns one of: refreshed, still_active, rejected, error. Never logs the token value."""
		try:
			response = self.session.post(
				REFRESH_URL, params={"access_token": self.token}, headers={'User-Agent': USER_AGENT}, timeout=self.timeout)
			result = response.json()
		except Exception as err:
			log.warning(f"Pushshift token refresh failed: {type(err).__name__}: {err}")
			return "error"

		if isinstance(result, dict) and 'access_token' in result:
			self.token = result['access_token']
			self.token_store.save(self.token)
			log.info("Refreshed pushshift token")
			return "refreshed"

		detail = result.get('detail') if isinstance(result, dict) else None
		if detail == STILL_ACTIVE_DETAIL:
			log.info("Pushshift token still active, keeping it")
			return "still_active"

		log.warning(f"Pushshift token refresh rejected: {detail}")
		return "rejected"
