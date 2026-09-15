import sqlite3
from pathlib import Path
import discord_logging

import counters
from matching import CLIENT_NAMES

log = discord_logging.get_logger()

WINDOW_SECONDS = 24 * 3600
FLOOR_SECONDS = 15 * 60
WARN_THRESHOLD = 5
WARN_INTERVAL_SECONDS = 3600
SIDES = ("streamer_only", "pushshift_only")


def read_streamer_audit(path, start_utc, end_utc):
	"""Read-only. Returns a set of (comment_id, client_name) from the streamer's audit table."""
	uri = f"{Path(path).resolve().as_uri()}?mode=ro"
	connection = sqlite3.connect(uri, uri=True, timeout=20)
	try:
		rows = connection.execute(
			"SELECT a.id, c.name FROM ingest_audit a JOIN clients c ON c.id = a.client_id "
			"WHERE a.created_utc >= ? AND a.created_utc <= ?",
			(start_utc, end_utc),
		).fetchall()
	finally:
		connection.close()
	return {(row[0], row[1]) for row in rows}


class Comparison:
	def __init__(self, store, streamer_db_path):
		self.store = store
		self.streamer_db_path = streamer_db_path
		self.last_warning_utc = None

	def run(self, now_utc):
		"""Diff both sides inside the window. Returns {client: {both, streamer_only, pushshift_only}}."""
		start_utc = now_utc - WINDOW_SECONDS
		comparison_start = self.store.get_int_key("comparison_start_utc")
		if comparison_start is not None:
			start_utc = max(start_utc, comparison_start)
		end_utc = now_utc - FLOOR_SECONDS

		streamer_side = read_streamer_audit(self.streamer_db_path, start_utc, end_utc)
		seen_rows = self.store.get_seen_between(start_utc, end_utc)
		pushshift_side = {(row.id, row.client) for row in seen_rows}
		created_by_key = {(row.id, row.client): row.created_utc for row in seen_rows}

		result = {client: {"both": 0, "streamer_only": 0, "pushshift_only": 0} for client in CLIENT_NAMES}
		for key in streamer_side & pushshift_side:
			result[key[1]]["both"] += 1
			self._resolve_if_flagged(key, now_utc)
		for key in streamer_side - pushshift_side:
			result[key[1]]["streamer_only"] += 1
			self._flag(key, "streamer_only", created_by_key.get(key, 0), now_utc)
		for key in pushshift_side - streamer_side:
			result[key[1]]["pushshift_only"] += 1
			self._flag(key, "pushshift_only", created_by_key.get(key, 0), now_utc)

		for client, counts in result.items():
			for name, value in counts.items():
				counters.comparison.labels(client=client, result=name).set(value)

		self._maybe_warn(now_utc)
		return result

	def _flag(self, key, side, created_utc, now_utc):
		comment_id, client = key
		if self.store.get_miss(comment_id, client) is not None:
			return
		self.store.add_miss(comment_id, client, side, created_utc=created_utc, flagged_utc=now_utc)
		counters.misses.labels(client=client, side=side).inc()
		log.info(f"Comparison miss {side} for {client}: https://www.reddit.com/comments/{comment_id}")

	def _resolve_if_flagged(self, key, now_utc):
		miss = self.store.get_miss(*key)
		if miss is not None and miss.resolved_utc is None:
			self.store.resolve_miss(miss, now_utc)
			log.info(f"Comparison miss resolved ({miss.side}) for {key[1]}: {key[0]}")

	def _maybe_warn(self, now_utc):
		if self.last_warning_utc is not None and now_utc - self.last_warning_utc < WARN_INTERVAL_SECONDS:
			return
		recent = self.store.count_unresolved_misses_since(now_utc - WARN_INTERVAL_SECONDS)
		if recent > WARN_THRESHOLD:
			log.warning(f"{recent} unresolved comparison misses in the last hour")
			self.last_warning_utc = now_utc
