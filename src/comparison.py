import sqlite3
from pathlib import Path
import discord_logging

import counters
import matching
from matching import CLIENT_NAMES

log = discord_logging.get_logger()

WINDOW_SECONDS = 24 * 3600
FLOOR_SECONDS = 15 * 60
WARN_THRESHOLD = 5
WARN_INTERVAL_SECONDS = 3600


def read_streamer_audit(path, start_utc, end_utc):
	"""Read-only. Returns {(comment_id, client_name): (created_utc, permalink, body)} from the streamer's audit table."""
	uri = f"{Path(path).resolve().as_uri()}?mode=ro"
	connection = sqlite3.connect(uri, uri=True, timeout=20)
	try:
		rows = connection.execute(
			"SELECT a.id, c.name, a.created_utc, a.permalink, a.body FROM ingest_audit a "
			"JOIN clients c ON c.id = a.client_id "
			"WHERE a.created_utc >= ? AND a.created_utc <= ?",
			(start_utc, end_utc),
		).fetchall()
	finally:
		connection.close()
	return {(row[0], row[1]): (row[2], row[3], row[4]) for row in rows}


class Comparison:
	def __init__(self, store, streamer_db_path):
		self.store = store
		self.streamer_db_path = streamer_db_path
		self.last_warning_utc = None
		self.warned_unknown_clients = set()
		self.window_combos = set()  # (client, side, kind, term) gauge labels set last cycle, to zero when they disappear

	def run(self, now_utc):
		"""Diff both sides inside the window. Returns {client: {both, streamer_only, pushshift_only}}."""
		start_utc = now_utc - WINDOW_SECONDS
		comparison_start = self.store.get_int_key("comparison_start_utc")
		if comparison_start is not None:
			start_utc = max(start_utc, comparison_start)
		end_utc = now_utc - FLOOR_SECONDS

		streamer_side = read_streamer_audit(self.streamer_db_path, start_utc, end_utc)
		seen_rows = self.store.get_seen_between(start_utc, end_utc)
		pushshift_side = {(row.id, row.client): (row.created_utc, row.permalink, row.body) for row in seen_rows}

		result = {client: {"both": 0, "streamer_only": 0, "pushshift_only": 0} for client in CLIENT_NAMES}
		kind_counts = {}
		window_misses = {}
		streamer_keys = set(streamer_side.keys())
		pushshift_keys = set(pushshift_side.keys())
		for key in streamer_keys & pushshift_keys:
			self._counts_for(result, key[1])["both"] += 1
			_, _, body = pushshift_side[key]
			term, kind = matching.classify(key[1], body)
			self._bump_kind(kind_counts, key[1], "both", kind)
			self._resolve_if_flagged(key, now_utc)
		for key in streamer_keys - pushshift_keys:
			self._counts_for(result, key[1])["streamer_only"] += 1
			created_utc, permalink, body = streamer_side[key]
			term, kind = matching.classify(key[1], body)
			self._bump_kind(kind_counts, key[1], "streamer_only", kind)
			combo = (key[1], "streamer_only", kind, term)
			window_misses[combo] = window_misses.get(combo, 0) + 1
			self._flag(key, "streamer_only", created_utc, permalink, term, kind, now_utc)
		for key in pushshift_keys - streamer_keys:
			self._counts_for(result, key[1])["pushshift_only"] += 1
			created_utc, permalink, body = pushshift_side[key]
			term, kind = matching.classify(key[1], body)
			self._bump_kind(kind_counts, key[1], "pushshift_only", kind)
			combo = (key[1], "pushshift_only", kind, term)
			window_misses[combo] = window_misses.get(combo, 0) + 1
			self._flag(key, "pushshift_only", created_utc, permalink, term, kind, now_utc)

		for client in result:
			by_side = kind_counts.get(client, {})
			for side in ("both", "streamer_only", "pushshift_only"):
				by_kind = by_side.get(side, {})
				for kind in matching.KINDS:
					counters.comparison.labels(client=client, result=side, kind=kind).set(by_kind.get(kind, 0))

		for combo in self.window_combos - set(window_misses):
			counters.misses_window.labels(*combo).set(0)
		for combo, count in window_misses.items():
			counters.misses_window.labels(*combo).set(count)
		self.window_combos = set(window_misses)

		self._maybe_warn(now_utc)
		return result

	def _bump_kind(self, kind_counts, client, side, kind):
		by_side = kind_counts.setdefault(client, {})
		by_kind = by_side.setdefault(side, {})
		by_kind[kind] = by_kind.get(kind, 0) + 1

	def _counts_for(self, result, client):
		if client not in result:
			result[client] = {"both": 0, "streamer_only": 0, "pushshift_only": 0}
			if client not in self.warned_unknown_clients:
				log.warning(f"Comparison saw unknown client name: {client}")
				self.warned_unknown_clients.add(client)
		return result[client]

	def _flag(self, key, side, created_utc, permalink, term, kind, now_utc):
		comment_id, client = key
		if self.store.get_miss(comment_id, client) is not None:
			return
		self.store.add_miss(comment_id, client, side, created_utc=created_utc, flagged_utc=now_utc, kind=kind, term=term)
		counters.misses.labels(client=client, side=side, kind=kind, term=term).inc()
		log.info(f"Comparison miss {side} {kind} {term} for {client}: https://www.reddit.com{permalink}")

	def _resolve_if_flagged(self, key, now_utc):
		miss = self.store.get_miss(*key)
		if miss is not None and miss.resolved_utc is None:
			self.store.resolve_miss(miss, now_utc)
			log.info(f"Comparison miss resolved ({miss.side}) for {key[1]}: {key[0]}")

	def _maybe_warn(self, now_utc):
		if self.last_warning_utc is not None and now_utc - self.last_warning_utc < WARN_INTERVAL_SECONDS:
			return
		recent = self.store.count_unresolved_misses_since(now_utc - WARN_INTERVAL_SECONDS, kind="command")
		if recent > WARN_THRESHOLD:
			log.warning(f"{recent} unresolved comparison misses in the last hour")
			self.last_warning_utc = now_utc
