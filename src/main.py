#!/usr/bin/python3

import argparse
import logging.handlers
import os
import signal
import sys
import time
import traceback
import discord_logging

# get_logger(init=True) reuses the logger conftest already initialised in tests
log = discord_logging.get_logger(init=True)

# counters must be imported before praw_wrapper: praw_wrapper imports prometheus_client,
# and counters sets PROMETHEUS_DISABLE_CREATED_SERIES, which prometheus_client reads at import time.
import counters
import praw_wrapper

import matching
import pushshift
from comparison import Comparison, read_streamer_audit
from metrics_push import MetricsPusher, PUSH_INTERVAL_SECONDS
from store import Store

CATCH_UP_AFTER_SECONDS = 1800  # one page covers roughly an hour of matches, so catch up well inside that
CATCH_UP_MARGIN_SECONDS = 15 * 60
MAX_CATCH_UP_PAGES = 10
PRUNE_INTERVAL_SECONDS = 3600
FAILURE_WARN_THRESHOLD = 5
SUMMARY_INTERVAL_SECONDS = 300
ACTIVATION_MARGIN_SECONDS = 60

REQUIRED_COMMENT_FIELDS = ("id", "subreddit", "created_utc", "link_id", "body")

store = None
ingest_database = None


class LoopState:
	def __init__(self):
		self.first_failure_utc = None
		self.warned = False
		self.last_prune_utc = None
		self.last_summary_utc = None
		self.summary_cycles = 0
		self.summary_new = {}
		self.last_push_utc = None


def normalise_comment(comment):
	"""Return a cleaned copy of comment, or None if a required field is missing or invalid."""
	missing = [field for field in REQUIRED_COMMENT_FIELDS if comment.get(field) is None]
	if missing:
		log.warning(f"Skipping malformed comment: missing {sorted(missing)} : {comment.get('id')}")
		return None

	cleaned = dict(comment)
	try:
		cleaned["created_utc"] = int(cleaned["created_utc"])
	except (TypeError, ValueError):
		log.warning(f"Skipping malformed comment: invalid created_utc : {comment.get('id')}")
		return None

	if not cleaned.get("permalink"):
		thread_id = cleaned["link_id"]
		if thread_id.startswith("t3_"):
			thread_id = thread_id[len("t3_"):]
		cleaned["permalink"] = f"/r/{cleaned['subreddit']}/comments/{thread_id}/_/{cleaned['id']}/"

	return cleaned


def process_page(comments, store, active, now_utc, queue_after_utc=None, tally=None):
	"""Upsert every matching comment. Returns (new_row_count, oldest_created_utc).

	If tally is a dict, new rows are counted into it per client for the periodic summary.
	"""
	new_count = 0
	oldest = None
	for raw_comment in comments:
		comment = normalise_comment(raw_comment)
		if comment is None:
			counters.malformed_comments.inc()
			continue
		created_utc = comment["created_utc"]
		if oldest is None or created_utc < oldest:
			oldest = created_utc
		if not matching.is_valid_author(comment.get("author")):
			continue
		for client, term in matching.match_clients(comment["body"]):
			row = store.upsert_seen(comment, client, term, now_utc)
			if row is None:
				continue
			new_count += 1
			term, kind = matching.classify(client, comment["body"])
			counters.seen.labels(client=client, term=term, kind=kind).inc()
			if tally is not None:
				tally[client] = tally.get(client, 0) + 1
			if active and (queue_after_utc is None or row.created_utc >= queue_after_utc):
				store.queue(row)
				counters.queued.labels(client=client).inc()
				log.info(f"Queued {client} comment {row.id} from u/{row.author} in r/{row.subreddit}")
	return new_count, oldest


def catch_up(client, store, active, now_utc, last_success_utc, before, queue_after_utc=None, tally=None):
	"""Page backward until we pass last_success_utc minus a margin, an empty page, or the page cap."""
	target = last_success_utc - CATCH_UP_MARGIN_SECONDS
	pages = 1  # the first page was already fetched by run_cycle
	while pages < MAX_CATCH_UP_PAGES and before is not None and before > target:
		comments, reason = client.search(before=before)
		pages += 1
		if comments is None:
			counters.request_results.labels(result=reason).inc()
			log.info(f"Catch up page failed: {reason}")
			return
		counters.request_results.labels(result="success").inc()
		if not comments:
			return
		new_count, oldest = process_page(comments, store, active, now_utc, queue_after_utc, tally)
		store.commit()
		log.info(f"Catch up page {pages}: {len(comments)} comments, {new_count} new, oldest {oldest}")
		before = oldest

	if pages >= MAX_CATCH_UP_PAGES and before is not None and before > target:
		counters.catch_up_truncated.inc()
		log.warning(f"Catch up hit the {MAX_CATCH_UP_PAGES} page cap with {before - target} seconds still uncovered")


def run_cycle(client, store, comparison, active, state, now_utc):
	last_success_utc = store.get_int_key("last_success_utc")
	if store.get_int_key("comparison_start_utc") is None:
		store.set_int_key("comparison_start_utc", now_utc)
	if last_success_utc is None:
		store.set_int_key("last_success_utc", now_utc)
		last_success_utc = now_utc

	queue_after_utc = None
	if active:
		activated_utc = store.get_int_key("activated_utc")
		if activated_utc is None:
			store.set_int_key("activated_utc", now_utc)
			activated_utc = now_utc
		queue_after_utc = activated_utc - ACTIVATION_MARGIN_SECONDS

	comments, reason = client.search()
	counters.consecutive_failures.set(client.consecutive_failures)
	if comments is None:
		counters.request_results.labels(result=reason).inc()
		if state.first_failure_utc is None:
			state.first_failure_utc = now_utc
		if client.consecutive_failures >= FAILURE_WARN_THRESHOLD and not state.warned:
			log.warning(f"Pushshift failing: {client.consecutive_failures} consecutive failures, latest {reason}")
			state.warned = True
		else:
			log.info(f"Pushshift request failed: {reason}")
		store.commit()
		return

	counters.request_results.labels(result="success").inc()
	if state.first_failure_utc is not None:
		log.info(f"Pushshift recovered after {now_utc - state.first_failure_utc} seconds")
		state.first_failure_utc = None
		state.warned = False

	new_count, oldest = process_page(comments, store, active, now_utc, queue_after_utc, state.summary_new)
	lag_seconds = None
	if comments:
		valid_created_utcs = []
		for comment in comments:
			try:
				valid_created_utcs.append(int(comment["created_utc"]))
			except (KeyError, TypeError, ValueError):
				continue
		if valid_created_utcs:
			lag_seconds = max(now_utc - max(valid_created_utcs), 0)
			counters.lag.set(lag_seconds)
	log.debug(f"Page: {len(comments)} comments, {new_count} new")

	if now_utc - last_success_utc > CATCH_UP_AFTER_SECONDS and comments:
		log.info(f"Last success was {now_utc - last_success_utc} seconds ago, catching up")
		store.commit()  # release the write lock before the first catch up request
		catch_up(client, store, active, now_utc, last_success_utc, oldest, queue_after_utc, state.summary_new)

	store.set_int_key("last_success_utc", now_utc)

	comparison_result = None
	if comparison is not None:
		comparison_result = comparison.run(now_utc)

	state.summary_cycles += 1
	if state.last_summary_utc is None or now_utc - state.last_summary_utc >= SUMMARY_INTERVAL_SECONDS:
		log_summary(state, lag_seconds, comparison_result, active)
		state.last_summary_utc = now_utc
		state.summary_cycles = 0
		state.summary_new = {}

	if state.last_prune_utc is None or now_utc - state.last_prune_utc >= PRUNE_INTERVAL_SECONDS:
		store.prune(now_utc)
		state.last_prune_utc = now_utc

	for client_name in matching.CLIENT_NAMES:
		counters.ingest_pending.labels(client=client_name).set(store.count_pending(client_name))

	store.commit()


def log_summary(state, lag_seconds, comparison_result, active):
	"""One info line so the log shows progress in shadow mode, where nothing else is logged at info."""
	per_client = ", ".join(f"{client}={state.summary_new.get(client, 0)}" for client in matching.CLIENT_NAMES)
	parts = [
		f"Summary: {state.summary_cycles} cycles, {sum(state.summary_new.values())} new matches ({per_client})",
		f"mode {'active' if active else 'shadow'}",
		f"pushshift lag {lag_seconds if lag_seconds is not None else 'n/a'}s",
	]
	if comparison_result is not None:
		comparison_parts = []
		for client, counts in comparison_result.items():
			comparison_parts.append(f"{client} both={counts['both']} streamer_only={counts['streamer_only']} pushshift_only={counts['pushshift_only']}")
		parts.append("comparison " + "; ".join(comparison_parts))
	log.info(", ".join(parts))


def maybe_push_metrics(pusher, state, now_utc):
	"""Push once per PUSH_INTERVAL_SECONDS. Runs after every cycle, including failed ones, so failure metrics still arrive."""
	if pusher is None:
		return None
	if state.last_push_utc is not None and now_utc - state.last_push_utc < PUSH_INTERVAL_SECONDS:
		return None
	state.last_push_utc = now_utc
	return pusher.push()


def signal_handler(signal, frame):
	log.info("Handling interrupt")
	try:
		if store is not None:
			store.commit()
		if ingest_database is not None:
			ingest_database.close()
	except Exception as err:
		log.warning(f"Error while handling interrupt: {err}")
	finally:
		discord_logging.flush_discord()
		sys.exit(0)


if __name__ == "__main__":
	signal.signal(signal.SIGINT, signal_handler)
	signal.signal(signal.SIGTERM, signal_handler)

	parser = argparse.ArgumentParser(description="Pushshift trigger comment poller for RemindMeBot and UpdateMeBot")
	parser.add_argument("user", help="The praw.ini section to use for discord logging")
	parser.add_argument("--db", help="Path to this poller's ingest database", default="database.db")
	parser.add_argument("--streamer_db", help="Path to CommentStreamer's database, enables the comparison", default=None)
	parser.add_argument("--token_file", help="Path to the pushshift bearer token file", default="pushshift_token.txt")
	parser.add_argument("--active", help="Write ingest rows for the bots. Default is shadow mode", action='store_const', const=True, default=False)
	parser.add_argument("--interval", help="Seconds between polls", type=int, default=30)
	parser.add_argument("--port", help="Prometheus port", type=int, default=8006)
	parser.add_argument("--debug", help="Set the log level to debug", action='store_const', const=True, default=False)
	parser.add_argument("--once", help="Run one cycle and exit", action='store_const', const=True, default=False)
	parser.add_argument("--metrics_push_url", help="Full URL of the metrics proxy to POST this process's metrics to once a minute. Unset disables pushing", default=None)
	parser.add_argument("--metrics_push_key_file", help="Path to the metrics push proxy's bearer key file", default="metrics_push_key.txt")
	args = parser.parse_args()

	if args.debug:
		discord_logging.set_level(logging.DEBUG)
	discord_logging.init_discord_logging(args.user, logging.WARNING, 1)

	token_store = pushshift.TokenStore(args.token_file)
	if token_store.load() is None:
		log.error(f"No pushshift token in {args.token_file}")
		sys.exit(1)

	if args.metrics_push_url is not None:
		metrics_push_key = pushshift.TokenStore(args.metrics_push_key_file).load()
		if metrics_push_key is None:
			log.error(f"No metrics push key in {args.metrics_push_key_file}")
			sys.exit(1)
		pusher = MetricsPusher(args.metrics_push_url, metrics_push_key)
	else:
		pusher = None

	if args.streamer_db is not None:
		if not os.path.exists(args.streamer_db):
			log.error(f"Streamer database not found: {args.streamer_db}")
			sys.exit(1)
		try:
			read_streamer_audit(args.streamer_db, 0, 0)
		except Exception as err:
			log.error(f"Streamer database not readable: {args.streamer_db}: {err}")
			sys.exit(1)

	counters.init(args.port)
	counters.mode.labels(mode="active").set(1 if args.active else 0)
	counters.mode.labels(mode="shadow").set(0 if args.active else 1)

	try:
		ingest_database = praw_wrapper.IngestDatabase(location=args.db)
		for client_name, terms in matching.SEARCH_TERMS.items():
			for term in terms:
				ingest_database.register_search(search_term=term, client_name=client_name)
		ingest_database.commit()
	except Exception as err:
		log.error(f"Unable to open database: {args.db}: {err}")
		sys.exit(1)
	store = Store(ingest_database)

	comparison = Comparison(store, args.streamer_db) if args.streamer_db is not None else None
	client = pushshift.PushshiftClient(token_store)
	state = LoopState()

	log.info(f"Starting in {'active' if args.active else 'shadow'} mode, db {args.db}, comparison {'on' if comparison else 'off'}, metrics push {'on' if pusher else 'off'}")

	while True:
		start_time = time.perf_counter()
		try:
			run_cycle(client, store, comparison, args.active, state, int(time.time()))
		except Exception as err:
			log.warning(f"Uncaught error in cycle: {err}")
			log.warning(traceback.format_exc())
			try:
				store.session.rollback()
			except Exception:
				pass
		maybe_push_metrics(pusher, state, int(time.time()))
		discord_logging.flush_discord()

		if args.once:
			break

		sleep_seconds = client.sleep_seconds(args.interval)
		log.debug(f"Cycle took {time.perf_counter() - start_time:.2f}s, sleeping {sleep_seconds}")
		time.sleep(sleep_seconds)
