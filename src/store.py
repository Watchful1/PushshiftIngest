import discord_logging
from sqlalchemy import Column, Integer, String, Boolean
from sqlalchemy.orm import declarative_base
from praw_wrapper import IngestComment
# Reaches into praw_wrapper's internal module because the public API has no lookup-only
# client getter; re-check this import on a PrawWrapper upgrade.
from praw_wrapper.ingest import Client

log = discord_logging.get_logger()

Base = declarative_base()

SEEN_RETENTION_SECONDS = 7 * 24 * 3600
MISS_RETENTION_SECONDS = 30 * 24 * 3600


class SeenComment(Base):
	__tablename__ = 'seen_comments'

	id = Column(String(12), primary_key=True)
	client = Column(String(20), primary_key=True)
	matched_term = Column(String(40), nullable=False)
	author = Column(String(80), nullable=False)
	subreddit = Column(String(80), nullable=False)
	created_utc = Column(Integer, nullable=False, index=True)
	retrieved_utc = Column(Integer, nullable=True)
	permalink = Column(String(400), nullable=False)
	link_id = Column(String(12), nullable=False)
	body = Column(String(10000), nullable=False)
	first_seen_utc = Column(Integer, nullable=False)
	queued = Column(Boolean, nullable=False, default=False)


class ComparisonMiss(Base):
	__tablename__ = 'comparison_misses'

	id = Column(String(12), primary_key=True)
	client = Column(String(20), primary_key=True)
	side = Column(String(20), nullable=False)
	created_utc = Column(Integer, nullable=False)
	flagged_utc = Column(Integer, nullable=False, index=True)
	resolved_utc = Column(Integer, nullable=True)
	kind = Column(String(10), nullable=False, default="prose")
	term = Column(String(40), nullable=False, default="unknown")


class Store:
	def __init__(self, ingest_database):
		self.db = ingest_database
		self.session = ingest_database.session
		Base.metadata.create_all(ingest_database.engine)
		self._ensure_columns()

	def _ensure_columns(self):
		"""create_all never alters existing tables; add columns introduced after first deploy."""
		with self.db.engine.begin() as connection:
			existing = {row[1] for row in connection.exec_driver_sql("PRAGMA table_info(comparison_misses)")}
			if "kind" not in existing:
				connection.exec_driver_sql("ALTER TABLE comparison_misses ADD COLUMN kind VARCHAR(10) NOT NULL DEFAULT 'prose'")
			if "term" not in existing:
				connection.exec_driver_sql("ALTER TABLE comparison_misses ADD COLUMN term VARCHAR(40) NOT NULL DEFAULT 'unknown'")

	def commit(self):
		self.session.commit()

	# seen comments

	def upsert_seen(self, comment, client, term, now_utc):
		"""Insert a seen row if absent. Returns the new row, or None if it already existed."""
		existing = self.session.get(SeenComment, {"id": comment["id"], "client": client})
		if existing is not None:
			return None
		row = SeenComment(
			id=comment["id"],
			client=client,
			matched_term=term,
			author=comment["author"],
			subreddit=comment["subreddit"],
			created_utc=int(comment["created_utc"]),
			retrieved_utc=int(comment["retrieved_utc"]) if comment.get("retrieved_utc") is not None else None,
			permalink=comment["permalink"],
			link_id=comment["link_id"],
			body=comment["body"],
			first_seen_utc=now_utc,
			queued=False,
		)
		self.session.add(row)
		return row

	def queue(self, row):
		# get_or_add_client commits the session if it creates a client, so every client in
		# matching.SEARCH_TERMS must already be registered at startup (main.py does this) to
		# keep a page's writes atomic.
		client = self.db.get_or_add_client(row.client)
		self.db.add_comment(IngestComment(
			id=row.id,
			author=row.author,
			subreddit=row.subreddit,
			created_utc=row.created_utc,
			permalink=row.permalink,
			link_id=row.link_id,
			body=row.body,
			client_id=client.id,
		))
		row.queued = True

	def count_seen(self):
		return self.session.query(SeenComment).count()

	def get_seen_between(self, start_utc, end_utc):
		return self.session.query(SeenComment) \
			.filter(SeenComment.created_utc >= start_utc) \
			.filter(SeenComment.created_utc <= end_utc) \
			.all()

	def count_pending(self, client_name):
		client = self.session.query(Client).filter_by(name=client_name).first()
		if client is None:
			return 0
		return self.db.get_count_comments(client)

	# comparison misses

	def get_miss(self, comment_id, client):
		return self.session.get(ComparisonMiss, {"id": comment_id, "client": client})

	def add_miss(self, comment_id, client, side, created_utc, flagged_utc, kind, term):
		miss = ComparisonMiss(
			id=comment_id, client=client, side=side, created_utc=created_utc, flagged_utc=flagged_utc,
			kind=kind, term=term,
		)
		self.session.add(miss)
		return miss

	def resolve_miss(self, miss, resolved_utc):
		miss.resolved_utc = resolved_utc

	def count_unresolved_misses_since(self, flagged_after, kind=None):
		query = self.session.query(ComparisonMiss) \
			.filter(ComparisonMiss.flagged_utc > flagged_after) \
			.filter(ComparisonMiss.resolved_utc.is_(None))
		if kind is not None:
			query = query.filter(ComparisonMiss.kind == kind)
		return query.count()

	# retention

	def prune(self, now_utc):
		# default synchronize_session so deleted objects leave the identity map
		seen_deleted = self.session.query(SeenComment) \
			.filter(SeenComment.created_utc < now_utc - SEEN_RETENTION_SECONDS) \
			.delete()
		miss_deleted = self.session.query(ComparisonMiss) \
			.filter(ComparisonMiss.flagged_utc < now_utc - MISS_RETENTION_SECONDS) \
			.delete()
		if seen_deleted or miss_deleted:
			log.info(f"Pruned {seen_deleted} seen comments and {miss_deleted} misses")

	# integer key value

	def get_int_key(self, key):
		value = self.db.get_keystore(key)
		return int(value) if value is not None else None

	def set_int_key(self, key, value):
		self.db.save_keystore(key, str(int(value)))
