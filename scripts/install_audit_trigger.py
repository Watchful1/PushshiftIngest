"""Install or remove the audit trigger in CommentStreamer's ingest database.

The trigger copies every row inserted into ingest_comments into ingest_audit so the
PushshiftIngest comparison can see what the streamer queued after the bots drain it.
Safe to run while the streamer is running. Idempotent.

    python scripts/install_audit_trigger.py --db /path/to/streamer/database.db
    python scripts/install_audit_trigger.py --db /path/to/streamer/database.db --uninstall
"""
import argparse
import sqlite3

CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS ingest_audit (
	id TEXT NOT NULL,
	client_id INTEGER NOT NULL,
	author TEXT,
	subreddit TEXT,
	created_utc INTEGER,
	permalink TEXT,
	link_id TEXT,
	body TEXT,
	inserted_utc INTEGER NOT NULL
)
"""

CREATE_INDEX = "CREATE INDEX IF NOT EXISTS ingest_audit_created ON ingest_audit (created_utc)"

CREATE_TRIGGER = """
CREATE TRIGGER IF NOT EXISTS ingest_audit_insert
AFTER INSERT ON ingest_comments
BEGIN
	INSERT INTO ingest_audit (id, client_id, author, subreddit, created_utc, permalink, link_id, body, inserted_utc)
	VALUES (NEW.id, NEW.client_id, NEW.author, NEW.subreddit, NEW.created_utc, NEW.permalink, NEW.link_id, NEW.body, strftime('%s', 'now'));
END
"""


def install(path):
	connection = sqlite3.connect(path, timeout=20)
	try:
		connection.execute(CREATE_TABLE)
		connection.execute(CREATE_INDEX)
		connection.execute(CREATE_TRIGGER)
		connection.commit()
	finally:
		connection.close()


def uninstall(path):
	connection = sqlite3.connect(path, timeout=20)
	try:
		connection.execute("DROP TRIGGER IF EXISTS ingest_audit_insert")
		connection.execute("DROP TABLE IF EXISTS ingest_audit")
		connection.commit()
	finally:
		connection.close()


def is_installed(path):
	connection = sqlite3.connect(path, timeout=20)
	try:
		row = connection.execute(
			"SELECT count(*) FROM sqlite_master WHERE type = 'trigger' AND name = 'ingest_audit_insert'").fetchone()
		return row[0] == 1
	finally:
		connection.close()


if __name__ == "__main__":
	parser = argparse.ArgumentParser(description="Install the ingest audit trigger in the CommentStreamer database")
	parser.add_argument("--db", required=True, help="Path to the streamer's ingest database file")
	parser.add_argument("--uninstall", action='store_const', const=True, default=False, help="Remove the trigger and table")
	args = parser.parse_args()

	if args.uninstall:
		uninstall(args.db)
		print(f"Removed ingest_audit trigger and table from {args.db}")
	else:
		install(args.db)
		print(f"Installed ingest_audit trigger in {args.db}")
