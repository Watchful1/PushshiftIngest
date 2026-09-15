import sqlite3
import sys
import os

sys.path.append("scripts")

import install_audit_trigger
from praw_wrapper import IngestDatabase, IngestComment


def make_streamer_db(tmp_path):
	path = str(tmp_path / "streamer.db")
	database = IngestDatabase(location=path)
	database.set_default_client("remindme")
	return path, database


def add_comment(database, comment_id):
	client = database.get_or_add_client("remindme")
	database.add_comment(IngestComment(
		id=comment_id, author="Watchful1", subreddit="test", created_utc=1000,
		permalink=f"/r/test/comments/t1/_/{comment_id}/", link_id="t3_t1", body="RemindMe! 1 day",
		client_id=client.id,
	))
	database.commit()


def audit_rows(path):
	connection = sqlite3.connect(path)
	rows = connection.execute("select id, client_id, author, body, inserted_utc from ingest_audit order by inserted_utc").fetchall()
	connection.close()
	return rows


def test_install_records_inserts(tmp_path):
	path, database = make_streamer_db(tmp_path)
	install_audit_trigger.install(path)
	add_comment(database, "c1")
	rows = audit_rows(path)
	assert len(rows) == 1
	assert rows[0][0] == "c1"
	assert rows[0][2] == "Watchful1"
	assert rows[0][3] == "RemindMe! 1 day"
	assert rows[0][4] > 0


def test_audit_survives_ingest_delete(tmp_path):
	path, database = make_streamer_db(tmp_path)
	install_audit_trigger.install(path)
	add_comment(database, "c1")
	comment = database.get_comments()[0]
	database.delete_comment(comment)
	database.commit()
	assert len(audit_rows(path)) == 1


def test_install_twice_is_noop(tmp_path):
	path, database = make_streamer_db(tmp_path)
	install_audit_trigger.install(path)
	install_audit_trigger.install(path)
	add_comment(database, "c1")
	assert len(audit_rows(path)) == 1


def test_uninstall_removes_trigger_and_table(tmp_path):
	path, database = make_streamer_db(tmp_path)
	install_audit_trigger.install(path)
	install_audit_trigger.uninstall(path)
	connection = sqlite3.connect(path)
	names = {row[0] for row in connection.execute("select name from sqlite_master").fetchall()}
	connection.close()
	assert "ingest_audit" not in names
	assert "ingest_audit_insert" not in names
	add_comment(database, "c1")  # must not error


def test_is_installed(tmp_path):
	path, database = make_streamer_db(tmp_path)
	assert install_audit_trigger.is_installed(path) is False
	install_audit_trigger.install(path)
	assert install_audit_trigger.is_installed(path) is True
