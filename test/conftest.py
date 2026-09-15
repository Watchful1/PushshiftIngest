import sys
import pytest
import discord_logging

log = discord_logging.init_logging(debug=True)

sys.path.append("src")

from praw_wrapper import IngestDatabase
from store import Store


@pytest.fixture
def ingest_db():
	database = IngestDatabase(debug=True)
	database.set_default_client("remindme")
	database.get_or_add_client("updateme")
	return database


@pytest.fixture
def store(ingest_db):
	return Store(ingest_db)
