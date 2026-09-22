import sys
import pytest
import discord_logging

log = discord_logging.init_logging(debug=True)

sys.path.append("src")

# Import counters before anything that pulls in prometheus_client (praw_wrapper's
# reddit module does) so the PROMETHEUS_DISABLE_CREATED_SERIES env var it sets
# takes effect before prometheus_client's first import in the test process.
import counters
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
