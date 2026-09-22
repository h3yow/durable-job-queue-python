import logging
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from jobqueue import BackoffPolicy, JobQueue, SQLiteStore  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)


@pytest.fixture()
def db_path(tmp_path):
    return str(tmp_path / "jobs.db")


@pytest.fixture()
def queue(db_path):
    q = JobQueue(
        SQLiteStore(db_path),
        capacity=100,
        backoff=BackoffPolicy(base_seconds=0.01, factor=2.0, max_seconds=0.1),
        lease_seconds=0.2,
    )
    yield q
    q.store.close()
