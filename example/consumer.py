from contextlib import asynccontextmanager
import asyncpg
from pgqueuer import PgQueuer
from pgqueuer.db import AsyncpgDriver
from pgqueuer.models import Job

DATABASE_URL = "postgresql://queue_user:queue_pass@localhost:5432/queue_db"

# Python consumer for jobs enqueued from Haskell or Python.
@asynccontextmanager
async def main():
    connection = await asyncpg.connect(dsn=DATABASE_URL)

    try:
        pgq = PgQueuer(AsyncpgDriver(connection))

        @pgq.entrypoint("fetch")
        async def process(job: Job) -> None:
            print(f"Processed: {job!r}")

        yield pgq

    finally:
        await connection.close()
