import asyncpg
from pgqueuer.db import AsyncpgDriver
import asyncio
from pgqueuer.queries import Queries

async def main() -> None:
    DATABASE_URL = "postgresql://queue_user:queue_pass@localhost:5432/queue_db"
    connection = await asyncpg.connect(dsn=DATABASE_URL)
    queries = Queries(AsyncpgDriver(connection))
    # Enqueue a job that can be consumed by the Haskell example too.
    await queries.enqueue("fetch", b"hello world")
    print("done")

asyncio.run(main())
