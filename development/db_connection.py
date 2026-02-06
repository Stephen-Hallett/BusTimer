import os

import psycopg2
from psycopg2.extras import RealDictCursor


class BaseDatabase:
    def __init__(self) -> None:
        self.connection_params: dict[str, str] = {
            "host": os.environ["POSTGRES_HOST"],
            "port": int(os.environ["POSTGRES_PORT"]),
            "database": os.environ["POSTGRES_DB"],
            "user": os.environ["POSTGRES_USER"],
            "password": os.environ["POSTGRES_PW"],
        }

    def get_connection(self):  # NOQA
        return psycopg2.connect(**self.connection_params)


db = BaseDatabase()
print(db.connection_params)

with db.get_connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
    # Build dynamic query based on filters
    query = "SELECT * FROM trips WHERE 1=1"
    cur.execute(query)
    print(cur.fetchall())
