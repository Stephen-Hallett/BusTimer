import os

import psycopg2


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
        conn = psycopg2.connect(**self.connection_params)
        with conn.cursor() as cur:
            cur.execute("SET timezone = 'Pacific/Auckland';")
        return conn
