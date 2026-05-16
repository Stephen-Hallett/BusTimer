import os

from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.orm import Session, sessionmaker


class BaseDatabase:
    def __init__(self) -> None:
        self.engine = create_engine(
            URL.create(
                drivername="postgresql+psycopg2",
                host=os.environ["POSTGRES_HOST"],
                port=int(os.environ["POSTGRES_PORT"]),
                database=os.environ["POSTGRES_DB"],
                username=os.environ["POSTGRES_USER"],
                password=os.environ["POSTGRES_PW"],
            ),
            connect_args={"options": "-c timezone=Pacific/Auckland"},
        )
        self.Session = sessionmaker(bind=self.engine)

    def get_session(self) -> Session:
        return self.Session()
