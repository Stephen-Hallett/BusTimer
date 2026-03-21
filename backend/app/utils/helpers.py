import datetime

import holidays
import polars as pl
from psycopg2.extras import RealDictCursor
from pytz import timezone

from ..utils.db import BaseDatabase

db = BaseDatabase()


def get_service_id() -> list[str]:
    """Get the current service-id to find what trips are running.

    Public holidays get treated as sunday's - at least for the inner link. The holidays
    function doesn't work for anniversary holidays, but I can live with 364/365 days
    correct.
    """
    with db.get_connection() as conn, conn.cursor(cursor_factory=RealDictCursor) as cur:
        query = """
    select * from calendar
"""
        cur.execute(query)
        calendar = pl.from_dicts([dict(row) for row in cur.fetchall()])

    tz = timezone("Pacific/Auckland")
    today = datetime.datetime.now(tz=tz).date()
    nz_holidays = holidays.NZ()

    days = [
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
        "sunday",
    ]
    day = "sunday" if today in nz_holidays else days[today.weekday()]

    return calendar.filter(pl.col(day) == 1)["service_id"].to_list()
