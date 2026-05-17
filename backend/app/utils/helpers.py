import datetime

import holidays
from pytz import timezone
from sqlalchemy import select

from ..models.models import Calendar
from ..utils.db import BaseDatabase

db = BaseDatabase()


def get_service_id() -> list[str]:
    """Get the current service-id to find what trips are running.

    Public holidays get treated as sunday's - at least for the inner link. The holidays
    function doesn't work for anniversary holidays, but I can live with 364/365 days
    correct.
    """
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

    with db.get_session() as session:
        return (
            session.execute(
                select(Calendar.service_id).where(getattr(Calendar, day) == 1)
            )
            .scalars()
            .all()
        )
