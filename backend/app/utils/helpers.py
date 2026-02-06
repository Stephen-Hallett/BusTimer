import datetime

import holidays
from pytz import timezone


def get_service_id() -> None:
    """Get the current service-id to find what trips are running.

    Public holidays get treated as sunday's - at least for the inner link. The holidays
    function doesn't work for anniversary holidays, but I can live with 364/365 days
    correct.
    """
    tz = timezone("Pacific/Auckland")
    today = datetime.datetime.now(tz=tz).date()
    nz_holidays = holidays.NZ()
    if today in nz_holidays:
        return "Sunday-1"
    match today.weekday():
        case n if n <= 4:  # NOQA:PLR2004
            return "Weekday-1"
        case n if n == 5:  # NOQA:PLR2004
            return "Saturday-1"
        case n if n == 6:  # NOQA:PLR2004
            return "Sunday-1"
        case _:
            raise ValueError("I don't know what day it is today...")
