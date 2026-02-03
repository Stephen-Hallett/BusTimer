import logging
import os

import pytz
from psycopg2.extras import RealDictCursor

from ..schemas.trips import Trip
from ..utils.db import BaseDatabase
from ..utils.logger import MyLogger, log


class Controller(BaseDatabase):
    def __init__(self) -> None:
        super().__init__()
        self.logger: logging.Logger = MyLogger().get_logger()
        self.tz: pytz.BaseTzInfo = pytz.timezone("Pacific/Auckland")
        self.realtime_api = "https://api.at.govt.nz/realtime/legacy"
        self.gtfs_api = "https://api.at.govt.nz/gtfs/v3"
        self.headers = {"Ocp-Apim-Subscription-Key": os.environ["SUBSCRIPTION_KEY"]}

    @log
    def create_trip(self, trip: Trip) -> None:
        with (
            self.get_connection() as conn,
            conn.cursor(cursor_factory=RealDictCursor) as cur,
        ):
            # Get latest for each account/platform for today and yesterday (NZ time)
            cur.execute(
                """
                INSERT INTO trips (trip_id, route_id, service_id, direction_id, shape_id)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (trip_id) DO NOTHING
                """,
                (
                    trip.trip_id,
                    trip.route_id,
                    trip.service_id,
                    trip.direction_id,
                    trip.shape_id,
                ),
            )
            conn.commit()
