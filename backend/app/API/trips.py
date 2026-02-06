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

    @log
    def get_trips(
        self,
        route_id: str | None = None,
        service_id: str | None = None,
        direction_id: str | None = None,
        shape_id: str | None = None,
        limit: int | None = None,
        offset: int | None = 0,
    ) -> list[dict]:
        """
        Get trips with optional filtering.

        Args:
            route_id: Filter by route ID (single ID or comma-separated list)
            service_id: Filter by service ID (single ID or comma-separated list)
            direction_id: Filter by direction ID (single ID or comma-separated list)
            shape_id: Filter by shape ID (single ID or comma-separated list)
            limit: Maximum number of results to return
            offset: Number of results to skip (for pagination)

        Returns:
            List of trip dictionaries
        """
        with (
            self.get_connection() as conn,
            conn.cursor(cursor_factory=RealDictCursor) as cur,
        ):
            # Build dynamic query based on filters
            query = "SELECT * FROM trips WHERE 1=1"
            params = []

            if route_id is not None:
                route_ids = [rid.strip() for rid in route_id.split(",")]
                query += " AND route_id = ANY(%s)"
                params.append(route_ids)

            if service_id is not None:
                service_ids = [sid.strip() for sid in service_id.split(",")]
                query += " AND service_id = ANY(%s)"
                params.append(service_ids)

            if direction_id is not None:
                direction_ids = [int(did.strip()) for did in direction_id.split(",")]
                query += " AND direction_id = ANY(%s)"
                params.append(direction_ids)

            if shape_id is not None:
                shape_ids = [shid.strip() for shid in shape_id.split(",")]
                query += " AND shape_id = ANY(%s)"
                params.append(shape_ids)

            query += " ORDER BY trip_id"

            if limit is not None:
                query += " LIMIT %s"
                params.append(limit)

            if offset:
                query += " OFFSET %s"
                params.append(offset)

            cur.execute(query, params)
            return cur.fetchall()

    @log
    def get_trip(self, trip_id: str) -> dict | None:
        """
        Get a single trip by ID.

        Args:
            trip_id: The trip ID to retrieve

        Returns:
            Trip dictionary or None if not found
        """
        with (
            self.get_connection() as conn,
            conn.cursor(cursor_factory=RealDictCursor) as cur,
        ):
            cur.execute("SELECT * FROM trips WHERE trip_id = %s", (trip_id,))
            return cur.fetchone()

    @log
    def update_trip(self, trip_id: str, trip: Trip) -> bool:
        """
        Update an existing trip.

        Args:
            trip_id: The trip ID to update
            trip: Trip object with updated data

        Returns:
            True if trip was updated, False if trip not found
        """
        with (
            self.get_connection() as conn,
            conn.cursor(cursor_factory=RealDictCursor) as cur,
        ):
            cur.execute(
                """
                UPDATE trips
                SET route_id = %s,
                    service_id = %s,
                    direction_id = %s,
                    shape_id = %s
                WHERE trip_id = %s
                """,
                (
                    trip.route_id,
                    trip.service_id,
                    trip.direction_id,
                    trip.shape_id,
                    trip_id,
                ),
            )
            conn.commit()
            return cur.rowcount > 0

    @log
    def delete_trip(self, trip_id: str) -> bool:
        """
        Delete a trip by ID.

        Args:
            trip_id: The trip ID to delete

        Returns:
            True if trip was deleted, False if trip not found
        """
        with (
            self.get_connection() as conn,
            conn.cursor(cursor_factory=RealDictCursor) as cur,
        ):
            cur.execute("DELETE FROM trips WHERE trip_id = %s", (trip_id,))
            conn.commit()
            return cur.rowcount > 0
