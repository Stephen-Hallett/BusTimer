import logging
import os

import pytz
from sqlalchemy import select

from ..models.models import Trip as TripModel
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
        with self.get_session() as session:
            session.add(TripModel(**trip.model_dump()))
            session.commit()

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
        with self.get_session() as session:
            query = select(TripModel)

            if route_id is not None:
                route_ids = [rid.strip() for rid in route_id.split(",")]
                query = query.where(TripModel.route_id.in_(route_ids))

            if service_id is not None:
                service_ids = [sid.strip() for sid in service_id.split(",")]
                query = query.where(TripModel.service_id.in_(service_ids))

            if direction_id is not None:
                direction_ids = [int(did.strip()) for did in direction_id.split(",")]
                query = query.where(TripModel.direction_id.in_(direction_ids))

            if shape_id is not None:
                shape_ids = [shid.strip() for shid in shape_id.split(",")]
                query = query.where(TripModel.shape_id.in_(shape_ids))

            query = query.order_by(TripModel.trip_id)

            if limit is not None:
                query = query.limit(limit)

            if offset:
                query = query.offset(offset)

            return [Trip.model_validate(t) for t in session.scalars(query).all()]

    @log
    def get_trip(self, trip_id: str) -> dict | None:
        """
        Get a single trip by ID.

        Args:
            trip_id: The trip ID to retrieve

        Returns:
            Trip dictionary or None if not found
        """
        with self.get_session() as session:
            trip = session.get(TripModel, trip_id)
        if trip is None:
            return None
        return Trip.model_validate(trip)

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
        with self.get_session() as session:
            existing = session.get(TripModel, trip_id)
            if existing is None:
                return False
            for key, value in trip.model_dump().items():
                setattr(existing, key, value)
            session.commit()
            return True

    @log
    def delete_trip(self, trip_id: str) -> bool:
        """
        Delete a trip by ID.

        Args:
            trip_id: The trip ID to delete

        Returns:
            True if trip was deleted, False if trip not found
        """
        with self.get_session() as session:
            trip = session.get(TripModel, trip_id)
            if trip is None:
                return False
            session.delete(trip)
            session.commit()
            return True
