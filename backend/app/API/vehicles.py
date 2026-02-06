import logging
import os

import pytz
import requests
from psycopg2.extras import RealDictCursor, execute_values

from ..API.trips import Controller as TripController
from ..schemas.vehicles import VehicleLocation
from ..utils.db import BaseDatabase
from ..utils.helpers import get_service_id
from ..utils.logger import MyLogger, log

trip_con = TripController()


class Controller(BaseDatabase):
    def __init__(self) -> None:
        super().__init__()
        self.logger: logging.Logger = MyLogger().get_logger()
        self.tz: pytz.BaseTzInfo = pytz.timezone("Pacific/Auckland")
        self.realtime_api = "https://api.at.govt.nz/realtime/legacy"
        self.headers = {"Ocp-Apim-Subscription-Key": os.environ["SUBSCRIPTION_KEY"]}

    @log
    def get_vehicle_location(self, vehicle_id: int) -> float:
        location = requests.get(
            f"{self.realtime_api}/vehiclelocations",
            headers=self.headers,
            params={"vehicleid": vehicle_id},
            timeout=5,
        ).json()
        if location.get("status") == "OK":
            return location["response"]["entity"][0]
        raise ValueError(f"No vehicle found with the ID {vehicle_id}.")

    @log
    def create_vehicle_location(self, vehicle_location: VehicleLocation) -> None:
        with (
            self.get_connection() as conn,
            conn.cursor(cursor_factory=RealDictCursor) as cur,
        ):
            cur.execute(
                """
                INSERT INTO vehicle_locations (
                    id,
                    trip_id,
                    occupancy_status,
                    bearing,
                    latitude,
                    longitude,
                    speed,
                    timestamp,
                    start_time,
                    route_id,
                    direction_id
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    vehicle_location.id,
                    vehicle_location.trip_id,
                    vehicle_location.occupancy_status,
                    vehicle_location.bearing,
                    vehicle_location.latitude,
                    vehicle_location.longitude,
                    vehicle_location.speed,
                    vehicle_location.timestamp,
                    vehicle_location.start_time,
                    vehicle_location.route_id,
                    vehicle_location.direction_id,
                ),
            )
            conn.commit()

    @log
    def create_vehicle_locations(
        self, vehicle_locations: list[VehicleLocation]
    ) -> None:
        """Insert multiple vehicle locations in a single transaction."""
        if not vehicle_locations:
            return

        with (
            self.get_connection() as conn,
            conn.cursor(cursor_factory=RealDictCursor) as cur,
        ):
            values = [
                (
                    vl.id,
                    vl.trip_id,
                    vl.occupancy_status,
                    vl.bearing,
                    vl.latitude,
                    vl.longitude,
                    vl.speed,
                    vl.timestamp,
                    vl.start_time,
                    vl.route_id,
                    vl.direction_id,
                )
                for vl in vehicle_locations
            ]

            execute_values(
                cur,
                """
                INSERT INTO vehicle_locations (
                    id, trip_id, occupancy_status, bearing, latitude, 
                    longitude, speed, timestamp, start_time, route_id, direction_id
                ) VALUES %s
                """,
                values,
            )
            conn.commit()

    @log
    def save_vehicle_locations(self) -> int:
        filtered_trips = trip_con.get_trips(service_id=f"Daily-1,{get_service_id()}")
        id_string = ",".join([trip["trip_id"] for trip in filtered_trips])
        res = requests.get(
            f"{self.realtime_api}/vehiclelocations?tripid={id_string}",
            headers=self.headers,
            timeout=15,
        ).json()

        vehicle_locations = []
        for item in res["response"]["entity"]:
            if item.get("trip"):
                try:
                    vehicle_locations.append(VehicleLocation.model_validate(item))
                except Exception as e:  # NOQA
                    self.logger.warning(f"Failed to validate vehicle location: {e}")
                    continue

        if vehicle_locations:
            self.create_vehicle_locations(vehicle_locations)

        return len(vehicle_locations)
