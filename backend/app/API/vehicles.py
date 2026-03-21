import logging
import os

import pytz
import requests
from psycopg2.extras import RealDictCursor, execute_values

from ..API.trips import Controller as TripController
from ..schemas.vehicles import VehicleData, VehicleLocation, VehicleStop
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
                    vl.schedule_relationship,
                    vl.is_deleted,
                    vl.stop_sequence,
                    vl.stop_id,
                    vl.stop_schedule_relationship,
                    vl.departure_delay,
                    vl.departure_time,
                    vl.departure_uncertainty,
                    vl.vehicle_id,
                    vl.label,
                    vl.license_plate,
                    vl.delay,
                )
                for vl in vehicle_locations
            ]

            execute_values(
                cur,
                """
                INSERT INTO vehicle_locations (
                    id, trip_id, occupancy_status, bearing, latitude,
                    longitude, speed, timestamp, start_time, route_id,
                    direction_id, schedule_relationship, is_deleted,
                    stop_sequence, stop_id, stop_schedule_relationship,
                    departure_delay, departure_time, departure_uncertainty,
                    vehicle_id, label, license_plate, delay
                ) VALUES %s
                """,
                values,
            )
            conn.commit()

    @log
    def save_vehicle_locations(self) -> int:
        filtered_trips = trip_con.get_trips(service_id=",".join(get_service_id()))
        if not len(filtered_trips):
            return 0
        id_string = ",".join([trip["trip_id"] for trip in filtered_trips])

        vehicle_locations_res = requests.get(
            f"{self.realtime_api}/vehiclelocations?tripid={id_string}",
            headers=self.headers,
            timeout=15,
        ).json()
        trip_updates_res = requests.get(
            f"{self.realtime_api}/tripupdates?tripid={id_string}",
            headers=self.headers,
            timeout=15,
        ).json()

        self.logger.info(vehicle_locations_res)
        self.logger.info(trip_updates_res)

        vehicle_data: dict[str, VehicleData] = {}
        for item in vehicle_locations_res["response"]["entity"]:
            if item.get("vehicle", {}).get("trip"):
                try:
                    vd = VehicleData.model_validate(item)
                    vehicle_data[vd.trip_id] = vd
                except Exception as e:
                    self.logger.warning(f"Failed to validate vehicle data: {e}")

        vehicle_stops: dict[str, VehicleStop] = {}
        for item in trip_updates_res["response"]["entity"]:
            if item.get("trip_update"):
                try:
                    vs = VehicleStop.model_validate(item)
                    vehicle_stops[vs.trip_id] = vs
                except Exception as e:
                    self.logger.warning(f"Failed to validate vehicle stop: {e}")

        vehicle_locations = []
        for trip_id, data in vehicle_data.items():
            stop = vehicle_stops.get(trip_id)
            if stop:
                try:
                    vehicle_locations.append(
                        VehicleLocation.from_vehicle_data_and_stop(data, stop)
                    )
                except Exception as e:
                    self.logger.warning(f"Failed to create vehicle location: {e}")

        if vehicle_locations:
            self.create_vehicle_locations(vehicle_locations)

        return len(vehicle_locations)
