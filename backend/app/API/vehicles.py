import logging
import os

import pytz
import requests
from sqlalchemy.dialects.postgresql import insert

from ..API.trips import Controller as TripController
from ..models.models import VehicleLocation as VehicleLocationModel
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

        with self.get_session() as session:
            session.execute(
                insert(VehicleLocationModel).on_conflict_do_nothing(
                    index_elements=["id", "timestamp"]
                ),
                [location.model_dump() for location in vehicle_locations],
            )
            session.commit()

    @log
    def save_vehicle_locations(self) -> int:
        service_ids = get_service_id()
        filtered_trips = trip_con.get_trips(service_id=",".join(service_ids))
        if not len(filtered_trips):
            return 0
        trip_ids = {t.trip_id for t in filtered_trips}
        vehicle_locations_res = requests.get(
            f"{self.realtime_api}/vehiclelocations", headers=self.headers, timeout=15
        ).json()
        trip_updates_res = requests.get(
            f"{self.realtime_api}/tripupdates", headers=self.headers, timeout=15
        ).json()

        self.logger.info(vehicle_locations_res)
        self.logger.info(trip_updates_res)

        vehicle_data: dict[str, VehicleData] = {}
        for item in vehicle_locations_res["response"]["entity"]:
            if item.get("vehicle", {}).get("trip"):
                try:
                    vd = VehicleData.model_validate(item)
                    if vd.trip_id in trip_ids:
                        vehicle_data[vd.trip_id] = vd
                except Exception as e:
                    self.logger.warning(f"Failed to validate vehicle data: {e}")

        vehicle_stops: dict[str, VehicleStop] = {}
        for raw_item in trip_updates_res["response"]["entity"]:
            trip_update = raw_item.get("trip_update")
            if trip_update:
                stu = trip_update.get("stop_time_update")
                if isinstance(stu, list):
                    item = {
                        **raw_item,
                        "trip_update": {
                            **trip_update,
                            "stop_time_update": stu[0] if stu else {},
                        },
                    }
                else:
                    item = raw_item
                try:
                    vs = VehicleStop.model_validate(item)
                    if vs.trip_id in trip_ids:
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
