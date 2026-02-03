import logging
import os

import pytz
import requests

from ..schemas.vehicles import VehicleLocation
from ..utils.db import BaseDatabase
from ..utils.logger import MyLogger, log


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
    def save_vehicle_locations(self) -> float:
        filtered_trips = ...  # TODO: Add db call to get all trips
        id_string = ",".join(filtered_trips["trip_id"].to_list())
        res = requests.get(
            f"https://api.at.govt.nz/realtime/legacy/vehiclelocations?tripid={id_string}",
            headers=self.headers,
            timeout=15,
        ).json()
        return [
            VehicleLocation.model_validate(item)
            for item in res["response"]["entity"]
            if item.get("trip")
        ]
