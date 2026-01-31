import logging
import os

import pytz
import requests

from ..utils.logger import MyLogger, log


class Controller:
    def __init__(self) -> None:
        self.logger: logging.Logger = MyLogger().get_logger()
        self.tz: pytz.BaseTzInfo = pytz.timezone("Pacific/Auckland")
        self.realtime_api = "https://api.at.govt.nz/realtime/legacy"
        self.headers = {
            "Ocp-Apim-Subscription-Key": os.environ["SUBSCRIPTION_KEY"],
            "accept": "application/json",
        }

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
