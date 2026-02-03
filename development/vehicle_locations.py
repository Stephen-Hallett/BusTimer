import logging
import os
from concurrent.futures import ThreadPoolExecutor  # NOQA:F401
from datetime import datetime
from pprint import pprint

import polars as pl
import pytz
import requests
from pydantic import AliasChoices, AliasPath, BaseModel, Field, field_validator

logger = logging.getLogger(__name__)

trips = pl.read_json("work_trips.json")
pprint(trips)
filtered_trips = trips.filter(pl.col.service_id == "Sunday-1")
headers = {"Ocp-Apim-Subscription-Key": os.environ["SUBSCRIPTION_KEY"]}

id_string = ",".join(filtered_trips["trip_id"].to_list())

res = requests.get(
    f"https://api.at.govt.nz/realtime/legacy/vehiclelocations?tripid={id_string}",
    headers=headers,
    timeout=15,
).json()
pprint(res)


class VehicleLocation(BaseModel):
    id: int = Field(validation_alias=AliasChoices(AliasPath("id"), "id"))
    trip_id: str = Field(
        validation_alias=AliasChoices(
            AliasPath("vehicle", "trip", "trip_id"), "trip_id"
        )
    )
    occupancy_status: int = Field(
        validation_alias=AliasChoices(
            AliasPath("vehicle", "occupancy_status"), "occupancy_status"
        )
    )
    bearing: float = Field(
        validation_alias=AliasChoices(
            AliasPath("vehicle", "position", "bearing"), "bearing"
        )
    )
    latitude: float = Field(
        validation_alias=AliasChoices(
            AliasPath("vehicle", "position", "latitude"), "latitude"
        )
    )
    longitude: float = Field(
        validation_alias=AliasChoices(
            AliasPath("vehicle", "position", "longitude"), "longitude"
        )
    )
    speed: int = Field(
        validation_alias=AliasChoices(
            AliasPath("vehicle", "position", "speed"), "speed"
        )
    )
    timestamp: int = Field(
        validation_alias=AliasChoices(AliasPath("vehicle", "timestamp"), "timestamp")
    )
    # start_time is constructed from vehicle.trip.start_date + vehicle.trip.start_time
    start_time: datetime = Field(
        validation_alias=AliasChoices(AliasPath("vehicle", "trip"), "start_time")
    )
    route_id: str = Field(
        validation_alias=AliasChoices(
            AliasPath("vehicle", "trip", "route_id"), "route_id"
        )
    )
    direction_id: int = Field(
        validation_alias=AliasChoices(
            AliasPath("vehicle", "trip", "direction_id"), "direction_id"
        )
    )

    @field_validator("start_time", mode="before")
    @classmethod
    def _build_start_time_from_trip(cls, v) -> datetime:
        # v will be the dict at vehicle.trip because of AliasPath above
        if isinstance(v, dict):
            sd = v.get("start_date")
            st = v.get("start_time")
            if sd and st:
                try:
                    # parse as naive datetime then attach UTC timezone to make it aware
                    return datetime.strptime(f"{sd} {st}", "%Y%m%d %H:%M:%S").replace(
                        tzinfo=pytz.timezone("Pacific/Auckland")
                    )
                except ValueError as exc:
                    logger.exception(
                        "Failed to parse start_date/start_time into datetime: %s", exc
                    )
        return v


for item in res["response"]["entity"]:
    if item.get("trip"):
        pprint(VehicleLocation.model_validate(item))

example = VehicleLocation(
    id=16117,
    trip_id="1286-79821-46200-2-6cea8abc",
    occupancy_status=1,
    bearing=18.0,
    latitude=-36.8708502,
    longitude=174.7772602,
    speed=7,
    timestamp=1769905103,
    start_time=datetime(2026, 2, 1, 12, 50, tzinfo=pytz.timezone("Pacific/Auckland")),
    route_id="INN-202",
    direction_id=0,
)

pprint(example.model_dump())
