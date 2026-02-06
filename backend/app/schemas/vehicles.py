from datetime import datetime

import pytz
from pydantic import AliasChoices, AliasPath, BaseModel, Field, field_validator

from ..utils.logger import MyLogger

logger = MyLogger().get_logger()


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
        if isinstance(v, dict):
            sd = v.get("start_date")
            st = v.get("start_time")
            if sd and st:
                try:
                    # Parse as naive datetime in NZ timezone
                    nz_tz = pytz.timezone("Pacific/Auckland")
                    naive_dt = datetime.strptime(f"{sd} {st}", "%Y%m%d %H:%M:%S")  # NOQA
                    return nz_tz.localize(naive_dt)
                except ValueError as exc:
                    logger.exception(
                        "Failed to parse start_date/start_time into datetime: %s", exc
                    )
        return v
