from datetime import datetime

import pytz
from pydantic import AliasChoices, AliasPath, BaseModel, Field, field_validator

from ..utils.logger import MyLogger

logger = MyLogger().get_logger()


class VehicleData(BaseModel):
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
                    nz_tz = pytz.timezone("Pacific/Auckland")
                    naive_dt = datetime.strptime(f"{sd} {st}", "%Y%m%d %H:%M:%S")
                    return nz_tz.localize(naive_dt)
                except ValueError as exc:
                    logger.exception(
                        "Failed to parse start_date/start_time into datetime: %s", exc
                    )
        return v


class VehicleStop(BaseModel):
    is_deleted: bool = Field(
        validation_alias=AliasChoices(AliasPath("is_deleted"), "is_deleted")
    )
    trip_id: str = Field(
        validation_alias=AliasChoices(
            AliasPath("trip_update", "trip", "trip_id"), "trip_id"
        )
    )
    route_id: str = Field(
        validation_alias=AliasChoices(
            AliasPath("trip_update", "trip", "route_id"), "route_id"
        )
    )
    direction_id: int = Field(
        validation_alias=AliasChoices(
            AliasPath("trip_update", "trip", "direction_id"), "direction_id"
        )
    )
    schedule_relationship: int = Field(
        validation_alias=AliasChoices(
            AliasPath("trip_update", "trip", "schedule_relationship"),
            "schedule_relationship",
        )
    )
    start_time: datetime = Field(
        validation_alias=AliasChoices(AliasPath("trip_update", "trip"), "start_time")
    )
    stop_sequence: int = Field(
        validation_alias=AliasChoices(
            AliasPath("trip_update", "stop_time_update", "stop_sequence"),
            "stop_sequence",
        )
    )
    stop_id: str = Field(
        validation_alias=AliasChoices(
            AliasPath("trip_update", "stop_time_update", "stop_id"), "stop_id"
        )
    )
    stop_schedule_relationship: int = Field(
        validation_alias=AliasChoices(
            AliasPath("trip_update", "stop_time_update", "schedule_relationship"),
            "stop_schedule_relationship",
        )
    )
    departure_delay: int | None = Field(
        default=None,
        validation_alias=AliasChoices(
            AliasPath("trip_update", "stop_time_update", "departure", "delay"),
            "departure_delay",
        ),
    )
    departure_time: int | None = Field(
        default=None,
        validation_alias=AliasChoices(
            AliasPath("trip_update", "stop_time_update", "departure", "time"),
            "departure_time",
        ),
    )
    departure_uncertainty: int | None = Field(
        default=None,
        validation_alias=AliasChoices(
            AliasPath("trip_update", "stop_time_update", "departure", "uncertainty"),
            "departure_uncertainty",
        ),
    )
    vehicle_id: str = Field(
        validation_alias=AliasChoices(
            AliasPath("trip_update", "vehicle", "id"), "vehicle_id"
        )
    )
    label: str = Field(
        validation_alias=AliasChoices(
            AliasPath("trip_update", "vehicle", "label"), "label"
        )
    )
    license_plate: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            AliasPath("trip_update", "vehicle", "license_plate"), "license_plate"
        ),
    )
    timestamp: int = Field(
        validation_alias=AliasChoices(
            AliasPath("trip_update", "timestamp"), "timestamp"
        )
    )
    delay: int = Field(
        validation_alias=AliasChoices(AliasPath("trip_update", "delay"), "delay")
    )

    @field_validator("start_time", mode="before")
    @classmethod
    def _build_start_time_from_trip(cls, v) -> datetime:
        if isinstance(v, dict):
            sd = v.get("start_date")
            st = v.get("start_time")
            if sd and st:
                try:
                    nz_tz = pytz.timezone("Pacific/Auckland")
                    naive_dt = datetime.strptime(f"{sd} {st}", "%Y%m%d %H:%M:%S")
                    return nz_tz.localize(naive_dt)
                except ValueError as exc:
                    logger.exception(
                        "Failed to parse start_date/start_time into datetime: %s", exc
                    )
        return v


class VehicleLocation(BaseModel):
    id: int
    trip_id: str
    occupancy_status: int | None = None
    bearing: float | None = None
    latitude: float
    longitude: float
    speed: int | None = None
    timestamp: int
    start_time: datetime
    route_id: str
    direction_id: int
    schedule_relationship: int | None = None
    is_deleted: bool
    stop_sequence: int | None = None
    stop_id: str
    stop_schedule_relationship: int | None = None
    departure_delay: int | None = None
    departure_time: int | None = None
    departure_uncertainty: int | None = None
    vehicle_id: str
    label: str
    license_plate: str | None
    delay: int

    @classmethod
    def from_vehicle_data_and_stop(
        cls, data: VehicleData, stop: VehicleStop
    ) -> "VehicleLocation":
        return cls(
            **data.model_dump(),
            is_deleted=stop.is_deleted,
            schedule_relationship=stop.schedule_relationship,
            stop_sequence=stop.stop_sequence,
            stop_id=stop.stop_id,
            stop_schedule_relationship=stop.stop_schedule_relationship,
            departure_delay=stop.departure_delay,
            departure_time=stop.departure_time,
            departure_uncertainty=stop.departure_uncertainty,
            vehicle_id=stop.vehicle_id,
            label=stop.label,
            license_plate=stop.license_plate,
            delay=stop.delay,
        )
