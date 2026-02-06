import pytz
from fastapi import APIRouter, HTTPException, Query

from ..API.trips import Controller
from ..schemas.trips import Trip
from ..utils.logger import MyLogger

logger = MyLogger().get_logger()

con = Controller()

tz = pytz.timezone("Pacific/Auckland")

router = APIRouter()


@router.post("")
async def create(trip: Trip) -> None:
    return con.create_trip(trip)


@router.get("")
async def get_trips(
    route_id: str | None = Query(None, description="Filter by route ID"),
    service_id: str | None = Query(None, description="Filter by service ID"),
    direction_id: int | None = Query(None, description="Filter by direction ID"),
    shape_id: str | None = Query(None, description="Filter by shape ID"),
    limit: int | None = Query(None, description="Maximum number of results"),
    offset: int | None = Query(0, description="Number of results to skip"),
) -> list[dict]:
    return con.get_trips(
        route_id=route_id,
        service_id=service_id,
        direction_id=direction_id,
        shape_id=shape_id,
        limit=limit,
        offset=offset,
    )


@router.get("/{trip_id}")
async def get_trip(trip_id: str) -> dict:
    trip = con.get_trip(trip_id)
    if trip is None:
        raise HTTPException(status_code=404, detail="Trip not found")
    return trip


@router.put("/{trip_id}")
async def update_trip(trip_id: str, trip: Trip) -> dict:
    success = con.update_trip(trip_id, trip)
    if not success:
        raise HTTPException(status_code=404, detail="Trip not found")
    return {"message": "Trip updated successfully", "trip_id": trip_id}


@router.delete("/{trip_id}")
async def delete_trip(trip_id: str) -> dict:
    success = con.delete_trip(trip_id)
    if not success:
        raise HTTPException(status_code=404, detail="Trip not found")
    return {"message": "Trip deleted successfully", "trip_id": trip_id}
