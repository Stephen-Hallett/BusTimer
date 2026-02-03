import pytz
from fastapi import APIRouter

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
