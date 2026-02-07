import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import APIRouter

from ..API.vehicles import Controller
from ..utils.logger import MyLogger

logger = MyLogger().get_logger()

con = Controller()

tz = pytz.timezone("Pacific/Auckland")


def save_data() -> None:
    logger.info("Saving vehicle locations")
    con.save_vehicle_locations()


@asynccontextmanager
async def lifespan(_: APIRouter) -> AsyncGenerator[None, None]:
    scheduler = BackgroundScheduler(timezone=tz)
    minute, hour, day, month, wday = os.environ["SAVE_TIME"].strip('"').split(" ")
    scheduler.add_job(
        save_data,
        "cron",
        minute=minute,
        hour=hour,
        day=day,
        month=month,
        day_of_week=wday,
    )
    scheduler.start()
    logger.info(f"Scheduler started with cron: {os.environ['SAVE_TIME']} (NZ timezone)")
    yield
    scheduler.shutdown()  # Clean shutdown on app stop


router = APIRouter(lifespan=lifespan)
