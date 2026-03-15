import os
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import pytz
from apscheduler.schedulers.background import BackgroundScheduler
from fastapi import APIRouter

from ..API.gtfs import Controller as GtfsController
from ..API.vehicles import Controller
from ..utils.logger import MyLogger

logger = MyLogger().get_logger()

con = Controller()
gtfs_con = GtfsController()

tz = pytz.timezone("Pacific/Auckland")


def save_data() -> None:
    logger.info("Saving vehicle locations")
    con.save_vehicle_locations()


def refresh_gtfs() -> None:
    logger.info("Running weekly GTFS refresh")
    gtfs_con.refresh_gtfs()


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

    update_minute, update_hour, update_day, update_month, update_wday = (
        os.environ["UPDATE_TRIPS_TIME"].strip('"').split(" ")
    )
    scheduler.add_job(
        refresh_gtfs,
        "cron",
        minute=update_minute,
        hour=update_hour,
        day=update_day,
        month=update_month,
        day_of_week=update_wday,
    )

    scheduler.start()
    logger.info(f"Scheduler started with cron: {os.environ['SAVE_TIME']} (NZ timezone)")

    logger.info("Running GTFS refresh on startup...")
    gtfs_con.refresh_gtfs()

    yield
    scheduler.shutdown()  # Clean shutdown on app stop


router = APIRouter(lifespan=lifespan)
