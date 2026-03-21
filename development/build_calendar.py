import datetime
import io
import zipfile
from pathlib import Path

import holidays
import polars as pl
import requests
from pytz import timezone

data_path = Path("../data/gtfs")
data_path.mkdir(parents=True, exist_ok=True)

# The GTFS files needed
files_needed = {
    "routes.txt",
    "trips.txt",
    "stop_times.txt",
    "stops.txt",
    "shapes.txt",
    "calendar.txt",
}

url = "https://gtfs.at.govt.nz/gtfs.zip"

data_path = Path("../data/gtfs")

print("Downloading zip...")
response = requests.get(url, timeout=10)
response.raise_for_status()

print("Extracting required files...")
with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
    available = set(zf.namelist())
    for filename in files_needed:
        if filename in available:
            zf.extract(filename, data_path)
            print(f"  ✓ Extracted {filename}")
        else:
            print(f"  ✗ Not found: {filename}")

print("Done!")

calendar = pl.read_csv(
    data_path / "calendar.txt",
    columns=[
        "service_id",
        "monday",
        "tuesday",
        "wednesday",
        "thursday",
        "friday",
        "saturday",
        "sunday",
    ],
)

print(calendar)
tz = timezone("Pacific/Auckland")
today = datetime.datetime.now(tz=tz).date()
nz_holidays = holidays.NZ()

days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
day = "sunday" if today in nz_holidays else days[today.weekday()]

print(calendar.filter(pl.col(day) == 1)["service_id"].to_list())
print(list(calendar.iter_rows()))

# def get_service_id() -> None:
#     """Get the current service-id to find what trips are running.

#     Public holidays get treated as sunday's - at least for the inner link. The holidays
#     function doesn't work for anniversary holidays, but I can live with 364/365 days
#     correct.
#     """
#     tz = timezone("Pacific/Auckland")
#     today = datetime.datetime.now(tz=tz).date()
#     nz_holidays = holidays.NZ()
#     if today in nz_holidays:
#         return "Sunday-1"
#     match today.weekday():
#         case n if n <= 4:
#             return "Weekday-1"
#         case n if n == 5:
#             return "Saturday-1"
#         case n if n == 6:
#             return "Sunday-1"
#         case _:
#             raise ValueError("I don't know what day it is today...")
