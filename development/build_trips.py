import io
import json
import os
import zipfile
from pathlib import Path

import polars as pl
import requests

data_path = Path("../data/gtfs")
data_path.mkdir(parents=True, exist_ok=True)

# The GTFS files needed
files_needed = {"routes.txt", "trips.txt", "stop_times.txt", "stops.txt", "shapes.txt"}

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


journeys = [tuple(j) for j in json.loads(os.environ["JOURNEYS"])]

routes = pl.read_csv(data_path / "routes.txt")
trips = pl.read_csv(data_path / "trips.txt")
stop_times = pl.read_csv(data_path / "stop_times.txt").join(
    trips, how="inner", on="trip_id"
)
stops = pl.read_csv(data_path / "stops.txt").join(stop_times, how="inner", on="stop_id")
shapes = pl.read_csv(data_path / "shapes.txt")

trip_stops = trips.join(stops, on="trip_id", how="left").join(
    shapes,
    how="inner",
    left_on=["shape_id", "stop_lat", "stop_lon"],
    right_on=["shape_id", "shape_pt_lat", "shape_pt_lon"],
)

trip_info = trip_stops[
    [
        "route_id",
        "trip_id",
        "service_id",
        "direction_id",
        "shape_id",
        "stop_id",
        "stop_code",
        "stop_lat",
        "stop_lon",
        "stop_sequence",
    ]
].unique()

trip_info = (
    trip_info.with_columns(
        stop=pl.struct(
            ["stop_id", "stop_code", "stop_lat", "stop_lon", "stop_sequence"]
        )
    )
    .drop(["stop_code", "stop_lat", "stop_lon"])
    .sort(by=["trip_id", "stop_sequence"], descending=False)
)

detailed_trips = (
    trip_info.with_columns(stops=pl.col.stop.over("trip_id", mapping_strategy="join"))
    .drop("stop_sequence", "stop")
    .unique()
)

filters = [
    pl.col.stops.list.eval(pl.element().struct["stop_code"].is_in(journey)).list.sum()
    == len(journey)
    for journey in journeys
]

key_trips = detailed_trips.filter(pl.any_horizontal(filters))

# Collect the small set of segment IDs from key trips only
key_segments = (
    key_trips.with_columns(
        start_stop=pl.col("stops").list.eval(pl.element().struct.field("stop_code")),
        start_stop_id=pl.col("stops").list.eval(pl.element().struct.field("stop_id")),
        start_lat=pl.col("stops").list.eval(pl.element().struct.field("stop_lat")),
        start_lon=pl.col("stops").list.eval(pl.element().struct.field("stop_lon")),
    )
    .with_columns(
        end_stop=pl.col.start_stop.list.eval(pl.element().shift(-1)),
        end_stop_id=pl.col.start_stop_id.list.eval(pl.element().shift(-1)),
        end_lat=pl.col.start_lat.list.eval(pl.element().shift(-1)),
        end_lon=pl.col.start_lon.list.eval(pl.element().shift(-1)),
    )
    .explode([pl.col.start_stop, pl.col.end_stop])
    .filter(pl.col.end_stop.is_not_null())
    .with_columns(
        segment_id=pl.col.start_stop.cast(str) + pl.lit("-") + pl.col.end_stop.cast(str)
    )
)

key_segment_ids = list((key_segments.select("segment_id").unique())["segment_id"])

segments = (
    trip_info.lazy()
    .with_columns(stop_code=pl.col("stop").struct.field("stop_code"))
    .sort(["trip_id", "stop_sequence"])
    .with_columns(end_stop_code=pl.col("stop_code").shift(-1).over("trip_id"))
    .filter(pl.col.end_stop_code.is_not_null())
    .with_columns(
        segment_id=pl.col("stop_code").cast(str)
        + pl.lit("-")
        + pl.col("end_stop_code").cast(str)
    )
    .filter(pl.col.segment_id.is_in(key_segment_ids))
    .select("trip_id", "segment_id")
    .unique()
    .collect(engine="streaming")
)

relevant_trips = detailed_trips.join(
    segments[["trip_id"]].unique(), on="trip_id", how="inner"
)

# Segments - The content for the trip_segments df.
#   All segments which are on the specified routes, and every trip
#   that goes through them.
# Relevant trips - all trips which go through any key segment
# Key segments - Information for each segment
