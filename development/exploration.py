from pathlib import Path

import folium
import polars as pl

data_path = Path("../data/gtfs")

bus_set = ["INN-202"]

journey = (7191, 7016)  # From home to britomart

routes = pl.read_csv(data_path / "routes.txt").filter(pl.col.route_id.is_in(bus_set))
trips = pl.read_csv(data_path / "trips.txt").filter(pl.col.route_id.is_in(bus_set))
stop_times = pl.read_csv(data_path / "stop_times.txt").join(
    trips, how="inner", on="trip_id"
)
stops = pl.read_csv(data_path / "stops.txt").join(stop_times, how="inner", on="stop_id")
shapes = pl.read_csv(data_path / "shapes.txt")

trip_stops = trips.join(stops, on="trip_id", how="left")

trip_stops = trip_stops.join(
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

# print(trip_info)
# print(trip_info.unique())

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


relevant_trips = detailed_trips.filter(
    pl.col.stops.list.eval(pl.element().struct["stop_code"].is_in(journey)).list.sum()
    == len(journey)
)
from pprint import pprint

pprint(relevant_trips.drop("stop_id").unique())

relevant_trips.drop("stop_id").unique().write_json(
    "work_trips.json"
)  # counter clockwise, direction_id = 0

relevant_trips.drop("stop_id", "stops").unique().write_csv("trips.csv")

example_trip = relevant_trips.head(1)

example_stops = example_trip.select("stops").explode("stops").unnest("stops")
example_stops.write_json("stops.json")

# Create map with folium

map_center = [example_stops["stop_lat"].mean(), example_stops["stop_lon"].mean()]
m = folium.Map(location=map_center, zoom_start=13)

# Add markers for each stop
for row in example_stops.iter_rows(named=True):
    folium.Marker(
        location=[row["stop_lat"], row["stop_lon"]],
        popup=row["stop_code"],
        tooltip=row["stop_code"],
    ).add_to(m)

coordinates = [
    [row["stop_lat"], row["stop_lon"]] for row in example_stops.iter_rows(named=True)
]
folium.PolyLine(coordinates, color="red", weight=3, opacity=0.8).add_to(m)

# Save as HTML (can't directly save as JPG, but see below)
m.save("bus_route.html")
