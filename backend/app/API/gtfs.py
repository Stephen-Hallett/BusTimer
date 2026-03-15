import io
import json
import logging
import os
import tempfile
import zipfile

import polars as pl
import requests
from psycopg2.extras import execute_values

from ..utils.db import BaseDatabase
from ..utils.logger import MyLogger, log

GTFS_URL = "https://gtfs.at.govt.nz/gtfs.zip"
FILES_NEEDED = {"routes.txt", "trips.txt", "stop_times.txt", "stops.txt", "shapes.txt"}


class Controller(BaseDatabase):
    def __init__(self) -> None:
        super().__init__()
        self.logger: logging.Logger = MyLogger().get_logger()

    @log
    def _download_gtfs(self, data_path: str) -> None:
        self.logger.info("Downloading GTFS zip...")
        response = requests.get(GTFS_URL, timeout=60)
        response.raise_for_status()

        self.logger.info("Extracting required GTFS files...")
        with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
            available = set(zf.namelist())
            for filename in FILES_NEEDED:
                if filename in available:
                    zf.extract(filename, data_path)
                else:
                    raise RuntimeError(
                        f"Required GTFS file not found in zip: {filename}"
                    )

    @log
    def _build_dataframes(
        self, data_path: str
    ) -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
        """Process GTFS files and return (relevant_trips, segments, key_segments)."""
        journeys = [tuple(j) for j in json.loads(os.environ["JOURNEYS"])]

        from pathlib import Path

        p = Path(data_path)
        trips_df = pl.read_csv(p / "trips.txt")
        stop_times = pl.read_csv(p / "stop_times.txt").join(
            trips_df, how="inner", on="trip_id"
        )
        stops = pl.read_csv(p / "stops.txt").join(stop_times, how="inner", on="stop_id")
        shapes = pl.read_csv(p / "shapes.txt")

        trip_stops = trips_df.join(stops, on="trip_id", how="left").join(
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
            trip_info.with_columns(
                stops=pl.col.stop.over("trip_id", mapping_strategy="join")
            )
            .drop("stop_sequence", "stop")
            .unique()
        )

        filters = [
            pl.col.stops.list.eval(
                pl.element().struct["stop_code"].is_in(journey)
            ).list.sum()
            == len(journey)
            for journey in journeys
        ]

        key_trips = detailed_trips.filter(pl.any_horizontal(filters))

        key_segments = (
            key_trips.with_columns(
                start_stop=pl.col("stops").list.eval(
                    pl.element().struct.field("stop_code")
                ),
                start_stop_id=pl.col("stops").list.eval(
                    pl.element().struct.field("stop_id")
                ),
                start_lat=pl.col("stops").list.eval(
                    pl.element().struct.field("stop_lat")
                ),
                start_lon=pl.col("stops").list.eval(
                    pl.element().struct.field("stop_lon")
                ),
            )
            .with_columns(
                end_stop=pl.col.start_stop.list.eval(pl.element().shift(-1)),
                end_stop_id=pl.col.start_stop_id.list.eval(pl.element().shift(-1)),
                end_lat=pl.col.start_lat.list.eval(pl.element().shift(-1)),
                end_lon=pl.col.start_lon.list.eval(pl.element().shift(-1)),
            )
            .explode(
                [
                    pl.col.start_stop,
                    pl.col.end_stop,
                    pl.col.start_stop_id,
                    pl.col.end_stop_id,
                    pl.col.start_lat,
                    pl.col.end_lat,
                    pl.col.start_lon,
                    pl.col.end_lon,
                ]
            )
            .filter(pl.col.end_stop.is_not_null())
            .with_columns(
                segment_id=pl.col.start_stop.cast(str)
                + pl.lit("-")
                + pl.col.end_stop.cast(str)
            )
        )

        key_segment_ids = list(
            (key_segments.select("segment_id").unique())["segment_id"]
        )

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

        return relevant_trips, segments, key_segments

    @log
    def refresh_gtfs(self) -> dict[str, int]:
        """Download GTFS data and refresh trips, trip_segments, and segments tables."""
        with tempfile.TemporaryDirectory() as tmp:
            self._download_gtfs(tmp)
            relevant_trips, trip_segments_df, key_segments = self._build_dataframes(tmp)

        self.logger.info(
            f"GTFS processed: {len(relevant_trips)} trips, "
            f"{len(trip_segments_df)} trip-segment mappings, "
            f"{key_segments.select('segment_id').n_unique()} unique segments"
        )

        unique_segments = key_segments.select(
            "segment_id",
            "start_stop",
            "end_stop",
            "start_stop_id",
            "end_stop_id",
            "start_lat",
            "start_lon",
            "end_lat",
            "end_lon",
        ).unique(subset=["segment_id"])

        trip_rows = [
            (
                row["trip_id"],
                row["route_id"],
                row["service_id"],
                row["direction_id"],
                row["shape_id"],
            )
            for row in relevant_trips.iter_rows(named=True)
        ]

        trip_segment_rows = [
            (row["trip_id"], row["segment_id"])
            for row in trip_segments_df.iter_rows(named=True)
        ]

        segment_rows = [
            (
                row["segment_id"],
                row["start_stop"],
                row["end_stop"],
                row["start_stop_id"],
                row["end_stop_id"],
                row["start_lat"],
                row["start_lon"],
                row["end_lat"],
                row["end_lon"],
            )
            for row in unique_segments.iter_rows(named=True)
        ]

        with self.get_connection() as conn, conn.cursor() as cur:
            cur.execute("TRUNCATE trips, trip_segments, segments")

            execute_values(
                cur,
                """
                INSERT INTO trips (trip_id, route_id, service_id, direction_id, shape_id)
                VALUES %s
                """,
                trip_rows,
            )

            execute_values(
                cur,
                "INSERT INTO trip_segments (trip_id, segment_id) VALUES %s",
                trip_segment_rows,
            )

            execute_values(
                cur,
                """
                INSERT INTO segments (
                    segment_id, start_stop, end_stop,
                    start_stop_id, end_stop_id,
                    start_lat, start_lon, end_lat, end_lon
                ) VALUES %s
                """,
                segment_rows,
            )

            conn.commit()

        counts = {
            "trips": len(trip_rows),
            "trip_segments": len(trip_segment_rows),
            "segments": len(segment_rows),
        }
        self.logger.info(f"GTFS refresh complete: {counts}")
        return counts
