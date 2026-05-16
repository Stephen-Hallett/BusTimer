import io
import json
import logging
import os
import tempfile
import zipfile

import polars as pl
import requests
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session
from sqlalchemy.orm.decl_api import DeclarativeAttributeIntercept

from ..models.models import Calendar, Segment, Stop, Trip, TripSegment
from ..utils.db import BaseDatabase
from ..utils.logger import MyLogger, log

GTFS_URL = "https://gtfs.at.govt.nz/gtfs.zip"
FILES_NEEDED = {
    "routes.txt",
    "trips.txt",
    "stop_times.txt",
    "stops.txt",
    "shapes.txt",
    "calendar.txt",
}


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
        """Process GTFS files and return (relevant_trips, segments, key_segments).

        Three-phase approach to avoid peak memory from a full-dataset shapes join:
          Phase 1 — scan stop_times + stops (no shapes) to find candidate trip_ids
                     whose stop sets are supersets of at least one journey.
          Phase 2 — run the full shapes join only for candidate trips (~100x smaller).
          Phase 3 — reuse Phase 1 data to find all trips traversing key segments
                     (no shapes needed for this).
        """
        journeys = [tuple(j) for j in json.loads(os.environ["JOURNEYS"])]

        from pathlib import Path

        p = Path(data_path)

        calendar = pl.read_csv(
            p / "calendar.txt",
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

        # ------------------------------------------------------------------
        # Phase 1: lightweight scan — stop_times + stops only, no shapes
        # ------------------------------------------------------------------
        stops_minimal = pl.read_csv(
            p / "stops.txt",
            columns=[
                "stop_id",
                "location_type",
                "stop_code",
                "stop_lat",
                "stop_lon",
                "stop_name",
            ],
        )

        # (trip_id, stop_id, stop_sequence, stop_code) for every trip
        trip_stop_codes = (
            pl.scan_csv(p / "stop_times.txt")
            .select(["trip_id", "stop_id", "stop_sequence"])
            .join(stops_minimal.lazy().select(["stop_id", "stop_code"]), on="stop_id")
            .collect()
        )

        # Trips whose stop-code set is a superset of at least one journey
        journey_stop_sets = [set(j) for j in journeys]
        trip_stop_groups = trip_stop_codes.group_by("trip_id").agg(pl.col("stop_code"))
        candidate_trip_ids = [
            row["trip_id"]
            for row in trip_stop_groups.iter_rows(named=True)
            if any(jset.issubset(set(row["stop_code"])) for jset in journey_stop_sets)
        ]
        self.logger.info(f"Candidate trips for key journeys: {len(candidate_trip_ids)}")

        # ------------------------------------------------------------------
        # Phase 2: full join (including shapes) for candidate trips only
        # ------------------------------------------------------------------
        trips_df = pl.read_csv(p / "trips.txt")
        candidate_trips_df = trips_df.filter(
            pl.col("trip_id").is_in(candidate_trip_ids)
        )
        candidate_shape_ids = candidate_trips_df["shape_id"].unique().to_list()

        # Reuse trip_stop_codes — no need to re-read stop_times.txt
        stop_times_candidate = (
            trip_stop_codes.filter(pl.col("trip_id").is_in(candidate_trip_ids))
            .drop("stop_code")  # re-added via stops join below
            .join(candidate_trips_df, how="inner", on="trip_id")
        )
        stops_candidate = stops_minimal.join(
            stop_times_candidate, how="inner", on="stop_id"
        )

        stops = stops_candidate.select(
            "stop_id", "location_type", "stop_code", "stop_lat", "stop_lon", "stop_name"
        ).unique()

        shapes_candidate = (
            pl.scan_csv(p / "shapes.txt")
            .filter(pl.col("shape_id").is_in(candidate_shape_ids))
            .collect()
        )

        trip_stops = candidate_trips_df.join(
            stops_candidate, on="trip_id", how="left"
        ).join(
            shapes_candidate,
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

        # ------------------------------------------------------------------
        # Phase 3: scan all trips for key segments — no shapes needed
        # ------------------------------------------------------------------
        segments = (
            trip_stop_codes.lazy()
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

        relevant_trips = (
            trips_df[["trip_id", "route_id", "service_id", "direction_id", "shape_id"]]
            .unique()
            .filter(pl.col("trip_id").is_in(segments["trip_id"].unique().to_list()))
        )

        return relevant_trips, segments, key_segments, calendar, stops

    def _upsert(
        self, session: Session, model: DeclarativeAttributeIntercept, rows: list[dict]
    ) -> None:
        """Insert rows, updating all non-PK columns on conflict."""
        if not rows:
            return

        stmt = insert(model).values(rows)

        # Check if there are any non primary key rows that need to be updated
        pk_cols = {col.name for col in model.__table__.primary_key}
        set_ = {
            col.name: stmt.excluded[col.name]
            for col in model.__table__.columns
            if not col.primary_key
        }

        if set_:
            stmt = stmt.on_conflict_do_update(index_elements=list(pk_cols), set_=set_)
        else:
            stmt = stmt.on_conflict_do_nothing(index_elements=list(pk_cols))

        session.execute(stmt)

    @log
    def refresh_gtfs(self) -> dict[str, int]:
        """Download GTFS data and refresh trips, trip_segments, and segments tables."""
        with tempfile.TemporaryDirectory() as tmp:
            self._download_gtfs(tmp)
            relevant_trips, trip_segments_df, key_segments, calendar, stops = (
                self._build_dataframes(tmp)
            )

        self.logger.info(
            f"GTFS processed: {len(relevant_trips)} trips, "
            f"{len(trip_segments_df)} trip-segment mappings, "
            f"{key_segments.select('segment_id').n_unique()} unique segments"
        )

        unique_segments = key_segments.select(
            "segment_id", "start_stop_id", "end_stop_id"
        ).unique(subset=["segment_id"])

        assert calendar["service_id"].n_unique() == calendar.shape[0]
        assert stops["stop_id"].n_unique() == stops.shape[0]
        assert relevant_trips["trip_id"].n_unique() == relevant_trips.shape[0]
        assert unique_segments["segment_id"].n_unique() == unique_segments.shape[0]
        assert (
            trip_segments_df[["segment_id", "trip_id"]].n_unique()
            == trip_segments_df.shape[0]
        )

        with self.get_session() as session:
            self._upsert(session, Calendar, calendar.to_dicts())
            self._upsert(session, Stop, stops.unique().to_dicts())
            self._upsert(session, Trip, relevant_trips.to_dicts())
            self._upsert(session, Segment, unique_segments.to_dicts())
            self._upsert(session, TripSegment, trip_segments_df.to_dicts())
            session.commit()

        counts = {
            "stops": stops.shape[0],
            "trips": relevant_trips.shape[0],
            "trip_segments": trip_segments_df.shape[0],
            "segments": unique_segments.shape[0],
        }
        self.logger.info(f"GTFS refresh complete: {counts}")
        return counts
