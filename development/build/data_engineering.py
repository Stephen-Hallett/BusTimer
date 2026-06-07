import logging
import os
import pathlib

import polars as pl
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values
import argparse
import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

# Load .env from project root
env_path = pathlib.Path("../../.env")
if env_path.exists():
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip())

CONNECTION_PARAMS = {
    "host": "100.111.121.51",
    "port": 5433,
    "database": os.environ["POSTGRES_DB"],
    "user": os.environ["POSTGRES_USER"],
    "password": os.environ["POSTGRES_PW"],
}

API_BASE = "http://100.111.121.51:8081"


def parse_args():
    parser = argparse.ArgumentParser(description="Build analytics training data for a given week")
    parser.add_argument(
        "--week-start",
        required=True,
        type=lambda s: datetime.date.fromisoformat(s),
        help="Monday of the week to process (YYYY-MM-DD)",
    )
    return parser.parse_args()

def query(sql: str, params=None) -> pl.DataFrame:
    with (
        psycopg2.connect(**CONNECTION_PARAMS) as conn,
        conn.cursor(cursor_factory=RealDictCursor) as cur,
    ):
        cur.execute(sql, params)
        rows = cur.fetchall()
    return pl.DataFrame([dict(r) for r in rows])

def get_locations_data(week_start: datetime.date) -> pl.DataFrame:
    week_end = week_start + datetime.timedelta(days=7)
    log.info(
        "Connected to %s:%s db=%s | loading week %s → %s",
        CONNECTION_PARAMS["host"], CONNECTION_PARAMS["port"], CONNECTION_PARAMS["database"],
        week_start, week_end,
    )

    matched_locations = query("""
        SELECT
            vl.id,
            vl.timestamp,
            s.segment_id,
            vl.trip_id,
            vl.stop_id,
            s.end_stop_id,
            vl.stop_sequence,
            vl.direction_id,
            vl.bearing,
            vl.speed,
            vl.delay,
            vl.departure_delay,
            vl.departure_uncertainty,
            vl.occupancy_status,
            vl.latitude,
            vl.longitude,
            extract(MINUTE FROM to_timestamp(vl.timestamp)) AS obs_minute,
            extract(HOUR   FROM to_timestamp(vl.timestamp)) AS obs_hour,
            extract(DOW    FROM to_timestamp(vl.timestamp)) AS day_of_week,
            extract(DAY    FROM to_timestamp(vl.timestamp)) AS day_of_month,
            extract(MONTH  FROM to_timestamp(vl.timestamp)) AS obs_month,
            extract(YEAR   FROM to_timestamp(vl.timestamp)) AS obs_year,
            extract(DOW    FROM to_timestamp(vl.timestamp)) BETWEEN 1 AND 5 AS is_weekday
        FROM vehicle_locations vl
        JOIN trip_segments ts ON ts.trip_id = vl.trip_id
        JOIN segments s       ON s.segment_id = ts.segment_id
                             AND s.start_stop_id = vl.stop_id
        WHERE to_timestamp(vl.timestamp)::date >= %(week_start)s
          AND to_timestamp(vl.timestamp)::date <  %(week_end)s
    """, {"week_start": week_start, "week_end": week_end})

    log.info("matched_locations (after segment join): %d rows, %d cols", *matched_locations.shape)
    return matched_locations

log.info("Building features...")
## Build generic features -- keep in mind, a model will be needed for the
# majority of these features.
def get_generic_features(matched_locations: pl.DataFrame) -> pl.DataFrame:
    return matched_locations.select(
        "id",
        "timestamp",
        "trip_id",
        "segment_id",
        "day_of_month",
        "obs_month",
        "obs_year",
        # -----
        "direction_id",
        "bearing",
        "speed",
        "delay",
        "occupancy_status",
        "stop_id",
        "end_stop_id",
        "departure_delay",
        "departure_uncertainty",
        "obs_minute",
        "obs_hour",
        "day_of_week",
    )

def get_average_features(matched_locations: pl.DataFrame, avg_features: list = ["bearing", "speed", "delay", "occupancy_status"]) -> pl.DataFrame:
    # Segment average features
    # global
    global_segment_features = matched_locations.group_by("segment_id").agg(
        [pl.mean(col).alias(f"global_{col}_avg") for col in avg_features]
    )
    daily_segment_features = matched_locations.group_by("segment_id", "day_of_week").agg(
        [pl.mean(col).alias(f"daily_{col}_avg") for col in avg_features]
    )
    monthly_segment_features = matched_locations.group_by("segment_id", "obs_month").agg(
        [pl.mean(col).alias(f"monthly_{col}_avg") for col in avg_features]
    )
    hourly_segment_features = matched_locations.group_by("segment_id", "obs_hour").agg(
        [pl.mean(col).alias(f"hourly_{col}_avg") for col in avg_features]
    )
    log.info(
        "Segment features — global: %d, daily: %d, monthly: %d, hourly: %d",
        len(global_segment_features),
        len(daily_segment_features),
        len(monthly_segment_features),
        len(hourly_segment_features),
    )
    return global_segment_features, daily_segment_features, monthly_segment_features, hourly_segment_features

def get_lagged_features(matched_locations: pl.DataFrame, avg_features: list = ["bearing", "speed", "delay", "occupancy_status"]) -> pl.DataFrame:
    # Lagged features
    lagged_features = (
        matched_locations
        .select(*["id", "timestamp", "trip_id", "day_of_month", "obs_month", "obs_year", "segment_id"], *avg_features, "stop_sequence")
        .sort(by="timestamp", descending=False)
        .with_columns(
            *[
                pl.col(col)
                .shift(1)
                .over(["trip_id", "day_of_month", "obs_month", "obs_year"])
                .alias(f"1_lag_{col}")
                for col in avg_features
            ],
            *[
                pl.mean_horizontal(
                    [
                        pl.col(col)
                        .shift(i)
                        .over(["trip_id", "day_of_month", "obs_month", "obs_year"])
                        for i in range(5)
                    ]
                ).alias(f"5_lag_{col}_avg")
                for col in avg_features
            ],
            *[
                pl.mean_horizontal(
                    [
                        pl.col(col)
                        .shift(i)
                        .over(["trip_id", "day_of_month", "obs_month", "obs_year"])
                        for i in range(10)
                    ]
                ).alias(f"10_lag_{col}_avg")
                for col in avg_features
            ],
        )
        .select(
            "id",
            "timestamp",
            pl.selectors.contains("_lag_")
        )
    )
    log.info("lagged_features: %d rows, %d cols", *lagged_features.shape)
    return lagged_features

def build_target(matched_locations: pl.DataFrame) -> pl.DataFrame:
    ## Target Variable (lat/long)
    # Note that each trip_id only runs one time per day, so finding all observations
    # per trip is found by grouping by trip_id, day_of_month, obs_month, obs_year
    return (
        matched_locations.sort(by="timestamp", descending=False)
        .with_columns(
            target_latitude=pl.col.latitude.shift(-1).over(
                ["trip_id", "day_of_month", "obs_month", "obs_year"]
            ),
            target_longitude=pl.col.longitude.shift(-1).over(
                ["trip_id", "day_of_month", "obs_month", "obs_year"]
            ),
        )
        .select(
            "id",
            "timestamp",
            "target_longitude",
            "target_latitude",
        )
    )


def _pg_type(dtype: pl.DataType) -> str:
    _PG_TYPE_MAP: dict[type, str] = {
        pl.Int8: "SMALLINT",
        pl.Int16: "SMALLINT",
        pl.Int32: "INTEGER",
        pl.Int64: "BIGINT",
        pl.UInt8: "SMALLINT",
        pl.UInt16: "INTEGER",
        pl.UInt32: "BIGINT",
        pl.Float32: "REAL",
        pl.Float64: "DOUBLE PRECISION",
        pl.Boolean: "BOOLEAN",
        pl.Date: "DATE",
        pl.Datetime: "TIMESTAMPTZ",
        pl.String: "TEXT",
    }
    return _PG_TYPE_MAP.get(type(dtype), "TEXT")


def ensure_analytics_table(df: pl.DataFrame, table: str = "analytics_data") -> None:
    """Create the partitioned table and indexes if they don't already exist."""
    col_defs = ",\n    ".join(
        f'"{col}" {_pg_type(dtype)}' for col, dtype in zip(df.columns, df.dtypes)
    )
    with psycopg2.connect(**CONNECTION_PARAMS) as conn:
        with conn.cursor() as cur:
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    {col_defs},
                    PRIMARY KEY (id, obs_year)
                ) PARTITION BY RANGE (obs_year)
            """)
            # Partitions — create for any year in the data; IF NOT EXISTS is safe to re-run
            years = sorted(df["obs_year"].cast(pl.Int32).unique().to_list())
            for year in years:
                cur.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table}_{year}
                    PARTITION OF {table}
                    FOR VALUES FROM ({year}) TO ({year + 1})
                """)
                log.info("  Ensured partition %s_%s", table, year)
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {table}_default
                PARTITION OF {table} DEFAULT
            """)

            # Indexes — CREATE INDEX IF NOT EXISTS requires a name
            for idx_name, idx_cols in [
                (f"{table}_trip_id_idx",    "(trip_id)"),
                (f"{table}_stop_id_idx",    "(stop_id)"),
                (f"{table}_date_idx",       "(obs_year, obs_month, day_of_month)"),
                (f"{table}_dow_hour_idx",   "(day_of_week, obs_hour)"),
                (f"{table}_direction_idx",  "(direction_id)"),
            ]:
                cur.execute(f"CREATE INDEX IF NOT EXISTS {idx_name} ON {table} {idx_cols}")
        conn.commit()
    log.info("Table '%s' is ready", table)


def write_analytics_data(df: pl.DataFrame, table: str = "analytics_data") -> None:
    columns = ", ".join(f'"{c}"' for c in df.columns)
    # All non-PK columns get updated on conflict
    update_cols = [c for c in df.columns if c != "id"]
    update_clause = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in update_cols)

    log.info("Upserting %d rows into '%s'", len(df), table)
    with psycopg2.connect(**CONNECTION_PARAMS) as conn:
        with conn.cursor() as cur:
            execute_values(
                cur,
                f"""
                INSERT INTO {table} ({columns}) VALUES %s
                ON CONFLICT (id, obs_year)
                DO UPDATE SET {update_clause}
                """,
                df.rows(),
                page_size=1000,
            )
        conn.commit()
    log.info("Done — upserted %d rows into '%s'", len(df), table) 


if __name__ == "__main__":
    args = parse_args()

    matched_locations = get_locations_data(args.week_start)
    matched_locations = get_locations_data(datetime.date.fromisoformat("2026-05-20"))
    generic_features = get_generic_features(matched_locations)
    global_segment_features, daily_segment_features, monthly_segment_features, hourly_segment_features = get_average_features(matched_locations)
    lagged_features = get_lagged_features(matched_locations)
    target = build_target(matched_locations)

    log.info("Assembling training_set...")
    training_set = (
        generic_features
        .join(global_segment_features, on="segment_id", how="left")
        .join(daily_segment_features, on=["segment_id", "day_of_week"], how="left")
        .join(monthly_segment_features, on=["segment_id", "obs_month"], how="left")
        .join(hourly_segment_features, on=["segment_id", "obs_hour"], how="left")
        .join(lagged_features, on=["id", "timestamp"])
        .join(target, on=["id", "timestamp"], how="inner")
        .filter(pl.col.target_latitude.is_not_null(), pl.col.target_longitude.is_not_null())
    )
    
    del generic_features, global_segment_features, daily_segment_features
    del monthly_segment_features, hourly_segment_features, lagged_features, target
    log.info("training_set: %d rows, %d cols", *training_set.shape)

    NULL_THRESHOLD = 0.40
    n = len(training_set)
    high_null_cols = [
        (col, training_set[col].null_count() / n)
        for col in training_set.columns
        if training_set[col].null_count() / n > NULL_THRESHOLD
    ]
    if high_null_cols:
        for col, rate in high_null_cols:
            log.warning("High null rate in column '%s': %.1f%%", col, rate * 100)
    else:
        log.info("No columns exceed %.0f%% null rate", NULL_THRESHOLD * 100)

    duplicate_ids = len(training_set) - training_set[["id", "timestamp"]].n_unique()
    if duplicate_ids > 0:
        raise ValueError(f"training_set contains {duplicate_ids:,} duplicate 'id' values — aborting write")

    ensure_analytics_table(training_set)
    write_analytics_data(training_set)