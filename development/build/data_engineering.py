import logging
import os
import pathlib

import polars as pl
import psycopg2
from psycopg2.extras import RealDictCursor, execute_values

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


def query(sql: str, params=None) -> pl.DataFrame:
    with (
        psycopg2.connect(**CONNECTION_PARAMS) as conn,
        conn.cursor(cursor_factory=RealDictCursor) as cur,
    ):
        cur.execute(sql, params)
        rows = cur.fetchall()
    return pl.DataFrame([dict(r) for r in rows])


log.info("Connected to %s:%s db=%s", CONNECTION_PARAMS["host"], CONNECTION_PARAMS["port"], CONNECTION_PARAMS["database"])

trip_segments = query("SELECT * FROM trip_segments")
log.info("trip_segments: %d rows, %d cols", *trip_segments.shape)

segments = query("""
    SELECT s.*,
           st.stop_name AS start_stop_name,
           en.stop_name AS end_stop_name
    FROM segments s
    JOIN stops st ON st.stop_id = s.start_stop_id
    JOIN stops en ON en.stop_id = s.end_stop_id
""")
log.info("segments: %d rows, %d cols", *segments.shape)

segments_wide = trip_segments.join(segments, on="segment_id")
log.info("segments_wide: %d rows, %d cols", *segments_wide.shape)
del trip_segments, segments

vehicle_locations = query("""
    SELECT
        id,
        trip_id,
        stop_id,
        stop_sequence,
        direction_id,
        bearing,
        speed,
        delay,
        departure_delay,
        departure_uncertainty,
        occupancy_status,
        latitude,
        longitude,
        extract(MINUTE FROM to_timestamp(timestamp)) AS obs_minute,
        extract(HOUR   FROM to_timestamp(timestamp)) AS obs_hour,
        extract(DOW    FROM to_timestamp(timestamp)) AS day_of_week,
        extract(DAY    FROM to_timestamp(timestamp)) AS day_of_month,
        extract(MONTH  FROM to_timestamp(timestamp)) AS obs_month,
        extract(YEAR   FROM to_timestamp(timestamp)) AS obs_year
    FROM vehicle_locations
""")
vehicle_locations = vehicle_locations.with_columns(
    is_weekday=pl.col.day_of_week.is_in((1, 2, 3, 4, 5))
)
log.info("vehicle_locations: %d rows, %d cols", *vehicle_locations.shape)

matched_locations = (
    vehicle_locations.join(
        segments_wide,
        left_on=["trip_id", "stop_id"],
        right_on=["trip_id", "start_stop_id"],
        how="inner",
    )
    .select(
        "id", "segment_id", "trip_id", "stop_id", "end_stop_id",
        "stop_sequence", "direction_id", "bearing", "speed", "delay",
        "departure_delay", "departure_uncertainty", "occupancy_status",
        "latitude", "longitude", "is_weekday",
        "obs_minute", "obs_hour", "day_of_week", "day_of_month", "obs_month", "obs_year",
    )
)
del vehicle_locations, segments_wide
log.info("matched_locations (after segment join): %d rows, %d cols", *matched_locations.shape)

log.info("Building features...")
## Build generic features -- keep in mind, a model will be needed for the
# majority of these features.
generic_features = matched_locations.select(
    "id",
    "trip_id",
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

# Segment average features
# We want these features on global average, daily average, and as hourly average
avg_features = ["bearing", "speed", "delay", "occupancy_status"]
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

# Lagged features
lagged_features = (
    generic_features.join(matched_locations[["id", "stop_sequence"]], on="id", how="left").sort(by="stop_sequence", descending=False)
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
        "trip_id",
        "day_of_month",
        "obs_month",
        "obs_year",
        pl.selectors.contains("_lag_")
    )
)
log.info("lagged_features: %d rows, %d cols", *lagged_features.shape)

## Target Variable (lat/long)
# Note that each trip_id only runs one time per day, so finding all observations
# per trip is found by grouping by trip_id, day_of_month, obs_month, obs_year
target = (
    matched_locations.sort(by="stop_sequence", descending=False)
    .with_columns(
        target_latitude=pl.col.latitude.shift(-1).over(
            ["trip_id", "day_of_month", "obs_month", "obs_year"]
        ),
        target_longitude=pl.col.longitude.shift(-1).over(
            ["trip_id", "day_of_month", "obs_month", "obs_year"]
        ),
    )
    .select(
        "trip_id",
        "day_of_month",
        "obs_month",
        "obs_year",
        "target_longitude",
        "target_latitude",
    )
)
del matched_locations

log.info("Assembling training_set...")
training_set = (
    generic_features.lazy()
    .join(global_segment_features.lazy(), on="segment_id", how="left")
    .join(daily_segment_features.lazy(), on=["segment_id", "day_of_week"], how="left")
    .join(monthly_segment_features.lazy(), on=["segment_id", "obs_month"], how="left")
    .join(hourly_segment_features.lazy(), on=["segment_id", "obs_hour"], how="left")
    .join(lagged_features.lazy(), on=["trip_id", "day_of_month", "obs_month", "obs_year"])
    .join(target.lazy(), on=["trip_id", "day_of_month", "obs_month", "obs_year"], how="inner")
    .filter(pl.col.target_latitude.is_not_null(), pl.col.target_longitude.is_not_null())
    .collect()
)
del generic_features, global_segment_features, daily_segment_features
del monthly_segment_features, hourly_segment_features, lagged_features, target
log.info("training_set: %d rows, %d cols", *training_set.shape)

# Warn about high-null columns
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

# Ensure id is unique before writing
duplicate_ids = len(training_set) - training_set["id"].n_unique()
if duplicate_ids > 0:
    raise ValueError(f"training_set contains {duplicate_ids:,} duplicate 'id' values — aborting write")

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


def _pg_type(dtype: pl.DataType) -> str:
    return _PG_TYPE_MAP.get(type(dtype), "TEXT")


def write_analytics_data(df: pl.DataFrame, table: str = "analytics_data") -> None:
    years = sorted(df["obs_year"].cast(pl.Int32).unique().to_list())
    col_defs = ",\n    ".join(
        f'"{col}" {_pg_type(dtype)}' for col, dtype in zip(df.columns, df.dtypes)
    )
    columns = ", ".join(f'"{c}"' for c in df.columns)

    log.info("Writing %d rows to '%s' (partitions: %s)", len(df), table, years)
    with psycopg2.connect(**CONNECTION_PARAMS) as conn:
        with conn.cursor() as cur:
            log.info("Dropping and recreating table '%s'", table)
            cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
            cur.execute(f"""
                CREATE TABLE {table} (
                    {col_defs}
                ) PARTITION BY RANGE (obs_year)
            """)
            for year in years:
                cur.execute(f"""
                    CREATE TABLE {table}_{year}
                    PARTITION OF {table}
                    FOR VALUES FROM ({year}) TO ({year + 1})
                """)
                log.info("  Created partition %s_%s", table, year)
            # Default partition catches any years not explicitly listed above
            cur.execute(f"CREATE TABLE {table}_default PARTITION OF {table} DEFAULT")

            log.info("Creating indexes on '%s'", table)
            cur.execute(f'CREATE INDEX ON {table} (trip_id)')
            cur.execute(f'CREATE INDEX ON {table} (stop_id)')
            cur.execute(f'CREATE INDEX ON {table} (obs_year, obs_month, day_of_month)')
            cur.execute(f'CREATE INDEX ON {table} (day_of_week, obs_hour)')
            cur.execute(f'CREATE INDEX ON {table} (direction_id)')

            log.info("Inserting rows...")
            execute_values(
                cur,
                f"INSERT INTO {table} ({columns}) VALUES %s",
                df.rows(),
                page_size=1000,
            )
        conn.commit()

    log.info("Done — wrote %d rows to '%s'", len(df), table)


write_analytics_data(training_set)
