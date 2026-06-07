#!/usr/bin/env python3
"""
Comprehensive EDA for the analytics_data training table.
Usage:  uv run eda/analytics_eda.py
Output: eda/analytics_eda.html
"""

import base64
import datetime
import io
import logging
import os
import pathlib

import folium
from folium.plugins import HeatMap
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import polars as pl
import psycopg2
from psycopg2.extras import RealDictCursor
import seaborn as sns

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

env_path = pathlib.Path(__file__).parent.parent.parent / ".env"
if env_path.exists():
    for line in env_path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            k, _, v = line.partition("=")
            os.environ.setdefault(k.strip(), v.strip())

CONNECTION_PARAMS = {
    "host": "100.111.121.51",
    "port": 5433,
    "database": os.environ["POSTGRES_DB"],
    "user": os.environ["POSTGRES_USER"],
    "password": os.environ["POSTGRES_PW"],
}

OUTPUT = pathlib.Path(__file__).parent / "analytics_eda.html"

sns.set_theme(style="whitegrid", palette="husl", font_scale=1.0)
PALETTE = sns.color_palette("husl", 12)

ID_COLS = {"id", "trip_id", "stop_id", "end_stop_id", "segment_id", "timestamp"}
TARGET_COLS = ["target_latitude", "target_longitude"]
NUMERIC_DTYPES = (
    pl.Float32, pl.Float64,
    pl.Int8, pl.Int16, pl.Int32, pl.Int64,
    pl.UInt8, pl.UInt16, pl.UInt32,
)


# ── shared helpers ────────────────────────────────────────────────────────────

def db_query(sql: str, params=None) -> pl.DataFrame:
    with (
        psycopg2.connect(**CONNECTION_PARAMS) as conn,
        conn.cursor(cursor_factory=RealDictCursor) as cur,
    ):
        cur.execute(sql, params)
        rows = cur.fetchall()
    return pl.DataFrame([dict(r) for r in rows]) if rows else pl.DataFrame()


def fig_to_b64(fig: plt.Figure) -> str:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=120, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    return base64.b64encode(buf.read()).decode()


def img_tag(b64: str, caption: str = "") -> str:
    cap = f"<p class='caption'>{caption}</p>" if caption else ""
    return f"<div class='chart-wrap'><img src='data:image/png;base64,{b64}' />{cap}</div>"


def section_wrap(title: str, anchor: str, *blocks: str) -> str:
    body = "\n".join(blocks)
    return f"<section id='{anchor}'><h2>{title}</h2>{body}</section>"


def df_to_html_table(df: pl.DataFrame) -> str:
    headers = "".join(f"<th>{c}</th>" for c in df.columns)
    rows_html = ""
    for row in df.iter_rows():
        cells = "".join(f"<td>{v if v is not None else '—'}</td>" for v in row)
        rows_html += f"<tr>{cells}</tr>"
    return f"<div class='table-wrap'><table><thead><tr>{headers}</tr></thead><tbody>{rows_html}</tbody></table></div>"


def clip_pct(arr: np.ndarray, lo: float = 1.0, hi: float = 99.0) -> np.ndarray:
    low, high = np.nanpercentile(arr, [lo, hi])
    return arr[(arr >= low) & (arr <= high)]


# ── section builders ──────────────────────────────────────────────────────────

def build_overview(df: pl.DataFrame) -> tuple[str, str]:
    rows, cols = df.shape

    null_df = pl.DataFrame({
        "column":     df.columns,
        "dtype":      [str(d) for d in df.dtypes],
        "null_count": [df[c].null_count() for c in df.columns],
        "null_%":     [round(df[c].null_count() / rows * 100, 2) for c in df.columns],
    }).sort("null_%", descending=True)

    non_zero = null_df.filter(pl.col("null_%") > 0)
    fig, ax = plt.subplots(figsize=(10, max(3, len(non_zero) * 0.35 + 1)))
    if len(non_zero):
        ax.barh(non_zero["column"].to_list(), non_zero["null_%"].to_list(), color="#e07b54")
        ax.axvline(40, color="red", linestyle="--", lw=1, alpha=0.7, label="40% threshold")
        ax.set_xlabel("Null %")
        ax.set_title("Columns with missing values")
        ax.legend(fontsize=8)
    else:
        ax.text(0.5, 0.5, "No missing values", ha="center", va="center",
                transform=ax.transAxes, fontsize=14)
        ax.set_title("Missing Values")
    b64_nulls = fig_to_b64(fig)

    toc = "<li><a href='#overview'>1. Dataset Overview</a></li>"
    html = section_wrap(
        "1. Dataset Overview", "overview",
        f"<p><strong>{rows:,} rows &times; {cols} columns</strong></p>",
        "<h3>Schema &amp; Null Rates</h3>",
        df_to_html_table(null_df),
        "<h3>Null Rate per Column</h3>",
        img_tag(b64_nulls),
    )
    return toc, html


def build_target_distributions(df: pl.DataFrame) -> tuple[str, str]:
    valid = df.select("target_latitude", "target_longitude", "delay", "obs_hour").drop_nulls()
    tgt_lat = valid["target_latitude"].to_numpy()
    tgt_lon = valid["target_longitude"].to_numpy()
    delay   = valid["delay"].to_numpy()
    hours   = valid["obs_hour"].to_numpy()

    # marginal histograms
    fig, axes = plt.subplots(1, 2, figsize=(12, 4))
    axes[0].hist(tgt_lat, bins=80, color=PALETTE[0], edgecolor="none", alpha=0.85)
    axes[0].set_xlabel("target_latitude")
    axes[0].set_ylabel("Count")
    axes[0].set_title("Target Latitude Distribution")
    axes[1].hist(tgt_lon, bins=80, color=PALETTE[1], edgecolor="none", alpha=0.85)
    axes[1].set_xlabel("target_longitude")
    axes[1].set_title("Target Longitude Distribution")
    fig.tight_layout()
    b64_hist = fig_to_b64(fig)

    # 2D hexbin — geographic density
    fig, ax = plt.subplots(figsize=(8, 7))
    hb = ax.hexbin(tgt_lon, tgt_lat, gridsize=80, cmap="YlOrRd", mincnt=1)
    fig.colorbar(hb, ax=ax, label="Count")
    ax.set_xlabel("Longitude")
    ax.set_ylabel("Latitude")
    ax.set_title("Geographic Density of Target Positions")
    b64_hex = fig_to_b64(fig)

    # scatter: target positions coloured by delay (sample 10k)
    rng = np.random.default_rng(42)
    idx = rng.choice(len(tgt_lat), min(10_000, len(tgt_lat)), replace=False)
    d_clip = np.clip(delay[idx], *np.nanpercentile(delay[idx], [2, 98]))

    fig, axes = plt.subplots(1, 2, figsize=(13, 5))
    sc = axes[0].scatter(tgt_lon[idx], tgt_lat[idx], c=d_clip,
                         cmap="coolwarm", s=4, alpha=0.3, linewidths=0)
    fig.colorbar(sc, ax=axes[0], label="delay (s)")
    axes[0].set_xlabel("target_longitude")
    axes[0].set_ylabel("target_latitude")
    axes[0].set_title("Target Positions coloured by Delay")

    sc2 = axes[1].scatter(hours[idx], tgt_lat[idx], c=d_clip,
                          cmap="coolwarm", s=4, alpha=0.3, linewidths=0)
    fig.colorbar(sc2, ax=axes[1], label="delay (s)")
    axes[1].set_xlabel("Hour of day")
    axes[1].set_ylabel("target_latitude")
    axes[1].set_title("Target Latitude vs Hour")
    fig.tight_layout()
    b64_scatter = fig_to_b64(fig)

    toc = "<li><a href='#targets'>2. Target Distributions</a></li>"
    html = section_wrap(
        "2. Target Variable Analysis", "targets",
        img_tag(b64_hist, "Marginal histograms of target latitude &amp; longitude"),
        img_tag(b64_hex, "2-D geographic density — target positions"),
        img_tag(b64_scatter, "10k sampled target positions coloured by delay"),
    )
    return toc, html


def build_geo_map(df: pl.DataFrame) -> tuple[str, str]:
    sample = (
        df.select("target_latitude", "target_longitude", "delay")
        .drop_nulls()
        .sample(min(8_000, len(df)), seed=42)
    )
    lat_c = float(sample["target_latitude"].mean())
    lon_c = float(sample["target_longitude"].mean())

    fmap = folium.Map(location=[lat_c, lon_c], zoom_start=12, tiles="CartoDB positron")
    heat_data = sample.select("target_latitude", "target_longitude").to_numpy().tolist()
    HeatMap(heat_data, radius=10, blur=12, min_opacity=0.3).add_to(fmap)

    map_html = fmap._repr_html_()
    toc = "<li><a href='#geomap'>3. Geographic Heatmap</a></li>"
    html = section_wrap(
        "3. Geographic Heatmap of Target Positions", "geomap",
        f"<p>{len(sample):,} sampled target positions — heatmap intensity reflects observation density.</p>",
        f"<div class='map-wrap'>{map_html}</div>",
    )
    return toc, html


def build_feature_distributions(df: pl.DataFrame) -> tuple[str, str]:
    features = [
        ("speed",                 "Speed",                PALETTE[2]),
        ("delay",                 "Delay (s)",            PALETTE[3]),
        ("bearing",               "Bearing (°)",          PALETTE[4]),
        ("departure_delay",       "Departure Delay (s)",  PALETTE[5]),
        ("departure_uncertainty", "Departure Uncertainty",PALETTE[6]),
    ]
    blocks = []
    for col, label, color in features:
        vals = df[col].drop_nulls().to_numpy()
        clipped = clip_pct(vals)
        fig, ax = plt.subplots(figsize=(9, 3.5))
        ax.hist(clipped, bins=80, color=color, edgecolor="none", alpha=0.85)
        ax.set_xlabel(label)
        ax.set_ylabel("Count")
        ax.set_title(f"{label}  (1st–99th percentile, n={len(clipped):,})")
        blocks.append(img_tag(fig_to_b64(fig)))

    toc = "<li><a href='#features'>4. Feature Distributions</a></li>"
    html = section_wrap("4. Core Feature Distributions", "features", *blocks)
    return toc, html


def build_categorical(df: pl.DataFrame) -> tuple[str, str]:
    cat_cols = [
        ("occupancy_status", "Occupancy Status"),
        ("direction_id",     "Direction ID"),
        ("day_of_week",      "Day of Week (0=Sun)"),
        ("obs_hour",         "Hour of Day"),
        ("obs_minute",       "Minute of Hour"),
    ]
    blocks = []
    for col, label in cat_cols:
        vc = (
            df[col].cast(pl.Int32, strict=False).alias(col)
            .value_counts()
            .sort(col)
        )
        fig, ax = plt.subplots(figsize=(9, 3.5))
        ax.bar(
            [str(x) for x in vc[col].to_list()],
            vc["count"].to_list(),
            color=PALETTE[0],
        )
        ax.set_xlabel(label)
        ax.set_ylabel("Count")
        ax.set_title(f"Value counts: {label}")
        plt.xticks(rotation=30, ha="right", fontsize=8)
        blocks.append(img_tag(fig_to_b64(fig), label))

    toc = "<li><a href='#categorical'>5. Categorical Features</a></li>"
    html = section_wrap("5. Categorical Feature Distributions", "categorical", *blocks)
    return toc, html


def build_temporal(df: pl.DataFrame) -> tuple[str, str]:
    blocks = []

    # observations per hour
    obs_hour = df.group_by("obs_hour").agg(pl.len().alias("n")).sort("obs_hour")
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.bar(obs_hour["obs_hour"].cast(pl.Int32).to_list(), obs_hour["n"].to_list(), color=PALETTE[2])
    ax.set_xlabel("Hour of day")
    ax.set_ylabel("Observations")
    ax.set_title("Observation count by hour")
    blocks.append(img_tag(fig_to_b64(fig)))

    # delay by hour — mean ± std + median
    hd = (
        df.select("obs_hour", "delay").drop_nulls()
        .group_by("obs_hour")
        .agg(
            pl.mean("delay").alias("mean"),
            pl.std("delay").alias("std"),
            pl.median("delay").alias("median"),
        )
        .sort("obs_hour")
    )
    h = hd["obs_hour"].cast(pl.Int32).to_numpy()
    means = hd["mean"].to_numpy()
    stds  = hd["std"].to_numpy()
    meds  = hd["median"].to_numpy()
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.fill_between(h, means - stds, means + stds, alpha=0.22, color=PALETTE[3], label="±1 std")
    ax.plot(h, means, color=PALETTE[3], lw=2, label="mean")
    ax.plot(h, meds,  color=PALETTE[3], lw=1.5, linestyle="--", label="median")
    ax.set_xlabel("Hour of day")
    ax.set_ylabel("Delay (s)")
    ax.set_title("Delay by hour — mean ± std, median")
    ax.legend()
    blocks.append(img_tag(fig_to_b64(fig)))

    # speed by hour
    hs = (
        df.select("obs_hour", "speed").drop_nulls()
        .group_by("obs_hour")
        .agg(pl.mean("speed").alias("mean"), pl.std("speed").alias("std"))
        .sort("obs_hour")
    )
    hv = hs["obs_hour"].cast(pl.Int32).to_numpy()
    ms = hs["mean"].to_numpy()
    ss = hs["std"].to_numpy()
    fig, ax = plt.subplots(figsize=(10, 4))
    ax.fill_between(hv, ms - ss, ms + ss, alpha=0.22, color=PALETTE[4])
    ax.plot(hv, ms, color=PALETTE[4], lw=2)
    ax.set_xlabel("Hour of day")
    ax.set_ylabel("Speed")
    ax.set_title("Speed by hour — mean ± std")
    blocks.append(img_tag(fig_to_b64(fig)))

    # delay by day of week
    DOW = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
    dd = (
        df.select("day_of_week", "delay").drop_nulls()
        .group_by("day_of_week")
        .agg(pl.mean("delay").alias("mean"), pl.median("delay").alias("median"))
        .sort("day_of_week")
    )
    dows = dd["day_of_week"].cast(pl.Int32).to_list()
    fig, ax = plt.subplots(figsize=(8, 4))
    ax.bar([DOW[d] for d in dows], dd["mean"].to_list(), color=PALETTE[5], label="mean")
    ax.plot([DOW[d] for d in dows], dd["median"].to_list(),
            "o--", color="#334155", lw=1.5, ms=5, label="median")
    ax.set_xlabel("Day of week")
    ax.set_ylabel("Delay (s)")
    ax.set_title("Delay by day of week")
    ax.legend()
    blocks.append(img_tag(fig_to_b64(fig)))

    # heatmap: mean delay — hour × day_of_week
    pivot = (
        df.select("obs_hour", "day_of_week", "delay").drop_nulls()
        .group_by("obs_hour", "day_of_week")
        .agg(pl.mean("delay").alias("mean_delay"))
        .sort("obs_hour", "day_of_week")
    )
    hours_u = sorted(pivot["obs_hour"].cast(pl.Int32).unique().to_list())
    dows_u  = sorted(pivot["day_of_week"].cast(pl.Int32).unique().to_list())
    matrix = np.full((len(dows_u), len(hours_u)), np.nan)
    for row in pivot.iter_rows(named=True):
        r = dows_u.index(int(row["day_of_week"]))
        c = hours_u.index(int(row["obs_hour"]))
        matrix[r, c] = row["mean_delay"]
    fig, ax = plt.subplots(figsize=(14, 4))
    sns.heatmap(
        matrix, ax=ax, cmap="RdYlGn_r",
        xticklabels=hours_u, yticklabels=[DOW[d] for d in dows_u],
        linewidths=0.3, annot=False, cbar_kws={"label": "Mean delay (s)"},
    )
    ax.set_xlabel("Hour of day")
    ax.set_ylabel("Day of week")
    ax.set_title("Mean Delay Heatmap — Hour × Day of Week")
    blocks.append(img_tag(fig_to_b64(fig), "Colour = mean delay; red = more delayed"))

    toc = "<li><a href='#temporal'>6. Temporal Patterns</a></li>"
    html = section_wrap("6. Temporal Patterns", "temporal", *blocks)
    return toc, html


def build_segment_analysis(df: pl.DataFrame) -> tuple[str, str]:
    seg = (
        df.group_by("segment_id")
        .agg(
            pl.len().alias("count"),
            pl.mean("delay").alias("mean_delay"),
            pl.mean("speed").alias("mean_speed"),
            pl.std("delay").alias("std_delay"),
        )
        .sort("count", descending=True)
    )
    top20 = seg.head(20)
    seg_ids = top20["segment_id"].cast(pl.String).to_list()

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))
    axes[0].barh(seg_ids[::-1], top20["count"].to_list()[::-1], color=PALETTE[0])
    axes[0].set_xlabel("Observations")
    axes[0].set_title("Top 20 segments by record count")
    axes[0].tick_params(axis="y", labelsize=8)

    by_delay = top20.sort("mean_delay", descending=True)
    axes[1].barh(
        by_delay["segment_id"].cast(pl.String).to_list()[::-1],
        by_delay["mean_delay"].to_list()[::-1],
        color=PALETTE[3],
    )
    axes[1].set_xlabel("Mean delay (s)")
    axes[1].set_title("Mean delay — top 20 segments")
    axes[1].tick_params(axis="y", labelsize=8)
    fig.tight_layout()
    b64_top = fig_to_b64(fig)

    # delay std (variability) per top-20 segment
    fig, ax = plt.subplots(figsize=(10, 5))
    by_std = top20.sort("std_delay", descending=True)
    ax.barh(
        by_std["segment_id"].cast(pl.String).to_list()[::-1],
        by_std["std_delay"].to_list()[::-1],
        color=PALETTE[7],
    )
    ax.set_xlabel("Std deviation of delay (s)")
    ax.set_title("Delay variability — top 20 segments")
    ax.tick_params(axis="y", labelsize=8)
    b64_std = fig_to_b64(fig)

    toc = "<li><a href='#segments'>7. Segment Analysis</a></li>"
    html = section_wrap(
        "7. Segment Analysis", "segments",
        f"<p>Total unique segments: <strong>{df['segment_id'].n_unique():,}</strong></p>",
        img_tag(b64_top),
        img_tag(b64_std, "Higher std = more unpredictable delay on that segment"),
    )
    return toc, html


def build_lag_correlations(df: pl.DataFrame) -> tuple[str, str]:
    lag_cols = [c for c in df.columns if "_lag_" in c]

    rows = []
    for col in lag_cols:
        sub = df.select(col, "target_latitude", "target_longitude").drop_nulls()
        x = sub[col].to_numpy()
        if len(x) < 2:
            continue
        r_lat = float(np.corrcoef(x, sub["target_latitude"].to_numpy())[0, 1])
        r_lon = float(np.corrcoef(x, sub["target_longitude"].to_numpy())[0, 1])
        rows.append({"feature": col, "r_target_lat": r_lat, "r_target_lon": r_lon})

    corr_df = pl.DataFrame(rows).sort("r_target_lat", descending=True)

    fig, axes = plt.subplots(1, 2, figsize=(13, max(4, len(rows) * 0.42)))
    for ax, col, palette_idx in zip(axes, ["r_target_lat", "r_target_lon"], [0, 1]):
        sorted_df = corr_df.sort(col, descending=True)
        vals = sorted_df[col].to_list()
        labels = sorted_df["feature"].to_list()
        colors = [PALETTE[palette_idx] if v >= 0 else PALETTE[9] for v in vals]
        ax.barh(labels[::-1], vals[::-1], color=colors[::-1])
        ax.axvline(0, color="#334155", lw=0.8)
        ax.set_xlabel("Pearson r")
        ax.set_title(f"Lag features vs\n{col.replace('r_', '')}")
        ax.tick_params(axis="y", labelsize=8)
    fig.tight_layout()
    b64 = fig_to_b64(fig)

    toc = "<li><a href='#lag'>8. Lag Feature Correlations</a></li>"
    html = section_wrap(
        "8. Lag Feature Correlations with Targets", "lag",
        img_tag(b64, "Pearson r of each lag feature against the two target variables"),
    )
    return toc, html


def build_correlation_heatmap(df: pl.DataFrame) -> tuple[str, str]:
    numeric_cols = [
        c for c in df.columns
        if c not in ID_COLS and df[c].dtype in NUMERIC_DTYPES
    ]
    sub = df.select(numeric_cols).drop_nulls()
    arr = sub.to_numpy().astype(float)
    corr = np.corrcoef(arr, rowvar=False)

    # full lower-triangle heatmap
    n = len(numeric_cols)
    fs = max(12, n * 0.38)
    fig, ax = plt.subplots(figsize=(fs, fs))
    mask = np.triu(np.ones((n, n), dtype=bool))
    sns.heatmap(
        corr, mask=mask, ax=ax,
        cmap="RdBu_r", center=0, vmin=-1, vmax=1,
        linewidths=0.25, square=True,
        xticklabels=numeric_cols, yticklabels=numeric_cols,
        cbar_kws={"shrink": 0.5},
    )
    ax.set_title("Pearson Correlation Matrix (lower triangle)")
    plt.xticks(rotation=90, fontsize=6)
    plt.yticks(rotation=0,  fontsize=6)
    b64_full = fig_to_b64(fig)

    # focused: all features vs targets only
    target_idx = [numeric_cols.index(t) for t in TARGET_COLS if t in numeric_cols]
    non_target  = [i for i, c in enumerate(numeric_cols) if c not in TARGET_COLS]
    non_target_names = [numeric_cols[i] for i in non_target]

    fig, axes = plt.subplots(1, 2, figsize=(12, max(5, len(non_target) * 0.3)))
    for ax, t_idx, t_name in zip(axes, target_idx, TARGET_COLS):
        vals = corr[non_target, t_idx]
        order = np.argsort(vals)
        colors = [PALETTE[0] if v >= 0 else PALETTE[9] for v in vals[order]]
        ax.barh([non_target_names[i] for i in order], vals[order], color=colors)
        ax.axvline(0, color="#334155", lw=0.8)
        ax.set_xlabel("Pearson r")
        ax.set_title(f"Feature correlations\nwith {t_name}")
        ax.tick_params(axis="y", labelsize=7)
    fig.tight_layout()
    b64_focused = fig_to_b64(fig)

    toc = "<li><a href='#correlations'>9. Correlation Analysis</a></li>"
    html = section_wrap(
        "9. Correlation Analysis", "correlations",
        img_tag(b64_full, "Full Pearson correlation matrix — lower triangle only"),
        "<h3>Feature Correlations with Target Variables</h3>",
        img_tag(b64_focused, "Every numeric feature's Pearson r against target_latitude (left) and target_longitude (right)"),
    )
    return toc, html


def build_segment_avg_features(df: pl.DataFrame) -> tuple[str, str]:
    groups = {
        "global":  [c for c in df.columns if c.startswith("global_")  and c.endswith("_avg")],
        "daily":   [c for c in df.columns if c.startswith("daily_")   and c.endswith("_avg")],
        "monthly": [c for c in df.columns if c.startswith("monthly_") and c.endswith("_avg")],
        "hourly":  [c for c in df.columns if c.startswith("hourly_")  and c.endswith("_avg")],
    }
    blocks = []
    for group_name, cols in groups.items():
        if not cols:
            continue
        fig, axes = plt.subplots(1, len(cols), figsize=(4 * len(cols), 3.5))
        axes = [axes] if len(cols) == 1 else list(axes)
        for ax, col in zip(axes, cols):
            vals = clip_pct(df[col].drop_nulls().to_numpy())
            ax.hist(vals, bins=60, edgecolor="none", alpha=0.85, color=PALETTE[2])
            ax.set_title(col, fontsize=8)
            ax.set_xlabel("value", fontsize=7)
            ax.tick_params(labelsize=7)
        fig.suptitle(f"{group_name.capitalize()} segment averages", fontsize=11)
        fig.tight_layout()
        blocks.append(img_tag(fig_to_b64(fig), f"{group_name} segment-average distributions (clipped 1–99th pct)"))

    toc = "<li><a href='#seg_avgs'>10. Segment Average Features</a></li>"
    html = section_wrap("10. Segment Average Feature Distributions", "seg_avgs", *blocks)
    return toc, html


def build_descriptive_stats(df: pl.DataFrame) -> tuple[str, str]:
    """Describe table for all numeric features."""
    numeric_cols = [
        c for c in df.columns
        if c not in ID_COLS and df[c].dtype in NUMERIC_DTYPES
    ]
    desc = df.select(numeric_cols).describe()
    toc = "<li><a href='#describe'>11. Descriptive Statistics</a></li>"
    html = section_wrap(
        "11. Descriptive Statistics (all numeric columns)", "describe",
        "<p>Mean, std, min, max, and quartiles for every numeric column.</p>",
        df_to_html_table(desc),
    )
    return toc, html


# ── HTML template ─────────────────────────────────────────────────────────────

CSS = """
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
  background: #f0f2f5;
  color: #1a1a2e;
  display: flex;
  min-height: 100vh;
}
nav {
  position: sticky;
  top: 0;
  height: 100vh;
  width: 230px;
  flex-shrink: 0;
  background: #1e3a5f;
  color: #cdd9e5;
  padding: 1.5rem 1rem;
  overflow-y: auto;
}
nav h1 {
  font-size: 0.9rem;
  font-weight: 700;
  color: #fff;
  margin-bottom: 1.4rem;
  letter-spacing: 0.04em;
  text-transform: uppercase;
  line-height: 1.4;
}
nav ul { list-style: none; }
nav li { margin: 0.3rem 0; }
nav a { color: #a3c4f3; text-decoration: none; font-size: 0.8rem; line-height: 1.5; }
nav a:hover { color: #fff; }
nav .generated {
  margin-top: 2rem;
  font-size: 0.7rem;
  color: #6b8cb0;
  line-height: 1.5;
}
main {
  flex: 1;
  padding: 2rem 2.5rem;
  max-width: 1200px;
}
section {
  background: #fff;
  border-radius: 10px;
  box-shadow: 0 1px 6px rgba(0,0,0,.09);
  padding: 2rem;
  margin-bottom: 2rem;
}
h2 {
  font-size: 1.25rem;
  color: #1e3a5f;
  border-bottom: 2px solid #e2e8f0;
  padding-bottom: 0.5rem;
  margin-bottom: 1.2rem;
}
h3 { font-size: 1rem; color: #334155; margin: 1.2rem 0 0.6rem; }
p  { font-size: 0.88rem; color: #475569; margin-bottom: 0.8rem; line-height: 1.7; }
.chart-wrap { margin: 1.2rem 0; text-align: center; }
.chart-wrap img { max-width: 100%; border-radius: 6px; border: 1px solid #e2e8f0; }
.caption {
  font-size: 0.76rem;
  color: #64748b;
  margin-top: 0.4rem;
  font-style: italic;
}
.map-wrap {
  width: 100%;
  height: 500px;
  border-radius: 8px;
  overflow: hidden;
  border: 1px solid #e2e8f0;
  margin: 1rem 0;
}
.map-wrap iframe { width: 100%; height: 100%; border: none; }
.table-wrap { overflow-x: auto; margin: 1rem 0; }
table { border-collapse: collapse; width: 100%; font-size: 0.78rem; }
thead { background: #1e3a5f; color: #fff; }
th, td { padding: 0.4rem 0.75rem; border-bottom: 1px solid #e2e8f0; text-align: left; white-space: nowrap; }
tr:nth-child(even) { background: #f8fafc; }
tr:hover { background: #eef2ff; }
"""


def build_html(toc_items: list[str], sections: list[str], generated_at: str) -> str:
    toc = "\n".join(toc_items)
    body = "\n".join(sections)
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width,initial-scale=1">
  <title>BusTimer — Analytics Data EDA</title>
  <style>{CSS}</style>
</head>
<body>
  <nav>
    <h1>BusTimer<br>Analytics EDA</h1>
    <ul>{toc}</ul>
    <p class="generated">Generated<br>{generated_at}</p>
  </nav>
  <main>{body}</main>
</body>
</html>"""


# ── entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    log.info("Loading analytics_data from Postgres…")
    df = db_query("SELECT * FROM analytics_data")
    log.info("Loaded %d rows × %d columns", *df.shape)

    sections_to_build = [
        build_overview,
        build_target_distributions,
        build_geo_map,
        build_feature_distributions,
        build_categorical,
        build_temporal,
        build_segment_analysis,
        build_lag_correlations,
        build_correlation_heatmap,
        build_segment_avg_features,
        build_descriptive_stats,
    ]

    toc_links, section_blocks = [], []
    for fn in sections_to_build:
        log.info("Building %s…", fn.__name__)
        toc, html = fn(df)
        toc_links.append(toc)
        section_blocks.append(html)

    generated_at = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    html_out = build_html(toc_links, section_blocks, generated_at)
    OUTPUT.write_text(html_out)
    log.info("Wrote %s  (%.2f MB)", OUTPUT, OUTPUT.stat().st_size / 1e6)
