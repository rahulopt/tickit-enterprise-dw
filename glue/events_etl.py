"""
TICKIT Enterprise Data Warehouse
Glue Python Shell ETL: events_etl.py

Purpose:    Bronze CSVs → Silver Parquet for event table (denormalized)
Tables:     bronze/event.csv (3M rows), bronze/venue.csv, bronze/category.csv
Why Python Shell: 3M rows + denormalization joins. 1 DPU = 4GB.
                  Pandas merge handles this comfortably in memory.

Key Design Decision - Denormalization:
    stage.event contains denormalized venue + category columns.
    Data modelers decided this because:
    - 95% of event queries include venue/category context
    - Eliminates 2 JOINs per analytical query at 22.5M row fact scale
    - dim.event becomes a single flat lookup table
    
    This ETL does the join ONCE (at ingest time) so Redshift 
    stored procedures don't have to join every night.

Transformations:
    - Read event.csv, venue.csv, category.csv from bronze
    - Join event ← venue (on venueid) ← category (on catid)
    - Filter null eventid rows
    - Cast timestamp correctly
    - Write Parquet to silver/event/

Trigger: Called by Step Functions
"""

import sys
import boto3
import pandas as pd
import io
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

S3_BUCKET = "ent-dw-ticket-sales-519749210589"
SILVER_PREFIX = "silver/event/"

s3_client = boto3.client("s3", region_name="ap-south-1")


def _read_csv_from_s3(key: str, names=None, dtype=str) -> pd.DataFrame:
    """Read a CSV from S3 and return DataFrame."""
    logger.info(f"Reading s3://{S3_BUCKET}/{key}")
    response = s3_client.get_object(Bucket=S3_BUCKET, Key=key)
    data = response["Body"].read()
    logger.info(f"  Downloaded {len(data) / 1024 / 1024:.1f} MB")
    return pd.read_csv(io.BytesIO(data), dtype=dtype, low_memory=False)


def main():
    logger.info("=" * 60)
    logger.info("Starting event ETL: Bronze CSV → Silver Parquet (denormalized)")
    start_time = datetime.utcnow()

    # ── Step 1: Read all 3 source tables from Bronze ─────────────
    df_event = _read_csv_from_s3(
        "bronze/event.csv",
        names=None,  # CSV has header row
    )
    df_venue = _read_csv_from_s3(
        "bronze/venue.csv",
        names=None,  # CSV has header row
    )
    df_cat = _read_csv_from_s3(
        "bronze/category.csv",
        names=None,  # CSV has header row
    )
    logger.info(
        f"Bronze read: event={len(df_event):,} | venue={len(df_venue):,} | category={len(df_cat):,}"
    )

    # ── Step 2: Filter nulls on primary keys ─────────────────────
    df_event = df_event[df_event["eventid"].notna() & (df_event["eventid"].str.strip() != "")]
    df_venue = df_venue[df_venue["venueid"].notna() & (df_venue["venueid"].str.strip() != "")]
    df_cat = df_cat[df_cat["catid"].notna() & (df_cat["catid"].str.strip() != "")]

    # ── Step 3: Cast key columns for join ────────────────────────
    df_event["eventid"] = df_event["eventid"].str.strip().astype("int32")
    df_event["venueid"] = pd.to_numeric(df_event["venueid"].str.strip(), errors="coerce").astype("Int16")
    df_event["catid"] = pd.to_numeric(df_event["catid"].str.strip(), errors="coerce").astype("Int16")
    df_event["dateid"] = pd.to_numeric(df_event["dateid"].str.strip(), errors="coerce").astype("Int16")

    df_venue["venueid"] = df_venue["venueid"].str.strip().astype("int16")
    df_cat["catid"] = df_cat["catid"].str.strip().astype("int16")

    # ── Step 4: String cleaning ───────────────────────────────────
    df_event["eventname"] = df_event["eventname"].str.strip()

    # Parse starttime: handle various formats
    df_event["starttime"] = pd.to_datetime(df_event["starttime"].str.strip(), errors="coerce")

    df_venue["venuename"] = df_venue["venuename"].str.strip()
    df_venue["venuecity"] = df_venue["venuecity"].str.strip()
    df_venue["venuestate"] = df_venue["venuestate"].str.strip().str.upper()
    df_venue.loc[df_venue["venuestate"].str.len() != 2, "venuestate"] = None
    df_venue["venueseats"] = pd.to_numeric(
        df_venue["venueseats"].str.strip(), errors="coerce"
    ).fillna(0).astype("int32")

    df_cat["catgroup"] = df_cat["catgroup"].str.strip().str.upper()
    df_cat["catname"] = df_cat["catname"].str.strip()
    df_cat["catdesc"] = df_cat["catdesc"].str.strip()

    # ── Step 5: Denormalize — join venue + category into event ───
    logger.info("Denormalizing: joining venue and category into event...")

    # Left join: keep all events, even if venue/category missing
    df_merged = df_event.merge(
        df_venue[["venueid", "venuename", "venuecity", "venuestate", "venueseats"]],
        on="venueid",
        how="left",
    ).merge(
        df_cat[["catid", "catgroup", "catname", "catdesc"]],
        on="catid",
        how="left",
    )

    logger.info(f"After join: {len(df_merged):,} rows (should equal event count)")

    # ── Step 6: Deduplicate on eventid ───────────────────────────
    before_dedup = len(df_merged)
    df_merged = df_merged.drop_duplicates(subset=["eventid"], keep="last")
    dedup_removed = before_dedup - len(df_merged)
    if dedup_removed > 0:
        logger.warning(f"Removed {dedup_removed:,} duplicate eventids")

    logger.info(f"Final: {len(df_merged):,} clean rows")

    # ── Step 7: Ensure correct column order for stage.event ──────
    output_columns = [
        "eventid", "venueid", "catid", "dateid", "eventname", "starttime",
        "venuename", "venuecity", "venuestate", "venueseats",
        "catgroup", "catname", "catdesc",
    ]
    df_merged = df_merged[output_columns]

    # ── Step 8: Clear existing Silver + write Parquet ────────────
    _clear_silver_prefix(SILVER_PREFIX)

    logger.info("Writing Parquet to silver...")
    parquet_buffer = io.BytesIO()
    df_merged.to_parquet(
        parquet_buffer,
        index=False,
        engine="pyarrow",
        compression="snappy",
    )
    parquet_buffer.seek(0)

    silver_key = f"{SILVER_PREFIX}event.parquet"
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=silver_key,
        Body=parquet_buffer.getvalue(),
    )

    duration = (datetime.utcnow() - start_time).total_seconds()
    file_size_mb = len(parquet_buffer.getvalue()) / 1024 / 1024
    logger.info(f"Written: s3://{S3_BUCKET}/{silver_key} ({file_size_mb:.1f} MB)")
    logger.info(f"Event ETL DONE in {duration:.1f}s | {len(df_merged):,} rows")
    logger.info("=" * 60)


def _clear_silver_prefix(prefix: str) -> None:
    """Delete existing silver files."""
    paginator = s3_client.get_paginator("list_objects_v2")
    objects_to_delete = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            objects_to_delete.append({"Key": obj["Key"]})

    if objects_to_delete:
        for i in range(0, len(objects_to_delete), 1000):
            batch = objects_to_delete[i : i + 1000]
            s3_client.delete_objects(Bucket=S3_BUCKET, Delete={"Objects": batch})
        logger.info(f"Cleared {len(objects_to_delete)} files from {prefix}")


if __name__ == "__main__":
    main()
