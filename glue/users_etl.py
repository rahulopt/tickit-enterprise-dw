"""
TICKIT Enterprise Data Warehouse
Glue Python Shell ETL: users_etl.py

Purpose:    Bronze CSV → Silver Parquet for users table
Table:      stage.users (2M rows, 282 MB)
Why Python Shell: 1K-10M rows range. 1 DPU = 4GB memory.
                  Pandas handles 2M rows in memory comfortably.
                  Much cheaper than Spark (no cluster overhead).

Real-world issue: At 0.0625 DPU (default 512MB), this job gets
                  OOM error (exit code 37) on 282MB CSV.
                  Solution: Use 1 DPU (4GB) — specified in job config.

Transformations:
    - Read CSV from bronze/users.csv in chunks (2M rows)
    - Filter null userid rows
    - Cast boolean columns (0/1 → True/False)
    - Phone format: strip to standard format
    - Email: lowercase
    - Write Parquet partitioned by state to silver/users/

Trigger: Called by Step Functions
"""

import sys
import boto3
import pandas as pd
import io
import logging
from datetime import datetime

# Glue Python Shell uses print for logging (no CloudWatch handler by default)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

S3_BUCKET = "ent-dw-ticket-sales-519749210589"
BRONZE_KEY = "bronze/users.csv"
SILVER_PREFIX = "silver/users/"

# Column definitions
COLUMNS = [
    "userid", "username", "firstname", "lastname",
    "city", "state", "email", "phone",
    "likesports", "liketheatre", "likeconcerts", "likejazz",
    "likeclassical", "likeopera", "likerock", "likevegas",
    "likebroadway", "likemusicals",
]

BOOL_COLUMNS = [
    "likesports", "liketheatre", "likeconcerts", "likejazz",
    "likeclassical", "likeopera", "likerock", "likevegas",
    "likebroadway", "likemusicals",
]

DTYPE_MAP = {col: str for col in COLUMNS}

s3_client = boto3.client("s3", region_name="ap-south-1")

def main():
    logger.info("=" * 60)
    logger.info("Starting users ETL: Bronze CSV → Silver Parquet")
    logger.info(f"Source: s3://{S3_BUCKET}/{BRONZE_KEY}")
    start_time = datetime.utcnow()

    # ── Step 1: Read from Bronze ────────────────────────────────
    logger.info("Reading bronze CSV into memory...")
    response = s3_client.get_object(Bucket=S3_BUCKET, Key=BRONZE_KEY)
    csv_bytes = response["Body"].read()
    logger.info(f"Downloaded {len(csv_bytes) / 1024 / 1024:.1f} MB")

    df = pd.read_csv(
        io.BytesIO(csv_bytes),
        dtype=str,
        low_memory=False,
    )
    # Drop extra columns not in our schema (e.g., created_date from source)
    df = df[[c for c in COLUMNS if c in df.columns]]
    logger.info(f"Bronze read: {len(df):,} rows x {len(df.columns)} columns")

    # ── Step 2: Transform ────────────────────────────────────────
    # 2a. Drop rows where primary key is null
    before = len(df)
    df = df[df["userid"].notna() & (df["userid"].str.strip() != "")]
    dropped = before - len(df)
    if dropped > 0:
        logger.warning(f"Dropped {dropped:,} rows with null userid")

    # 2b. Cast primary key
    df["userid"] = df["userid"].str.strip().astype("int32")

    # 2c. String cleaning for name/contact fields
    for col in ["username", "firstname", "lastname", "city", "phone"]:
        df[col] = df[col].str.strip()

    # 2d. Email: lowercase + strip
    df["email"] = df["email"].str.strip().str.lower()

    # 2e. State: 2-char uppercase
    df["state"] = df["state"].str.strip().str.upper()
    df.loc[df["state"].str.len() != 2, "state"] = None

    # 2f. Boolean columns: 'TRUE'/'FALSE'/'t'/'f'/'1'/'0' → bool
    for col in BOOL_COLUMNS:
        df[col] = df[col].str.strip().str.upper().map(
            {"TRUE": True, "FALSE": False, "T": True, "F": False,
             "1": True, "0": False, "YES": True, "NO": False}
        ).astype("boolean")

    # 2g. Deduplicate on userid (keep last = most recent nightly delivery)
    before_dedup = len(df)
    df = df.drop_duplicates(subset=["userid"], keep="last")
    dedup_removed = before_dedup - len(df)
    if dedup_removed > 0:
        logger.warning(f"Removed {dedup_removed:,} duplicate userids")

    logger.info(f"After transforms: {len(df):,} clean rows")

    # ── Step 3: Clear existing Silver files ──────────────────────
    logger.info(f"Clearing existing silver files at {SILVER_PREFIX}")
    _clear_silver_prefix(SILVER_PREFIX)

    # ── Step 4: Write to Silver as Parquet ───────────────────────
    # Single parquet file (2M rows × 18 cols is ~60MB compressed)
    logger.info("Writing Parquet to silver...")
    parquet_buffer = io.BytesIO()
    df.to_parquet(
        parquet_buffer,
        index=False,
        engine="pyarrow",
        compression="snappy",
    )
    parquet_buffer.seek(0)

    silver_key = f"{SILVER_PREFIX}users.parquet"
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=silver_key,
        Body=parquet_buffer.getvalue(),
    )

    duration = (datetime.utcnow() - start_time).total_seconds()
    file_size_mb = len(parquet_buffer.getvalue()) / 1024 / 1024
    logger.info(f"Written: s3://{S3_BUCKET}/{silver_key} ({file_size_mb:.1f} MB)")
    logger.info(f"Users ETL DONE in {duration:.1f}s | {len(df):,} rows")
    logger.info("=" * 60)


def _clear_silver_prefix(prefix: str) -> None:
    """Delete existing silver files (tonight's overwrite pattern)."""
    paginator = s3_client.get_paginator("list_objects_v2")
    objects_to_delete = []
    for page in paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            objects_to_delete.append({"Key": obj["Key"]})

    if objects_to_delete:
        # S3 delete_objects max 1000 per call
        for i in range(0, len(objects_to_delete), 1000):
            batch = objects_to_delete[i : i + 1000]
            s3_client.delete_objects(Bucket=S3_BUCKET, Delete={"Objects": batch})
        logger.info(f"Cleared {len(objects_to_delete)} files from {prefix}")


if __name__ == "__main__":
    main()
