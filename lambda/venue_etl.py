"""
TICKIT Enterprise Data Warehouse
Lambda ETL: venue_etl.py

Purpose:    Bronze CSV → Silver Parquet for venue table
Table:      stage.venue (202 rows)
Why Lambda: <1K rows — instant execution, essentially free

Transformations:
    - Read CSV from bronze/venue.csv
    - Filter null venueid rows
    - State code validation and cleaning
    - Null venueseats handling (0 for outdoor venues)
    - Write Parquet to silver/venue/

Trigger: Called by Step Functions
"""

import json
import boto3
import pandas as pd
import io
import logging
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

S3_BUCKET = "ent-dw-ticket-sales-519749210589"
BRONZE_KEY = "bronze/venue.csv"
SILVER_PREFIX = "silver/venue/"

s3_client = boto3.client("s3")


def lambda_handler(event, context):
    logger.info("Starting venue ETL: Bronze CSV → Silver Parquet")
    start_time = datetime.utcnow()

    try:
        # ── Step 1: Read from Bronze ────────────────────────────
        logger.info(f"Reading s3://{S3_BUCKET}/{BRONZE_KEY}")
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=BRONZE_KEY)
        csv_content = response["Body"].read().decode("utf-8")

        df = pd.read_csv(
            io.StringIO(csv_content),
            dtype=str,
        )

        logger.info(f"Bronze read: {len(df)} rows")

        # ── Step 2: Transform ───────────────────────────────────
        # 2a. Drop rows where primary key is null
        before = len(df)
        df = df[df["venueid"].notna() & (df["venueid"].str.strip() != "")]
        after = len(df)
        if before != after:
            logger.warning(f"Dropped {before - after} rows with null venueid")

        # 2b. Cast primary key
        df["venueid"] = df["venueid"].str.strip().astype("int16")

        # 2c. String cleaning
        df["venuename"] = df["venuename"].str.strip()
        df["venuecity"] = df["venuecity"].str.strip()

        # 2d. State code: 2-char uppercase, empty string if invalid
        df["venuestate"] = df["venuestate"].str.strip().str.upper()
        df.loc[df["venuestate"].str.len() != 2, "venuestate"] = ""

        # 2e. venueseats: nulls → 0 (outdoor/no-seat venues)
        df["venueseats"] = (
            df["venueseats"]
            .str.strip()
            .replace({"": "0", "nan": "0"})
            .fillna("0")
            .astype("int32")
        )

        # 2f. Deduplicate on primary key
        df = df.drop_duplicates(subset=["venueid"], keep="last")

        logger.info(f"After transforms: {len(df)} clean rows")

        # ── Step 3: Write to Silver as Parquet ──────────────────
        _clear_silver_prefix(SILVER_PREFIX)

        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False, engine="pyarrow", compression="snappy")
        parquet_buffer.seek(0)

        silver_key = f"{SILVER_PREFIX}venue.parquet"
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=silver_key,
            Body=parquet_buffer.getvalue(),
        )

        duration = (datetime.utcnow() - start_time).total_seconds()
        logger.info(f"Silver write complete: s3://{S3_BUCKET}/{silver_key}")
        logger.info(f"Venue ETL DONE in {duration:.2f}s | {len(df)} rows")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "status": "SUCCESS",
                "table": "venue",
                "rows_written": len(df),
                "silver_path": f"s3://{S3_BUCKET}/{silver_key}",
                "duration_seconds": round(duration, 2),
            }),
        }

    except Exception as e:
        logger.error(f"Venue ETL FAILED: {str(e)}", exc_info=True)
        return {
            "statusCode": 500,
            "body": json.dumps({
                "status": "FAILED",
                "table": "venue",
                "error": str(e),
            }),
        }


def _clear_silver_prefix(prefix: str) -> None:
    """Delete existing silver files before overwrite."""
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix)

    objects_to_delete = []
    for page in pages:
        for obj in page.get("Contents", []):
            objects_to_delete.append({"Key": obj["Key"]})

    if objects_to_delete:
        s3_client.delete_objects(
            Bucket=S3_BUCKET,
            Delete={"Objects": objects_to_delete},
        )
        logger.info(f"Cleared {len(objects_to_delete)} existing files from {prefix}")
