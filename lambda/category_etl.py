"""
TICKIT Enterprise Data Warehouse
Lambda ETL: category_etl.py

Purpose:    Bronze CSV → Silver Parquet for category table
Table:      stage.category (11 rows)
Why Lambda: <1K rows — instant execution, essentially free
            Lambda completes in <10 seconds vs minutes for Glue

Transformations:
    - Read CSV from bronze/category.csv
    - Filter null catid rows
    - Cast datatypes properly
    - Write Parquet to tickit-v2/silver/category/
    
Trigger: Called by Step Functions (not event-driven)
"""

import json
import boto3
import pandas as pd
import io
import os
import logging
from datetime import datetime, timezone

logger = logging.getLogger()
logger.setLevel(logging.INFO)

S3_BUCKET = os.environ["TICKIT_BUCKET"]
BRONZE_KEY = "bronze/category.csv"
SILVER_PREFIX = "tickit-v2/silver/category/"

s3_client = boto3.client("s3")


def lambda_handler(event, context, *, batch_client=None, batch_fence=None):
    if 'batch_id' in event or 'manifest_sha256' in event:
        from tickit_control.etl import run_pandas_branch
        return run_pandas_branch(globals(), 'category', event,
                                 lambda: lambda_handler({}, context), batch_client, batch_fence)

    logger.info("Starting category ETL: Bronze CSV → Silver Parquet")
    start_time = datetime.now(timezone.utc)

    try:
        # ── Step 1: Read from Bronze ────────────────────────────
        logger.info(f"Reading s3://{S3_BUCKET}/{BRONZE_KEY}")
        response = s3_client.get_object(Bucket=S3_BUCKET, Key=BRONZE_KEY)
        csv_content = response["Body"].read().decode("utf-8")

        df = pd.read_csv(
            io.StringIO(csv_content),
            dtype=str,          # read everything as str first, cast later
        )

        logger.info(f"Bronze read: {len(df)} rows")

        # ── Step 2: Transform ───────────────────────────────────
        # 2a. Drop rows where primary key is null
        before = len(df)
        df = df.copy()
        df["catid"] = pd.to_numeric(df["catid"].str.strip(), errors="coerce")
        df = df.dropna(subset=["catid"]).copy()
        after = len(df)
        if before != after:
            logger.warning(f"Dropped {before - after} rows with null catid")

        # 2b. Cast types
        df["catid"] = df["catid"].astype("int16")
        df["catgroup"] = df["catgroup"].str.strip().str.upper()
        df["catname"] = df["catname"].str.strip()
        df["catdesc"] = df["catdesc"].str.strip()

        # 2c. Remove duplicates on primary key (keep last)
        df = df.drop_duplicates(subset=["catid"], keep="last")

        logger.info(f"After transforms: {len(df)} clean rows")

        # ── Step 3: Write to Silver as Parquet ──────────────────
        # Clear old silver data (tonight's overwrite pattern)
        # Write parquet to in-memory buffer
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False, engine="pyarrow", compression="snappy")
        parquet_buffer.seek(0)

        silver_key = f"{SILVER_PREFIX}category.parquet"
        s3_client.put_object(
            Bucket=S3_BUCKET,
            Key=silver_key,
            Body=parquet_buffer.getvalue(),
        )

        _clear_silver_prefix(SILVER_PREFIX, keep_key=silver_key)

        duration = (datetime.now(timezone.utc) - start_time).total_seconds()
        logger.info(f"Silver write complete: s3://{S3_BUCKET}/{silver_key}")
        logger.info(f"Category ETL DONE in {duration:.2f}s | {len(df)} rows")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "status": "SUCCESS",
                "table": "category",
                "rows_written": len(df),
                "silver_path": f"s3://{S3_BUCKET}/{silver_key}",
                "duration_seconds": round(duration, 2),
            }),
        }

    except Exception as e:
        logger.error(f"Category ETL FAILED: {str(e)}", exc_info=True)
        raise


def _clear_silver_prefix(prefix: str, keep_key: str) -> None:
    """Remove leftover old files only after the new Parquet upload succeeds."""
    paginator = s3_client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=S3_BUCKET, Prefix=prefix)

    objects_to_delete = []
    for page in pages:
        for obj in page.get("Contents", []):
            if obj["Key"] != keep_key:
                objects_to_delete.append({"Key": obj["Key"]})

    if objects_to_delete:
        for i in range(0, len(objects_to_delete), 1000):
            response = s3_client.delete_objects(
                Bucket=S3_BUCKET, Delete={"Objects": objects_to_delete[i:i + 1000]}
            )
            if response.get("Errors"):
                raise RuntimeError(f"Failed removing obsolete Silver objects: {response['Errors']}")
        logger.info(f"Removed {len(objects_to_delete)} stale files from {prefix}")
