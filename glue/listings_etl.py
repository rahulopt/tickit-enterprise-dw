"""
TICKIT Enterprise Data Warehouse
Glue Spark ETL: listings_etl.py

Purpose:    Bronze CSV → Silver Parquet for listing table
Table:      stage.listing (25M rows, 1.7 GB)
Why Spark:  >10M rows — Pandas would OOM (1.7GB CSV + DataFrame overhead
            = ~5-8GB in memory). Spark distributes across workers.

Spike Day Configuration:
    Normal days:  2 workers G1.X (4GB each) = 8GB total
    Spike days:   10 workers G2.X (8GB each) = 80GB total
    We set: 10 workers G2.X + dynamic scaling enabled

Key Spark Optimizations:
    - inferSchema=False + explicit schema: 3x faster than inference
    - repartition(10): match worker count, balanced partitions
    - snappy compression: best read performance for Redshift COPY
    - coalesce before write: avoid thousands of tiny files

Transformations:
    - Filter null listid rows
    - Cast decimal columns correctly
    - Handle null numtickets (default 0)
    - Overwrite Silver (tonight's batch pattern)

Trigger: Called by Step Functions
"""

import sys
import logging
from datetime import datetime

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, ShortType, DecimalType, TimestampType,
)

# ── Logging Setup ───────────────────────────────────────────────
MSG_FORMAT = "%(asctime)s %(levelname)s %(message)s"
logging.basicConfig(format=MSG_FORMAT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ── Constants ───────────────────────────────────────────────────
S3_BUCKET = "ent-dw-ticket-sales-519749210589"
BRONZE_PATH = f"s3://{S3_BUCKET}/bronze/listing.csv"
SILVER_PATH = f"s3://{S3_BUCKET}/silver/listing/"


def main():
    # ── Glue Context Setup ──────────────────────────────────────
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    logger.info("=" * 60)
    logger.info("Starting listings ETL: Bronze CSV → Silver Parquet")
    logger.info(f"Source: {BRONZE_PATH}")
    start_time = datetime.utcnow()

    # ── Step 1: Define Schema (no inferSchema — 3x faster) ───────
    # inferSchema reads the file twice. Explicit schema: one pass.
    listing_schema = StructType([
        StructField("listid",           IntegerType(),      True),
        StructField("sellerid",         IntegerType(),      True),
        StructField("eventid",          IntegerType(),      True),
        StructField("dateid",           ShortType(),        True),
        StructField("numtickets",       ShortType(),        True),
        StructField("priceperticket",   DecimalType(8, 2),  True),
        StructField("totalprice",       DecimalType(8, 2),  True),
        StructField("listtime",         TimestampType(),    True),
    ])

    # ── Step 2: Read from Bronze ─────────────────────────────────
    logger.info("Reading Bronze CSV with explicit schema...")
    df = spark.read.csv(
        BRONZE_PATH,
        schema=listing_schema,
        header=True,
        nullValue="",
        timestampFormat="yyyy-MM-dd HH:mm:ss",
    )

    raw_count = df.count()
    logger.info(f"Bronze read: {raw_count:,} rows")

    # ── Step 3: Transform ────────────────────────────────────────
    # 3a. Filter null primary key
    df = df.filter(F.col("listid").isNotNull())
    after_null_filter = df.count()
    if raw_count != after_null_filter:
        logger.warning(f"Dropped {raw_count - after_null_filter:,} rows with null listid")

    # 3b. Handle null numtickets (outdoor/free events have no ticket count)
    df = df.fillna({"numtickets": 0})

    # 3c. Handle null prices (free listings)
    df = df.fillna({"priceperticket": 0.0, "totalprice": 0.0})

    # 3d. Remove duplicates on listid (keep last)
    # Use row_number() window function for distributed dedup
    from pyspark.sql.window import Window
    window = Window.partitionBy("listid").orderBy(F.col("listtime").desc_nulls_last())
    df = (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    final_count = df.count()
    logger.info(f"After transforms: {final_count:,} clean rows")

    # ── Step 4: Write to Silver ──────────────────────────────────
    # repartition(10) matches our 10 G2.X workers
    # Each partition ~170MB compressed parquet = manageable
    logger.info(f"Writing Parquet to {SILVER_PATH}")
    (
        df.repartition(10)
        .write
        .mode("overwrite")          # nightly overwrite pattern
        .parquet(SILVER_PATH, compression="snappy")
    )

    duration = (datetime.utcnow() - start_time).total_seconds()
    logger.info(f"Listings ETL DONE in {duration:.1f}s | {final_count:,} rows written")
    logger.info("=" * 60)

    job.commit()


if __name__ == "__main__":
    main()
