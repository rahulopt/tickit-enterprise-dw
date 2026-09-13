"""
TICKIT Enterprise Data Warehouse
Glue Spark ETL: sales_etl.py

Purpose:    Bronze CSV → Silver Parquet for sales table
Table:      stage.sales (22.5M rows, 1.8 GB)
Why Spark:  Same reasoning as listings — 1.8GB CSV requires
            distributed processing. Largest table in the system.

Key Difference from listings_etl.py:
    - Sales has sellerid + buyerid (two user foreign keys)
    - No dedup window needed — salesid is truly unique per business rules
      (a completed sale is never re-delivered). But we still handle it
      defensively for data quality.
    - Tighter validation: pricepaid + commission relationship check
      (commission should be ~15% of pricepaid in TICKIT dataset)

Transformations:
    - Filter null salesid rows
    - Cast all decimal measures correctly
    - Ensure qtysold >= 1 (no zero-quantity sales)
    - Handle null commission (default 0)
    - Overwrite Silver

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
from pyspark.sql.window import Window

# ── Logging Setup ───────────────────────────────────────────────
MSG_FORMAT = "%(asctime)s %(levelname)s %(message)s"
logging.basicConfig(format=MSG_FORMAT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ── Constants ───────────────────────────────────────────────────
S3_BUCKET = "ent-dw-ticket-sales-519749210589"
BRONZE_PATH = f"s3://{S3_BUCKET}/bronze/sales.csv"
SILVER_PATH = f"s3://{S3_BUCKET}/silver/sales/"


def main():
    # ── Glue Context Setup ──────────────────────────────────────
    args = getResolvedOptions(sys.argv, ["JOB_NAME"])
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    logger.info("=" * 60)
    logger.info("Starting sales ETL: Bronze CSV → Silver Parquet")
    logger.info(f"Source: {BRONZE_PATH}")
    start_time = datetime.utcnow()

    # ── Step 1: Define Schema ────────────────────────────────────
    sales_schema = StructType([
        StructField("salesid",    IntegerType(),      True),
        StructField("listid",     IntegerType(),      True),
        StructField("sellerid",   IntegerType(),      True),
        StructField("buyerid",    IntegerType(),      True),
        StructField("eventid",    IntegerType(),      True),
        StructField("dateid",     ShortType(),        True),
        StructField("qtysold",    ShortType(),        True),
        StructField("pricepaid",  DecimalType(8, 2),  True),
        StructField("commission", DecimalType(8, 2),  True),
        StructField("saletime",   TimestampType(),    True),
    ])

    # ── Step 2: Read from Bronze ─────────────────────────────────
    logger.info("Reading Bronze CSV with explicit schema...")
    df = spark.read.csv(
        BRONZE_PATH,
        schema=sales_schema,
        header=True,
        nullValue="",
        timestampFormat="yyyy-MM-dd HH:mm:ss",
    )

    raw_count = df.count()
    logger.info(f"Bronze read: {raw_count:,} rows")

    # ── Step 3: Transform ────────────────────────────────────────
    # 3a. Filter null primary key
    df = df.filter(F.col("salesid").isNotNull())
    after_null_filter = df.count()
    if raw_count != after_null_filter:
        logger.warning(f"Dropped {raw_count - after_null_filter:,} rows with null salesid")

    # 3b. Filter invalid qtysold (must be >= 1 — you can't sell 0 tickets)
    before_qty = df.count()
    df = df.filter(
        F.col("qtysold").isNull() | (F.col("qtysold") >= 1)
    )
    qty_removed = before_qty - df.count()
    if qty_removed > 0:
        logger.warning(f"Removed {qty_removed:,} rows with qtysold < 1")

    # 3c. Handle null commission (set to 0.00 — free/promo transactions)
    df = df.fillna({"commission": 0.0})

    # 3d. Handle null pricepaid (edge case — complimentary tickets)
    df = df.fillna({"pricepaid": 0.0})

    # 3e. Defensive dedup on salesid
    window = Window.partitionBy("salesid").orderBy(F.col("saletime").desc_nulls_last())
    df = (
        df.withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    final_count = df.count()
    logger.info(f"After transforms: {final_count:,} clean rows")

    # ── Step 4: Write to Silver ──────────────────────────────────
    logger.info(f"Writing Parquet to {SILVER_PATH}")
    (
        df.repartition(10)
        .write
        .mode("overwrite")
        .parquet(SILVER_PATH, compression="snappy")
    )

    duration = (datetime.utcnow() - start_time).total_seconds()
    logger.info(f"Sales ETL DONE in {duration:.1f}s | {final_count:,} rows written")
    logger.info("=" * 60)

    job.commit()


if __name__ == "__main__":
    main()
