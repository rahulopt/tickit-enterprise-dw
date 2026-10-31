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

# ── Logging Setup ───────────────────────────────────────────────
MSG_FORMAT = "%(asctime)s %(levelname)s %(message)s"
logging.basicConfig(format=MSG_FORMAT)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


def main(batch_request=None, batch_client=None, batch_fence=None, spark_runner=None):
    from tickit_control.context import job_request
    request = batch_request if batch_request is not None else job_request()
    if request is not None:
        from tickit_control.spark_etl import run_spark_branch
        return (spark_runner or run_spark_branch)('sales', request, batch_client, batch_fence)

    from awsglue.utils import getResolvedOptions
    from pyspark.context import SparkContext
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        StructType, StructField,
        IntegerType, ShortType, DecimalType, TimestampType,
    )
    from pyspark.sql.window import Window


    # ── Glue Context Setup ──────────────────────────────────────
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "TICKIT_BUCKET"])
    s3_bucket = args["TICKIT_BUCKET"]
    bronze_path = f"s3://{s3_bucket}/bronze/sales.csv"
    silver_path = f"s3://{s3_bucket}/tickit-v2/silver/sales/"
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)
    job.init(args["JOB_NAME"], args)

    logger.info("=" * 60)
    logger.info("Starting sales ETL: Bronze CSV → Silver Parquet")
    logger.info(f"Source: {bronze_path}")
    start_time = datetime.utcnow()

    # ── Step 1: Define Schema ────────────────────────────────────
    sales_schema = StructType([
        StructField("salesid",    IntegerType(),      True),
        StructField("listid",     IntegerType(),      True),
        StructField("sellerid",   IntegerType(),      True),
        StructField("buyerid",    IntegerType(),      True),
        StructField("eventid",    IntegerType(),      True),
        StructField("dateid",     IntegerType(),      True),
        StructField("qtysold",    ShortType(),        True),
        StructField("pricepaid",  DecimalType(8, 2),  True),
        StructField("commission", DecimalType(8, 2),  True),
        StructField("saletime",   TimestampType(),    True),
    ])

    # ── Step 2: Read from Bronze ─────────────────────────────────
    logger.info("Reading Bronze CSV with explicit schema...")
    df = spark.read.csv(
        bronze_path,
        schema=sales_schema,
        header=True,
        nullValue="",
        timestampFormat="yyyy-MM-dd HH:mm:ss.SSSSSS",
    )

    if df.filter("saletime IS NULL").limit(1).count():
        raise ValueError("sales: NULL saletime detected after CSV parsing")

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
    logger.info(f"Writing Parquet to {silver_path}")
    (
        df.repartition(10)
        .write
        .mode("overwrite")
        .parquet(silver_path, compression="snappy")
    )

    duration = (datetime.utcnow() - start_time).total_seconds()
    logger.info(f"Sales ETL DONE in {duration:.1f}s | {final_count:,} rows written")
    logger.info("=" * 60)

    job.commit()


if __name__ == "__main__":
    main()
