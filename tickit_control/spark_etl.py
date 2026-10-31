"""Manifest-native Spark branch. Only an actual Glue runtime calls this entry.

Local acceptance uses an explicit small-data reference; it does not pretend to
execute these Spark transformations. Scratch outputs are never COPY sources.
"""
import csv
import hashlib
import io
import sys

from .context import Context, runtime_fence
from .schema import SCHEMA
from .storage import pinned_file, put_file


def run_spark_branch(table, request, client=None, fence=None):
    from awsglue.utils import getResolvedOptions
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from pyspark.context import SparkContext
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType, StructField, IntegerType, ShortType, DecimalType, TimestampType
    from pyspark.sql.window import Window
    import boto3
    import pyarrow.parquet as pq

    context = Context.parse(request)
    fence = fence or runtime_fence(context)
    fence.check()
    client = client or boto3.client('s3')
    bucket = context.batch.data['bucket']
    source = context.batch.data['files'][table]
    key = context.batch.attempt_root(context.attempt)+f'pinned-inputs/{table}/{context.task_run}/source.csv'
    with pinned_file(client, bucket, source) as (stream, size):
        header = stream.readline().decode('utf-8-sig')
        if next(csv.reader(io.StringIO(header)), None) != [name for name, _ in SCHEMA[table]]:
            raise ValueError('Spark input columns differ from manifest schema v1')
        fence.check()
        put_file(client, bucket, key, stream, source['sha256'], size)
    fence.check()
    args = getResolvedOptions(sys.argv, ['JOB_NAME'])
    glue = GlueContext(SparkContext.getOrCreate())
    spark = glue.spark_session
    spark.conf.set('spark.sql.session.timeZone', 'UTC')
    job = Job(glue)
    job.init(args['JOB_NAME'], args)
    types = {'int32': IntegerType(), 'int16': ShortType(), 'decimal': DecimalType(8,2), 'timestamp': TimestampType()}
    schema = StructType([StructField(name, types[kind], True) for name, kind in SCHEMA[table]])
    frame = spark.read.csv(f's3://{bucket}/{key}', schema=schema, header=True, mode='FAILFAST',
                           nullValue='', timestampFormat='yyyy-MM-dd HH:mm:ss.SSSSSS')
    stamp, natural = ('saletime','salesid') if table == 'sales' else ('listtime','listid')
    if frame.filter(F.col(stamp).isNull()).limit(1).count():
        raise ValueError('Transaction timestamp failed strict parsing')
    if frame.count() != source['rows']:
        raise ValueError('Input row count differs from immutable manifest')
    frame = frame.filter(F.col(natural).isNotNull())
    if table == 'sales':
        frame = frame.filter(F.col('qtysold').isNull() | (F.col('qtysold') >= 1)).fillna({'commission':0.0,'pricepaid':0.0})
    else:
        frame = frame.fillna({'numtickets':0,'priceperticket':0.0,'totalprice':0.0})
    frame = frame.withColumn('_rn', F.row_number().over(Window.partitionBy(natural).orderBy(F.col(stamp).desc_nulls_last()))).filter(F.col('_rn') == 1).drop('_rn')
    rows = frame.count()
    scratch = context.batch.attempt_root(context.attempt)+f'scratch/{table}/{context.task_run}/'
    fence.check()
    frame.repartition(2).write.mode('errorifexists').parquet(f's3://{bucket}/{scratch}', compression='snappy')
    parts, counted = [], 0
    for page in client.get_paginator('list_objects_v2').paginate(Bucket=bucket, Prefix=scratch):
        for obj in page.get('Contents', []):
            if not obj['Key'].endswith('.parquet'):
                continue
            head = client.head_object(Bucket=bucket, Key=obj['Key'])
            version = head.get('VersionId')
            if not version or version == 'null':
                raise ValueError('Scratch bucket must be versioned')
            # Resolve checksum from the pinned scratch version, not a shared latest path.
            result = client.get_object(Bucket=bucket, Key=obj['Key'], VersionId=version)
            import tempfile
            with tempfile.TemporaryFile() as stream:
                digest, size = hashlib.sha256(), 0
                try:
                    while True:
                        chunk = result['Body'].read(1024*1024)
                        if not chunk:
                            break
                        digest.update(chunk); size += len(chunk); stream.write(chunk)
                finally:
                    result['Body'].close()
                stream.seek(0)
                metadata = pq.ParquetFile(stream)
                counted += metadata.metadata.num_rows
                if metadata.schema_arrow.names != [name for name, _ in SCHEMA[table]]:
                    raise ValueError('Spark output column order differs from stage schema')
                final = context.batch.output_prefix(context.attempt, table, context.task_run)+f'part-{len(parts):05d}.parquet'
                fence.check()
                parts.append(put_file(client, bucket, final, stream, digest.hexdigest(), size))
    if not parts or counted != rows:
        raise ValueError('Incomplete Spark output publication')
    fence.check()
    job.commit()
    from .etl import finish_branch
    return finish_branch(client, context, table, {'task_run':context.task_run,
            'prefix':context.batch.output_prefix(context.attempt, table, context.task_run), 'rows':rows, 'objects':parts}, fence)
