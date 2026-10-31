#!/usr/bin/env python3
"""Build input manifest from local CSVs plus actual approved S3 upload receipts.

Does not upload, HEAD or invent S3 versions. Bucket/account ownership is an AWS
preflight gate; this helper validates the exact keys/receipts supplied to it.
"""
import argparse
import csv
import hashlib
import io
import json
import sys
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
from tickit_control.batch import Batch, TABLES, canonical
from tickit_control.etl import validate_raw


def make(directory, receipts, bucket, batch_id, load_date):
    if set(receipts)!=set(TABLES):raise ValueError('Exactly six upload receipts required')
    files={}
    for table in TABLES:
        receipt=receipts[table]
        key=f'tickit-v2/batches/{batch_id}/bronze/{table}.csv'
        if receipt['Bucket']!=bucket or receipt['Key']!=key:
            raise ValueError('Upload receipt crosses batch/bucket boundary')
        body=(Path(directory)/(table+'.csv')).read_bytes()
        rows=len(list(csv.DictReader(io.StringIO(body.decode('utf-8-sig')))))
        validate_raw(table,body,rows)
        sha=hashlib.sha256(body).hexdigest()
        # Record this checksum beside the real VersionId at upload time. The
        # ingress/ETL will independently verify the uploaded pinned bytes.
        if receipt['sha256']!=sha:raise ValueError('Local fixture changed after upload')
        files[table]={'key':key,'version_id':receipt['VersionId'],'sha256':sha,'rows':rows}
    return Batch.parse({'schema_version':1,'datasets':list(TABLES),'batch_id':batch_id,'bucket':bucket,
                        'load_date':load_date,'business_timezone':'UTC','files':files})


if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--csv-dir',type=Path,required=True);parser.add_argument('--receipts',type=Path,required=True)
    parser.add_argument('--bucket',required=True);parser.add_argument('--batch-id',required=True)
    parser.add_argument('--load-date',required=True);parser.add_argument('--out',type=Path,required=True)
    args=parser.parse_args()
    document=make(args.csv_dir,json.loads(args.receipts.read_text()),args.bucket,args.batch_id,args.load_date)
    args.out.write_text(canonical(document.data)+'\n')
    print('Prepared local manifest '+document.fingerprint+'; no AWS calls')
