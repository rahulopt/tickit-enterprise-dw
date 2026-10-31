#!/usr/bin/env python3
"""Prepare synthetic fixtures and an isolated workflow locally. Makes no AWS calls."""
import argparse
import copy
import csv
import hashlib
import json
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
INITIAL_DAY = '2026-10-29'
CHANGED_DAY = '2026-10-30'
ROLLBACK_DAY = '2026-10-31'
TABLES = ('category', 'venue', 'users', 'event', 'listing', 'sales')


def stage_columns():
    text = re.sub(r'--[^\n]*', '', (ROOT / 'sql/02_stage_tables.sql').read_text())
    result = {}
    for table, body in re.findall(r'CREATE TABLE stage\.(\w+)\s*\((.*?)\)\s*DISTSTYLE', text, re.S):
        result[table] = re.findall(r'^\s*(\w+)\s+(?:SMALLINT|INTEGER|BIGINT|BOOLEAN|TIMESTAMP|CHAR|VARCHAR|DECIMAL)', body, re.M)
    return result


def fixtures():
    columns = stage_columns()
    people = []
    for userid, username, city, sports in [(1, 'seller01', 'Austin', 'TRUE'), (2, 'buyer001', 'Miami', 'FALSE')]:
        person = {name: 'FALSE' for name in columns['users'] if name.startswith('like')}
        person.update(userid=userid, username=username, firstname='Test', lastname='Person', city=city,
                      state='TX' if userid == 1 else 'FL', email=f'{username}@example.invalid',
                      phone='000-000-0000', likesports=sports, likejazz='')
        people.append(person)
    def listing(key, when, dateid=1827):
        return dict(listid=key, sellerid=1, eventid=11, dateid=dateid, numtickets=1,
                    priceperticket='10.00', totalprice='10.00', listtime=when)
    def sale(key, listing_key, when):
        return dict(salesid=key, listid=listing_key, sellerid=1, buyerid=2, eventid=11,
                    dateid=1827, qtysold=1, pricepaid='10.00', commission='1.00', saletime=when)
    initial = {
        'category': [dict(catid=1, catgroup='SHOWS', catname='Rock', catdesc='Synthetic concert')],
        'venue': [dict(venueid=1, venuename='Test Hall', venuecity='Austin', venuestate='TX', venueseats=100)],
        'users': people,
        'event': [dict(eventid=11, venueid=1, catid=1, dateid=1827, eventname='Synthetic Concert', starttime=INITIAL_DAY+' 18:00:00.000000')],
        'listing': [listing(10, INITIAL_DAY+' 10:00:00.000000')],
        'sales': [sale(100, 10, INITIAL_DAY+' 12:00:00.000000')],
    }
    changed = copy.deepcopy(initial)
    changed['users'][0]['city'] = ''  # A genuine value-to-NULL transition.
    changed['venue'][0]['venuename'] = 'New Test Hall'
    changed['category'][0]['catdesc'] = 'Updated synthetic concert'
    changed['listing'] += [listing(11, INITIAL_DAY+' 23:59:58.000000'), listing(12, CHANGED_DAY+' 00:00:00.000000')]
    changed['sales'] += [sale(101, 12, CHANGED_DAY+' 00:00:00.000000'), sale(102, 11, INITIAL_DAY+' 23:59:59.999999')]
    conflict = copy.deepcopy(changed)
    conflict['users'][0]['city'] = 'Conflict City'
    rollback = copy.deepcopy(changed)
    rollback['users'][0]['city'] = 'Boston'
    rollback['listing'].append(listing(13, ROLLBACK_DAY+' 11:00:00.000000', dateid=''))
    rollback['sales'].append(sale(103, 13, ROLLBACK_DAY+' 12:00:00.000000'))
    missing = copy.deepcopy(changed)
    missing['users'][0]['city'] = 'Boston'
    missing['sales'].append(sale(103, 10, '2026-10-28 12:00:00.000000'))
    return {'initial': initial, 'changed': changed, 'same_day_conflict': conflict,
            'rollback_after_sales': rollback, 'missing_history': missing}


def isolated_workflow(template):
    flow = copy.deepcopy(template)
    old_start = flow['StartAt']
    flow['StartAt'] = 'ValidateBusinessDate'
    flow['States']['ValidateBusinessDate'] = {
        'Type': 'Task', 'Resource': 'arn:aws:states:::lambda:invoke',
        'Parameters': {'FunctionName': '{{INPUT_VALIDATOR_LAMBDA_ARN}}', 'Payload.$': '$'},
        'OutputPath': '$.Payload', 'Next': old_start,
        'Catch': [{'ErrorEquals': ['States.ALL'], 'Next': 'InvalidBusinessDate'}],
    }
    flow['States']['InvalidBusinessDate'] = {'Type': 'Fail', 'Error': 'InvalidBusinessDate',
                                           'Cause': 'Input validation failed before ETL.'}
    # Low-cost proposal, not resource configuration or an AWS execution.
    flow['TimeoutSeconds'] = 900
    return flow


def validate_isolation(candidate, production):
    """Reject production reuse locally; this cannot prove actual IAM isolation."""
    account = candidate.get('ACCOUNT_ID', '')
    if not re.fullmatch(r'[0-9]{12}', account):
        raise ValueError('Supply a resolved test ACCOUNT_ID')
    prod_account = production['CATEGORY_LAMBDA_ARN'].split(':')[4]
    if account == prod_account:
        raise ValueError('Acceptance resources must use a separate test AWS account')
    if candidate.get('BUSINESS_TIMEZONE') != 'UTC':
        raise ValueError('The isolated business timezone must be UTC')
    if not re.fullmatch(r'[a-z]{2}(?:-[a-z]+)+-[0-9]+', candidate.get('REGION', '')):
        raise ValueError('Supply a test REGION')
    required = ('CATEGORY_LAMBDA_ARN', 'VENUE_LAMBDA_ARN', 'INPUT_VALIDATOR_LAMBDA_ARN',
                'TRIGGER_LAMBDA_ARN', 'STATE_MACHINE_ARN', 'SNS_TOPIC_ARN', 'SNS_QUEUE_ARN',
                'REDSHIFT_COPY_ROLE_ARN')
    expected_services = {'STATE_MACHINE_ARN': 'states', 'SNS_TOPIC_ARN': 'sns',
                         'SNS_QUEUE_ARN': 'sqs', 'REDSHIFT_COPY_ROLE_ARN': 'iam'}
    for key in required:
        value = candidate.get(key, '')
        parts = value.split(':', 5)
        service = expected_services.get(key, 'lambda')
        if len(parts) != 6 or parts[:2] != ['arn', 'aws'] or parts[2] != service or parts[4] != account:
            raise ValueError(f'{key} must resolve to a {service} resource in the separate test account')
        if service != 'iam' and parts[3] != candidate['REGION']:
            raise ValueError(f'{key} must resolve to the test region')
        if any(token in value for token in ('REPLACE_WITH', '{{', '*')) or not parts[5]:
            raise ValueError(f'{key} is unresolved or contains a wildcard')
        if value == production.get(key):
            raise ValueError(f'{key} reuses production')
    for key in ('S3_BUCKET', 'REDSHIFT_WORKGROUP', 'REDSHIFT_DATABASE', 'USERS_GLUE_JOB',
                'EVENTS_GLUE_JOB', 'LISTINGS_GLUE_JOB', 'SALES_GLUE_JOB'):
        value = candidate.get(key, '')
        if not value or any(t in value for t in ('REPLACE_WITH', '{{', '*')) or value == production.get(key):
            raise ValueError(f'{key} must be a resolved, distinct test resource')
    return {'local_reference_check': 'PASS', 'aws_iam_isolation': 'PENDING',
            'resource_existence': 'PENDING'}


def prepare(destination):
    destination = Path(destination)
    columns = stage_columns()
    columns['event'] = columns['event'][:6]  # Bronze event precedes venue/category enrichment.
    batches = fixtures()
    summary = {'business_timezone': 'UTC', 'aws_actions': 0, 'batches': {}}
    for name, tables in batches.items():
        directory = destination / name / 'bronze'
        directory.mkdir(parents=True, exist_ok=True)
        files = {}
        for table in TABLES:
            path = directory / (table+'.csv')
            with path.open('w', newline='') as stream:
                writer = csv.DictWriter(stream, fieldnames=columns[table], lineterminator='\n')
                writer.writeheader()
                writer.writerows(tables[table])
            files[table] = {'rows': len(tables[table]), 'bytes': path.stat().st_size,
                            'sha256': hashlib.sha256(path.read_bytes()).hexdigest()}
        day = INITIAL_DAY if name == 'initial' else CHANGED_DAY if name in ('changed', 'same_day_conflict') else ROLLBACK_DAY
        manifest = {'batch_id': name, 'load_date': day, 'business_timezone': 'UTC',
                    'files': files, 'synthetic': True,
                    'intentionally_invalid': name in ('rollback_after_sales', 'missing_history')}
        # Fixture inventory is not an executable batch manifest: actual S3
        # version IDs exist only after an explicitly approved isolated upload.
        (directory.parent / 'fixture-inventory.json').write_text(json.dumps(manifest, indent=2)+'\n')
        summary['batches'][name] = manifest
    template = json.loads((ROOT/'stepfunctions/tickit_pipeline_v2.template.json').read_text())
    (destination/'workflow.template.json').write_text(json.dumps(isolated_workflow(template), indent=2)+'\n')
    (destination/'readiness.json').write_text(json.dumps(summary, indent=2)+'\n')
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--out', type=Path, default=ROOT/'tests/isolated/generated')
    args = parser.parse_args()
    result = prepare(args.out)
    total = sum(info['bytes'] for batch in result['batches'].values() for info in batch['files'].values())
    print(f'Prepared {len(result["batches"])} synthetic batches / 30 CSV files / {total} bytes in {args.out}')
    print('No AWS calls made. Resource and IAM isolation verification remains PENDING.')


if __name__ == '__main__':
    main()
