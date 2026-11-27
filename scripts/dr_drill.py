"""Disaster-recovery drill (Milestone 7): restore RDS and DynamoDB to a point in time, measure, verify, clean up.

    python scripts/dr_drill.py --environment dev [--region ap-south-1] [--minutes-ago 10] [--keep] [--report dr.json]

1. RDS: restore-db-instance-to-point-in-time (medislot-<env>-mysql -> medislot-<env>-mysql-drill-<stamp>) in the
   same private subnet group and security group, wait until 'available', record the elapsed time.
2. DynamoDB: restore-table-to-point-in-time for the Slots table (the booking state), wait ACTIVE, compare the item
   count with the source table (scan count; the drill copy is never used by the application).
3. Delete both copies (unless --keep) and write the measurements to the report (paste into docs/DR_DRILL.md).

The production resources are only READ. Nothing in the running system is changed.
"""
import argparse
import json
import subprocess
import time
from datetime import datetime, timedelta, timezone


def aws(region, *args, check=True):
    out = subprocess.run(['aws', *args, '--region', region, '--output', 'json'], check=check, text=True,
                         capture_output=True)
    if out.returncode and check:
        raise SystemExit(out.stderr)
    return json.loads(out.stdout) if out.stdout.strip() else {}


def count_items(region, table):
    total, token = 0, None
    while True:
        args = ['dynamodb', 'scan', '--table-name', table, '--select', 'COUNT']
        if token:
            args += ['--starting-token', token]
        page = aws(region, *args)
        total += int(page.get('Count', 0))
        token = page.get('NextToken')
        if not token:
            return total


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--environment', default='dev')
    p.add_argument('--region', default='ap-south-1')
    p.add_argument('--minutes-ago', type=int, default=10)
    p.add_argument('--keep', action='store_true')
    p.add_argument('--report', default='dr-drill-report.json')
    a = p.parse_args()
    env, region = a.environment, a.region
    stamp = datetime.now(timezone.utc).strftime('%Y%m%d%H%M')
    restore_to = (datetime.now(timezone.utc) - timedelta(minutes=a.minutes_ago)).strftime('%Y-%m-%dT%H:%M:%SZ')
    report = {'environment': env, 'restorePoint': restore_to, 'startedAt': datetime.now(timezone.utc).isoformat()}

    # ---- RDS point-in-time restore
    source = aws(region, 'rds', 'describe-db-instances', '--db-instance-identifier', f'medislot-{env}-mysql')['DBInstances'][0]
    report['rds'] = {'source': source['DBInstanceIdentifier'], 'latestRestorableTime': source.get('LatestRestorableTime'),
                     'backupRetentionDays': source['BackupRetentionPeriod']}
    target = f'medislot-{env}-mysql-drill-{stamp}'
    began = time.time()
    aws(region, 'rds', 'restore-db-instance-to-point-in-time', '--source-db-instance-identifier', source['DBInstanceIdentifier'],
        '--target-db-instance-identifier', target, '--restore-time', restore_to,
        '--db-subnet-group-name', source['DBSubnetGroup']['DBSubnetGroupName'],
        '--vpc-security-group-ids', *[g['VpcSecurityGroupId'] for g in source['VpcSecurityGroups']],
        '--db-instance-class', source['DBInstanceClass'], '--no-publicly-accessible', '--no-multi-az',
        '--tags', 'Key=Project,Value=MediSlot', f'Key=Environment,Value={env}', 'Key=Purpose,Value=dr-drill')
    print(f'RDS restore started: {target} (to {restore_to}) ...')
    subprocess.run(['aws', 'rds', 'wait', 'db-instance-available', '--db-instance-identifier', target, '--region', region],
                   check=True)
    restored = aws(region, 'rds', 'describe-db-instances', '--db-instance-identifier', target)['DBInstances'][0]
    report['rds'].update({'restoredInstance': target, 'status': restored['DBInstanceStatus'],
                          'endpoint': restored.get('Endpoint', {}).get('Address'),
                          'restoreMinutes': round((time.time() - began) / 60, 1)})
    print(f"RDS restored in {report['rds']['restoreMinutes']} min")

    # ---- DynamoDB point-in-time restore (Slots table)
    table = f'MediSlot-{env}-DoctorSlots'
    copy = f'{table}-drill-{stamp}'
    began = time.time()
    aws(region, 'dynamodb', 'restore-table-to-point-in-time', '--source-table-name', table,
        '--target-table-name', copy, '--restore-date-time', restore_to)
    subprocess.run(['aws', 'dynamodb', 'wait', 'table-exists', '--table-name', copy, '--region', region], check=True)
    while aws(region, 'dynamodb', 'describe-table', '--table-name', copy)['Table']['TableStatus'] != 'ACTIVE':
        time.sleep(10)
    report['dynamodb'] = {'source': table, 'restoredTable': copy, 'restoreMinutes': round((time.time() - began) / 60, 1),
                          'sourceItems': count_items(region, table), 'restoredItems': count_items(region, copy)}
    print(f"DynamoDB restored in {report['dynamodb']['restoreMinutes']} min "
          f"({report['dynamodb']['restoredItems']} items vs {report['dynamodb']['sourceItems']} now)")

    # ---- clean up
    if not a.keep:
        aws(region, 'rds', 'delete-db-instance', '--db-instance-identifier', target, '--skip-final-snapshot',
            '--delete-automated-backups')
        aws(region, 'dynamodb', 'delete-table', '--table-name', copy)
        report['cleanedUp'] = True
    report['finishedAt'] = datetime.now(timezone.utc).isoformat()
    with open(a.report, 'w') as fh:
        json.dump(report, fh, indent=2)
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
