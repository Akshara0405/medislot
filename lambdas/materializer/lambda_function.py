"""Daily job (EventBridge): make sure every doctor has slots for the next 30 days in DynamoDB.

It only ADDS missing slots, except for one clean-up: an unbooked slot whose consultation type is no
longer an active type of that doctor (for example a slot still carrying an old UUID type id) is deleted.
"""
import os
from datetime import date, datetime, timedelta

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

from common import config, dbconn, emit, logged, release
from core import choose_non_overlapping, generate_slots

_table = None


def slots_table():
    global _table
    if _table is None:
        _table = boto3.resource('dynamodb').Table(os.environ['SLOTS_TABLE'])
    return _table


def as_time(value):
    return (datetime.min + value).time() if isinstance(value, timedelta) else value


def stored_slots(table, doctor_id, day):
    items, kwargs = [], dict(IndexName='GSI_DoctorDate', KeyConditionExpression=Key('doctor_date').eq(f'{doctor_id}#{day}'))
    while True:
        page = table.query(**kwargs)
        items.extend(page.get('Items', []))
        if not page.get('LastEvaluatedKey'):
            return items
        kwargs['ExclusiveStartKey'] = page['LastEvaluatedKey']


def in_use(item):
    return int(item.get('booked_count', 0) or 0) > 0 or int(item.get('held_count', 0) or 0) > 0


@logged('materializer')
def handler(event, context):
    event = event or {}
    first = date.fromisoformat(event['date']) if event.get('date') else datetime.now().date()
    days = int(event.get('days', 30))
    if not 1 <= days <= 30:
        raise ValueError('days must be 1..30')
    table = slots_table()
    conn = dbconn(config())
    try:
        with conn.cursor() as cur:
            cur.execute("""SELECT d.user_id AS doctor_id, COALESCE(d.timezone, 'Asia/Kolkata') AS timezone,
                                  a.day_of_week, a.start_time, a.end_time, t.consultation_type_id, t.capacity,
                                  c.duration_min, c.min_capacity, c.max_capacity
                           FROM availability_patterns a
                           JOIN doctors d ON d.user_id = a.doctor_id
                           JOIN consultation_types t ON t.consultation_type_id = a.consultation_type_id
                           JOIN consultations c ON c.consultation_id = t.consultation_id
                           WHERE t.is_active = 1""")
            patterns = cur.fetchall()
            cur.execute('SELECT doctor_id, blackout_date FROM blackout_dates')
            blocked = {(str(x['doctor_id']), str(x['blackout_date'])) for x in cur.fetchall()}
    finally:
        release(conn)

    active_types = {}
    for p in patterns:
        active_types.setdefault(p['doctor_id'], set()).add(str(p['consultation_type_id']))

    created = existing = skipped_overlap = removed_stale = 0
    for offset in range(days):
        day = first + timedelta(days=offset)
        by_doctor = {}
        for p in patterns:
            if p['day_of_week'] != day.weekday():
                continue
            # Blackout days still get their slots, marked CLOSED so they show as Closed and cannot be booked.
            status = 'CLOSED' if (str(p['doctor_id']), day.isoformat()) in blocked else 'OPEN'
            capacity = max(int(p['min_capacity']), min(int(p['max_capacity']), int(p['capacity'])))
            by_doctor.setdefault(p['doctor_id'], []).extend(generate_slots(
                p['doctor_id'], p['consultation_type_id'], day, p['timezone'], as_time(p['start_time']),
                as_time(p['end_time']), int(p['duration_min']), capacity, status))
        for doctor_id, items in by_doctor.items():
            stored = []
            for item in stored_slots(table, doctor_id, day.isoformat()):
                if not in_use(item) and str(item.get('consultation_type_id')) not in active_types[doctor_id]:
                    table.delete_item(Key={'slot_id': item['slot_id']})
                    removed_stale += 1
                else:
                    stored.append(item)
            # Never create a slot that overlaps an existing one (e.g. a booked slot whose pattern was removed).
            to_create, there, skipped = choose_non_overlapping(items, stored)
            existing += there
            skipped_overlap += skipped
            for item in to_create:
                try:
                    table.put_item(Item=item, ConditionExpression='attribute_not_exists(slot_id)')
                    created += 1
                except ClientError as exc:
                    if exc.response['Error']['Code'] != 'ConditionalCheckFailedException':
                        raise
                    existing += 1
    summary = {'created': created, 'alreadyExisting': existing, 'skippedOverlap': skipped_overlap,
               'removedStale': removed_stale, 'startDate': first.isoformat(), 'days': days}
    emit('info', 'slots_materialized', None, **summary)
    return summary
