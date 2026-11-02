"""Slot materialisation for the doctor Lambda.

Keeps the DynamoDB Slots table in step with what the doctor configured in MySQL
(weekly availability, consultation types, blackout dates, timezone) for a rolling
30-day window starting today (in the doctor's own timezone).

Rules
  * A consultation type's duration decides the slot size: 09:00-13:00 with a
    30-minute type gives 8 slots, with a 15-minute type 16 slots, with a 60-minute
    type 4 slots.
  * Blackout dates get no slots.
  * Slots that already have bookings/holds are never deleted or overwritten.
  * Re-running is safe (idempotent): it only adds what is missing, removes
    unbooked slots that no longer belong, and refreshes unbooked slots whose
    end time or capacity changed.
"""
import os
from datetime import datetime, timedelta, timezone, time as dtime
from zoneinfo import ZoneInfo

import boto3
from boto3.dynamodb.conditions import Key

WINDOW_DAYS = 30
DEFAULT_TZ = 'Asia/Kolkata'


def as_time(value):
    """MySQL TIME arrives as timedelta; the API may hand us 'HH:MM[:SS]'."""
    if isinstance(value, dtime):
        return value
    if isinstance(value, timedelta):
        return (datetime.min + value).time()
    parts = str(value).split(':')
    return dtime(int(parts[0]), int(parts[1]))


def _valid_instants(naive, tz):
    result = {}
    for fold in (0, 1):
        aware = naive.replace(tzinfo=tz, fold=fold)
        utc = aware.astimezone(timezone.utc)
        if utc.astimezone(tz).replace(tzinfo=None) == naive:
            result[utc] = utc
    return sorted(result)


def generate_slots(doctor_id, consultation_type_id, day, timezone_name, start, end, duration, capacity):
    """Cut [start, end) on `day` into back-to-back slots of `duration` minutes."""
    tz = ZoneInfo(timezone_name)
    cursor = datetime.combine(day, start)
    finish = datetime.combine(day, end)
    while cursor + timedelta(minutes=duration) <= finish:
        end_local = cursor + timedelta(minutes=duration)
        for start_utc in _valid_instants(cursor, tz):
            end_utc = start_utc + timedelta(minutes=duration)
            if end_utc.astimezone(tz).replace(tzinfo=None) != end_local:
                continue
            start_str = start_utc.isoformat().replace('+00:00', 'Z')
            end_str = end_utc.isoformat().replace('+00:00', 'Z')
            yield {
                'slot_id': f'{doctor_id}#{consultation_type_id}#{start_str}',
                'doctor_date': f'{doctor_id}#{day.isoformat()}',
                'doctor_id': str(doctor_id),
                'consultation_type_id': str(consultation_type_id),
                'start_at': start_str, 'end_at': end_str,
                'timezone': timezone_name, 'local_date': day.isoformat(),
                'capacity': int(capacity), 'booked_count': 0, 'held_count': 0,
                'version': 0, 'status': 'OPEN',
            }
        cursor = end_local


def doctor_timezone(cur, doctor_id):
    cur.execute("SELECT COALESCE(timezone,%s) AS tz FROM doctors WHERE user_id=%s", (DEFAULT_TZ, doctor_id))
    tz_name = (cur.fetchone() or {}).get('tz') or DEFAULT_TZ
    try:
        ZoneInfo(tz_name)
    except Exception:
        tz_name = DEFAULT_TZ
    return tz_name


def desired_slots(cur, doctor_id, first_day, days, tz_name):
    """{iso_day: {slot_id: item}} for every day of the window."""
    cur.execute("""SELECT a.day_of_week, a.start_time, a.end_time, t.consultation_type_id, t.duration_min, t.capacity
                   FROM availability_patterns a
                   JOIN consultation_types t ON t.consultation_type_id=a.consultation_type_id
                   WHERE a.doctor_id=%s AND t.is_active=1""", (doctor_id,))
    patterns = cur.fetchall()
    cur.execute('SELECT blackout_date FROM blackout_dates WHERE doctor_id=%s', (doctor_id,))
    blocked = {str(r['blackout_date']) for r in cur.fetchall()}

    plan = {}
    for offset in range(days):
        day = first_day + timedelta(days=offset)
        slots = {}
        if day.isoformat() not in blocked:
            for p in patterns:
                if int(p['day_of_week']) != day.weekday():
                    continue
                for item in generate_slots(doctor_id, p['consultation_type_id'], day, tz_name,
                                           as_time(p['start_time']), as_time(p['end_time']),
                                           int(p['duration_min']), int(p['capacity'])):
                    slots[item['slot_id']] = item
        plan[day.isoformat()] = slots
    return plan


def _existing(table, doctor_id, day_iso):
    items, kwargs = [], dict(IndexName='GSI_DoctorDate',
                             KeyConditionExpression=Key('doctor_date').eq(f'{doctor_id}#{day_iso}'))
    while True:
        page = table.query(**kwargs)
        items.extend(page.get('Items', []))
        if not page.get('LastEvaluatedKey'):
            return items
        kwargs['ExclusiveStartKey'] = page['LastEvaluatedKey']


def _in_use(item):
    return int(item.get('booked_count', 0) or 0) > 0 or int(item.get('held_count', 0) or 0) > 0


def resync_slots(cur, doctor_id, days=WINDOW_DAYS, table=None, today=None):
    """Bring DynamoDB in line with MySQL. Returns counts of what changed."""
    table = table or boto3.resource('dynamodb').Table(os.environ['SLOTS_TABLE'])
    tz_name = doctor_timezone(cur, doctor_id)
    first_day = today or datetime.now(ZoneInfo(tz_name)).date()
    plan = desired_slots(cur, doctor_id, first_day, days, tz_name)

    created = updated = removed = kept_booked = 0
    with table.batch_writer() as batch:
        for day_iso, wanted in plan.items():
            current = {i['slot_id']: i for i in _existing(table, doctor_id, day_iso)}
            for slot_id, item in current.items():
                if slot_id not in wanted:
                    if _in_use(item):
                        kept_booked += 1          # never cancel a patient's booking
                    else:
                        batch.delete_item(Key={'slot_id': slot_id})
                        removed += 1
            for slot_id, item in wanted.items():
                old = current.get(slot_id)
                if old is None:
                    batch.put_item(Item=item)
                    created += 1
                elif not _in_use(old) and (old.get('end_at') != item['end_at'] or int(old.get('capacity', 0)) != item['capacity']):
                    batch.put_item(Item=item)
                    updated += 1
    return {'created': created, 'updated': updated, 'removed': removed,
            'keptBooked': kept_booked, 'days': days, 'startDate': first_day.isoformat()}
