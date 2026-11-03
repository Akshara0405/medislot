"""Slot materialisation for the doctor Lambda.

Keeps the DynamoDB Slots table in step with what the doctor configured in MySQL
(weekly availability, consultation types, blackout dates, timezone) for a rolling
30-day window starting today (in the doctor's own timezone).

Rules
  * A consultation type's duration decides the slot size: 09:00-13:00 with a
    30-minute type gives 8 slots, with a 15-minute type 16 slots, with a 60-minute
    type 4 slots.
  * Blackout dates keep their slots but every slot is marked status CLOSED, so
    the doctor still sees them (as Closed) and nobody can book them. Removing
    the blackout re-opens them.
  * Capacity is 1 for every type except HEALTH_CAMP (one patient at a time).
  * Slots that already have bookings/holds are never deleted or overwritten;
    on a blackout day they are only switched to CLOSED (the booking stays).
  * Re-running is safe (idempotent): it only adds what is missing, removes
    unbooked slots that no longer belong, and refreshes unbooked slots whose
    end time, capacity or status changed.
"""
import os
from datetime import datetime, timedelta, timezone, time as dtime
from zoneinfo import ZoneInfo

import boto3
from boto3.dynamodb.conditions import Key

WINDOW_DAYS = 30
DEFAULT_TZ = 'Asia/Kolkata'
OPEN, CLOSED, FULL = 'OPEN', 'CLOSED', 'FULL'
CAMP_MIN_CAPACITY, CAMP_MAX_CAPACITY = 2, 10


def effective_capacity(type_name, stored):
    """1 for every normal consultation; a health camp keeps its configured group size."""
    if type_name != 'HEALTH_CAMP':
        return 1
    return max(CAMP_MIN_CAPACITY, min(CAMP_MAX_CAPACITY, int(stored)))


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


def generate_slots(doctor_id, consultation_type_id, day, timezone_name, start, end, duration, capacity, status=OPEN):
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
                'version': 0, 'status': status,
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
    cur.execute("""SELECT a.day_of_week, a.start_time, a.end_time, t.consultation_type_id, t.name AS type_name, t.duration_min, t.capacity
                   FROM availability_patterns a
                   JOIN consultation_types t ON t.consultation_type_id=a.consultation_type_id
                   WHERE a.doctor_id=%s AND t.is_active=1""", (doctor_id,))
    patterns = cur.fetchall()
    cur.execute('SELECT blackout_date FROM blackout_dates WHERE doctor_id=%s', (doctor_id,))
    blocked = {str(r['blackout_date']) for r in cur.fetchall()}

    plan = {}
    for offset in range(days):
        day = first_day + timedelta(days=offset)
        status = CLOSED if day.isoformat() in blocked else OPEN
        slots = {}
        for p in patterns:
            if int(p['day_of_week']) != day.weekday():
                continue
            capacity = effective_capacity(p.get('type_name'), p['capacity'])
            for item in generate_slots(doctor_id, p['consultation_type_id'], day, tz_name,
                                       as_time(p['start_time']), as_time(p['end_time']),
                                       int(p['duration_min']), capacity, status):
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


def _blocks_overlap(item):
    # CLOSED slots still represent occupied schedule time. Do not reuse them for
    # another consultation type just because they are not currently bookable.
    return _in_use(item) or str(item.get('status', '')).upper() == CLOSED


def _overlap(a, b):
    """True when two slots (ISO 'Z' strings, same format so they compare as text) share any time."""
    return a['start_at'] < b['end_at'] and b['start_at'] < a['end_at']


def _dedupe(wanted, current):
    """Drop wanted slots that overlap a booked, held, CLOSED, or already accepted slot.

    Order: earliest start first, longer slot first on a tie, so the result is the same every run.
    Returns (kept_wanted, number_dropped). Unbooked overlapping slots left over from older saves are not in
    kept_wanted, so the caller removes them.
    """
    taken = [i for i in current.values() if _blocks_overlap(i)]
    kept = {sid: i for sid, i in wanted.items() if _blocks_overlap(current.get(sid) or {})}
    dropped = 0
    ordered = sorted(sorted(wanted.values(), key=lambda i: i['end_at'], reverse=True), key=lambda i: i['start_at'])
    for item in ordered:
        if item['slot_id'] in kept:
            continue
        if any(_overlap(item, t) for t in taken):
            dropped += 1
            continue
        kept[item['slot_id']] = item
        taken.append(item)
    return kept, dropped


def _blockers(current, wanted):
    """Booked, held, or CLOSED slots omitted from the new schedule; they still reserve the doctor's time."""
    return [i for sid, i in current.items() if _blocks_overlap(i) and sid not in wanted]


def find_booking_conflicts(cur, doctor_id, patterns, days=WINDOW_DAYS, table=None, today=None):
    """Messages for every new slot that would overlap an existing booked, held, or CLOSED slot. Empty list = safe to save.

    `patterns` is the weekly list about to be saved: dicts with day_of_week, start_time, end_time
    (time or 'HH:MM'), consultation_type_id and duration_min.
    """
    table = table or boto3.resource('dynamodb').Table(os.environ['SLOTS_TABLE'])
    tz_name = doctor_timezone(cur, doctor_id)
    tz = ZoneInfo(tz_name)
    first_day = today or datetime.now(tz).date()
    problems = []
    for offset in range(days):
        day = first_day + timedelta(days=offset)
        wanted = {}
        for p in patterns:
            if int(p['day_of_week']) == day.weekday():
                for item in generate_slots(doctor_id, p['consultation_type_id'], day, tz_name, as_time(p['start_time']),
                                           as_time(p['end_time']), int(p['duration_min']), 1):
                    wanted[item['slot_id']] = item
        # Existing CLOSED slots are schedule occupancy too. A new consultation
        # type must not be placed on top of them, even when booked_count is zero.
        blockers = _blockers({i['slot_id']: i for i in _existing(table, doctor_id, day.isoformat())}, wanted)
        for item in wanted.values():
            for b in blockers:
                if _overlap(item, b):
                    def local(x): return datetime.fromisoformat(x.replace('Z', '+00:00')).astimezone(tz).strftime('%H:%M')
                    problems.append(f"{day.strftime('%A %d %b')} {local(item['start_at'])}-{local(item['end_at'])} overlaps an existing "
                                    f"consultation slot at {local(b['start_at'])}-{local(b['end_at'])}")
                    break
        if len(problems) >= 3:
            break
    return problems


def _open_status(item):
    taken = int(item.get('booked_count', 0) or 0) + int(item.get('held_count', 0) or 0)
    return FULL if taken >= int(item.get('capacity', 1) or 1) else OPEN


def resync_slots(cur, doctor_id, days=WINDOW_DAYS, table=None, today=None):
    """Bring DynamoDB in line with MySQL. Returns counts of what changed."""
    table = table or boto3.resource('dynamodb').Table(os.environ['SLOTS_TABLE'])
    tz_name = doctor_timezone(cur, doctor_id)
    first_day = today or datetime.now(ZoneInfo(tz_name)).date()
    plan = desired_slots(cur, doctor_id, first_day, days, tz_name)

    created = updated = removed = kept_booked = closed = reopened = skipped = 0
    status_only = []                       # booked/held slots: change status, never replace the item
    with table.batch_writer() as batch:
        for day_iso, wanted in plan.items():
            current = {i['slot_id']: i for i in _existing(table, doctor_id, day_iso)}
            wanted, dropped = _dedupe(wanted, current)
            skipped += dropped
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
                    closed += item['status'] == CLOSED
                    continue
                if _in_use(old):
                    # keep the booking; only the status can change (CLOSED on a blackout, back to OPEN/FULL after)
                    new_status = CLOSED if item['status'] == CLOSED else _open_status(old)
                    if old.get('status') != new_status:
                        status_only.append((slot_id, new_status))
                        closed += new_status == CLOSED
                        reopened += new_status != CLOSED
                    continue
                if (old.get('end_at') != item['end_at'] or int(old.get('capacity', 0)) != item['capacity']
                        or old.get('status') != item['status']):
                    batch.put_item(Item=item)
                    updated += 1
                    closed += item['status'] == CLOSED and old.get('status') != CLOSED
                    reopened += item['status'] != CLOSED and old.get('status') == CLOSED
    for slot_id, new_status in status_only:
        table.update_item(Key={'slot_id': slot_id}, UpdateExpression='SET #s = :s',
                          ExpressionAttributeNames={'#s': 'status'}, ExpressionAttributeValues={':s': new_status})
    return {'created': created, 'updated': updated, 'removed': removed, 'closed': closed, 'reopened': reopened,
            'keptBooked': kept_booked, 'skippedOverlap': skipped, 'days': days, 'startDate': first_day.isoformat()}
