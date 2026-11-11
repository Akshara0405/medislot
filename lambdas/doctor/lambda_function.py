"""Doctor Lambda: profile, photo, consultation types, weekly availability, blackouts, single-slot blocks, schedule.

Every route needs a doctor token (the authorizer checks it; this Lambda re-checks the role).
All ids are integers (AUTO_INCREMENT). Consultation kinds (Quick, Standard, ...) come from the
`consultations` table, not from code.
"""
import os
import uuid
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import boto3
import pymysql
from botocore.config import Config

from common import body, config, dbconn, emit, logged, release, response
from slots import WINDOW_DAYS, find_booking_conflicts, query_schedule_window, resync_slots, slots_table

WEEKDAYS = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']


class ApiError(Exception):
    def __init__(self, status, message):
        super().__init__(message)
        self.status, self.message = status, message


# --------------------------------------------------------------------------- small helpers

def fail(code, message):
    return response(code, {'message': message})


def hhmm(t):
    return f'{t.hour:02d}:{t.minute:02d}'


def as_id(value, what):
    """'12' or 12 -> 12. Anything else is a 400."""
    text = str(value if value is not None else '').strip()
    if not text.isdigit() or int(text) <= 0:
        raise ApiError(400, f'{what} must be a positive whole number')
    return int(text)


def valid_timezone(value):
    try:
        ZoneInfo(value)
        return True
    except (ZoneInfoNotFoundError, KeyError, TypeError, ValueError):
        return False


def jsonable(row):
    out = {}
    for k, v in row.items():
        if isinstance(v, (datetime, date)):
            v = v.isoformat()
        elif isinstance(v, Decimal):
            v = str(v)
        elif k in ('start_time', 'end_time'):
            v = str(v)
        out[k] = v
    return out


_s3 = None


def s3_client():
    # Regional endpoint + SigV4 so pre-signed URLs work straight from the browser. Created once per container.
    global _s3
    if _s3 is None:
        _s3 = boto3.client('s3', region_name=os.environ.get('AWS_REGION'),
                           config=Config(signature_version='s3v4', s3={'addressing_style': 'virtual'}))
    return _s3


def photo_url(key):
    """Short-lived link the dashboard uses to show the doctor's profile photo."""
    if not key or not os.environ.get('PHOTO_BUCKET'):
        return None
    try:
        return s3_client().generate_presigned_url('get_object', Params={'Bucket': os.environ['PHOTO_BUCKET'], 'Key': key},
                                                  ExpiresIn=3600)
    except Exception as exc:
        emit('warning', 'photo_url_failed', None, errorType=type(exc).__name__)
        return None


def sync(cur, doctor_id):
    """Rebuild the next 30 days of slots in DynamoDB. Never fails the request that triggered it."""
    try:
        return {'slotSync': resync_slots(cur, doctor_id, WINDOW_DAYS)}
    except Exception as exc:
        emit('error', 'slot_sync_failed', None, doctorId=str(doctor_id), errorType=type(exc).__name__, error=str(exc)[:300])
        return {'slotSyncError': 'Saved, but the slot calendar could not be updated (' + type(exc).__name__ + '). Save again to retry.'}


# --------------------------------------------------------------------------- router

@logged('doctor')
def handler(event, context):
    ctx = (event.get('requestContext') or {}).get('authorizer') or {}
    if ctx.get('role') != 'doctor' or not str(ctx.get('userId', '')).isdigit():
        return fail(403, 'Doctor role required')
    doctor_id = int(ctx['userId'])
    method = event.get('httpMethod', 'GET')
    path = event.get('path', '').rstrip('/')
    data = body(event) if method in ('POST', 'PUT', 'PATCH') else {}
    if data is None or not isinstance(data, dict):
        return fail(400, 'Invalid JSON object')
    conn = dbconn(config())
    try:
        with conn.cursor() as cur:
            if path.endswith('/doctor/me'):
                return profile(conn, cur, doctor_id, method, data)
            if path.endswith('/doctor/me/photo') and method == 'POST':
                return upload_photo(conn, cur, doctor_id, data)
            if '/consultation-types' in path:
                ident = path.split('/consultation-types/', 1)[1] if '/consultation-types/' in path else None
                return consultation_types(conn, cur, doctor_id, method, ident, data)
            if path.endswith('/doctor/me/availability'):
                return availability(conn, cur, doctor_id, method, data)
            if '/blackouts' in path:
                ident = path.split('/blackouts/', 1)[1] if '/blackouts/' in path else None
                return blackouts(conn, cur, doctor_id, method, ident, data)
            if path.endswith('/doctor/me/slot-blocks') and method == 'POST':
                return slot_block(cur, doctor_id, data)
            if path.endswith('/doctor/me/schedule') and method == 'GET':
                return schedule(doctor_id, event.get('queryStringParameters') or {})
            return fail(404, 'Unknown doctor endpoint')
    except ApiError as exc:
        conn.rollback()
        return fail(exc.status, exc.message)
    except pymysql.IntegrityError:
        conn.rollback()
        return fail(409, 'Duplicate or conflicting record')
    finally:
        release(conn)


# --------------------------------------------------------------------------- profile + photo

PROFILE_FIELDS = ('first_name', 'last_name', 'phone', 'specialization', 'license_number', 'hospital_id',
                  'qualification', 'experience_years', 'timezone', 'languages')


def profile(conn, cur, doctor_id, method, data):
    if method == 'GET':
        cur.execute('SELECT d.*, h.name AS hospital_name FROM doctors d '
                    'LEFT JOIN hospitals h ON h.hospital_id = d.hospital_id WHERE d.user_id = %s', (doctor_id,))
        item = cur.fetchone()
        if not item:
            return fail(404, 'Profile not found')
        out = jsonable(item)
        out['photo_url'] = photo_url(item.get('photo_key'))
        return response(200, out)
    if method != 'PUT':
        return fail(405, 'Method not allowed')
    updates = {k: v for k, v in data.items() if k in PROFILE_FIELDS}
    if not updates:
        return fail(400, 'No editable fields supplied')
    if 'timezone' in updates and not valid_timezone(updates['timezone']):
        return fail(400, 'Invalid IANA timezone')
    if 'experience_years' in updates and (not isinstance(updates['experience_years'], int)
                                          or not 0 <= updates['experience_years'] <= 80):
        return fail(400, 'Invalid experience')
    if 'hospital_id' in updates:
        cur.execute('SELECT hospital_id FROM hospitals WHERE hospital_id = %s AND is_active = 1', (updates['hospital_id'],))
        if not cur.fetchone():
            return fail(400, 'Hospital not found')
    cur.execute('UPDATE doctors SET ' + ', '.join(f'{k} = %s' for k in updates) + ' WHERE user_id = %s',
                (*updates.values(), doctor_id))
    conn.commit()
    extra = sync(cur, doctor_id) if 'timezone' in updates else {}
    return response(200, {'message': 'Profile updated', **extra})


def upload_photo(conn, cur, doctor_id, data):
    mime = data.get('contentType', '')
    if mime not in ('image/jpeg', 'image/png', 'image/webp'):
        return fail(400, 'Unsupported photo content type')
    key = f'doctors/{doctor_id}/{uuid.uuid4()}'          # S3 object name, not a database id
    url = s3_client().generate_presigned_url('put_object', Params={'Bucket': os.environ['PHOTO_BUCKET'], 'Key': key,
                                                                   'ContentType': mime}, ExpiresIn=300)
    cur.execute('UPDATE doctors SET photo_key = %s WHERE user_id = %s', (key, doctor_id))
    conn.commit()
    return response(200, {'uploadUrl': url, 'photoKey': key, 'expiresIn': 300, 'contentType': mime})


# --------------------------------------------------------------------------- consultation types

TYPE_SELECT = ('SELECT t.consultation_type_id, t.consultation_id, c.code AS name, c.label, c.duration_min, '
               't.fee_amount, t.capacity, t.is_active, c.min_capacity, c.max_capacity '
               'FROM consultation_types t JOIN consultations c ON c.consultation_id = t.consultation_id ')


def catalog(cur):
    cur.execute('SELECT consultation_id, code, label, duration_min, min_capacity, max_capacity '
                'FROM consultations WHERE is_active = 1 ORDER BY sort_order, consultation_id')
    return cur.fetchall()


def find_consultation(cur, data):
    """The consultation a request means: by consultation_id, or by its code in "name" (e.g. "STANDARD")."""
    if data.get('consultation_id') not in (None, ''):
        cur.execute('SELECT * FROM consultations WHERE consultation_id = %s AND is_active = 1',
                    (as_id(data['consultation_id'], 'consultation_id'),))
    else:
        cur.execute('SELECT * FROM consultations WHERE code = %s AND is_active = 1', (str(data.get('name') or ''),))
    found = cur.fetchone()
    if not found:
        raise ApiError(400, 'Unknown consultation. Choose one from the consultations list.')
    return found


def check_offer(consultation, data):
    """Validate capacity and fee against the consultations row. Returns (capacity, fee)."""
    if data.get('duration_min') not in (None, '') and data['duration_min'] != consultation['duration_min']:
        raise ApiError(400, f"{consultation['label']} lasts {consultation['duration_min']} minutes")
    capacity = data.get('capacity', consultation['min_capacity'])
    if type(capacity) is not int:
        raise ApiError(400, 'Capacity must be a whole number')
    low, high = consultation['min_capacity'], consultation['max_capacity']
    if not low <= capacity <= high:
        if low == high == 1:
            raise ApiError(400, f"Capacity is fixed at 1 for {consultation['label']} (one patient at a time); "
                                f"only a group session can take more")
        raise ApiError(400, f"{consultation['label']} capacity must be between {low} and {high}")
    try:
        fee = Decimal(str(data.get('fee_amount')))
        if not fee.is_finite() or fee < 0:
            raise InvalidOperation()
    except (InvalidOperation, ValueError, TypeError):
        raise ApiError(400, 'Invalid fee')
    return capacity, fee


def consultation_types(conn, cur, doctor_id, method, ident, data):
    if method == 'GET' and ident is None:
        cur.execute(TYPE_SELECT + 'WHERE t.doctor_id = %s ORDER BY c.sort_order', (doctor_id,))
        return response(200, {'items': [jsonable(r) for r in cur.fetchall()],
                              'catalog': [jsonable(r) for r in catalog(cur)]})

    if method == 'POST' and ident is None:
        consultation = find_consultation(cur, data)
        capacity, fee = check_offer(consultation, data)
        # One row per doctor + consultation. Re-adding a removed one re-activates it, so a duplicate
        # INSERT never fails (a failed INSERT would still use up an AUTO_INCREMENT number).
        cur.execute('SELECT consultation_type_id, is_active FROM consultation_types '
                    'WHERE doctor_id = %s AND consultation_id = %s', (doctor_id, consultation['consultation_id']))
        existing = cur.fetchone()
        if existing and existing['is_active']:
            return fail(409, f"You already offer {consultation['label']}. Edit it in the list instead.")
        if existing:
            type_id = existing['consultation_type_id']
            cur.execute('UPDATE consultation_types SET fee_amount = %s, capacity = %s, is_active = 1 '
                        'WHERE consultation_type_id = %s', (str(fee), capacity, type_id))
        else:
            cur.execute('INSERT INTO consultation_types (doctor_id, consultation_id, fee_amount, capacity, is_active) '
                        'VALUES (%s, %s, %s, %s, 1)', (doctor_id, consultation['consultation_id'], str(fee), capacity))
            type_id = cur.lastrowid
        conn.commit()
        extra = sync(cur, doctor_id) if existing else {}
        return response(200 if existing else 201, {'consultationTypeId': type_id, 'reactivated': bool(existing), **extra})

    if ident is None:
        return fail(405, 'Method not allowed')
    type_id = as_id(ident, 'Consultation type id')
    cur.execute(TYPE_SELECT + 'WHERE t.consultation_type_id = %s AND t.doctor_id = %s', (type_id, doctor_id))
    current = cur.fetchone()
    if not current:
        return fail(404, 'Type not found')

    if method == 'PUT':
        asked = data.get('consultation_id')
        if (data.get('name') not in (None, current['name'])
                or (asked not in (None, '') and as_id(asked, 'consultation_id') != current['consultation_id'])):
            return fail(400, 'A consultation type cannot change its kind. Add the other kind as a new type.')
        capacity, fee = check_offer(current, data)
        cur.execute('UPDATE consultation_types SET fee_amount = %s, capacity = %s, is_active = 1 '
                    'WHERE consultation_type_id = %s AND doctor_id = %s', (str(fee), capacity, type_id, doctor_id))
        conn.commit()
        return response(200, {'consultationTypeId': type_id, **sync(cur, doctor_id)})

    if method == 'DELETE':
        cur.execute('UPDATE consultation_types SET is_active = 0 WHERE consultation_type_id = %s AND doctor_id = %s',
                    (type_id, doctor_id))
        conn.commit()
        return response(200, {'deactivated': True, **sync(cur, doctor_id)})
    return fail(405, 'Method not allowed')


# --------------------------------------------------------------------------- weekly availability

def availability(conn, cur, doctor_id, method, data):
    if method == 'GET':
        cur.execute('SELECT pattern_id, consultation_type_id, day_of_week, start_time, end_time '
                    'FROM availability_patterns WHERE doctor_id = %s ORDER BY day_of_week, start_time', (doctor_id,))
        return response(200, {'items': [jsonable(r) for r in cur.fetchall()]})
    if method != 'PUT':
        return fail(405, 'Method not allowed')

    patterns = data.get('patterns')
    if not isinstance(patterns, list) or len(patterns) > 100:
        return fail(400, 'patterns must be a list of at most 100 entries')
    # All of this doctor's active types in one query (not one query per row).
    cur.execute(TYPE_SELECT + 'WHERE t.doctor_id = %s AND t.is_active = 1', (doctor_id,))
    types = {int(t['consultation_type_id']): t for t in cur.fetchall()}

    by_day, checked, seen = {}, [], set()
    for x in patterns:
        if not isinstance(x, dict) or type(x.get('day_of_week')) is not int or not 0 <= x['day_of_week'] <= 6:
            return fail(400, 'Invalid weekday (Monday=0)')
        try:
            start = datetime.strptime(x['start_time'], '%H:%M').time()
            end = datetime.strptime(x['end_time'], '%H:%M').time()
            if start >= end:
                raise ValueError()
        except (ValueError, KeyError, TypeError):
            return fail(400, 'Invalid local time range')
        type_id = int(x['consultation_type_id']) if str(x.get('consultation_type_id', '')).isdigit() else None
        ctype = types.get(type_id)
        if not ctype:
            return fail(400, 'Unknown or inactive consultation type')
        minutes = (end.hour * 60 + end.minute) - (start.hour * 60 + start.minute)
        duration = int(ctype['duration_min'])
        if minutes < duration:
            return fail(400, f"{x['start_time']}-{x['end_time']} is shorter than one {duration}-minute consultation")
        key = (x['day_of_week'], type_id, start, end)
        if key in seen:      # the same pattern twice is a duplicate, not just an overlap
            return fail(409, f"Pattern already exists: {ctype['label']} {hhmm(start)}-{hhmm(end)} on "
                             f"{WEEKDAYS[x['day_of_week']]}. Remove the duplicate row. Nothing was saved.")
        seen.add(key)
        # Only whole slots count: 09:00-09:34 with a 15-minute type really ends at 09:30.
        last = (start.hour * 60 + start.minute) + (minutes // duration) * duration
        by_day.setdefault(x['day_of_week'], []).append(
            (start, datetime.strptime(f'{last // 60:02d}:{last % 60:02d}', '%H:%M').time(), ctype['label']))
        checked.append({'day_of_week': x['day_of_week'], 'consultation_type_id': type_id, 'duration_min': duration,
                        'start_time': hhmm(start), 'end_time': hhmm(end)})

    for day_no, ranges in by_day.items():
        ranges.sort()
        a = ranges[0]
        for b in ranges[1:]:
            if b[0] < a[1]:          # a doctor cannot be in two consultations at once
                return fail(400, f'Overlapping availability on {WEEKDAYS[day_no]}: {a[2]} {hhmm(a[0])}-{hhmm(a[1])} '
                                 f'clashes with {b[2]} {hhmm(b[0])}-{hhmm(b[1])}. Change one of the times. Nothing was saved.')
            if b[1] > a[1]:
                a = b

    # Booked slots stay, so new slots must not overlap them. Fail closed if bookings cannot be checked.
    try:
        conflicts = find_booking_conflicts(cur, doctor_id, checked)
    except Exception as exc:
        emit('error', 'booking_conflict_check_failed', None, doctorId=str(doctor_id), errorType=type(exc).__name__)
        return fail(503, 'Unable to verify existing bookings right now. Please retry later. Nothing was saved.')
    if conflicts:
        return fail(409, 'Cannot save: ' + '; '.join(conflicts) + '. Cancel or move that booking first, '
                                                                  'or choose another time. Nothing was saved.')

    # Save only the difference: unchanged rows keep their pattern_id, so ids do not jump on every save.
    cur.execute('SELECT pattern_id, consultation_type_id, day_of_week, start_time, end_time '
                'FROM availability_patterns WHERE doctor_id = %s', (doctor_id,))
    old = {(r['day_of_week'], int(r['consultation_type_id']), _hm(r['start_time']), _hm(r['end_time'])): r['pattern_id']
           for r in cur.fetchall()}
    new = {(p['day_of_week'], p['consultation_type_id'], p['start_time'], p['end_time']) for p in checked}
    gone = [pid for key, pid in old.items() if key not in new]
    added = sorted(key for key in new if key not in old)
    if gone:
        cur.execute('DELETE FROM availability_patterns WHERE doctor_id = %s AND pattern_id IN (' +
                    ', '.join(['%s'] * len(gone)) + ')', (doctor_id, *gone))
    if added:
        cur.executemany('INSERT INTO availability_patterns (doctor_id, consultation_type_id, day_of_week, start_time, '
                        'end_time) VALUES (%s, %s, %s, %s, %s)', [(doctor_id, t, d, s, e) for d, t, s, e in added])
    conn.commit()
    return response(200, {'patternsSaved': len(patterns), 'added': len(added), 'removed': len(gone), **sync(cur, doctor_id)})


def _hm(value):
    """MySQL TIME (timedelta) or 'H:MM:SS' -> 'HH:MM'."""
    if hasattr(value, 'total_seconds'):
        minutes = int(value.total_seconds()) // 60
        return f'{minutes // 60:02d}:{minutes % 60:02d}'
    parts = str(value).split(':')
    return f'{int(parts[0]):02d}:{int(parts[1]):02d}'


# --------------------------------------------------------------------------- blackouts, slot blocks, schedule

def blackouts(conn, cur, doctor_id, method, ident, data):
    if method == 'GET' and ident is None:
        cur.execute('SELECT blackout_id, blackout_date, reason FROM blackout_dates WHERE doctor_id = %s '
                    'ORDER BY blackout_date', (doctor_id,))
        return response(200, {'items': [jsonable(r) for r in cur.fetchall()]})
    if method == 'POST' and ident is None:
        try:
            day = date.fromisoformat(data['blackout_date'])
        except (KeyError, ValueError, TypeError):
            return fail(400, 'Invalid blackout date')
        cur.execute('SELECT 1 FROM blackout_dates WHERE doctor_id = %s AND blackout_date = %s', (doctor_id, day))
        if cur.fetchone():       # checked first so a duplicate never uses up an id
            return fail(409, f'{day.isoformat()} is already a blackout date')
        cur.execute('INSERT INTO blackout_dates (doctor_id, blackout_date, reason) VALUES (%s, %s, %s)',
                    (doctor_id, day, str(data.get('reason', ''))[:200]))
        blackout_id = cur.lastrowid
        conn.commit()
        return response(201, {'blackoutId': blackout_id, **sync(cur, doctor_id)})
    if method == 'DELETE' and ident is not None:
        cur.execute('DELETE FROM blackout_dates WHERE blackout_id = %s AND doctor_id = %s',
                    (as_id(ident, 'Blackout id'), doctor_id))
        done = bool(cur.rowcount)
        conn.commit()
        return response(200, {'deleted': done, **(sync(cur, doctor_id) if done else {})})
    return fail(405, 'Method not allowed')


def slot_block(cur, doctor_id, data):
    """Close (blocked=true) or re-open (blocked=false) ONE slot. A booking on it is never cancelled."""
    slot_id, blocked = data.get('slot_id'), data.get('blocked', True)
    if (not isinstance(slot_id, str) or not slot_id.startswith(f'{doctor_id}#') or len(slot_id) > 190
            or type(blocked) is not bool):
        return fail(400, 'Invalid slot')
    table = slots_table()
    slot = table.get_item(Key={'slot_id': slot_id}).get('Item')
    if not slot or str(slot.get('doctor_id')) != str(doctor_id):
        return fail(404, 'Slot not found')
    cur.execute('SELECT 1 FROM blackout_dates WHERE doctor_id = %s AND blackout_date = %s', (doctor_id, slot.get('local_date')))
    if cur.fetchone():
        return fail(409, f"{slot.get('local_date')} is a blackout day, so every slot that day stays closed. "
                         f"Remove the blackout date first (Blackouts tab), then open or close single slots.")
    if blocked:
        table.update_item(Key={'slot_id': slot_id}, UpdateExpression='SET manual_block = :t',
                          ExpressionAttributeValues={':t': True})
    else:
        table.update_item(Key={'slot_id': slot_id}, UpdateExpression='REMOVE manual_block')
    return response(200, {'slotId': slot_id, 'blocked': blocked,
                          'hasBooking': int(slot.get('booked_count', 0) or 0) > 0, **sync(cur, doctor_id)})


def schedule(doctor_id, params):
    start_value = params.get('start_date') or params.get('date')
    try:
        first_day = date.fromisoformat(start_value)
    except (ValueError, TypeError):
        return fail(400, 'date or start_date must be YYYY-MM-DD')
    try:
        days = int(params.get('days', '1' if params.get('date') else '30'))
    except (TypeError, ValueError):
        return fail(400, 'days must be an integer between 1 and 30')
    if not 1 <= days <= 30:
        return fail(400, 'days must be between 1 and 30')
    items = query_schedule_window(slots_table(), doctor_id, first_day, days)
    return response(200, {'startDate': first_day.isoformat(), 'days': days, 'slots': [jsonable(x) for x in items]})
