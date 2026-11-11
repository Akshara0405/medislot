"""MediSlot patient domain Lambda (Milestone 4).

One Lambda, two audiences:

  Public discovery (no token, AuthorizationType NONE in API Gateway)
    GET  /doctors                      search: name, hospital, specialisation, city + keyset pagination
    GET  /doctors/filters              values for the search dropdowns
    GET  /doctors/{doctorId}           public doctor profile with consultation types and fees
    GET  /doctors/{doctorId}/slots     free seats per slot for a date range (DynamoDB Slots table)

  Patient-only (JWT Lambda authorizer; the authorizer denies any non-patient token on /patient/*)
    POST   /patient/me                 create the profile row (registration normally does this)
    GET    /patient/me                 read own profile (+ ETag / version)
    PUT    /patient/me                 partial update with optimistic concurrency (If-Match or "version")
    DELETE /patient/me                 erase profile, deactivate account, revoke the tokens in use
    GET    /patient/profiles/{id}      isolation probe: only ever answers for the caller's own id

  Direct invocation only (not reachable through API Gateway)
    {"action": "explain-search"}       EXPLAIN every filter combination + report missing indexes/columns

The caller's identity comes ONLY from the authorizer context, never from the path, query or body.
"""
import base64
import hashlib
import itertools
import json
import logging
import os
import re
import time
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from zoneinfo import ZoneInfo

import boto3
import pymysql
from boto3.dynamodb.conditions import Key
from botocore.config import Config

from common import auth_table, body, config, correlation, dbconn, emit, logged, release, response, token_hash

log = logging.getLogger()

DEFAULT_TZ = 'Asia/Kolkata'
PAGE_DEFAULT, PAGE_MAX = 10, 50
SLOT_WINDOW_DAYS = 30            # the materializer keeps today + 29 days of slots
RANGE_DEFAULT_DAYS = 7
NEXT_SLOT_LOOKAHEAD_DAYS = 14

GENDERS = {'female': 'Female', 'male': 'Male', 'non-binary': 'Non-binary', 'other': 'Other',
           'prefer not to say': 'Prefer not to say'}
BLOOD_GROUPS = ('A+', 'A-', 'B+', 'B-', 'AB+', 'AB-', 'O+', 'O-')
DELETE_CONFIRMATION = 'DELETE'

# Columns Milestone 4 added to `patients` (schema/schema.sql; older databases get them from the migrate action).
M4_PATIENT_COLUMNS = ('city', 'blood_group', 'emergency_contact_name', 'emergency_contact_phone',
                      'profile_version', 'updated_at')
# Index name -> exact column list. explain-search reports any that are missing or different.
EXPECTED_INDEXES = {
    'doctors': {
        'ix_doctors_name': ['last_name', 'first_name'],
        'ix_doctors_first_name': ['first_name'],
        'ix_doctors_spec_name': ['specialization', 'last_name', 'first_name'],
        'ix_doctors_hospital_name': ['hospital_id', 'last_name', 'first_name'],
    },
    'hospitals': {
        'ix_hospitals_city': ['city', 'is_active'],
        'uq_hospitals_name_city': ['name', 'city'],
    },
}


class ApiError(Exception):
    def __init__(self, status, message, **extra):
        super().__init__(message)
        self.status, self.message, self.extra = status, message, extra


# --------------------------------------------------------------------------- helpers

def clean(value):
    """Make MySQL / DynamoDB values JSON friendly."""
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() and value.as_tuple().exponent >= 0 else str(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: clean(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [clean(v) for v in value]
    return value


def money(value):
    return None if value is None else f'{Decimal(str(value)):.2f}'


def reply(status, payload, cid, headers=None):
    out = response(status, clean(payload))
    out['headers']['x-correlation-id'] = cid
    out['headers'].update(headers or {})
    return out


def query_params(event):
    return {k: (v or '').strip() for k, v in (event.get('queryStringParameters') or {}).items()}


def positive_id(raw, name):
    raw = str(raw or '').strip()
    if not re.fullmatch(r'[1-9]\d{0,18}', raw):
        raise ApiError(400, f'{name} must be a positive whole number')
    return int(raw)


_s3 = None
_slots = None


def s3_client():
    """Created once per warm Lambda (search results presign one photo per doctor, so this matters)."""
    global _s3
    if _s3 is None:
        _s3 = boto3.client('s3', region_name=os.environ.get('AWS_REGION'),
                           config=Config(signature_version='s3v4', s3={'addressing_style': 'virtual'}))
    return _s3


def photo_url(key):
    if not key or not os.environ.get('PHOTO_BUCKET'):
        return None
    try:
        return s3_client().generate_presigned_url('get_object', Params={'Bucket': os.environ['PHOTO_BUCKET'], 'Key': key},
                                                  ExpiresIn=900)
    except Exception:
        log.exception('photo_url_failed')
        return None


def slots_table():
    global _slots
    if _slots is None:
        _slots = boto3.resource('dynamodb').Table(os.environ['SLOTS_TABLE'])
    return _slots


def zone(name):
    try:
        return ZoneInfo(name or DEFAULT_TZ)
    except Exception:
        return ZoneInfo(DEFAULT_TZ)


def type_payload(row):
    """One consultation type of a doctor. Kind, label, duration and seat limits come from the consultations table."""
    low, high = int(row.get('min_capacity') or 1), int(row.get('max_capacity') or 1)
    seats = max(low, min(high, int(row.get('capacity') or 1)))
    return {'consultationTypeId': int(row['consultation_type_id']), 'code': row['code'], 'label': row['label'],
            'durationMinutes': int(row['duration_min']), 'fee': money(row['fee_amount']), 'currency': 'INR',
            'seatsPerSlot': seats, 'groupSession': high > 1}


# --------------------------------------------------------------------------- router

ROUTES = {
    ('GET', '/doctors'): 'search_doctors',
    ('GET', '/doctors/filters'): 'search_filters',
    ('GET', '/doctors/{doctorId}'): 'doctor_profile',
    ('GET', '/doctors/{doctorId}/slots'): 'doctor_slots',
    ('GET', '/patient/me'): 'get_me',
    ('POST', '/patient/me'): 'create_me',
    ('PUT', '/patient/me'): 'update_me',
    ('DELETE', '/patient/me'): 'delete_me',
    ('GET', '/patient/profiles/{patientId}'): 'get_profile_by_id',
}


def resolve(event):
    """Use API Gateway's resource template when present; otherwise derive it from the raw path (tests, curl)."""
    resource = event.get('resource')
    params = dict(event.get('pathParameters') or {})
    if not resource:
        parts = [p for p in str(event.get('path', '')).split('/') if p]
        # Strip a leading stage name (e.g. /dev/doctors) if a raw execute-api path was passed through.
        while parts and parts[0] not in ('doctors', 'patient'):
            parts = parts[1:]
        if parts[:1] == ['doctors']:
            if len(parts) == 1:
                resource = '/doctors'
            elif len(parts) == 2 and parts[1] == 'filters':
                resource = '/doctors/filters'
            elif len(parts) in (2, 3):
                params['doctorId'] = parts[1]
                resource = '/doctors/{doctorId}' + ('/slots' if len(parts) == 3 and parts[2] == 'slots' else '')
                if len(parts) == 3 and parts[2] != 'slots':
                    resource = None
        elif parts[:2] == ['patient', 'me'] and len(parts) == 2:
            resource = '/patient/me'
        elif parts[:2] == ['patient', 'profiles'] and len(parts) == 3:
            params['patientId'] = parts[2]
            resource = '/patient/profiles/{patientId}'
    return resource, params


@logged('patient')
def handler(event, context):
    event = event or {}
    if event.get('action') == 'explain-search':
        return explain_search()

    cid = correlation(event)
    method = str(event.get('httpMethod', '')).upper()
    resource, params = resolve(event)
    name = ROUTES.get((method, resource))
    if not name:
        known = any(r == resource for _, r in ROUTES)
        return reply(405 if known else 404, {'message': 'Method not allowed' if known else 'Route not found',
                                             'correlationId': cid}, cid)
    conn = None
    try:
        conn = dbconn(config())
        status_body = globals()[name](conn, event, params, cid)
        status, payload = status_body[0], status_body[1]
        headers = status_body[2] if len(status_body) > 2 else None
        return reply(status, payload, cid, headers)        # @logged writes the success line to CloudWatch
    except ApiError as exc:
        rollback(conn)
        return reply(exc.status, {'message': exc.message, 'correlationId': cid, **exc.extra}, cid)
    except pymysql.err.MySQLError as exc:
        rollback(conn)
        raise_internal(exc, cid, method, resource)
        if exc.args and exc.args[0] in (1054, 1146):      # unknown column / table: schema not migrated
            return reply(503, {'message': 'The database schema is out of date. Run the migrate action '
                                          '(deploy pipeline step "Apply database schema"), then retry.',
                               'correlationId': cid}, cid)
        return reply(500, {'message': 'Internal server error', 'correlationId': cid}, cid)
    except Exception as exc:
        rollback(conn)
        raise_internal(exc, cid, method, resource)
        return reply(500, {'message': 'Internal server error', 'correlationId': cid}, cid)
    finally:
        release(conn)


def rollback(conn):
    try:
        if conn:
            conn.rollback()
    except Exception:
        log.exception('rollback_failed')


def raise_internal(exc, cid, method, resource):
    log.exception('patient_domain_failed route=%s %s correlationId=%s', method, resource, cid)   # stack trace
    emit('error', 'patient_domain_failed', cid, route=f'{method} {resource}', errorType=type(exc).__name__,
         error=str(exc)[:500])


def caller(event):
    """Patient id from the authorizer. The Lambda re-checks the role even though the authorizer already did."""
    ctx = (event.get('requestContext') or {}).get('authorizer') or {}
    if ctx.get('role') != 'patient' or not str(ctx.get('userId', '')).isdigit():
        raise ApiError(403, 'Patient role required')
    return int(ctx['userId']), ctx


# --------------------------------------------------------------------------- doctor search

NAME_ALLOWED = re.compile(r"^[^\W\d_](?:[^\W\d_]|[ .'\-])*$")


def normalise_filters(q):
    """Validate the query string and return a canonical filter dict (also used for the cursor fingerprint)."""
    filters = {}
    name = re.sub(r'\s+', ' ', q.get('name', '')).strip()
    if name:
        if len(name) > 75 or not NAME_ALLOWED.match(name):
            raise ApiError(400, 'name may contain letters, spaces, apostrophes, dots and hyphens (max 75 characters)')
        tokens = [t.strip(".'-") for t in name.split(' ')]
        tokens = [t for t in tokens if t]
        if tokens and tokens[0].lower() in ('dr', 'doctor'):
            tokens = tokens[1:]                     # "Dr Rao" searches for Rao
        if len(tokens) > 3:
            raise ApiError(400, 'name accepts at most three words')
        if tokens:
            filters['name'] = [t.lower() for t in tokens]
    spec = q.get('specialization') or q.get('specialisation') or ''
    if spec:
        if len(spec) > 120:
            raise ApiError(400, 'specialization is too long')
        filters['specialization'] = spec
    if q.get('hospitalId'):
        filters['hospitalId'] = positive_id(q['hospitalId'], 'hospitalId')
    hospital = re.sub(r'\s+', ' ', q.get('hospital', '')).strip()
    if hospital:
        if len(hospital) > 150:
            raise ApiError(400, 'hospital is too long')
        filters['hospital'] = hospital
    city = re.sub(r'\s+', ' ', q.get('city', '')).strip()
    if city:
        if len(city) > 100:
            raise ApiError(400, 'city is too long')
        filters['city'] = city
    return filters


def like_prefix(text):
    return text.replace('\\', '\\\\').replace('%', '\\%').replace('_', '\\_') + '%'


SEARCH_FROM = ('FROM doctors d '
               'JOIN users u ON u.user_id = d.user_id '
               'JOIN hospitals h ON h.hospital_id = d.hospital_id')


def search_sql(filters, after=None, limit=PAGE_DEFAULT):
    """(count_sql, count_args, page_sql, page_args). The ORDER BY matches ix_doctors_* so pages need no filesort."""
    where, args = ['u.is_active = TRUE', 'h.is_active = TRUE'], []
    for token in filters.get('name', []):
        # Each word must start the first OR last name. Both columns lead an index, so MySQL can range-scan them.
        where.append('(d.last_name LIKE %s OR d.first_name LIKE %s)')
        args += [like_prefix(token), like_prefix(token)]
    if 'specialization' in filters:
        where.append('d.specialization = %s')
        args.append(filters['specialization'])
    if 'hospitalId' in filters:
        where.append('d.hospital_id = %s')
        args.append(filters['hospitalId'])
    if 'hospital' in filters:
        where.append('h.name LIKE %s')
        args.append(like_prefix(filters['hospital']))
    if 'city' in filters:
        where.append('h.city = %s')
        args.append(filters['city'])
    count_sql = f"SELECT COUNT(*) AS total {SEARCH_FROM} WHERE {' AND '.join(where)}"
    page_where, page_args = list(where), list(args)
    if after:
        last, first, uid = after
        # Keyset: strictly after the last row of the previous page, in (last_name, first_name, user_id) order.
        page_where.append('(d.last_name > %s OR (d.last_name = %s AND (d.first_name > %s OR '
                          '(d.first_name = %s AND d.user_id > %s))))')
        page_args += [last, last, first, first, uid]
    page_sql = ('SELECT d.user_id, d.first_name, d.last_name, d.specialization, d.qualification, d.experience_years, '
                'd.languages, d.photo_key, h.hospital_id, h.name AS hospital_name, h.city, '
                '(SELECT MIN(ct.fee_amount) FROM consultation_types ct '
                ' WHERE ct.doctor_id = d.user_id AND ct.is_active = TRUE) AS fee_from, '
                "(SELECT GROUP_CONCAT(c.label ORDER BY c.sort_order SEPARATOR '|') FROM consultation_types ct "
                ' JOIN consultations c ON c.consultation_id = ct.consultation_id '
                ' WHERE ct.doctor_id = d.user_id AND ct.is_active = TRUE) AS type_labels '
                f"{SEARCH_FROM} WHERE {' AND '.join(page_where)} "
                'ORDER BY d.last_name, d.first_name, d.user_id LIMIT %s')
    page_args.append(limit + 1)                 # one extra row tells us whether another page exists
    return count_sql, args, page_sql, page_args


def fingerprint(filters):
    return hashlib.sha256(json.dumps(filters, sort_keys=True).encode()).hexdigest()[:16]


def encode_cursor(row, filters):
    raw = json.dumps({'k': [row['last_name'], row['first_name'], int(row['user_id'])], 'f': fingerprint(filters)},
                     separators=(',', ':'))
    return base64.urlsafe_b64encode(raw.encode()).decode().rstrip('=')


def decode_cursor(token, filters):
    try:
        if len(token) > 600:
            raise ValueError()
        data = json.loads(base64.urlsafe_b64decode(token + '=' * (-len(token) % 4)).decode())
        last, first, uid = data['k']
        if not isinstance(last, str) or not isinstance(first, str) or not isinstance(uid, int):
            raise ValueError()
    except Exception:
        raise ApiError(400, 'cursor is not valid; start again without a cursor')
    if data.get('f') != fingerprint(filters):
        raise ApiError(400, 'cursor belongs to a different search; start again without a cursor')
    return last, first, uid


def search_doctors(conn, event, params, cid):
    q = query_params(event)
    filters = normalise_filters(q)
    try:
        limit = int(q.get('limit') or PAGE_DEFAULT)
    except ValueError:
        raise ApiError(400, f'limit must be a whole number between 1 and {PAGE_MAX}')
    if not 1 <= limit <= PAGE_MAX:
        raise ApiError(400, f'limit must be a whole number between 1 and {PAGE_MAX}')
    after = decode_cursor(q['cursor'], filters) if q.get('cursor') else None
    count_sql, count_args, page_sql, page_args = search_sql(filters, after, limit)
    with conn.cursor() as cur:
        cur.execute(count_sql, count_args)
        total = int((cur.fetchone() or {}).get('total') or 0)
        cur.execute(page_sql, page_args)
        rows = cur.fetchall()
    more = len(rows) > limit
    rows = rows[:limit]
    items = []
    for r in rows:
        labels = [c for c in (r.get('type_labels') or '').split('|') if c]
        items.append({
            'doctorId': int(r['user_id']),
            'name': f"Dr. {r['first_name']} {r['last_name']}",
            'firstName': r['first_name'], 'lastName': r['last_name'],
            'specialization': r['specialization'], 'qualification': r['qualification'],
            'experienceYears': int(r['experience_years'] or 0), 'languages': r.get('languages'),
            'hospital': {'hospitalId': int(r['hospital_id']), 'name': r['hospital_name'], 'city': r['city']},
            'feeFrom': money(r.get('fee_from')), 'consultationTypes': labels,
            'acceptingBookings': bool(labels), 'photoUrl': photo_url(r.get('photo_key')),
        })
    applied = dict(filters)
    if 'name' in applied:
        applied['name'] = ' '.join(applied['name'])
    return 200, {'items': items, 'totalMatches': total, 'filters': applied,
                 'page': {'limit': limit, 'returned': len(items), 'hasMore': more,
                          'nextCursor': encode_cursor(rows[-1], filters) if more and rows else None}}


def search_filters(conn, event, params, cid):
    with conn.cursor() as cur:
        cur.execute('SELECT DISTINCT d.specialization FROM doctors d JOIN users u ON u.user_id = d.user_id '
                    'WHERE u.is_active = TRUE ORDER BY d.specialization')
        specs = [r['specialization'] for r in cur.fetchall()]
        cur.execute('SELECT hospital_id, name, city FROM hospitals WHERE is_active = TRUE ORDER BY name, city')
        hospitals = cur.fetchall()
    cities = sorted({h['city'] for h in hospitals if h.get('city')}, key=str.lower)
    return 200, {'specializations': specs, 'cities': cities,
                 'hospitals': [{'hospitalId': int(h['hospital_id']), 'name': h['name'], 'city': h['city']}
                               for h in hospitals]}


# --------------------------------------------------------------------------- public doctor profile + slots

def load_doctor(cur, doctor_id):
    cur.execute('SELECT d.user_id, d.first_name, d.last_name, d.specialization, d.qualification, d.experience_years, '
                'd.languages, d.photo_key, d.timezone, h.hospital_id, h.name AS hospital_name, h.city, h.address '
                'FROM doctors d JOIN users u ON u.user_id = d.user_id AND u.is_active = TRUE '
                'JOIN hospitals h ON h.hospital_id = d.hospital_id WHERE d.user_id = %s', (doctor_id,))
    row = cur.fetchone()
    if not row:
        raise ApiError(404, 'Doctor not found')
    cur.execute('SELECT t.consultation_type_id, t.fee_amount, t.capacity, c.code, c.label, c.duration_min, '
                'c.min_capacity, c.max_capacity FROM consultation_types t '
                'JOIN consultations c ON c.consultation_id = t.consultation_id '
                'WHERE t.doctor_id = %s AND t.is_active = TRUE ORDER BY c.sort_order', (doctor_id,))
    return row, cur.fetchall()


def day_slots(table, doctor_id, day_iso):
    kwargs = {'IndexName': 'GSI_DoctorDate', 'KeyConditionExpression': Key('doctor_date').eq(f'{doctor_id}#{day_iso}')}
    items = []
    while True:
        page = table.query(**kwargs)
        items.extend(page.get('Items', []))
        if not page.get('LastEvaluatedKey'):
            return items
        kwargs['ExclusiveStartKey'] = page['LastEvaluatedKey']


def seat_view(item, types, tz, now_utc):
    """Patient-facing view of one Slots item, or None when the patient should not see it."""
    ctype = types.get(str(item.get('consultation_type_id')))     # DynamoDB stores the id as text
    if not ctype:
        return None                                   # type deactivated since the slot was generated
    try:
        start = datetime.fromisoformat(str(item['start_at']).replace('Z', '+00:00'))
        end = datetime.fromisoformat(str(item['end_at']).replace('Z', '+00:00'))
    except (KeyError, ValueError):
        return None
    if start <= now_utc:
        return None                                   # already started
    capacity = int(item.get('capacity', 1) or 1)
    taken = int(item.get('booked_count', 0) or 0) + int(item.get('held_count', 0) or 0)
    free = 0 if item.get('status') == 'CLOSED' else max(0, capacity - taken)
    return {'slotId': item['slot_id'], 'consultationTypeId': ctype['consultationTypeId'], 'type': ctype['code'],
            'label': ctype['label'], 'durationMinutes': ctype['durationMinutes'], 'fee': ctype['fee'],
            'startsAt': item['start_at'], 'endsAt': item['end_at'],
            'localStart': start.astimezone(tz).strftime('%H:%M'), 'localEnd': end.astimezone(tz).strftime('%H:%M'),
            'capacity': capacity, 'booked': int(item.get('booked_count', 0) or 0),
            'held': int(item.get('held_count', 0) or 0), 'freeSeats': free,
            'status': 'CLOSED' if item.get('status') == 'CLOSED' else ('AVAILABLE' if free else 'FULL')}


def doctor_profile(conn, event, params, cid):
    doctor_id = positive_id(params.get('doctorId'), 'doctorId')
    with conn.cursor() as cur:
        row, type_rows = load_doctor(cur, doctor_id)
    types = [type_payload(t) for t in type_rows]
    tz_name = row.get('timezone') or DEFAULT_TZ
    tz = zone(tz_name)
    next_open = None
    if types:
        table, by_id = slots_table(), {str(t['consultationTypeId']): t for t in types}
        now_utc, today = datetime.now(timezone.utc), datetime.now(tz).date()
        for offset in range(NEXT_SLOT_LOOKAHEAD_DAYS):
            day = today + timedelta(days=offset)
            views = [v for v in (seat_view(i, by_id, tz, now_utc) for i in day_slots(table, doctor_id, day.isoformat()))
                     if v and v['freeSeats'] > 0]
            if views:
                first = min(views, key=lambda v: v['startsAt'])
                next_open = {'date': day.isoformat(), 'localStart': first['localStart'], 'label': first['label'],
                             'freeSeats': first['freeSeats'], 'slotId': first['slotId']}
                break
    fees = [Decimal(t['fee']) for t in types]
    status = 'NOT_CONFIGURED' if not types else ('ACCEPTING' if next_open else 'NO_OPEN_SLOTS_SOON')
    return 200, {
        'doctorId': doctor_id, 'name': f"Dr. {row['first_name']} {row['last_name']}",
        'firstName': row['first_name'], 'lastName': row['last_name'],
        'specialization': row['specialization'], 'qualification': row['qualification'],
        'experienceYears': int(row['experience_years'] or 0), 'languages': row.get('languages'),
        'timezone': tz_name, 'photoUrl': photo_url(row.get('photo_key')),
        'hospital': {'hospitalId': int(row['hospital_id']), 'name': row['hospital_name'], 'city': row['city'],
                     'address': row.get('address')},
        'consultationTypes': types,
        'feeRange': {'min': money(min(fees)), 'max': money(max(fees)), 'currency': 'INR'} if fees else None,
        'bookingStatus': status, 'nextOpenSlot': next_open,
    }


def doctor_slots(conn, event, params, cid):
    doctor_id = positive_id(params.get('doctorId'), 'doctorId')
    q = query_params(event)
    with conn.cursor() as cur:
        row, type_rows = load_doctor(cur, doctor_id)
    types = {str(t['consultation_type_id']): type_payload(t) for t in type_rows}
    wanted = q.get('consultationTypeId')
    if wanted:
        if wanted not in types:
            raise ApiError(400, 'consultationTypeId is not an active consultation type of this doctor')
        types = {wanted: types[wanted]}
    include_full = q.get('includeFull', 'true').lower()
    if include_full not in ('true', 'false'):
        raise ApiError(400, 'includeFull must be true or false')
    include_full = include_full == 'true'

    tz_name = row.get('timezone') or DEFAULT_TZ
    tz = zone(tz_name)
    today = datetime.now(tz).date()
    window_end = today + timedelta(days=SLOT_WINDOW_DAYS - 1)
    try:
        first = date.fromisoformat(q['from']) if q.get('from') else today
        last = date.fromisoformat(q['to']) if q.get('to') else first + timedelta(days=RANGE_DEFAULT_DAYS - 1)
    except ValueError:
        raise ApiError(400, 'from and to must be dates in YYYY-MM-DD format')
    if last < first:
        raise ApiError(400, 'to must be on or after from')
    if (last - first).days + 1 > SLOT_WINDOW_DAYS:
        raise ApiError(400, f'A date range can cover at most {SLOT_WINDOW_DAYS} days')
    if last < today:
        raise ApiError(400, 'The date range is entirely in the past')
    notices = []
    if first < today:
        first = today
        notices.append('Past dates were skipped.')
    if last > window_end:
        last = window_end
        notices.append(f'Slots open {SLOT_WINDOW_DAYS} days ahead; dates after {window_end.isoformat()} are not open yet.')

    table, now_utc = slots_table(), datetime.now(timezone.utc)
    days, total_free, bookable, shown = [], 0, 0, 0
    day = first
    while day <= last:
        raw = day_slots(table, doctor_id, day.isoformat()) if types else []
        relevant = [i for i in raw if str(i.get('consultation_type_id')) in types]
        views = [v for v in (seat_view(i, types, tz, now_utc) for i in relevant) if v]
        visible = [v for v in views if v['status'] != 'CLOSED' and (include_full or v['freeSeats'] > 0)]
        visible.sort(key=lambda v: (v['startsAt'], v['label']))
        free = sum(v['freeSeats'] for v in visible)
        open_count = sum(1 for v in visible if v['freeSeats'] > 0)
        closed_day = bool(relevant) and all(i.get('status') == 'CLOSED' for i in relevant)
        days.append({'date': day.isoformat(), 'weekday': day.strftime('%A'), 'closed': closed_day,
                     'fullyBooked': bool(views) and not closed_day and open_count == 0 and
                     any(v['status'] == 'FULL' for v in views),
                     'openSlots': open_count, 'freeSeats': free, 'slots': visible})
        total_free += free
        bookable += open_count
        shown += len(visible)
        day += timedelta(days=1)
    return 200, {'doctorId': doctor_id, 'timezone': tz_name, 'from': first.isoformat(), 'to': last.isoformat(),
                 'consultationTypeId': int(wanted) if wanted else None, 'includeFull': include_full,
                 'summary': {'days': len(days), 'slotsShown': shown, 'bookableSlots': bookable, 'freeSeats': total_free},
                 'notices': notices, 'days': days}


# --------------------------------------------------------------------------- patient profile CRUD

def v_name(value, label):
    value = re.sub(r'\s+', ' ', str(value)).strip()
    if not value or len(value) > 75 or not NAME_ALLOWED.match(value):
        raise ApiError(400, f'{label} must be 1-75 letters (spaces, apostrophes, dots and hyphens allowed)')
    return value


def v_phone(value, label='phone'):
    value = re.sub(r'\s+', ' ', str(value)).strip()
    digits = re.sub(r'\D', '', value)
    if not re.fullmatch(r'\+?[0-9][0-9 \-]{5,22}', value) or not 7 <= len(digits) <= 15:
        raise ApiError(400, f'{label} must be a phone number with 7 to 15 digits')
    return value


def v_dob(value):
    try:
        born = date.fromisoformat(str(value))
    except ValueError:
        raise ApiError(400, 'dateOfBirth must be a date in YYYY-MM-DD format')
    today = date.today()
    if born > today:
        raise ApiError(400, 'dateOfBirth cannot be in the future')
    if (today.year - born.year) > 120:
        raise ApiError(400, 'dateOfBirth is more than 120 years ago')
    return born.isoformat()


def v_gender(value):
    key = str(value).strip().lower().replace('_', ' ')
    if key not in GENDERS:
        raise ApiError(400, 'gender must be one of: ' + ', '.join(GENDERS.values()))
    return GENDERS[key]


def v_text(limit, label):
    def check(value):
        value = re.sub(r'\s+', ' ', str(value)).strip()
        if len(value) > limit or re.search(r'[<>]', value):
            raise ApiError(400, f'{label} must be at most {limit} characters and must not contain < or >')
        return value
    return check


def v_blood(value):
    value = str(value).strip().upper().replace(' ', '')
    if value not in BLOOD_GROUPS:
        raise ApiError(400, 'bloodGroup must be one of: ' + ', '.join(BLOOD_GROUPS))
    return value


# API field -> (column, validator, required)
PROFILE_FIELDS = {
    'firstName': ('first_name', lambda v: v_name(v, 'firstName'), True),
    'lastName': ('last_name', lambda v: v_name(v, 'lastName'), True),
    'phone': ('phone', v_phone, True),
    'dateOfBirth': ('date_of_birth', v_dob, False),
    'gender': ('gender', v_gender, False),
    'address': ('address', v_text(500, 'address'), False),
    'city': ('city', v_text(100, 'city'), False),
    'bloodGroup': ('blood_group', v_blood, False),
    'emergencyContactName': ('emergency_contact_name', v_text(150, 'emergencyContactName'), False),
    'emergencyContactPhone': ('emergency_contact_phone', lambda v: v_phone(v, 'emergencyContactPhone'), False),
}
READ_ONLY = {'email', 'patientId', 'userId', 'role', 'updatedAt', 'createdAt', 'profileComplete', 'missingFields'}
COMPLETENESS = ('dateOfBirth', 'gender', 'city', 'emergencyContactPhone')


def validate_profile(data, partial):
    """Return {column: value}. Unknown keys, read-only keys and empty required fields are rejected."""
    if not isinstance(data, dict):
        raise ApiError(400, 'Request body must be a JSON object')
    fields = {k: v for k, v in data.items() if k != 'version'}
    unknown = sorted(set(fields) - set(PROFILE_FIELDS) - READ_ONLY)
    if unknown:
        raise ApiError(400, 'Unknown field(s): ' + ', '.join(unknown), allowedFields=sorted(PROFILE_FIELDS))
    locked = sorted(set(fields) & READ_ONLY)
    if locked:
        raise ApiError(400, 'These fields cannot be changed here: ' + ', '.join(locked))
    if partial and not fields:
        raise ApiError(400, 'Send at least one field to update', allowedFields=sorted(PROFILE_FIELDS))
    if not partial:
        missing = [k for k, (_, _, req) in PROFILE_FIELDS.items() if req and not str(fields.get(k) or '').strip()]
        if missing:
            raise ApiError(400, 'Required field(s) missing: ' + ', '.join(missing))
    out = {}
    for key, value in fields.items():
        column, check, required = PROFILE_FIELDS[key]
        if value is None or (isinstance(value, str) and not value.strip()):
            if required:
                raise ApiError(400, f'{key} is required and cannot be empty')
            out[column] = None
            continue
        if isinstance(value, (dict, list, bool)):
            raise ApiError(400, f'{key} must be text')
        out[column] = check(value)
    return out


def profile_payload(row):
    out = {'patientId': int(row['user_id']), 'email': row['email']}
    for key, (column, _, _) in PROFILE_FIELDS.items():
        value = row.get(column)
        out[key] = value.isoformat() if isinstance(value, date) else value
    out['version'] = int(row.get('profile_version') or 1)
    out['updatedAt'] = clean(row.get('updated_at') or row.get('created_at'))
    out['createdAt'] = clean(row.get('created_at'))
    out['missingFields'] = [k for k in COMPLETENESS if not out.get(k)]
    out['profileComplete'] = not out['missingFields']
    return out


def fetch_profile(cur, patient_id):
    cur.execute('SELECT u.user_id, u.email, p.user_id AS profile_id, p.first_name, p.last_name, p.phone, '
                'p.date_of_birth, p.gender, p.address, p.city, p.blood_group, p.emergency_contact_name, '
                'p.emergency_contact_phone, p.profile_version, p.updated_at, p.created_at '
                'FROM users u LEFT JOIN patients p ON p.user_id = u.user_id '
                "WHERE u.user_id = %s AND u.role = 'patient' AND u.is_active = TRUE", (patient_id,))
    row = cur.fetchone()
    if not row:
        raise ApiError(404, 'Patient account not found or deleted')
    return row


def etag(version):
    return {'ETag': f'"v{int(version or 1)}"'}


def get_me(conn, event, params, cid):
    patient_id, _ = caller(event)
    with conn.cursor() as cur:
        row = fetch_profile(cur, patient_id)
    if not row.get('profile_id'):
        raise ApiError(404, 'Profile not created yet. POST /patient/me to create it.')
    payload = profile_payload(row)
    return 200, payload, etag(payload['version'])


def create_me(conn, event, params, cid):
    patient_id, _ = caller(event)
    values = validate_profile(body(event), partial=False)
    with conn.cursor() as cur:
        row = fetch_profile(cur, patient_id)
        if row.get('profile_id'):
            raise ApiError(409, 'Profile already exists. Use PUT /patient/me to change it.')
        columns = ['user_id'] + list(values)
        cur.execute(f"INSERT INTO patients ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})",
                    [patient_id] + list(values.values()))
        conn.commit()
        payload = profile_payload(fetch_profile(cur, patient_id))
    emit('info', 'patient_profile_created', cid, patientId=str(patient_id))
    return 201, payload, etag(payload['version'])


def expected_version(event, data):
    headers = {k.lower(): v for k, v in (event.get('headers') or {}).items()}
    raw = headers.get('if-match') or (data.get('version') if isinstance(data, dict) else None)
    if raw in (None, ''):
        return None
    match = re.fullmatch(r'(?:W/)?"?v?(\d{1,9})"?', str(raw).strip())
    if not match:
        raise ApiError(400, 'version / If-Match must look like 3 or "v3"')
    return int(match.group(1))


def update_me(conn, event, params, cid):
    patient_id, _ = caller(event)
    data = body(event)
    values = validate_profile(data, partial=True)
    want = expected_version(event, data)
    with conn.cursor() as cur:
        current = fetch_profile(cur, patient_id)
        if not current.get('profile_id'):
            raise ApiError(404, 'Profile not created yet. POST /patient/me to create it.')
        sets = ', '.join(f'{c} = %s' for c in values) + ', profile_version = profile_version + 1'
        sql, args = f'UPDATE patients SET {sets} WHERE user_id = %s', list(values.values()) + [patient_id]
        if want is not None:
            sql += ' AND profile_version = %s'
            args.append(want)
        cur.execute(sql, args)
        if cur.rowcount == 0:
            conn.rollback()
            latest = profile_payload(fetch_profile(cur, patient_id))
            raise ApiError(412, 'Your profile was changed somewhere else. Reload it and apply your edit again.',
                           currentVersion=latest['version'], current=latest)
        conn.commit()
        payload = profile_payload(fetch_profile(cur, patient_id))
    emit('info', 'patient_profile_updated', cid, patientId=str(patient_id), fields=sorted(values))
    return 200, payload, etag(payload['version'])


def delete_me(conn, event, params, cid):
    patient_id, ctx = caller(event)
    data = body(event)
    if not isinstance(data, dict) or data.get('confirm') != DELETE_CONFIRMATION:
        raise ApiError(400, f'To delete your account send {{"confirm": "{DELETE_CONFIRMATION}"}}')
    with conn.cursor() as cur:
        fetch_profile(cur, patient_id)
        cur.execute('DELETE FROM patients WHERE user_id = %s', (patient_id,))
        # Keep the users row (future bookings point at it) but make it unusable and free the email for re-use.
        cur.execute("UPDATE users SET is_active = FALSE, email = %s, password_hash = '!deleted' "
                    "WHERE user_id = %s AND role = 'patient'",
                    (f'deleted+{patient_id}.{int(time.time())}@medislot.invalid', patient_id))
        conn.commit()
    now = int(time.time())
    table = auth_table()
    if ctx.get('jti'):
        table.put_item(Item={'token_key': 'JTI#' + str(ctx['jti']), 'token_type': 'DENYLIST',
                             'user_id': str(patient_id), 'created_at': now, 'expires_at': now + 86400,
                             'status': 'REVOKED'})
    raw = data.get('refreshToken')
    if isinstance(raw, str) and raw:
        key = {'token_key': 'REFRESH#' + token_hash(raw), 'token_type': 'REFRESH'}
        item = table.get_item(Key=key).get('Item')
        if item and str(item.get('user_id')) == str(patient_id):
            table.delete_item(Key=key)
    emit('info', 'patient_account_deleted', cid, patientId=str(patient_id))
    return 200, {'deleted': True, 'message': 'Your profile was erased and your account is closed.'}


def get_profile_by_id(conn, event, params, cid):
    patient_id, _ = caller(event)
    target = positive_id(params.get('patientId'), 'patientId')
    if target != patient_id:
        # Same answer whether or not the other patient exists, and no database read: nothing leaks.
        emit('warning', 'patient_isolation_denied', cid, callerId=str(patient_id), targetId=str(target))
        raise ApiError(403, 'Patients can only read their own profile')
    return get_me(conn, event, params, cid)


# --------------------------------------------------------------------------- explain-search (direct invoke)

def explain_search():
    """Run EXPLAIN for all 16 combinations of the four search filters and audit the indexes they rely on."""
    conn = dbconn(config())
    try:
        with conn.cursor() as cur:
            cur.execute('SELECT TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX, COLUMN_NAME FROM information_schema.STATISTICS '
                        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME IN ('doctors', 'hospitals') "
                        'ORDER BY TABLE_NAME, INDEX_NAME, SEQ_IN_INDEX')
            present = {}
            for r in cur.fetchall():
                present.setdefault(r['TABLE_NAME'], {}).setdefault(r['INDEX_NAME'], []).append(r['COLUMN_NAME'])
            missing = [f'{t}.{name}({", ".join(cols)})' for t, idx in EXPECTED_INDEXES.items()
                       for name, cols in idx.items() if present.get(t, {}).get(name) != cols]
            cur.execute('SELECT COLUMN_NAME FROM information_schema.COLUMNS '
                        "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'patients'")
            have = {r['COLUMN_NAME'] for r in cur.fetchall()}
            missing_columns = [f'patients.{c}' for c in M4_PATIENT_COLUMNS if c not in have]

            cur.execute('SELECT d.last_name, d.specialization, d.hospital_id, h.city FROM doctors d '
                        'JOIN hospitals h ON h.hospital_id = d.hospital_id ORDER BY d.user_id LIMIT 1')
            sample = cur.fetchone() or {'last_name': 'Rao', 'specialization': 'Cardiology', 'hospital_id': 1,
                                        'city': 'Hyderabad'}
            cur.execute('SELECT COUNT(*) AS n FROM doctors')
            doctor_rows = int(cur.fetchone()['n'])
            values = {'name': [str(sample['last_name'])[:3].lower()], 'specialization': sample['specialization'],
                      'hospitalId': int(sample['hospital_id']), 'city': sample['city'] or 'Hyderabad'}
            plans = []
            keys = ['name', 'hospitalId', 'specialization', 'city']
            for size in range(len(keys) + 1):
                for combo in itertools.combinations(keys, size):
                    filters = {k: values[k] for k in combo}
                    _, _, page_sql, page_args = search_sql(filters, None, PAGE_DEFAULT)
                    cur.execute('EXPLAIN ' + page_sql, page_args)
                    steps = [{'table': r.get('table'), 'type': r.get('type'), 'possibleKeys': r.get('possible_keys'),
                              'key': r.get('key'), 'rows': r.get('rows'), 'extra': r.get('Extra')}
                             for r in cur.fetchall()]
                    plans.append({'filters': list(combo) or ['(none)'], 'doctorsStep': next(
                        (s for s in steps if s['table'] == 'd'), None), 'plan': steps})
        return {'statusCode': 200, 'body': {'missingIndexes': missing, 'missingColumns': missing_columns,
                                            'doctorRows': doctor_rows, 'indexes': present, 'plans': plans,
                                            'note': 'With only a handful of rows MySQL may still choose a full scan; '
                                                    'possibleKeys shows the index is a candidate.'}}
    finally:
        release(conn)
