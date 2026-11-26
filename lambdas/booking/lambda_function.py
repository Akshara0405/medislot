"""MediSlot booking engine Lambda (Milestone 5): bookings, concurrency, idempotency, cancellations, refunds.

Everything Milestone 5 adds lives in this ONE file. Read docs/CONCURRENCY_DESIGN_NOTE.md first.
RDS tables used: appointments, refund_ledger (plus the existing users, patients, doctors, hospitals,
consultation_types, consultations). DynamoDB tables used: Slots, Idempotency.

  Patient (JWT authorizer; /patient/* only accepts patient tokens)
    POST   /patient/appointments                            book a seat  (header Idempotency-Key required)
    GET    /patient/appointments?view=upcoming|past|cancelled|all&limit=&cursor=
    GET    /patient/appointments/{appointmentId}            one appointment (only the caller's own)
    GET    /patient/appointments/{appointmentId}/refund-quote   what a cancellation would refund right now
    POST   /patient/appointments/{appointmentId}/cancel     cancel + refund (repeat-safe)
    GET    /patient/refunds                                 the caller's refund ledger

  Doctor (JWT authorizer; /doctor/* only accepts doctor tokens)
    GET    /doctor/me/appointments?from=YYYY-MM-DD&days=1..30&includeCancelled=true|false
    POST   /doctor/me/appointments/{appointmentId}/status   {"status": CONFIRMED|COMPLETED|NO_SHOW|CANCELLED}
    POST   /doctor/me/slot-cancellations                    cancel a slot: every patient refunded in full
    POST   /doctor/me/day-cancellations                     {"date"}: cancel every booked slot that day (blackouts)

  Direct invocation only (never reachable through API Gateway)
    {"action": "release-pending"}                 EventBridge schedule (5 min): free seats a cancellation missed
    {"action": "reconcile-slot", "slotId": ...}   compare a slot's seats with RDS (+ "releaseOrphans": true)
    {"action": "policy"}                          show the refund policy and limits read from SSM

How a seat is taken (the short version)
  1. Idempotency-Key is claimed in DynamoDB (conditional put). A replay returns the ORIGINAL response.
  2. The slot item is read with ConsistentRead, checked (exists, future, open, seats left), then written with
     ConditionExpression "version = :seen" while bumping version. Losing the race -> ConditionalCheckFailed ->
     re-read and retry with full-jitter exponential backoff, at most maxOccAttempts times. Every retry is
     counted in the CloudWatch metric BookingConflictRetries (Embedded Metric Format log line).
  3. The seat is tagged "p<patientId>" in the slot's booking_refs set. appointments has
     UNIQUE(patient_id, slot_id, active_flag), so one patient holds at most one live seat per slot and the tag
     identifies it exactly: giving a seat back is idempotent.
  4. The appointment is written to RDS (system of record). If that fails, the seat is given back.
  5. AppointmentBooked / AppointmentCancelled / SlotSeatFreed / SlotCancelled go to EventBridge after commit.
"""
import base64
import hashlib
import json
import logging
import os
import random
import re
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation

import boto3
import pymysql
from botocore.exceptions import ClientError

from common import body, client, config, correlation, dbconn, emit, logged, release, response, slot_key_for

log = logging.getLogger()

OPEN, CLOSED, FULL = 'OPEN', 'CLOSED', 'FULL'
CONFIRMED, CANCELLED_BY_PATIENT, CANCELLED_BY_DOCTOR = 'CONFIRMED', 'CANCELLED_BY_PATIENT', 'CANCELLED_BY_DOCTOR'
COMPLETED, NO_SHOW = 'COMPLETED', 'NO_SHOW'
# What the doctor may send to POST /doctor/me/appointments/{id}/status -> the stored status.
DOCTOR_STATUS_TARGETS = {'CONFIRMED': CONFIRMED, 'COMPLETED': COMPLETED, 'NO_SHOW': NO_SHOW,
                         'CANCELLED': CANCELLED_BY_DOCTOR, 'CANCELLED_BY_DOCTOR': CANCELLED_BY_DOCTOR}
EVENT_SOURCE = 'medislot.booking'
METRIC_NAMESPACE = 'MediSlot/Booking'
IDEMPOTENCY_TTL_SECONDS = 24 * 3600
IDEMPOTENCY_LOCK_SECONDS = 30            # a little longer than the Lambda timeout (28 s)
IDEMPOTENCY_WAIT_SECONDS = 6             # a parallel replay waits this long for the first request to finish
CONFIG_TTL_SECONDS = 60                  # SSM policy edits take effect within a minute, no redeploy
DOCTOR_CANCEL_TIER = 'DOCTOR_CANCELLED'

IDEMPOTENCY_KEY = re.compile(r'^[A-Za-z0-9_\-:.]{8,64}$')
SLOT_ID = re.compile(r'^([1-9]\d{0,18})#([1-9]\d{0,18})#(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z)$')
TIER_NAME = re.compile(r'^[A-Z][A-Z_]{1,29}$')

# Used only when the SSM parameter cannot be read. The refund policy has NO fallback on purpose: refunding
# money from a guessed policy is worse than refusing the cancellation for a minute (fail closed).
DEFAULT_LIMITS = {
    'maxUpcomingPerPatient': 3,       # live future bookings one patient may hold
    'minLeadMinutes': 0,              # a slot must start at least this far in the future
    'maxOccAttempts': 10,             # optimistic-concurrency attempts before giving up (503)
    'backoffBaseMs': 15,
    'backoffCapMs': 400,
}
LIMIT_BOUNDS = {'maxUpcomingPerPatient': (1, 50), 'minLeadMinutes': (0, 7 * 24 * 60), 'maxOccAttempts': (1, 25),
                'backoffBaseMs': (1, 1000), 'backoffCapMs': (1, 5000)}


class ApiError(Exception):
    def __init__(self, status, code, message, headers=None, **extra):
        super().__init__(message)
        self.status, self.code, self.message, self.headers, self.extra = status, code, message, headers or {}, extra

    def body(self, cid):
        return {'message': self.message, 'code': self.code, 'correlationId': cid, **self.extra}


class PolicyError(Exception):
    """The refund policy in SSM is missing or invalid."""


# =============================================================================================== small helpers

def clock():
    """Current time in UTC. One function so tests can freeze time at exact cutoff instants."""
    return datetime.now(timezone.utc)


def utc(value):
    """MySQL DATETIME (naive, stored as UTC) -> aware UTC datetime."""
    if value is None:
        return None
    return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)


def naive(value):
    """Aware datetime -> naive UTC for MySQL."""
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def iso(value):
    if value is None:
        return None
    value = utc(value)
    text = value.isoformat(timespec='microseconds' if value.microsecond else 'seconds')
    return text.replace('+00:00', 'Z')


def parse_instant(text):
    return datetime.fromisoformat(str(text).replace('Z', '+00:00')).astimezone(timezone.utc)


def clean(value):
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() and value.as_tuple().exponent >= 0 else str(value)
    if isinstance(value, datetime):
        return iso(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, set):
        return sorted(clean(v) for v in value)
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


def headers_of(event):
    return {str(k).lower(): v for k, v in (event.get('headers') or {}).items()}


def query_params(event):
    return {k: (v or '').strip() for k, v in (event.get('queryStringParameters') or {}).items()}


def positive_id(raw, name):
    raw = str(raw if raw is not None else '').strip()
    if not re.fullmatch(r'[1-9]\d{0,18}', raw):
        raise ApiError(400, 'INVALID_ID', f'{name} must be a positive whole number')
    return int(raw)


def short_ref(appointment_id):
    return f'MS-{int(appointment_id):06d}'


def seat_ref(patient_id):
    """The tag a patient's seat carries in the slot's booking_refs set (one live seat per patient per slot)."""
    return f'p{int(patient_id)}'


def metric(name, value, unit='Count', **dimensions):
    """CloudWatch Embedded Metric Format: one stdout line becomes a metric. No PutMetricData call, no extra IAM.

    print() (not logging) because the Lambda log prefix would stop CloudWatch from parsing the JSON.
    """
    dims = {'Environment': os.environ.get('ENVIRONMENT', 'dev'), **{k: str(v) for k, v in dimensions.items()}}
    print(json.dumps({'_aws': {'Timestamp': int(time.time() * 1000), 'CloudWatchMetrics': [
        {'Namespace': METRIC_NAMESPACE, 'Dimensions': [list(dims)], 'Metrics': [{'Name': name, 'Unit': unit}]}]},
        **dims, name: value}))


# =============================================================================================== AWS handles

_tables = {}


def slots_table():
    if 'slots' not in _tables:
        _tables['slots'] = boto3.resource('dynamodb').Table(os.environ['SLOTS_TABLE'])
    return _tables['slots']


def idempotency_table():
    if 'idem' not in _tables:
        _tables['idem'] = boto3.resource('dynamodb').Table(os.environ['IDEMPOTENCY_TABLE'])
    return _tables['idem']


def events_client():
    return client('events')


def conditional_failed(exc):
    return isinstance(exc, ClientError) and exc.response.get('Error', {}).get('Code') == 'ConditionalCheckFailedException'


# =============================================================================================== SSM configuration

_ssm_cache = {}


def ssm_json(name):
    """(parsed JSON, SSM version) cached for CONFIG_TTL_SECONDS, so the mentor can edit a window and see it apply."""
    hit = _ssm_cache.get(name)
    if hit and time.time() - hit[0] < CONFIG_TTL_SECONDS:
        return hit[1], hit[2]
    param = client('ssm').get_parameter(Name=name)['Parameter']
    value = json.loads(param['Value'])
    _ssm_cache[name] = (time.time(), value, int(param.get('Version') or 0))
    return value, int(param.get('Version') or 0)


def param_prefix():
    return f"/medislot/{os.environ.get('ENVIRONMENT', 'dev')}/booking"


def parse_limits(raw):
    """Merge SSM values over the defaults; ignore unknown keys and out-of-range values (logged)."""
    out = dict(DEFAULT_LIMITS)
    for key, value in (raw or {}).items():
        low_high = LIMIT_BOUNDS.get(key)
        if low_high and type(value) is int and low_high[0] <= value <= low_high[1]:
            out[key] = value
        else:
            emit('warning', 'booking_limit_ignored', None, key=key, value=str(value)[:40])
    if out['backoffCapMs'] < out['backoffBaseMs']:
        out['backoffCapMs'] = out['backoffBaseMs']
    return out


def limits():
    try:
        raw, _ = ssm_json(f'{param_prefix()}/limits')
        return parse_limits(raw if isinstance(raw, dict) else {})
    except Exception as exc:
        emit('warning', 'booking_limits_default', None, errorType=type(exc).__name__)
        return dict(DEFAULT_LIMITS)


def hours_to_delta(hours):
    """Exact (Decimal, microsecond) conversion: 24 -> 24 h, 0.5 -> 30 min. No float rounding at the cutoff."""
    return timedelta(microseconds=int(Decimal(str(hours)) * 3600 * 1000000))


def parse_policy(raw, ssm_version=0):
    """Validate the refund policy JSON from SSM. Raises PolicyError when anything is off (fail closed).

    {"version": "2026-11-v1", "tiers": [
        {"tier": "FULL",    "minHoursBefore": 24, "refundPercent": 100},
        {"tier": "PARTIAL", "minHoursBefore": 6,  "refundPercent": 50},
        {"tier": "LATE",    "minHoursBefore": 0,  "refundPercent": 0}]}

    A tier applies when (start - cancellation instant) >= minHoursBefore. Exactly at the cutoff instant the
    HIGHER tier still applies; one microsecond later the next tier does.
    """
    if not isinstance(raw, dict) or not isinstance(raw.get('tiers'), list) or not 1 <= len(raw['tiers']) <= 8:
        raise PolicyError('policy needs a "tiers" list with 1 to 8 entries')
    tiers, names = [], set()
    for t in raw['tiers']:
        if not isinstance(t, dict):
            raise PolicyError('every tier must be an object')
        name, hours, pct = t.get('tier'), t.get('minHoursBefore'), t.get('refundPercent')
        if not isinstance(name, str) or not TIER_NAME.match(name) or name in names or name == DOCTOR_CANCEL_TIER:
            raise PolicyError(f'tier name {name!r} is invalid or repeated')
        if isinstance(hours, bool) or not isinstance(hours, (int, float)) or not 0 <= hours <= 24 * 60:
            raise PolicyError(f'{name}: minHoursBefore must be a number between 0 and 1440')
        if type(pct) is not int or not 0 <= pct <= 100:
            raise PolicyError(f'{name}: refundPercent must be a whole number 0..100')
        names.add(name)
        tiers.append({'tier': name, 'minHoursBefore': hours, 'refundPercent': pct, 'lead': hours_to_delta(hours)})
    tiers.sort(key=lambda t: t['lead'], reverse=True)
    for a, b in zip(tiers, tiers[1:]):
        if a['lead'] == b['lead']:
            raise PolicyError('two tiers share the same minHoursBefore')
        if b['refundPercent'] > a['refundPercent']:
            raise PolicyError('a later cancellation can never refund more than an earlier one')
    if tiers[-1]['lead'] != timedelta(0):
        raise PolicyError('the last tier must have minHoursBefore 0 so every cancellation has a tier')
    label = str(raw.get('version') or 'unversioned')[:40]
    return {'version': f'{label}@ssm-v{ssm_version}', 'currency': str(raw.get('currency') or 'INR')[:3],
            'tiers': tiers}


def refund_policy():
    try:
        raw, version = ssm_json(f'{param_prefix()}/refund-policy')
    except Exception as exc:
        raise PolicyError(f'could not read the refund policy from SSM ({type(exc).__name__})')
    return parse_policy(raw, version)


# =============================================================================================== refund maths (pure)

def refund_tier(policy, starts_at, cancelled_at):
    """The tier for a cancellation at `cancelled_at`, or None once the appointment has started."""
    lead = starts_at - cancelled_at
    if lead <= timedelta(0):
        return None
    for tier in policy['tiers']:
        if lead >= tier['lead']:
            return tier
    return None                                       # unreachable: the last tier is 0 h


def refund_amount(paid, percent):
    """Whole paise, rounded half-up, never more than was paid and never negative."""
    paid = Decimal(str(paid)).quantize(Decimal('0.01'))
    paise = int(paid * 100)
    refund_paise = int((Decimal(paise) * Decimal(int(percent)) / Decimal(100)).quantize(Decimal('1'), ROUND_HALF_UP))
    refund_paise = max(0, min(paise, refund_paise))
    return (Decimal(refund_paise) / Decimal(100)).quantize(Decimal('0.01'))


def refund_schedule(policy, starts_at, paid):
    """Every tier with the LAST instant it can still be claimed (inclusive). Used by quotes and the dashboard."""
    out = []
    for tier in policy['tiers']:
        until = starts_at - tier['lead']
        out.append({'tier': tier['tier'], 'refundPercent': tier['refundPercent'],
                    'refundAmount': money(refund_amount(paid, tier['refundPercent'])),
                    'cancelBy': iso(until) if tier['lead'] > timedelta(0) else None,
                    'cancelBefore': iso(starts_at) if tier['lead'] == timedelta(0) else None})
    return out


def cancellation_preview(policy, starts_at, paid, now):
    tier = refund_tier(policy, starts_at, now)
    if tier is None:
        return {'cancellable': False, 'reason': 'The appointment has already started.'}
    return {'cancellable': True, 'tier': tier['tier'], 'refundPercent': tier['refundPercent'],
            'refundAmount': money(refund_amount(paid, tier['refundPercent'])),
            'leadSeconds': int((starts_at - now).total_seconds()), 'policyVersion': policy['version'],
            'schedule': refund_schedule(policy, starts_at, paid)}


# =============================================================================================== seats (DynamoDB OCC)

def backoff_delay(attempt, base_ms, cap_ms, rng=random.random):
    """Full-jitter exponential backoff (AWS Architecture Blog): uniform(0, min(cap, base * 2^(attempt-1)))."""
    ceiling = min(cap_ms, base_ms * (2 ** (attempt - 1)))
    return rng() * ceiling / 1000.0


def seat_problem(item, now, min_lead_minutes=0):
    """ApiError explaining why this slot cannot take one more seat right now, or None."""
    try:
        start = parse_instant(item['start_at'])
    except (KeyError, ValueError):
        return ApiError(409, 'SLOT_INVALID', 'This slot is not bookable.')
    if start <= now + timedelta(minutes=min_lead_minutes):
        return ApiError(422, 'SLOT_IN_PAST', 'This slot has already started or is too close to its start time.',
                        startsAt=item.get('start_at'))
    if item.get('status') == CLOSED or item.get('manual_block'):
        return ApiError(409, 'SLOT_CLOSED', 'The doctor is not taking bookings for this slot.')
    capacity = int(item.get('capacity', 1) or 1)
    taken = int(item.get('booked_count', 0) or 0) + int(item.get('held_count', 0) or 0)
    if taken >= capacity or item.get('status') == FULL:
        return ApiError(409, 'SLOT_FULL', 'This slot is fully booked.', capacity=capacity, booked=taken)
    return None


def doctor_status_problem(row, target, now):
    """ApiError when the doctor may not move this appointment (a dict with status + slot_start_utc) to `target`."""
    if not row:
        return ApiError(404, 'APPOINTMENT_NOT_FOUND', 'Appointment not found.')
    current, started = row['status'], utc(row['slot_start_utc']) <= now
    if current == target:
        return None
    if current in (CANCELLED_BY_PATIENT, CANCELLED_BY_DOCTOR):
        return ApiError(409, 'APPOINTMENT_CANCELLED', 'This appointment is cancelled and the patient has been refunded, '
                                                      'so its status can no longer be changed.')
    if target in (COMPLETED, NO_SHOW) and not started:
        word = 'completed' if target == COMPLETED else 'a no-show'
        return ApiError(422, 'APPOINTMENT_NOT_STARTED', f'An appointment can be marked {word} once it has started.')
    if target == CANCELLED_BY_DOCTOR:
        if current != CONFIRMED:
            return ApiError(409, 'NOT_CANCELLABLE', 'Only a confirmed appointment can be cancelled.')
        if started:
            return ApiError(422, 'APPOINTMENT_STARTED', 'This appointment has already started and can no longer be '
                                                        'cancelled. Mark it completed instead.')
    return None


def read_slot(table, slot_id):
    """Strongly consistent read of one slot. The table is partitioned by doctor, so the key is (doctor_id, slot_id)."""
    try:
        key = slot_key_for(slot_id)
    except ValueError:
        return None                                   # not a real slot id: same answer as a missing slot
    return table.get_item(Key=key, ConsistentRead=True).get('Item')


def reserve_seat(table, slot_id, ref, now, lim, stats, sleep=None, rng=None):
    """Take one seat with optimistic concurrency. Returns {'version', 'attempts', 'retries', 'capacity', ...}.

    stats['retries'] is updated as we go, so the caller can publish BookingConflictRetries even when we end in
    409 SLOT_FULL. Never blocks other slots or other patients: there is no lock, only a version check.
    """
    sleep, rng = sleep or time.sleep, rng or random.random
    attempts = int(lim['maxOccAttempts'])
    for attempt in range(1, attempts + 1):
        stats['attempts'] = attempt
        item = read_slot(table, slot_id)
        if not item:
            raise ApiError(404, 'SLOT_NOT_FOUND', 'Slot not found.')
        if ref in (item.get('booking_refs') or set()):
            # This patient already holds a seat here (a live booking, one still being written, or one whose
            # cancellation is still releasing it). One seat per patient per slot.
            raise ApiError(409, 'ALREADY_BOOKED', 'You already have a seat in this slot (or a booking for it is '
                                                  'still being processed).')
        problem = seat_problem(item, now, lim['minLeadMinutes'])
        if problem:
            raise problem
        capacity = int(item.get('capacity', 1) or 1)
        booked, held = int(item.get('booked_count', 0) or 0), int(item.get('held_count', 0) or 0)
        has_version = 'version' in item
        seen = int(item.get('version', 0) or 0)
        values = {':one': 1, ':zero': 0, ':next': seen + 1, ':open': OPEN, ':limit': capacity - held,
                  ':status': FULL if booked + 1 + held >= capacity else OPEN, ':ref': ref, ':refset': {ref},
                  ':now': iso(now)}
        if has_version:
            values[':seen'] = seen
        try:
            table.update_item(
                Key=slot_key_for(slot_id),
                UpdateExpression='SET booked_count = if_not_exists(booked_count, :zero) + :one, version = :next, '
                                 '#s = :status, last_booked_at = :now ADD booking_refs :refset',
                ConditionExpression=('version = :seen' if has_version else 'attribute_not_exists(version)') +
                                    ' AND #s = :open AND attribute_not_exists(manual_block)'
                                    ' AND (attribute_not_exists(booked_count) OR booked_count < :limit)'
                                    ' AND (attribute_not_exists(booking_refs) OR NOT contains(booking_refs, :ref))',
                ExpressionAttributeNames={'#s': 'status'}, ExpressionAttributeValues=values)
            return {'version': seen + 1, 'attempts': attempt, 'retries': stats['retries'], 'capacity': capacity,
                    'bookedAfter': booked + 1, 'alreadyHeld': False, 'item': item}
        except ClientError as exc:
            if not conditional_failed(exc):
                raise
            stats['retries'] += 1                     # somebody else changed the slot between our read and write
            if attempt < attempts:
                sleep(backoff_delay(attempt, lim['backoffBaseMs'], lim['backoffCapMs'], rng))
    stats['exhausted'] = True
    raise ApiError(503, 'BOOKING_CONTENTION', 'This slot is in very high demand right now. Please try again.',
                   headers={'Retry-After': '1'}, attempts=attempts)


def release_seat(table, slot_id, ref, max_attempts=8, sleep=None):
    """Give a seat back. Idempotent: does nothing when the booking_ref is no longer on the slot. True = released."""
    sleep = sleep or time.sleep
    for attempt in range(1, max_attempts + 1):
        item = read_slot(table, slot_id)
        if not item or ref not in (item.get('booking_refs') or set()):
            return False
        seen = int(item.get('version', 0) or 0)
        stays_closed = item.get('status') == CLOSED or item.get('manual_block')
        values = {':one': 1, ':next': seen + 1, ':ref': ref, ':refset': {ref},
                  ':status': CLOSED if stays_closed else OPEN}      # DynamoDB rejects unused values: none here
        guard = 'attribute_not_exists(version)'
        if 'version' in item:
            values[':seen'], guard = seen, 'version = :seen'
        try:
            table.update_item(
                Key=slot_key_for(slot_id),
                UpdateExpression='SET booked_count = if_not_exists(booked_count, :one) - :one, version = :next, '
                                 '#s = :status DELETE booking_refs :refset',
                ConditionExpression=guard + ' AND contains(booking_refs, :ref)',
                ExpressionAttributeNames={'#s': 'status'}, ExpressionAttributeValues=values)
            return True
        except ClientError as exc:
            if not conditional_failed(exc):
                raise
            if attempt < max_attempts:
                sleep(backoff_delay(attempt, 15, 300))
    raise RuntimeError(f'could not release seat {ref} on {slot_id} after {max_attempts} attempts')


def close_slot(table, slot_id, now, max_attempts=8, sleep=None):
    """Doctor cancellation: CLOSED + manual_block, with the same version check. Returns the item as read."""
    sleep = sleep or time.sleep
    for attempt in range(1, max_attempts + 1):
        item = read_slot(table, slot_id)
        if not item:
            raise ApiError(404, 'SLOT_NOT_FOUND', 'Slot not found.')
        seen = int(item.get('version', 0) or 0)
        values = {':closed': CLOSED, ':t': True, ':next': seen + 1, ':now': iso(now)}
        guard = 'attribute_not_exists(version)'
        if 'version' in item:
            values[':seen'], guard = seen, 'version = :seen'
        try:
            table.update_item(
                Key=slot_key_for(slot_id),
                UpdateExpression='SET #s = :closed, manual_block = :t, cancelled_by_doctor = :t, cancelled_at = :now, '
                                 'version = :next',
                ConditionExpression=guard, ExpressionAttributeNames={'#s': 'status'}, ExpressionAttributeValues=values)
            return item
        except ClientError as exc:
            if not conditional_failed(exc):
                raise
            if attempt < max_attempts:
                sleep(backoff_delay(attempt, 15, 300))
    raise ApiError(503, 'BOOKING_CONTENTION', 'The slot is busy right now. Please try again.', headers={'Retry-After': '1'})


# =============================================================================================== idempotency store

class Idempotency:
    """DynamoDB Idempotency table. Key = booking#<patientId>#<Idempotency-Key>, so keys never collide across users.

    claim() returns one of
      ('new', item)       first time we see the key: this request owns it
      ('replay', item)    finished before: return the stored response unchanged
      ('wait', item)      another request with this key is running right now
      ('takeover', item)  a previous attempt died mid-way (lock expired): continue it with its booking_ref
    """

    def __init__(self, table):
        self.table = table

    def claim(self, key, request_hash, booking_ref, now_epoch):
        for _ in range(2):
            item = {'idempotency_key': key, 'status': 'IN_PROGRESS', 'request_hash': request_hash,
                    'booking_ref': booking_ref, 'created_at': int(now_epoch),
                    'lock_expires_at': int(now_epoch) + IDEMPOTENCY_LOCK_SECONDS,
                    'expires_at': int(now_epoch) + IDEMPOTENCY_TTL_SECONDS}
            try:
                self.table.put_item(Item=item, ConditionExpression='attribute_not_exists(idempotency_key)')
                return 'new', item
            except ClientError as exc:
                if not conditional_failed(exc):
                    raise
            found = self.table.get_item(Key={'idempotency_key': key}, ConsistentRead=True).get('Item')
            if not found:
                continue                              # expired and removed between our two calls: claim again
            if found.get('request_hash') != request_hash:
                raise ApiError(422, 'IDEMPOTENCY_KEY_REUSED',
                               'This Idempotency-Key was already used for a different booking request. '
                               'Use a new key for a new request.')
            if found.get('status') == 'COMPLETED':
                return 'replay', found
            if int(found.get('lock_expires_at', 0)) > now_epoch:
                return 'wait', found
            try:
                self.table.update_item(
                    Key={'idempotency_key': key}, UpdateExpression='SET lock_expires_at = :new',
                    ConditionExpression='#s = :progress AND lock_expires_at = :old',
                    ExpressionAttributeNames={'#s': 'status'},
                    ExpressionAttributeValues={':new': int(now_epoch) + IDEMPOTENCY_LOCK_SECONDS, ':progress': 'IN_PROGRESS',
                                               ':old': found['lock_expires_at']})
                return 'takeover', found
            except ClientError as exc:
                if not conditional_failed(exc):
                    raise
                return 'wait', found
        raise ApiError(409, 'IDEMPOTENCY_IN_PROGRESS', 'A request with this Idempotency-Key is being processed.',
                       headers={'Retry-After': '2'})

    def wait(self, key, timeout=IDEMPOTENCY_WAIT_SECONDS, poll=0.15, sleep=None):
        sleep = sleep or time.sleep
        deadline = time.time() + timeout
        while time.time() < deadline:
            sleep(poll)
            found = self.table.get_item(Key={'idempotency_key': key}, ConsistentRead=True).get('Item')
            if not found or found.get('status') == 'COMPLETED':
                return found
        return None

    def record_seat(self, key, version, attempts):
        """Remember which slot version the seat was won at, so a takeover can reuse it safely."""
        self.table.update_item(Key={'idempotency_key': key}, UpdateExpression='SET slot_version = :v, occ_attempts = :a',
                               ExpressionAttributeValues={':v': int(version), ':a': int(attempts)})

    def complete(self, key, status, payload):
        self.table.update_item(
            Key={'idempotency_key': key},
            UpdateExpression='SET #s = :done, response_status = :code, response_body = :body REMOVE lock_expires_at',
            ExpressionAttributeNames={'#s': 'status'},
            ExpressionAttributeValues={':done': 'COMPLETED', ':code': int(status),
                                       ':body': json.dumps(clean(payload), separators=(',', ':'))})

    def forget(self, key):
        """A server-side failure must not be replayed forever: drop the claim so the client can retry."""
        try:
            self.table.delete_item(Key={'idempotency_key': key}, ConditionExpression='#s = :progress',
                                   ExpressionAttributeNames={'#s': 'status'},
                                   ExpressionAttributeValues={':progress': 'IN_PROGRESS'})
        except ClientError as exc:
            if not conditional_failed(exc):
                log.exception('idempotency_forget_failed')


def request_hash(slot_id):
    canonical = json.dumps({'slotId': slot_id}, sort_keys=True, separators=(',', ':'))
    return hashlib.sha256(canonical.encode()).hexdigest()


# =============================================================================================== RDS store

APPOINTMENT_SELECT = (
    'SELECT a.appointment_id, a.patient_id, a.doctor_id, a.consultation_type_id, a.slot_id, a.slot_start_utc, '
    'a.slot_end_utc, a.status, a.active_flag, a.reschedule_count, a.fee_amount, a.created_at, a.updated_at, '
    'd.first_name AS doctor_first, d.last_name AS doctor_last, d.specialization, d.timezone, '
    'h.name AS hospital_name, h.city, c.code AS type_code, c.label AS type_label, c.duration_min, '
    'p.first_name AS patient_first, p.last_name AS patient_last, '
    'r.refund_id, r.reason AS refund_reason, r.tier, r.refund_percent, r.fee_amount AS refund_fee, r.refund_amount, '
    'r.created_at AS refunded_at '
    'FROM appointments a '
    'JOIN doctors d ON d.user_id = a.doctor_id '
    'JOIN hospitals h ON h.hospital_id = d.hospital_id '
    'JOIN consultation_types t ON t.consultation_type_id = a.consultation_type_id '
    'JOIN consultations c ON c.consultation_id = t.consultation_id '
    'LEFT JOIN patients p ON p.user_id = a.patient_id '
    'LEFT JOIN refund_ledger r ON r.appointment_id = a.appointment_id ')


def zone(name):
    from zoneinfo import ZoneInfo
    try:
        return ZoneInfo(name or 'Asia/Kolkata')
    except Exception:
        return ZoneInfo('Asia/Kolkata')


def local_day_bounds(day, tz_name):
    """[start, end) of a local calendar day as UTC instants (DST-safe)."""
    tz = zone(tz_name)
    start = datetime.combine(day, datetime.min.time()).replace(tzinfo=tz).astimezone(timezone.utc)
    end = datetime.combine(day + timedelta(days=1), datetime.min.time()).replace(tzinfo=tz).astimezone(timezone.utc)
    return start, end


class Store:
    """Every SQL statement of the booking engine. Tests replace it with an in-memory store."""

    def __init__(self, conn):
        self.conn = conn

    # ------------------------------------------------------------------ reads
    def patient(self, patient_id):
        with self.conn.cursor() as cur:
            cur.execute("SELECT u.user_id, u.is_active, p.user_id AS profile_id, p.first_name, p.last_name "
                        "FROM users u LEFT JOIN patients p ON p.user_id = u.user_id "
                        "WHERE u.user_id = %s AND u.role = 'patient'", (patient_id,))
            return cur.fetchone()

    def offer(self, doctor_id, type_id):
        with self.conn.cursor() as cur:
            cur.execute('SELECT t.consultation_type_id, t.fee_amount, t.is_active AS type_active, c.code, c.label, '
                        'c.duration_min, d.first_name, d.last_name, d.timezone, u.is_active AS doctor_active '
                        'FROM consultation_types t JOIN consultations c ON c.consultation_id = t.consultation_id '
                        'JOIN doctors d ON d.user_id = t.doctor_id JOIN users u ON u.user_id = d.user_id '
                        'WHERE t.consultation_type_id = %s AND t.doctor_id = %s', (type_id, doctor_id))
            return cur.fetchone()

    def doctor_timezone(self, doctor_id):
        with self.conn.cursor() as cur:
            cur.execute('SELECT timezone FROM doctors WHERE user_id = %s', (doctor_id,))
            return (cur.fetchone() or {}).get('timezone') or 'Asia/Kolkata'

    def active_for(self, patient_id, slot_id):
        with self.conn.cursor() as cur:
            cur.execute(APPOINTMENT_SELECT + 'WHERE a.patient_id = %s AND a.slot_id = %s AND a.active_flag = 1',
                        (patient_id, slot_id))
            return cur.fetchone()

    def appointment(self, patient_id, appointment_id):
        with self.conn.cursor() as cur:
            cur.execute(APPOINTMENT_SELECT + 'WHERE a.appointment_id = %s AND a.patient_id = %s',
                        (appointment_id, patient_id))
            return cur.fetchone()

    def list_appointments(self, patient_id, view, now, limit, after):
        where, args = ['a.patient_id = %s'], [patient_id]
        ascending = view == 'upcoming'
        if view == 'upcoming':
            where.append("a.status = 'CONFIRMED' AND a.slot_start_utc > %s")
            args.append(naive(now))
        elif view == 'past':
            where.append("a.status IN ('CONFIRMED', 'COMPLETED', 'NO_SHOW') AND a.slot_start_utc <= %s")
            args.append(naive(now))
        elif view == 'cancelled':
            where.append("a.status IN ('CANCELLED_BY_PATIENT', 'CANCELLED_BY_DOCTOR')")
        if after:
            start, ident = after
            op = '>' if ascending else '<'
            where.append(f'(a.slot_start_utc {op} %s OR (a.slot_start_utc = %s AND a.appointment_id {op} %s))')
            args += [start, start, ident]
        order = 'ASC' if ascending else 'DESC'
        with self.conn.cursor() as cur:
            cur.execute(APPOINTMENT_SELECT + 'WHERE ' + ' AND '.join(where) +
                        f' ORDER BY a.slot_start_utc {order}, a.appointment_id {order} LIMIT %s', args + [limit + 1])
            return cur.fetchall()

    def refunds(self, patient_id):
        with self.conn.cursor() as cur:
            cur.execute('SELECT r.refund_id, r.appointment_id, r.reason, r.tier, r.refund_percent, r.fee_amount, '
                        'r.refund_amount, r.created_at, a.slot_start_utc, a.slot_id, d.first_name AS doctor_first, '
                        'd.last_name AS doctor_last, d.timezone, c.label AS type_label FROM refund_ledger r '
                        'JOIN appointments a ON a.appointment_id = r.appointment_id '
                        'JOIN doctors d ON d.user_id = a.doctor_id '
                        'JOIN consultation_types t ON t.consultation_type_id = a.consultation_type_id '
                        'JOIN consultations c ON c.consultation_id = t.consultation_id '
                        'WHERE r.patient_id = %s ORDER BY r.created_at DESC, r.refund_id DESC LIMIT 200', (patient_id,))
            return cur.fetchall()

    def doctor_appointments(self, doctor_id, start_utc, end_utc, include_cancelled):
        status = '' if include_cancelled else "AND a.status = 'CONFIRMED' "
        with self.conn.cursor() as cur:
            cur.execute(APPOINTMENT_SELECT + 'WHERE a.doctor_id = %s AND a.slot_start_utc >= %s AND a.slot_start_utc < %s '
                        + status + 'ORDER BY a.slot_start_utc, a.appointment_id',
                        (doctor_id, naive(start_utc), naive(end_utc)))
            return cur.fetchall()

    def limit_problem(self, cur, booking, lim, now):
        """The first patient-limit rule this booking would break, as an ApiError, or None."""
        day_start, day_end = local_day_bounds(booking['local_date'], booking['timezone'])
        cur.execute("SELECT "
                    "COALESCE(SUM(a.slot_id = %s), 0) AS same_slot, "
                    "COALESCE(SUM(a.doctor_id = %s AND a.slot_start_utc >= %s AND a.slot_start_utc < %s), 0) AS same_doctor_day, "
                    "COALESCE(SUM(a.slot_start_utc > %s), 0) AS upcoming, "
                    "COALESCE(SUM(a.slot_start_utc < %s AND a.slot_end_utc > %s), 0) AS overlapping "
                    "FROM appointments a WHERE a.patient_id = %s AND a.active_flag = 1 "
                    "AND (a.slot_end_utc > %s OR (a.slot_start_utc >= %s AND a.slot_start_utc < %s))",
                    (booking['slot_id'], booking['doctor_id'], naive(day_start), naive(day_end), naive(now),
                     naive(booking['ends_at']), naive(booking['starts_at']), booking['patient_id'], naive(now),
                     naive(day_start), naive(day_end)))
        row = cur.fetchone() or {}
        if int(row.get('same_slot') or 0):
            return ApiError(409, 'ALREADY_BOOKED', 'You already have a seat in this slot.')
        if int(row.get('same_doctor_day') or 0):
            return ApiError(422, 'ONE_PER_DOCTOR_PER_DAY',
                            f'You already have an appointment with this doctor on {booking["local_date"]}.')
        if int(row.get('overlapping') or 0):
            return ApiError(422, 'OVERLAPPING_APPOINTMENT', 'You already have another appointment at this time.')
        if int(row.get('upcoming') or 0) >= lim['maxUpcomingPerPatient']:
            return ApiError(422, 'PATIENT_LIMIT_REACHED',
                            f'You already have {lim["maxUpcomingPerPatient"]} upcoming appointments (the maximum).')
        return None

    def precheck(self, booking, lim, now):
        with self.conn.cursor() as cur:
            problem = self.limit_problem(cur, booking, lim, now)
        self.conn.rollback()                         # end the read snapshot; nothing was written
        return problem

    # ------------------------------------------------------------------ writes (each one transaction)
    def confirm(self, booking, lim, now, slot_cancelled):
        """Write the appointment. `slot_cancelled()` re-reads the slot; raises ApiError when a rule now fails."""
        self.conn.rollback()                         # fresh transaction: its snapshot starts after the locks below
        with self.conn.cursor() as cur:
            # Serialises bookings of ONE patient only (their limits); other patients never wait here.
            cur.execute('SELECT user_id FROM users WHERE user_id = %s AND is_active = TRUE FOR UPDATE',
                        (booking['patient_id'],))
            if not cur.fetchone():
                raise ApiError(403, 'ACCOUNT_INACTIVE', 'This account is closed.')
            # SHARED lock on the doctor's row: any number of bookings hold it together. A doctor cancelling a
            # slot takes it EXCLUSIVELY, so a booking and that cancellation can never miss each other.
            cur.execute('SELECT user_id FROM doctors WHERE user_id = %s LOCK IN SHARE MODE', (booking['doctor_id'],))
            if slot_cancelled():
                raise ApiError(409, 'SLOT_CANCELLED', 'The doctor has cancelled this slot.')
            problem = self.limit_problem(cur, booking, lim, now)
            if problem:
                raise problem
            cur.execute('INSERT INTO appointments (patient_id, doctor_id, consultation_type_id, slot_id, slot_start_utc, '
                        "slot_end_utc, status, active_flag, fee_amount) VALUES (%s, %s, %s, %s, %s, %s, 'CONFIRMED', 1, %s)",
                        (booking['patient_id'], booking['doctor_id'], booking['consultation_type_id'],
                         booking['slot_id'], naive(booking['starts_at']), naive(booking['ends_at']), str(booking['fee'])))
            appointment_id = cur.lastrowid
        self.conn.commit()
        return appointment_id

    def cancel_by_patient(self, patient_id, appointment_id, now, policy):
        """Returns (row_before, refund dict or None, already_cancelled)."""
        self.conn.rollback()
        with self.conn.cursor() as cur:
            cur.execute('SELECT appointment_id, patient_id, doctor_id, status, slot_id, slot_start_utc, fee_amount '
                        'FROM appointments WHERE appointment_id = %s AND patient_id = %s FOR UPDATE',
                        (appointment_id, patient_id))
            row = cur.fetchone()
            if not row:
                raise ApiError(404, 'APPOINTMENT_NOT_FOUND', 'Appointment not found.')
            if row['status'] != CONFIRMED:
                self.conn.rollback()
                return row, None, True
            tier = refund_tier(policy, utc(row['slot_start_utc']), now)
            if tier is None:
                raise ApiError(422, 'APPOINTMENT_STARTED', 'This appointment has already started and can no longer '
                                                           'be cancelled.')
            amount = refund_amount(row['fee_amount'], tier['refundPercent'])
            cur.execute("UPDATE appointments SET status = 'CANCELLED_BY_PATIENT', active_flag = NULL "
                        "WHERE appointment_id = %s AND status = 'CONFIRMED'", (appointment_id,))
            cur.execute("INSERT INTO refund_ledger (appointment_id, patient_id, reason, tier, refund_percent, fee_amount, "
                        "refund_amount) VALUES (%s, %s, 'PATIENT_CANCEL', %s, %s, %s, %s)",
                        (appointment_id, patient_id, tier['tier'], tier['refundPercent'], str(row['fee_amount']), str(amount)))
        self.conn.commit()
        metric('CancellationsByTier', 1, Tier=tier['tier'])
        metric('RefundAmount', float(amount), 'None', Tier=tier['tier'])
        return row, {'tier': tier['tier'], 'refundPercent': tier['refundPercent'], 'refundAmount': money(amount)}, False

    def cancel_slot(self, doctor_id, slot_id, now):
        """Doctor cancelled a slot: every live booking -> CANCELLED_BY_DOCTOR + 100% refund. Returns the rows."""
        self.conn.rollback()
        with self.conn.cursor() as cur:
            # Exclusive: waits for bookings of this doctor that are mid-way, then blocks new ones until COMMIT.
            cur.execute('SELECT user_id FROM doctors WHERE user_id = %s FOR UPDATE', (doctor_id,))
            cur.execute("SELECT appointment_id, patient_id, doctor_id, slot_id, slot_start_utc, fee_amount FROM appointments "
                        "WHERE slot_id = %s AND doctor_id = %s AND status = 'CONFIRMED' FOR UPDATE", (slot_id, doctor_id))
            rows = cur.fetchall()
            for row in rows:
                amount = refund_amount(row['fee_amount'], 100)
                row['refundAmount'] = money(amount)
                cur.execute("UPDATE appointments SET status = 'CANCELLED_BY_DOCTOR', active_flag = NULL "
                            "WHERE appointment_id = %s AND status = 'CONFIRMED'", (row['appointment_id'],))
                cur.execute("INSERT INTO refund_ledger (appointment_id, patient_id, reason, tier, refund_percent, "
                            "fee_amount, refund_amount) VALUES (%s, %s, 'DOCTOR_CANCEL', %s, 100, %s, %s)",
                            (row['appointment_id'], row['patient_id'], DOCTOR_CANCEL_TIER, str(row['fee_amount']),
                             str(amount)))
        self.conn.commit()
        if rows:
            metric('CancellationsByTier', len(rows), Tier=DOCTOR_CANCEL_TIER)
        return rows

    def doctor_appointment(self, doctor_id, appointment_id):
        with self.conn.cursor() as cur:
            cur.execute(APPOINTMENT_SELECT + 'WHERE a.appointment_id = %s AND a.doctor_id = %s',
                        (appointment_id, doctor_id))
            row = cur.fetchone()
        self.conn.rollback()
        return row

    def doctor_booked_slots(self, doctor_id, start_utc, end_utc):
        """Distinct slots of this doctor in [start, end) that still hold at least one CONFIRMED appointment."""
        with self.conn.cursor() as cur:
            cur.execute("SELECT slot_id, MIN(slot_start_utc) AS slot_start_utc FROM appointments "
                        "WHERE doctor_id = %s AND status = 'CONFIRMED' AND slot_start_utc >= %s AND slot_start_utc < %s "
                        "GROUP BY slot_id ORDER BY MIN(slot_start_utc)", (doctor_id, naive(start_utc), naive(end_utc)))
            rows = cur.fetchall()
        self.conn.rollback()
        return rows

    def set_status_by_doctor(self, doctor_id, appointment_id, target, now):
        """Doctor changes one appointment's status. Returns (row_before, refund dict or None, changed).

        CONFIRMED -> COMPLETED / NO_SHOW  only once the appointment has started (no-show: no refund, seat kept)
        COMPLETED / NO_SHOW -> any of CONFIRMED, COMPLETED, NO_SHOW   to fix a mistaken choice
        CONFIRMED -> CANCELLED_BY_DOCTOR  only before it starts; the patient is refunded 100%
        Cancelled appointments are final (the refund is already in the ledger).
        """
        self.conn.rollback()
        with self.conn.cursor() as cur:
            cur.execute('SELECT appointment_id, patient_id, doctor_id, status, slot_id, slot_start_utc, fee_amount '
                        'FROM appointments WHERE appointment_id = %s AND doctor_id = %s FOR UPDATE',
                        (appointment_id, doctor_id))
            row = cur.fetchone()
            current, refund = (row or {}).get('status'), None
            problem = doctor_status_problem(row, target, now)
            if problem:
                self.conn.rollback()
                raise problem
            if current == target:
                self.conn.rollback()
                return row, None, False
            if target in (COMPLETED, NO_SHOW):
                cur.execute("UPDATE appointments SET status = %s, active_flag = NULL "
                            "WHERE appointment_id = %s AND status = %s", (target, appointment_id, current))
            elif target == CONFIRMED:
                cur.execute("UPDATE appointments SET status = 'CONFIRMED', active_flag = 1 "
                            "WHERE appointment_id = %s AND status = %s", (appointment_id, current))
            else:
                amount = refund_amount(row['fee_amount'], 100)
                cur.execute("UPDATE appointments SET status = 'CANCELLED_BY_DOCTOR', active_flag = NULL "
                            "WHERE appointment_id = %s AND status = 'CONFIRMED'", (appointment_id,))
                cur.execute("INSERT INTO refund_ledger (appointment_id, patient_id, reason, tier, refund_percent, "
                            "fee_amount, refund_amount) VALUES (%s, %s, 'DOCTOR_CANCEL', %s, 100, %s, %s)",
                            (appointment_id, row['patient_id'], DOCTOR_CANCEL_TIER, str(row['fee_amount']), str(amount)))
                refund = {'tier': DOCTOR_CANCEL_TIER, 'refundPercent': 100, 'refundAmount': money(amount)}
        self.conn.commit()
        if refund:
            metric('CancellationsByTier', 1, Tier=DOCTOR_CANCEL_TIER)
            metric('RefundAmount', float(refund['refundAmount']), 'None', Tier=DOCTOR_CANCEL_TIER)
        return row, refund, True

    def unreleased_cancellations(self, now, limit=200):
        """Cancelled appointments of future slots, where the patient holds no newer live booking of that slot."""
        with self.conn.cursor() as cur:
            cur.execute("SELECT a.appointment_id, a.patient_id, a.doctor_id, a.slot_id, a.slot_start_utc, a.status "
                        "FROM appointments a WHERE a.status IN ('CANCELLED_BY_PATIENT', 'CANCELLED_BY_DOCTOR') "
                        "AND a.updated_at > %s AND a.slot_start_utc > %s AND NOT EXISTS (SELECT 1 FROM appointments b "
                        "WHERE b.patient_id = a.patient_id AND b.slot_id = a.slot_id AND b.active_flag = 1) "
                        "ORDER BY a.updated_at LIMIT %s", (naive(now - timedelta(days=2)), naive(now), limit))
            rows = cur.fetchall()
        self.conn.rollback()
        return rows

    def slot_rows(self, slot_id):
        with self.conn.cursor() as cur:
            cur.execute('SELECT appointment_id, patient_id, status FROM appointments WHERE slot_id = %s', (slot_id,))
            rows = cur.fetchall()
        self.conn.rollback()
        return rows


def make_store(conn):
    return Store(conn)


# =============================================================================================== events

def publish(events, attempts=3, sleep=None):
    """Send [(detail_type, detail)] to EventBridge after the change is committed. Never fails the request:
    after `attempts` tries the full event is written to the log (EventPublishFailures metric) for replay."""
    if not events:
        return 0
    sleep = sleep or time.sleep
    bus = os.environ.get('EVENT_BUS_NAME', 'default')
    pending = [(t, dict(d, eventId=uuid.uuid4().hex)) for t, d in events]
    sent = 0
    for attempt in range(1, attempts + 1):
        failed = []
        for i in range(0, len(pending), 10):                 # PutEvents takes at most 10 entries
            batch = pending[i:i + 10]
            try:
                result = events_client().put_events(Entries=[
                    {'Source': EVENT_SOURCE, 'DetailType': t, 'EventBusName': bus, 'Resources': [],
                     'Detail': json.dumps(clean(d))} for t, d in batch])
                for item, entry in zip(batch, result.get('Entries', [])):
                    if entry.get('EventId') and not entry.get('ErrorCode'):
                        sent += 1
                        emit('info', 'event_published', None, detailType=item[0], eventId=item[1]['eventId'])
                    else:
                        failed.append(item)
            except Exception:
                failed.extend(batch)
        pending = failed
        if not pending:
            return sent
        if attempt < attempts:
            sleep(0.1 * attempt)
    metric('EventPublishFailures', len(pending))
    for t, d in pending:
        emit('error', 'event_publish_failed', None, detailType=t, detail=json.dumps(clean(d)))
    return sent


def appointment_event(kind, row, **extra):
    return (kind, dict({'appointmentId': int(row['appointment_id']), 'patientId': int(row['patient_id']),
                        'doctorId': int(row['doctor_id']), 'slotId': row['slot_id'],
                        'startsAt': iso(utc(row['slot_start_utc'])), 'occurredAt': iso(clock())}, **extra))


def free_seat(row, bookable, cause):
    """Give the patient's seat back, then (only if it really came back) announce SlotSeatFreed."""
    released = release_seat(slots_table(), row['slot_id'], seat_ref(row['patient_id']))
    if released:
        return [appointment_event('SlotSeatFreed', row, bookable=bookable, cause=cause)]
    return []


# =============================================================================================== payloads

def appointment_payload(row, policy=None, now=None):
    starts = utc(row['slot_start_utc'])
    tz_name = row.get('timezone') or 'Asia/Kolkata'
    local = starts.astimezone(zone(tz_name))
    out = {
        'appointmentId': int(row['appointment_id']), 'reference': short_ref(row['appointment_id']),
        'status': row['status'], 'patientId': int(row['patient_id']),
        'patientName': f"{row.get('patient_first') or ''} {row.get('patient_last') or ''}".strip(),
        'doctor': {'doctorId': int(row['doctor_id']), 'name': f"Dr. {row['doctor_first']} {row['doctor_last']}",
                   'specialization': row.get('specialization'),
                   'hospital': {'name': row.get('hospital_name'), 'city': row.get('city')}},
        'consultation': {'consultationTypeId': int(row['consultation_type_id']), 'code': row.get('type_code'),
                         'label': row.get('type_label'), 'durationMinutes': row.get('duration_min')},
        'slotId': row['slot_id'], 'startsAt': iso(starts), 'endsAt': iso(utc(row['slot_end_utc'])),
        'timezone': tz_name, 'localDate': local.date().isoformat(), 'localStart': local.strftime('%H:%M'),
        'fee': money(row['fee_amount']), 'currency': 'INR', 'rescheduleCount': int(row.get('reschedule_count') or 0),
        'createdAt': iso(row.get('created_at')), 'updatedAt': iso(row.get('updated_at')),
    }
    if row.get('refund_id'):
        out['refund'] = {'refundId': int(row['refund_id']), 'reason': row.get('refund_reason'), 'tier': row['tier'],
                         'refundPercent': row['refund_percent'], 'amountPaid': money(row['refund_fee']),
                         'refundAmount': money(row['refund_amount']), 'recordedAt': iso(row.get('refunded_at'))}
    if row['status'] == CONFIRMED and policy is not None and now is not None and starts > now:
        out['cancellation'] = cancellation_preview(policy, starts, row['fee_amount'], now)
    return out


def safe_policy():
    try:
        return refund_policy()
    except PolicyError as exc:
        emit('error', 'refund_policy_invalid', None, error=str(exc))
        return None


# =============================================================================================== routing

ROUTES = {
    ('POST', '/patient/appointments'): 'book',
    ('GET', '/patient/appointments'): 'list_appointments',
    ('GET', '/patient/appointments/{appointmentId}'): 'get_appointment',
    ('GET', '/patient/appointments/{appointmentId}/refund-quote'): 'refund_quote',
    ('POST', '/patient/appointments/{appointmentId}/cancel'): 'cancel_appointment',
    ('GET', '/patient/refunds'): 'list_refunds',
    ('GET', '/doctor/me/appointments'): 'doctor_appointments',
    ('POST', '/doctor/me/appointments/{appointmentId}/status'): 'doctor_set_status',
    ('POST', '/doctor/me/slot-cancellations'): 'doctor_cancel_slot',
    ('POST', '/doctor/me/day-cancellations'): 'doctor_cancel_day',
}


def resolve(event):
    """API Gateway's resource template when present; otherwise derived from the raw path (tests, curl)."""
    resource, params = event.get('resource'), dict(event.get('pathParameters') or {})
    if resource:
        return resource, params
    parts = [p for p in str(event.get('path', '')).split('/') if p]
    while parts and parts[0] not in ('patient', 'doctor'):
        parts = parts[1:]                                 # drop a stage prefix such as /dev
    if parts[:2] == ['patient', 'appointments']:
        if len(parts) == 2:
            return '/patient/appointments', params
        params['appointmentId'] = parts[2]
        if len(parts) == 3:
            return '/patient/appointments/{appointmentId}', params
        if len(parts) == 4 and parts[3] in ('refund-quote', 'cancel'):
            return '/patient/appointments/{appointmentId}/' + parts[3], params
    elif parts == ['patient', 'refunds']:
        return '/patient/refunds', params
    elif parts in (['doctor', 'me', 'appointments'], ['doctor', 'me', 'slot-cancellations'],
                   ['doctor', 'me', 'day-cancellations']):
        return '/' + '/'.join(parts), params
    elif len(parts) == 5 and parts[:3] == ['doctor', 'me', 'appointments'] and parts[4] == 'status':
        params['appointmentId'] = parts[3]
        return '/doctor/me/appointments/{appointmentId}/status', params
    return None, params


def caller(event, role):
    ctx = (event.get('requestContext') or {}).get('authorizer') or {}
    if ctx.get('role') != role or not str(ctx.get('userId', '')).isdigit():
        raise ApiError(403, 'FORBIDDEN', f'{role.title()} role required')
    return int(ctx['userId'])


@logged('booking')
def handler(event, context):
    event = event or {}
    if event.get('action'):
        return direct_action(event)
    cid = correlation(event)
    method = str(event.get('httpMethod', '')).upper()
    resource, params = resolve(event)
    name = ROUTES.get((method, resource))
    if not name:
        known = any(r == resource for _, r in ROUTES)
        return reply(405 if known else 404, {'message': 'Method not allowed' if known else 'Route not found',
                                             'code': 'ROUTE', 'correlationId': cid}, cid)
    conn = None
    try:
        conn = dbconn(config())
        result = globals()[name](make_store(conn), event, params, cid)
        status, payload, headers = (result + (None,))[:3]
        return reply(status, payload, cid, headers)
    except ApiError as exc:
        rollback(conn)
        return reply(exc.status, exc.body(cid), cid, exc.headers)
    except PolicyError as exc:
        rollback(conn)
        emit('error', 'refund_policy_invalid', cid, error=str(exc))
        return reply(503, {'message': 'Cancellations are paused: the refund policy is misconfigured. Nothing was '
                                      'changed.', 'code': 'REFUND_POLICY_INVALID', 'correlationId': cid}, cid)
    except pymysql.err.MySQLError as exc:
        rollback(conn)
        log.exception('booking_db_failed route=%s %s correlationId=%s', method, resource, cid)
        if exc.args and exc.args[0] in (1054, 1146):
            return reply(503, {'message': 'The database schema is out of date. Run the migrate action, then retry.',
                               'code': 'SCHEMA_OUTDATED', 'correlationId': cid}, cid)
        return reply(500, {'message': 'Internal server error', 'code': 'INTERNAL', 'correlationId': cid}, cid)
    except Exception:
        rollback(conn)
        log.exception('booking_failed route=%s %s correlationId=%s', method, resource, cid)
        return reply(500, {'message': 'Internal server error', 'code': 'INTERNAL', 'correlationId': cid}, cid)
    finally:
        release(conn)


def rollback(conn):
    try:
        if conn:
            conn.rollback()
    except Exception:
        log.exception('rollback_failed')


def json_body(event):
    data = body(event)
    if data is None or not isinstance(data, dict):
        raise ApiError(400, 'INVALID_JSON', 'Request body must be a JSON object')
    return data


def text_field(data, key, limit, required=False, minimum=0):
    value = data.get(key)
    if value is None or (isinstance(value, str) and not value.strip()):
        if required:
            raise ApiError(400, 'VALIDATION', f'{key} is required')
        return None
    if not isinstance(value, str):
        raise ApiError(400, 'VALIDATION', f'{key} must be text')
    value = re.sub(r'\s+', ' ', value).strip()
    if len(value) < minimum or len(value) > limit or re.search(r'[<>]', value):
        raise ApiError(400, 'VALIDATION', f'{key} must be {minimum}-{limit} characters and must not contain < or >')
    return value


# =============================================================================================== booking

def book(store, event, params, cid):
    patient_id = caller(event, 'patient')
    data = json_body(event)
    unknown = sorted(set(data) - {'slotId'})
    if unknown:
        raise ApiError(400, 'VALIDATION', 'Unknown field(s): ' + ', '.join(unknown))
    key = str(headers_of(event).get('idempotency-key') or '').strip()
    if not key:
        raise ApiError(400, 'IDEMPOTENCY_KEY_REQUIRED', 'Send an Idempotency-Key header (8-64 characters, for example '
                                                        'a UUID). Re-send the SAME key when retrying the same booking.')
    if not IDEMPOTENCY_KEY.match(key):
        raise ApiError(400, 'IDEMPOTENCY_KEY_INVALID', 'Idempotency-Key must be 8-64 letters, digits, - _ : or .')
    slot_id = data.get('slotId')
    match = SLOT_ID.match(slot_id) if isinstance(slot_id, str) and len(slot_id) <= 100 else None
    if not match:
        raise ApiError(400, 'VALIDATION', 'slotId must be a slot id from GET /doctors/{doctorId}/slots')

    idem = Idempotency(idempotency_table())
    scope = f'booking#{patient_id}#{key}'
    state, record = idem.claim(scope, request_hash(slot_id), seat_ref(patient_id), time.time())
    if state == 'wait':
        record = idem.wait(scope)
        if not record or record.get('status') != 'COMPLETED':
            raise ApiError(409, 'IDEMPOTENCY_IN_PROGRESS', 'The first request with this Idempotency-Key is still being '
                                                          'processed. Retry in a moment with the same key.',
                           headers={'Retry-After': '2'})
        state = 'replay'
    if state == 'replay':
        metric('IdempotentReplays', 1)
        payload = json.loads(record['response_body'])
        if isinstance(payload, dict) and int(record['response_status']) < 300:
            payload['replayed'] = True
        return int(record['response_status']), payload, {'Idempotent-Replayed': 'true'}

    try:
        status, payload = take_seat(store, patient_id, scope, idem, record, state, slot_id, match, cid)
    except ApiError as exc:
        if exc.status < 500 and exc.code != 'IDEMPOTENCY_IN_PROGRESS':
            idem.complete(scope, exc.status, exc.body(cid))   # same request again -> same answer
        else:
            idem.forget(scope)
        raise
    except Exception:
        idem.forget(scope)
        raise
    idem.complete(scope, status, payload)
    return status, payload


def take_seat(store, patient_id, scope, idem, record, state, slot_id, match, cid):
    ref = seat_ref(patient_id)
    table, now, lim = slots_table(), clock(), limits()
    stats = {'retries': 0, 'attempts': 0}

    if state == 'takeover':                       # a previous attempt with this key died part-way
        found = store.active_for(patient_id, slot_id)
        if found:
            return 201, dict(appointment_payload(found, safe_policy(), now), replayed=True)

    slot = read_slot(table, slot_id)
    if not slot:
        raise ApiError(404, 'SLOT_NOT_FOUND', 'Slot not found.')
    doctor_id, type_id = int(match.group(1)), int(match.group(2))
    if str(slot.get('doctor_id')) != str(doctor_id) or str(slot.get('consultation_type_id')) != str(type_id):
        raise ApiError(409, 'SLOT_INVALID', 'This slot is not bookable.')
    adopt = state == 'takeover' and record.get('slot_version') is not None and \
        ref in (slot.get('booking_refs') or set())        # the dead attempt already holds our seat: finish it
    if not adopt:
        problem = seat_problem(slot, now, lim['minLeadMinutes'])          # fail fast before touching RDS
        if problem:
            reject_metric(problem, stats)
            raise problem

    offer = store.offer(doctor_id, type_id)
    if not offer or not offer['type_active'] or not offer['doctor_active']:
        raise ApiError(409, 'NOT_BOOKABLE', 'The doctor no longer offers this consultation.')
    patient = store.patient(patient_id)
    if not patient or not patient['is_active']:
        raise ApiError(403, 'ACCOUNT_INACTIVE', 'This account is closed.')
    if not patient.get('profile_id'):
        raise ApiError(422, 'PROFILE_REQUIRED', 'Create your patient profile before booking.')

    booking = {'patient_id': patient_id, 'doctor_id': doctor_id, 'consultation_type_id': type_id, 'slot_id': slot_id,
               'starts_at': parse_instant(slot['start_at']), 'ends_at': parse_instant(slot['end_at']),
               'local_date': date.fromisoformat(slot['local_date']),
               'timezone': slot.get('timezone') or offer.get('timezone') or 'Asia/Kolkata',
               'fee': Decimal(str(offer['fee_amount']))}
    problem = store.precheck(booking, lim, now)
    if problem:
        reject_metric(problem, stats)
        raise problem

    try:
        if adopt:
            seat = {'version': int(record['slot_version']), 'attempts': int(record.get('occ_attempts') or 1)}
        else:
            seat = reserve_seat(table, slot_id, ref, now, lim, stats)
    except ApiError as problem:
        reject_metric(problem, stats)
        raise
    metric('BookingConflictRetries', stats['retries'])                 # also 0, so SampleCount = requests
    stats['reported'] = True

    def slot_cancelled():
        current = read_slot(table, slot_id) or {}
        return bool(current.get('cancelled_by_doctor'))

    try:
        idem.record_seat(scope, seat['version'], seat['attempts'])
        appointment_id = store.confirm(booking, lim, now, slot_cancelled)
    except ApiError as problem:
        rollback(store.conn)
        release_seat(table, slot_id, ref)
        reject_metric(problem, stats)
        raise
    except pymysql.err.IntegrityError as exc:
        rollback(store.conn)
        if 'uq_appt_patient_slot_active' in str(exc):     # a parallel request of this patient got there first:
            raise ApiError(409, 'ALREADY_BOOKED', 'You already have a seat in this slot.')   # its seat, keep it
        release_seat(table, slot_id, ref)
        raise
    except Exception:
        rollback(store.conn)
        release_seat(table, slot_id, ref)
        raise

    metric('BookingsConfirmed', 1)
    row = store.active_for(patient_id, slot_id)
    publish([appointment_event('AppointmentBooked', row, consultationTypeId=type_id, fee=money(booking['fee']),
                               slotVersion=seat['version'])])
    emit('info', 'appointment_booked', cid, appointmentId=appointment_id, slotId=slot_id, slotVersion=seat['version'],
         attempts=seat['attempts'], conflictRetries=stats['retries'], patientId=str(patient_id))
    payload = appointment_payload(row, safe_policy(), now)
    payload['concurrency'] = {'slotVersion': seat['version'], 'attempts': seat['attempts'],
                              'conflictRetries': stats['retries'], 'capacity': int(slot.get('capacity', 1) or 1)}
    return 201, payload


def reject_metric(problem, stats):
    if problem.code == 'BOOKING_CONTENTION':
        metric('BookingRetriesExhausted', 1)
    if stats.get('attempts') and not stats.get('reported'):
        metric('BookingConflictRetries', stats['retries'])
        stats['reported'] = True
    if stats.get('attempts'):
        problem.extra['conflictRetries'] = stats['retries']      # visible to the caller's own load script
    metric('BookingsRejected', 1, Reason=problem.code)


# =============================================================================================== listing + detail

def encode_cursor(view, row):
    raw = json.dumps({'v': view, 's': utc(row['slot_start_utc']).isoformat(), 'i': int(row['appointment_id'])},
                     separators=(',', ':'))
    return base64.urlsafe_b64encode(raw.encode()).decode().rstrip('=')


def decode_cursor(token, view):
    try:
        data = json.loads(base64.urlsafe_b64decode(token + '=' * (-len(token) % 4)).decode())
        if data['v'] != view or not isinstance(data['i'], int):
            raise ValueError()
        return naive(datetime.fromisoformat(data['s'])), data['i']
    except Exception:
        raise ApiError(400, 'INVALID_CURSOR', 'cursor is not valid for this view; start again without a cursor')


def list_appointments(store, event, params, cid):
    patient_id = caller(event, 'patient')
    q = query_params(event)
    view = q.get('view') or 'upcoming'
    if view not in ('upcoming', 'past', 'cancelled', 'all'):
        raise ApiError(400, 'VALIDATION', 'view must be upcoming, past, cancelled or all')
    try:
        limit = int(q.get('limit') or 20)
        if not 1 <= limit <= 50:
            raise ValueError()
    except ValueError:
        raise ApiError(400, 'VALIDATION', 'limit must be a whole number between 1 and 50')
    after = decode_cursor(q['cursor'], view) if q.get('cursor') else None
    now = clock()
    rows = store.list_appointments(patient_id, view, now, limit, after)
    more = len(rows) > limit
    rows = rows[:limit]
    policy = safe_policy()
    return 200, {'view': view, 'items': [appointment_payload(r, policy, now) for r in rows],
                 'page': {'limit': limit, 'returned': len(rows), 'hasMore': more,
                          'nextCursor': encode_cursor(view, rows[-1]) if more and rows else None},
                 'refundPolicyAvailable': policy is not None}


def owned_appointment(store, patient_id, params):
    appointment_id = positive_id(params.get('appointmentId'), 'appointmentId')
    row = store.appointment(patient_id, appointment_id)
    if not row:
        # Same answer whether it does not exist or belongs to someone else: nothing leaks.
        raise ApiError(404, 'APPOINTMENT_NOT_FOUND', 'Appointment not found.')
    return row


def get_appointment(store, event, params, cid):
    patient_id = caller(event, 'patient')
    row = owned_appointment(store, patient_id, params)
    return 200, appointment_payload(row, safe_policy(), clock())


def refund_quote(store, event, params, cid):
    patient_id = caller(event, 'patient')
    row = owned_appointment(store, patient_id, params)
    if row['status'] != CONFIRMED:
        raise ApiError(409, 'ALREADY_CANCELLED', 'This appointment is no longer active.',
                       refund=appointment_payload(row).get('refund'))
    policy, now = refund_policy(), clock()
    preview = cancellation_preview(policy, utc(row['slot_start_utc']), row['fee_amount'], now)
    return 200, dict(preview, appointmentId=int(row['appointment_id']), amountPaid=money(row['fee_amount']),
                     currency='INR', quotedAt=iso(now))


def cancel_appointment(store, event, params, cid):
    patient_id = caller(event, 'patient')
    appointment_id = positive_id(params.get('appointmentId'), 'appointmentId')
    data = body(event) if event.get('body') else {}
    if not isinstance(data, dict):
        raise ApiError(400, 'INVALID_JSON', 'Request body must be a JSON object')
    reason = text_field(data, 'reason', 300)             # goes into the event only (no column for it)
    policy, now = refund_policy(), clock()                # fail closed BEFORE anything changes
    row, refund, already = store.cancel_by_patient(patient_id, appointment_id, now, policy)
    if not already:
        events = [appointment_event('AppointmentCancelled', row, cancelledBy='PATIENT', reason=reason,
                                    refund=dict(refund, currency='INR'))]
        events += free_seat(row, bookable=True, cause='PATIENT_CANCELLED')
        publish(events)
        emit('info', 'appointment_cancelled', cid, appointmentId=appointment_id, tier=refund['tier'],
             refundAmount=refund['refundAmount'], patientId=str(patient_id))
    payload = appointment_payload(store.appointment(patient_id, appointment_id))
    payload['alreadyCancelled'] = already
    return 200, payload


def list_refunds(store, event, params, cid):
    patient_id = caller(event, 'patient')
    rows = store.refunds(patient_id)
    items, total = [], Decimal('0.00')
    for r in rows:
        total += Decimal(str(r['refund_amount']))
        items.append({'refundId': int(r['refund_id']), 'appointmentId': int(r['appointment_id']),
                      'reference': short_ref(r['appointment_id']), 'reason': r['reason'], 'tier': r['tier'],
                      'refundPercent': r['refund_percent'], 'amountPaid': money(r['fee_amount']),
                      'refundAmount': money(r['refund_amount']), 'recordedAt': iso(r['created_at']),
                      'appointmentStartsAt': iso(r['slot_start_utc']), 'timezone': r.get('timezone'),
                      'doctor': f"Dr. {r['doctor_first']} {r['doctor_last']}", 'consultation': r['type_label']})
    return 200, {'items': items, 'count': len(items), 'totalRefunded': money(total), 'currency': 'INR'}


# =============================================================================================== doctor side

def doctor_appointments(store, event, params, cid):
    doctor_id = caller(event, 'doctor')
    q = query_params(event)
    try:
        first = date.fromisoformat(q['from']) if q.get('from') else clock().date()
        days = int(q.get('days') or 30)
        if not 1 <= days <= 30:
            raise ValueError()
    except ValueError:
        raise ApiError(400, 'VALIDATION', 'from must be YYYY-MM-DD and days 1..30')
    include_cancelled = q.get('includeCancelled', 'false').lower() == 'true'
    tz_name = store.doctor_timezone(doctor_id)
    start, _ = local_day_bounds(first, tz_name)
    _, end = local_day_bounds(first + timedelta(days=days - 1), tz_name)
    rows = store.doctor_appointments(doctor_id, start, end, include_cancelled)
    items, by_slot = [], {}
    for r in rows:
        p = appointment_payload(r)
        items.append({k: p[k] for k in ('appointmentId', 'reference', 'status', 'patientName', 'consultation', 'slotId',
                                         'startsAt', 'endsAt', 'localDate', 'localStart', 'fee')})
        if r['status'] == CONFIRMED:
            by_slot.setdefault(r['slot_id'], []).append(p['patientName'])
    return 200, {'from': first.isoformat(), 'days': days, 'items': items, 'bySlot': by_slot}


def cancel_slot_core(store, table, doctor_id, slot_id, reason, now, cause='SLOT_CANCELLED_BY_DOCTOR'):
    """Close one slot and cancel + fully refund every live booking on it. Returns (rows, events to publish)."""
    close_slot(table, slot_id, now)                   # 1. CLOSED + cancelled_by_doctor: no new seat from here on
    rows = store.cancel_slot(doctor_id, slot_id, now)  # 2. every live booking -> cancelled + 100% refund
    events = []
    for row in rows:                                  # 3. seats back, then events
        events.append(appointment_event('AppointmentCancelled', row, cancelledBy='DOCTOR', reason=reason,
                                        refund={'tier': DOCTOR_CANCEL_TIER, 'refundPercent': 100,
                                                'refundAmount': row['refundAmount'], 'currency': 'INR'}))
        events += free_seat(row, bookable=False, cause=cause)
    events.append(('SlotCancelled', {'slotId': slot_id, 'doctorId': doctor_id, 'appointmentsCancelled': len(rows),
                                     'reason': reason, 'occurredAt': iso(now)}))
    return rows, events


def doctor_set_status(store, event, params, cid):
    """POST /doctor/me/appointments/{appointmentId}/status  {"status": "CONFIRMED"|"COMPLETED"|"CANCELLED", "reason"}"""
    doctor_id = caller(event, 'doctor')
    appointment_id = positive_id(params.get('appointmentId'), 'appointmentId')
    data = json_body(event)
    wanted = str(data.get('status') or '').strip().upper()
    if wanted not in DOCTOR_STATUS_TARGETS:
        raise ApiError(400, 'VALIDATION', 'status must be CONFIRMED, COMPLETED, NO_SHOW or CANCELLED')
    target = DOCTOR_STATUS_TARGETS[wanted]
    reason = text_field(data, 'reason', 300) or ('Cancelled by the doctor' if target == CANCELLED_BY_DOCTOR else None)
    now = clock()
    try:
        row, refund, changed = store.set_status_by_doctor(doctor_id, appointment_id, target, now)
    except pymysql.err.IntegrityError:
        rollback(store.conn)
        raise ApiError(409, 'ALREADY_BOOKED', 'The patient already holds another live booking for this slot, so this '
                                              'appointment cannot be confirmed again.')
    if changed:
        events = [appointment_event('AppointmentStatusChanged', row, fromStatus=row['status'], toStatus=target,
                                    changedBy='DOCTOR')]
        if target == CANCELLED_BY_DOCTOR:
            events.append(appointment_event('AppointmentCancelled', row, cancelledBy='DOCTOR', reason=reason,
                                            refund=dict(refund, currency='INR')))
            events += free_seat(row, bookable=True, cause='APPOINTMENT_CANCELLED_BY_DOCTOR')
        publish(events)
        emit('info', 'appointment_status_changed', cid, appointmentId=appointment_id, fromStatus=row['status'],
             toStatus=target, doctorId=str(doctor_id))
    payload = appointment_payload(store.doctor_appointment(doctor_id, appointment_id))
    payload['changed'] = changed
    labels = {CONFIRMED: 'confirmed', COMPLETED: 'completed', NO_SHOW: 'a no-show', CANCELLED_BY_DOCTOR: 'cancelled'}
    payload['message'] = (f"{payload['reference']} marked {labels[target]}." if changed else
                          f"{payload['reference']} is already {labels[target]}.")
    if refund:
        payload['message'] += f" The patient is refunded ₹{refund['refundAmount']} (100%)."
    return 200, payload


def doctor_cancel_day(store, event, params, cid):
    """POST /doctor/me/day-cancellations  {"date": "YYYY-MM-DD", "reason": "..."}

    Used right after a blackout date is added: every slot on that local date that still has a booking (and has not
    started) is cancelled exactly like POST /doctor/me/slot-cancellations, so each patient gets a 100% refund.
    Safe to repeat: a second call finds no live bookings and changes nothing.
    """
    doctor_id = caller(event, 'doctor')
    data = json_body(event)
    try:
        day = date.fromisoformat(str(data.get('date') or ''))
    except ValueError:
        raise ApiError(400, 'VALIDATION', 'date must be YYYY-MM-DD')
    reason = text_field(data, 'reason', 300) or f'The doctor is unavailable on {day.isoformat()}'
    if len(reason) < 3:
        reason = f'The doctor is unavailable on {day.isoformat()}'
    table, now = slots_table(), clock()
    start, end = local_day_bounds(day, store.doctor_timezone(doctor_id))
    slots, rows, events, skipped = [], [], [], 0
    for booked in store.doctor_booked_slots(doctor_id, start, end):
        if utc(booked['slot_start_utc']) <= now:
            skipped += 1                              # already started: leave it for the doctor to complete
            continue
        slot_rows, slot_events = cancel_slot_core(store, table, doctor_id, booked['slot_id'], reason, now,
                                                  cause='BLACKOUT_DAY')
        slots.append(booked['slot_id'])
        rows += slot_rows
        events += slot_events
    published = publish(events)
    total = sum((Decimal(r['refundAmount']) for r in rows), Decimal('0.00'))
    emit('info', 'day_cancelled_by_doctor', cid, doctorId=str(doctor_id), date=day.isoformat(), slots=len(slots),
         cancelled=len(rows), totalRefunded=money(total))
    return 200, {'date': day.isoformat(), 'slotsCancelled': len(slots), 'slotIds': slots,
                 'appointmentsCancelled': len(rows), 'totalRefunded': money(total), 'refundPercent': 100,
                 'skippedStarted': skipped, 'eventsPublished': published,
                 'message': (f'{len(rows)} booked appointment(s) on {day.isoformat()} cancelled and refunded in full.'
                             if rows else f'No booked appointments on {day.isoformat()} needed cancelling.')}


def doctor_cancel_slot(store, event, params, cid):
    doctor_id = caller(event, 'doctor')
    data = json_body(event)
    slot_id = data.get('slotId') or data.get('slot_id')
    match = SLOT_ID.match(slot_id) if isinstance(slot_id, str) else None
    if not match or int(match.group(1)) != doctor_id:
        raise ApiError(400, 'VALIDATION', 'slotId must be one of your own slots')
    reason = text_field(data, 'reason', 300, required=True, minimum=3)
    table, now = slots_table(), clock()
    slot = read_slot(table, slot_id)
    if not slot or str(slot.get('doctor_id')) != str(doctor_id):
        raise ApiError(404, 'SLOT_NOT_FOUND', 'Slot not found.')
    if parse_instant(slot['start_at']) <= now:
        raise ApiError(422, 'SLOT_IN_PAST', 'This slot has already started and cannot be cancelled.')
    rows, events = cancel_slot_core(store, table, doctor_id, slot_id, reason, now)
    published = publish(events)
    total = sum((Decimal(r['refundAmount']) for r in rows), Decimal('0.00'))
    refunds = [{'appointmentId': int(r['appointment_id']), 'patientId': int(r['patient_id']), 'refundPercent': 100,
                'refundAmount': r['refundAmount']} for r in rows]
    emit('info', 'slot_cancelled_by_doctor', cid, slotId=slot_id, doctorId=str(doctor_id), cancelled=len(rows),
         totalRefunded=money(total))
    return 200, {'slotId': slot_id, 'status': CLOSED, 'appointmentsCancelled': len(rows), 'refunds': refunds,
                 'totalRefunded': money(total), 'refundPercent': 100, 'eventsPublished': published,
                 'message': (f'Slot cancelled. {len(rows)} patient(s) refunded in full.' if rows
                             else 'Slot cancelled. Nobody had booked it.')}


# =============================================================================================== direct actions

def direct_action(event):
    action = event.get('action')
    if action == 'policy':
        try:
            policy = refund_policy()
            shown = {'version': policy['version'], 'tiers': [{k: v for k, v in t.items() if k != 'lead'}
                                                             for t in policy['tiers']]}
        except PolicyError as exc:
            shown = {'error': str(exc)}
        return {'statusCode': 200, 'body': {'refundPolicy': shown, 'limits': limits()}}
    conn = dbconn(config())
    try:
        store = make_store(conn)
        if action == 'release-pending':
            return {'statusCode': 200, 'body': release_pending(store)}
        if action == 'reconcile-slot':
            return {'statusCode': 200, 'body': reconcile_slot(store, str(event.get('slotId') or ''),
                                                              bool(event.get('releaseOrphans')))}
        return {'statusCode': 400, 'body': {'message': f'unknown action {action!r}'}}
    finally:
        release(conn)


def release_pending(store):
    """Every 5 minutes: a cancellation whose seat release failed (e.g. the Lambda stopped right after COMMIT) gets
    its seat back here. release_seat is idempotent, so already-released seats are a cheap no-op."""
    released, checked = 0, 0
    events = []
    for row in store.unreleased_cancellations(clock()):
        checked += 1
        try:
            freed = free_seat(row, bookable=row['status'] == CANCELLED_BY_PATIENT,
                              cause='RELEASED_BY_SWEEPER')
        except Exception as exc:
            emit('warning', 'sweeper_release_failed', None, slotId=row['slot_id'], errorType=type(exc).__name__)
            continue
        released += len(freed)
        events += freed
    publish(events)
    summary = {'checked': checked, 'released': released}
    emit('info', 'release_pending_swept', None, **summary)
    return summary


def reconcile_slot(store, slot_id, release_orphans=False):
    """Seat tags on the slot vs live appointments in RDS. Releases tags of patients with no live appointment
    (only with releaseOrphans=true: run it when no booking of that slot is in flight)."""
    item = read_slot(slots_table(), slot_id)
    if not item:
        return {'slotId': slot_id, 'found': False}
    refs = set(item.get('booking_refs') or set())
    # COMPLETED and NO_SHOW appointments keep their seat (the slot is in the past), so they count as live here too.
    live = {seat_ref(r['patient_id']) for r in store.slot_rows(slot_id)
            if r['status'] in (CONFIRMED, COMPLETED, NO_SHOW)}
    orphans = sorted(refs - live)
    released = [ref for ref in orphans if release_orphans and release_seat(slots_table(), slot_id, ref)]
    return {'slotId': slot_id, 'found': True, 'bookedCount': int(item.get('booked_count', 0) or 0),
            'seatTags': len(refs), 'liveAppointments': len(live), 'missingSeats': sorted(live - refs),
            'orphanTags': orphans, 'released': released,
            'consistent': int(item.get('booked_count', 0) or 0) == len(refs) == len(live) and refs == live}
