"""Milestone 5: booking engine, optimistic concurrency, idempotency, cancellation, refunds. ALL M5 tests live here.

Two kinds of test in this one file:

  * Unit tests (run by `pytest -q` in the CI job). They use REAL threads against a fake DynamoDB table that
    evaluates the exact ConditionExpression / UpdateExpression strings the Lambda sends, atomically under a
    lock (DynamoDB is linearisable per item). RDS is an in-memory store with the same unique constraints.

      MANDATORY TEST 1  test_mandatory_1_concurrency_24_parallel_bookings_capacity_3
      MANDATORY TEST 2  test_mandatory_2_idempotency_same_key_replayed_*

  * Live tests (marked `live`, skipped unless MEDISLOT_API_URL and MEDISLOT_CI_PASSWORD are set). The deploy
    job runs them against the real API Gateway -> Lambda -> DynamoDB -> RDS:  pytest -m live tests/test_m5_booking.py
"""
import collections
import concurrent.futures
import importlib.util
import itertools
import json
import os
import random
import re
import sys
import threading
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

os.environ.setdefault('AWS_DEFAULT_REGION', 'ap-south-1')
ROOT = os.path.join(os.path.dirname(__file__), '..', 'lambdas')
sys.path.insert(0, ROOT)
_spec = importlib.util.spec_from_file_location('booking_lambda', os.path.join(ROOT, 'booking', 'lambda_function.py'))
bl = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(bl)
sys.path.insert(0, os.path.join(ROOT, 'doctor'))
import slots as doctor_slots                     # noqa: E402  (the doctor-side resync writer)

from botocore.exceptions import ClientError     # noqa: E402
import pymysql                                   # noqa: E402

UTC = timezone.utc


# =============================================================================================== fake DynamoDB
# A tiny evaluator for the DynamoDB expression grammar the Lambdas use. If an expression string in the Lambda
# has a typo, these tests fail - which is the point of not hand-coding the semantics.

TOKEN = re.compile(r'\s*(<>|<=|>=|[=<>(),+\-]|:[A-Za-z0-9_]+|#[A-Za-z0-9_]+|[A-Za-z_][A-Za-z0-9_]*)')


def tokens(text):
    out, pos = [], 0
    while pos < len(text):
        m = TOKEN.match(text, pos)
        if not m:
            if text[pos:].strip() == '':
                break
            raise ValueError(f'cannot parse expression near {text[pos:]!r}')
        out.append(m.group(1))
        pos = m.end()
    return out


class Cond:
    def __init__(self, text, names, values, item):
        self.t, self.i, self.names, self.values, self.item = tokens(text), 0, names or {}, values or {}, item

    def peek(self): return self.t[self.i] if self.i < len(self.t) else None
    def take(self, want=None):
        tok = self.t[self.i]; self.i += 1
        if want and tok.upper() != want:
            raise ValueError(f'expected {want}, got {tok}')
        return tok

    def path(self, tok): return self.names.get(tok, tok)

    def operand(self):
        tok = self.take()
        if tok.startswith(':'):
            if tok not in self.values:
                raise ValueError(f'missing value {tok}')
            return self.values[tok]
        if tok.lower() == 'size':
            self.take('('); p = self.path(self.take()); self.take(')')
            return len(self.item.get(p, ()))
        return self.item.get(self.path(tok), _MISSING)

    def run(self):
        result = self.expr()
        if self.peek() is not None:
            raise ValueError(f'trailing tokens {self.t[self.i:]}')
        return result

    def expr(self):
        value = self.term()
        while self.peek() and self.peek().upper() == 'OR':
            self.take(); right = self.term(); value = value or right
        return value

    def term(self):
        value = self.factor()
        while self.peek() and self.peek().upper() == 'AND':
            self.take(); right = self.factor(); value = value and right
        return value

    def factor(self):
        tok = self.peek()
        if tok.upper() == 'NOT':
            self.take(); return not self.factor()
        if tok == '(':
            self.take(); v = self.expr(); self.take(')'); return v
        low = tok.lower()
        if low in ('attribute_exists', 'attribute_not_exists'):
            self.take(); self.take('('); p = self.path(self.take()); self.take(')')
            return (p in self.item) == (low == 'attribute_exists')
        if low == 'contains':
            self.take(); self.take('('); p = self.path(self.take()); self.take(','); v = self.operand(); self.take(')')
            have = self.item.get(p, _MISSING)
            return have is not _MISSING and v in have
        left = self.operand()
        op = self.take()
        right = self.operand()
        if left is _MISSING or right is _MISSING:
            return False if op != '<>' else True
        return {'=': left == right, '<>': left != right, '<': left < right, '<=': left <= right,
                '>': left > right, '>=': left >= right}[op]


class _Missing:
    def __repr__(self): return 'MISSING'


_MISSING = _Missing()


def split_top(text, sep=','):
    parts, depth, cur = [], 0, ''
    for ch in text:
        depth += ch == '('
        depth -= ch == ')'
        if ch == sep and depth == 0:
            parts.append(cur); cur = ''
        else:
            cur += ch
    parts.append(cur)
    return [p.strip() for p in parts if p.strip()]


def apply_update(item, text, names, values):
    names, values = names or {}, values or {}
    sections = re.split(r'\b(SET|ADD|DELETE|REMOVE)\b', text)
    def val(expr):
        expr = expr.strip()
        m = re.fullmatch(r'if_not_exists\(\s*([#\w]+)\s*,\s*(:\w+)\s*\)', expr)
        if m:
            p = names.get(m.group(1), m.group(1))
            return item[p] if p in item else values[m.group(2)]
        if expr.startswith(':'):
            return values[expr]
        return item[names.get(expr, expr)]
    for i in range(1, len(sections), 2):
        kind, body = sections[i], sections[i + 1]
        for clause in split_top(body):
            if kind == 'SET':
                target, expr = [x.strip() for x in clause.split('=', 1)]
                target = names.get(target, target)
                m = re.fullmatch(r'(.+?)\s*([+-])\s*(:\w+)', expr)
                item[target] = (val(m.group(1)) + (1 if m.group(2) == '+' else -1) * values[m.group(3)]) if m else val(expr)
            elif kind == 'ADD':
                target, v = clause.split(); target = names.get(target, target); v = values[v]
                item[target] = (set(item.get(target, set())) | v) if isinstance(v, set) else item.get(target, 0) + v
            elif kind == 'DELETE':
                target, v = clause.split(); target = names.get(target, target)
                left = set(item.get(target, set())) - values[v]
                if left:
                    item[target] = left
                else:
                    item.pop(target, None)          # DynamoDB drops empty sets
            elif kind == 'REMOVE':
                item.pop(names.get(clause, clause), None)


def validate_placeholders(*expressions, names=None, values=None):
    """Real DynamoDB refuses a request whose ExpressionAttributeNames/Values contain an entry that no expression
    uses (ValidationException), or that uses one that is not supplied. Enforce the same here."""
    text = ' '.join(e for e in expressions if e)
    used_values, used_names = set(re.findall(r':[A-Za-z0-9_]+', text)), set(re.findall(r'#[A-Za-z0-9_]+', text))
    for kind, given, used in (('values', set(values or {}), used_values), ('names', set(names or {}), used_names)):
        if given - used:
            raise ClientError({'Error': {'Code': 'ValidationException',
                                         'Message': f'Value provided in ExpressionAttribute{kind.title()} unused in '
                                                    f'expressions: keys: {sorted(given - used)}'}}, 'UpdateItem')
        if used - given:
            raise ClientError({'Error': {'Code': 'ValidationException',
                                         'Message': f'ExpressionAttribute{kind.title()} missing: {sorted(used - given)}'}},
                              'UpdateItem')


def ccf():
    return ClientError({'Error': {'Code': 'ConditionalCheckFailedException', 'Message': 'The conditional request failed'}},
                       'UpdateItem')


class FakeTable:
    """Thread-safe, per-item-atomic DynamoDB table. `read_pause` widens the read->write window to force races."""

    def __init__(self, key='slot_id', read_pause=0.0):
        self.key, self.items, self.lock, self.read_pause = key, {}, threading.Lock(), read_pause
        self.updates = self.conflicts = 0

    def _copy(self, item):
        return {k: (set(v) if isinstance(v, set) else v) for k, v in item.items()}

    def _k(self, Key):
        """The Slots table is partitioned by doctor: every key must be exactly {doctor_id, slot_id}."""
        if self.key == 'slot_id':
            assert set(Key) == {'doctor_id', 'slot_id'}, f'Slots key must be (doctor_id, slot_id), got {Key}'
            assert Key['slot_id'].startswith(Key['doctor_id'] + '#'), Key
        return Key[self.key]

    def get_item(self, Key, ConsistentRead=False, **kw):
        with self.lock:
            item = self.items.get(self._k(Key))
            out = {'Item': self._copy(item)} if item else {}
        if self.read_pause:
            time.sleep(random.uniform(0, self.read_pause))       # let other threads read the same version
        return out

    def _check(self, item, cond, names, values):
        if cond and not Cond(cond, names, values, item or {}).run():
            self.conflicts += 1
            raise ccf()

    def put_item(self, Item, ConditionExpression=None, ExpressionAttributeNames=None, ExpressionAttributeValues=None):
        validate_placeholders(ConditionExpression, names=ExpressionAttributeNames, values=ExpressionAttributeValues)
        with self.lock:
            self._check(self.items.get(Item[self.key]), ConditionExpression, ExpressionAttributeNames, ExpressionAttributeValues)
            self.items[Item[self.key]] = self._copy(Item)

    def update_item(self, Key, UpdateExpression, ConditionExpression=None, ExpressionAttributeNames=None,
                    ExpressionAttributeValues=None, **kw):
        validate_placeholders(UpdateExpression, ConditionExpression, names=ExpressionAttributeNames,
                              values=ExpressionAttributeValues)
        with self.lock:
            current = self.items.get(self._k(Key))
            self._check(current, ConditionExpression, ExpressionAttributeNames, ExpressionAttributeValues)
            item = self._copy(current) if current else dict(Key)
            apply_update(item, UpdateExpression, ExpressionAttributeNames, ExpressionAttributeValues)
            self.items[self._k(Key)] = item
            self.updates += 1

    def delete_item(self, Key, ConditionExpression=None, ExpressionAttributeNames=None, ExpressionAttributeValues=None):
        validate_placeholders(ConditionExpression, names=ExpressionAttributeNames, values=ExpressionAttributeValues)
        with self.lock:
            self._check(self.items.get(self._k(Key)), ConditionExpression, ExpressionAttributeNames, ExpressionAttributeValues)
            self.items.pop(self._k(Key), None)


# =============================================================================================== in-memory RDS

class Conn:
    def rollback(self): pass
    def commit(self): pass
    def cursor(self): raise AssertionError('MemoryStore does not use SQL')
    def ping(self, **k): pass


class MemoryDB:
    def __init__(self):
        self.lock = threading.RLock()
        self.locks = {}
        self.users, self.profiles, self.offers, self.doctors = {}, {}, {}, {}
        self.appointments, self.ledger = [], []
        self.next_id = itertools.count(1)

    def row_lock(self, key):
        with self.lock:
            return self.locks.setdefault(key, threading.Lock())


class MemoryStore:
    """Same methods, rules and unique key as bl.Store, kept in Python lists (appointments + refund_ledger)."""

    def __init__(self, db, conn=None):
        self.db, self.conn = db, conn or Conn()

    def _joined(self, a):
        d = self.db.doctors[a['doctor_id']]
        o = self.db.offers[(a['doctor_id'], a['consultation_type_id'])]
        p = self.db.profiles.get(a['patient_id'], {})
        r = next((x for x in self.db.ledger if x['appointment_id'] == a['appointment_id']), {})
        return dict(a, doctor_first=d['first_name'], doctor_last=d['last_name'], specialization='Cardiology',
                    timezone='Asia/Kolkata', hospital_name='MediSlot General Hospital', city='Hyderabad',
                    type_code=o['code'], type_label=o['label'], duration_min=o['duration_min'],
                    patient_first=p.get('first_name'), patient_last=p.get('last_name'), refund_id=r.get('refund_id'),
                    refund_reason=r.get('reason'), tier=r.get('tier'), refund_percent=r.get('refund_percent'),
                    refund_fee=r.get('fee_amount'), refund_amount=r.get('refund_amount'), refunded_at=r.get('created_at'))

    def _live(self):
        return [a for a in self.db.appointments if a['active_flag'] == 1]

    # ------------------------------------------------------------------ reads
    def patient(self, pid):
        u = self.db.users.get(pid)
        if not u:
            return None
        p = self.db.profiles.get(pid)
        return {'user_id': pid, 'is_active': u['is_active'], 'profile_id': pid if p else None,
                'first_name': (p or {}).get('first_name'), 'last_name': (p or {}).get('last_name')}

    def offer(self, doctor_id, type_id):
        o = self.db.offers.get((doctor_id, type_id))
        return dict(o, doctor_active=True, timezone='Asia/Kolkata') if o else None

    def doctor_timezone(self, doctor_id):
        return 'Asia/Kolkata'

    def active_for(self, pid, slot_id):
        with self.db.lock:
            a = next((a for a in self._live() if a['patient_id'] == pid and a['slot_id'] == slot_id), None)
            return self._joined(a) if a else None

    def appointment(self, pid, appointment_id):
        with self.db.lock:
            a = next((a for a in self.db.appointments if a['appointment_id'] == appointment_id and a['patient_id'] == pid), None)
            return self._joined(a) if a else None

    def list_appointments(self, pid, view, now, limit, after):
        n = bl.naive(now)
        key = lambda a: (a['slot_start_utc'], a['appointment_id'])
        rows = [a for a in self.db.appointments if a['patient_id'] == pid]
        if view == 'upcoming':
            rows = sorted((a for a in rows if a['status'] == 'CONFIRMED' and a['slot_start_utc'] > n), key=key)
        else:
            if view == 'past':
                rows = [a for a in rows if a['status'] in ('CONFIRMED', 'COMPLETED', 'NO_SHOW') and a['slot_start_utc'] <= n]
            elif view == 'cancelled':
                rows = [a for a in rows if a['status'].startswith('CANCELLED')]
            rows = sorted(rows, key=key, reverse=True)
        if after:
            asc = view == 'upcoming'
            rows = [a for a in rows if (key(a) > after) == asc and key(a) != after]
        return [self._joined(a) for a in rows[:limit + 1]]

    def refunds(self, pid):
        out = []
        for r in sorted(self.db.ledger, key=lambda r: r['refund_id'], reverse=True):
            if r['patient_id'] == pid:
                a = self._joined(next(a for a in self.db.appointments if a['appointment_id'] == r['appointment_id']))
                out.append(dict(r, slot_start_utc=a['slot_start_utc'], slot_id=a['slot_id'], doctor_first=a['doctor_first'],
                                doctor_last=a['doctor_last'], timezone='Asia/Kolkata', type_label=a['type_label']))
        return out

    def doctor_appointments(self, doctor_id, start, end, include_cancelled):
        return [self._joined(a) for a in self.db.appointments if a['doctor_id'] == doctor_id
                and bl.naive(start) <= a['slot_start_utc'] < bl.naive(end) and (include_cancelled or a['status'] == 'CONFIRMED')]

    def _limit_problem(self, b, lim, now):
        n = bl.naive(now)
        day_start, day_end = [bl.naive(x) for x in bl.local_day_bounds(b['local_date'], b['timezone'])]
        mine = [a for a in self._live() if a['patient_id'] == b['patient_id']
                and (a['slot_end_utc'] > n or day_start <= a['slot_start_utc'] < day_end)]
        if any(a['slot_id'] == b['slot_id'] for a in mine):
            return bl.ApiError(409, 'ALREADY_BOOKED', 'You already have a seat in this slot.')
        if any(a['doctor_id'] == b['doctor_id'] and day_start <= a['slot_start_utc'] < day_end for a in mine):
            return bl.ApiError(422, 'ONE_PER_DOCTOR_PER_DAY', 'one per doctor per day')
        if any(a['slot_start_utc'] < bl.naive(b['ends_at']) and a['slot_end_utc'] > bl.naive(b['starts_at']) for a in mine):
            return bl.ApiError(422, 'OVERLAPPING_APPOINTMENT', 'overlap')
        if sum(a['slot_start_utc'] > n for a in mine) >= lim['maxUpcomingPerPatient']:
            return bl.ApiError(422, 'PATIENT_LIMIT_REACHED', 'limit')
        return None

    def precheck(self, b, lim, now):
        with self.db.lock:
            return self._limit_problem(b, lim, now)

    # ------------------------------------------------------------------ writes
    def confirm(self, b, lim, now, slot_cancelled):
        with self.db.row_lock(('user', b['patient_id'])):            # = users ... FOR UPDATE
            with self.db.lock:                                       # (+ doctors LOCK IN SHARE MODE)
                if slot_cancelled():
                    raise bl.ApiError(409, 'SLOT_CANCELLED', 'cancelled')
                problem = self._limit_problem(b, lim, now)
                if problem:
                    raise problem
                if any(a['patient_id'] == b['patient_id'] and a['slot_id'] == b['slot_id'] for a in self._live()):
                    raise pymysql.err.IntegrityError(1062, "Duplicate entry for key 'uq_appt_patient_slot_active'")
                appointment_id = next(self.db.next_id)
                self.db.appointments.append({
                    'appointment_id': appointment_id, 'patient_id': b['patient_id'], 'doctor_id': b['doctor_id'],
                    'consultation_type_id': b['consultation_type_id'], 'slot_id': b['slot_id'],
                    'slot_start_utc': bl.naive(b['starts_at']), 'slot_end_utc': bl.naive(b['ends_at']),
                    'status': 'CONFIRMED', 'active_flag': 1, 'reschedule_count': 0, 'fee_amount': b['fee'],
                    'created_at': bl.naive(now), 'updated_at': bl.naive(now)})
                return appointment_id

    def _refund(self, a, reason, tier, pct, amount, now):
        self.db.ledger.append({'refund_id': next(self.db.next_id), 'appointment_id': a['appointment_id'],
                               'patient_id': a['patient_id'], 'reason': reason, 'tier': tier, 'refund_percent': pct,
                               'fee_amount': a['fee_amount'], 'refund_amount': amount, 'created_at': bl.naive(now)})

    def cancel_by_patient(self, pid, appointment_id, now, policy):
        with self.db.lock:
            a = next((a for a in self.db.appointments if a['appointment_id'] == appointment_id and a['patient_id'] == pid), None)
            if not a:
                raise bl.ApiError(404, 'APPOINTMENT_NOT_FOUND', 'Appointment not found.')
            if a['status'] != 'CONFIRMED':
                return dict(a), None, True
            tier = bl.refund_tier(policy, bl.utc(a['slot_start_utc']), now)
            if tier is None:
                raise bl.ApiError(422, 'APPOINTMENT_STARTED', 'started')
            amount = bl.refund_amount(a['fee_amount'], tier['refundPercent'])
            a.update(status='CANCELLED_BY_PATIENT', active_flag=None, updated_at=bl.naive(now))
            self._refund(a, 'PATIENT_CANCEL', tier['tier'], tier['refundPercent'], amount, now)
        bl.metric('CancellationsByTier', 1, Tier=tier['tier'])
        return dict(a), {'tier': tier['tier'], 'refundPercent': tier['refundPercent'], 'refundAmount': bl.money(amount)}, False

    def cancel_slot(self, doctor_id, slot_id, now):
        with self.db.lock:
            rows = [a for a in self._live() if a['slot_id'] == slot_id and a['doctor_id'] == doctor_id]
            out = []
            for a in rows:
                amount = bl.refund_amount(a['fee_amount'], 100)
                a.update(status='CANCELLED_BY_DOCTOR', active_flag=None, updated_at=bl.naive(now))
                self._refund(a, 'DOCTOR_CANCEL', bl.DOCTOR_CANCEL_TIER, 100, amount, now)
                out.append(dict(a, refundAmount=bl.money(amount)))
        if out:
            bl.metric('CancellationsByTier', len(out), Tier=bl.DOCTOR_CANCEL_TIER)
        return out

    def unreleased_cancellations(self, now, limit=200):
        with self.db.lock:
            return [dict(a) for a in self.db.appointments if a['status'].startswith('CANCELLED')
                    and a['slot_start_utc'] > bl.naive(now)
                    and not any(b['patient_id'] == a['patient_id'] and b['slot_id'] == a['slot_id'] for b in self._live())]

    def slot_rows(self, slot_id):
        return [{'appointment_id': a['appointment_id'], 'patient_id': a['patient_id'], 'status': a['status']}
                for a in self.db.appointments if a['slot_id'] == slot_id]


class FakeEvents:
    def __init__(self, slots, fail=False):
        self.sent, self.slots, self.fail, self.lock = [], slots, fail, threading.Lock()

    def put_events(self, Entries):
        if self.fail:
            raise RuntimeError('EventBridge unavailable')
        with self.lock:
            for e in Entries:
                detail = json.loads(e['Detail'])
                if e['DetailType'] == 'SlotSeatFreed':     # seat must already be back when this is published
                    item = self.slots.items.get(detail['slotId'], {})
                    detail['_refStillOnSlot'] = bl.seat_ref(detail['patientId']) in item.get('booking_refs', set())
                self.sent.append((e['DetailType'], detail, e['Source'], e['EventBusName']))
        return {'FailedEntryCount': 0, 'Entries': [{'EventId': uuid.uuid4().hex} for _ in Entries]}


# =============================================================================================== scenario

NOW = datetime(2026, 11, 20, 4, 0, tzinfo=UTC)                 # 09:30 in Kolkata
DOCTOR, DOCTOR2, CAMP, STD, STD2 = 7, 8, 31, 32, 33            # ids
POLICY = {'version': '2026-11-v1', 'tiers': [{'tier': 'FULL', 'minHoursBefore': 24, 'refundPercent': 100},
                                             {'tier': 'PARTIAL', 'minHoursBefore': 6, 'refundPercent': 50},
                                             {'tier': 'LATE', 'minHoursBefore': 0, 'refundPercent': 0}]}


def zulu(dt): return dt.astimezone(UTC).strftime('%Y-%m-%dT%H:%M:%SZ')


def make_slot(doctor, type_id, start, capacity=1, minutes=30, status='OPEN'):
    sid = f'{doctor}#{type_id}#{zulu(start)}'
    return sid, {'doctor_id': str(doctor), 'slot_id': sid,
                 'consultation_type_id': str(type_id), 'start_at': zulu(start),
                 'end_at': zulu(start + timedelta(minutes=minutes)), 'timezone': 'Asia/Kolkata',
                 'local_date': start.date().isoformat(), 'capacity': capacity, 'booked_count': 0, 'held_count': 0,
                 'version': 0, 'status': status}


class Env:
    def __init__(self, monkeypatch, read_pause=0.0, events_fail=False, limits=None):
        self.slots = FakeTable(read_pause=read_pause)
        self.idem = FakeTable(key='idempotency_key')
        self.db = MemoryDB()
        self.events = FakeEvents(self.slots, fail=events_fail)
        self.metrics, self.mlock = [], threading.Lock()
        self.now = NOW
        self.policy, self.limits = dict(POLICY), dict(limits or {})
        mp = monkeypatch
        mp.setattr(bl, 'slots_table', lambda: self.slots)
        mp.setattr(bl, 'idempotency_table', lambda: self.idem)
        mp.setattr(bl, 'events_client', lambda: self.events)
        mp.setattr(bl, 'make_store', lambda conn: MemoryStore(self.db, conn))
        mp.setattr(bl, 'config', lambda: {})
        mp.setattr(bl, 'dbconn', lambda c: Conn())
        mp.setattr(bl, 'clock', lambda: self.now)
        mp.setattr(bl, 'emit', lambda *a, **k: None)
        mp.setattr(bl, 'ssm_json', lambda name: (self.policy, 7) if name.endswith('refund-policy') else (self.limits, 3))
        mp.setattr(bl, 'metric', self._metric)
        # doctors, offers (fee, type), patients
        self.db.doctors[DOCTOR] = {'first_name': 'Asha', 'last_name': 'Rao'}
        self.db.doctors[DOCTOR2] = {'first_name': 'Vikram', 'last_name': 'Shah'}
        self.db.offers[(DOCTOR, CAMP)] = {'consultation_type_id': CAMP, 'fee_amount': Decimal('300.00'), 'type_active': True,
                                          'code': 'HEALTH_CAMP', 'label': 'Health camp', 'duration_min': 60}
        self.db.offers[(DOCTOR, STD)] = {'consultation_type_id': STD, 'fee_amount': Decimal('499.99'), 'type_active': True,
                                         'code': 'STANDARD', 'label': 'Standard consult', 'duration_min': 30}
        self.db.offers[(DOCTOR2, STD2)] = {'consultation_type_id': STD2, 'fee_amount': Decimal('650.00'), 'type_active': True,
                                           'code': 'STANDARD', 'label': 'Standard consult', 'duration_min': 30}

    def _metric(self, name, value, unit='Count', **dims):
        with self.mlock:
            self.metrics.append((name, value, dims))

    def metric_sum(self, name, **dims):
        return sum(v for n, v, d in self.metrics if n == name and all(d.get(k) == x for k, x in dims.items()))

    def add_patient(self, pid, first='Meera', last='Iyer'):
        self.db.users[pid] = {'is_active': True}
        self.db.profiles[pid] = {'first_name': first, 'last_name': last}
        return pid

    def add_slot(self, doctor, type_id, start, **kw):
        sid, item = make_slot(doctor, type_id, start, **kw)
        self.slots.items[sid] = item
        return sid

    def call(self, method, path, *, role='patient', user=21, body=None, headers=None, query=None):
        event = {'httpMethod': method, 'path': path, 'headers': headers or {}, 'queryStringParameters': query,
                 'body': json.dumps(body) if body is not None else None,
                 'requestContext': {'authorizer': {'role': role, 'userId': str(user)} if role else {}}}
        res = bl.handler(event, None)
        return res['statusCode'], json.loads(res['body']), res['headers']

    def book(self, slot_id, user=21, key=None, **extra):
        return self.call('POST', '/patient/appointments', user=user, body=dict(slotId=slot_id, **extra),
                         headers={'Idempotency-Key': key or uuid.uuid4().hex})

    def live(self):
        return [a for a in self.db.appointments if a['active_flag'] == 1]


@pytest.fixture
def env(monkeypatch):
    e = Env(monkeypatch)
    e.add_patient(21)
    return e


TOMORROW_9 = datetime(2026, 11, 21, 3, 30, tzinfo=UTC)           # 09:00 IST tomorrow


# =============================================================================================== MANDATORY TEST 1

def test_mandatory_1_concurrency_24_parallel_bookings_capacity_3(monkeypatch):
    """24 different patients hit one capacity-3 Health Camp slot at the same instant: exactly 3 seats are sold.

    Real threads, released together by a barrier; the fake table pauses after every read so many requests read the
    same version and lose the conditional write. That proves contention genuinely happened (conflict retries > 0)
    and was handled by optimistic concurrency, not avoided by accidental serialisation.
    """
    env = Env(monkeypatch, read_pause=0.004)
    slot = env.add_slot(DOCTOR, CAMP, TOMORROW_9, capacity=3, minutes=60)
    patients = [env.add_patient(1000 + i, f'Pat{chr(65 + i)}', 'Load') for i in range(24)]
    gate = threading.Barrier(len(patients))

    def one(pid):
        gate.wait()
        return env.book(slot, user=pid)

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(patients)) as pool:
        results = list(pool.map(one, patients))

    statuses = sorted(r[0] for r in results)
    assert statuses.count(201) == 3, statuses
    assert statuses.count(409) == 21, statuses
    assert {r[1]['code'] for r in results if r[0] == 409} == {'SLOT_FULL'}
    item = env.slots.items[slot]
    assert item['booked_count'] == 3 and len(item['booking_refs']) == 3 and item['status'] == 'FULL'
    assert len(env.live()) == 3                                            # RDS agrees with DynamoDB
    winners = [r[1] for r in results if r[0] == 201]
    assert sorted(w['concurrency']['slotVersion'] for w in winners) == [1, 2, 3]   # each seat won a distinct version
    assert {bl.seat_ref(a['patient_id']) for a in env.live()} == item['booking_refs']
    # contention really happened and was retried (this is what the BookingConflictRetries metric shows)
    assert env.metric_sum('BookingConflictRetries') > 0
    assert env.slots.conflicts == env.metric_sum('BookingConflictRetries')
    assert env.metric_sum('BookingsConfirmed') == 3
    assert env.metric_sum('BookingsRejected', Reason='SLOT_FULL') == 21
    assert sum(1 for t, *_ in env.events.sent if t == 'AppointmentBooked') == 3


@pytest.mark.parametrize('capacity, clients', [(1, 20), (5, 30), (10, 40)])
def test_capacity_is_exact_for_any_seat_count(monkeypatch, capacity, clients):
    env = Env(monkeypatch, read_pause=0.003)
    slot = env.add_slot(DOCTOR, CAMP, TOMORROW_9, capacity=capacity, minutes=60)
    patients = [env.add_patient(2000 + i, 'Pat', f'N{"abcdefghijklmnopqrstuvwxyzabcdefghijklmn"[i]}') for i in range(clients)]
    gate = threading.Barrier(clients)

    def one(pid):
        gate.wait()
        return env.book(slot, user=pid)[0]

    with concurrent.futures.ThreadPoolExecutor(max_workers=clients) as pool:
        statuses = list(pool.map(one, patients))
    assert statuses.count(201) == capacity and statuses.count(409) == clients - capacity
    assert env.slots.items[slot]['booked_count'] == capacity == len(env.live())


def test_retries_are_bounded_and_end_in_503_with_retry_after(env, monkeypatch):
    """If every write loses (pathological contention) we stop after maxOccAttempts and say so - never spin forever."""
    slot = env.add_slot(DOCTOR, STD, TOMORROW_9)
    env.limits = {'maxOccAttempts': 4}
    real_update = env.slots.update_item

    def always_conflict(**kw):
        if 'booked_count' in kw.get('UpdateExpression', '') and 'ADD booking_refs' in kw['UpdateExpression']:
            env.slots.conflicts += 1
            raise ccf()
        return real_update(**kw)
    monkeypatch.setattr(env.slots, 'update_item', always_conflict)
    sleeps = []
    monkeypatch.setattr(bl.time, 'sleep', lambda s: sleeps.append(s))
    status, body, headers = env.book(slot)
    assert status == 503 and body['code'] == 'BOOKING_CONTENTION' and headers['Retry-After'] == '1'
    assert len(sleeps) == 3 and all(0 <= s <= 0.4 for s in sleeps)       # backoff between the 4 attempts
    assert env.metric_sum('BookingConflictRetries') == 4 and env.metric_sum('BookingRetriesExhausted') == 1
    assert env.live() == [] and 'booking_refs' not in env.slots.items[slot]
    # a 5xx is not stored: the same key may be retried and succeed
    monkeypatch.setattr(env.slots, 'update_item', real_update)


def test_backoff_is_full_jitter_exponential_and_capped():
    assert bl.backoff_delay(1, 15, 400, rng=lambda: 1.0) == pytest.approx(0.015)
    assert bl.backoff_delay(3, 15, 400, rng=lambda: 1.0) == pytest.approx(0.060)
    assert bl.backoff_delay(10, 15, 400, rng=lambda: 1.0) == pytest.approx(0.400)       # capped
    assert bl.backoff_delay(5, 15, 400, rng=lambda: 0.0) == 0


# =============================================================================================== MANDATORY TEST 2

def test_mandatory_2_idempotency_same_key_replayed_sequentially(env):
    slot = env.add_slot(DOCTOR, STD, TOMORROW_9)
    first = env.book(slot, key='replay-key-0001')
    assert first[0] == 201 and 'replayed' not in first[1]
    for _ in range(4):
        again = env.book(slot, key='replay-key-0001')
        assert again[0] == 201 and again[2].get('Idempotent-Replayed') == 'true' and again[1]['replayed'] is True
        assert again[1]['appointmentId'] == first[1]['appointmentId']
    assert len(env.db.appointments) == 1                                   # exactly one appointment
    assert env.slots.items[slot]['booked_count'] == 1                      # and exactly one seat
    assert env.metric_sum('IdempotentReplays') == 4


def test_mandatory_2_idempotency_same_key_replayed_in_parallel(monkeypatch):
    """10 copies of the same request at the same instant (a double-click storm): still one appointment, one seat."""
    env = Env(monkeypatch, read_pause=0.003)
    env.add_patient(21)
    slot = env.add_slot(DOCTOR, CAMP, TOMORROW_9, capacity=5, minutes=60)
    gate = threading.Barrier(10)

    def one(_):
        gate.wait()
        return env.book(slot, key='same-key-parallel')

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
        results = list(pool.map(one, range(10)))
    assert [r[0] for r in results] == [201] * 10
    assert len({r[1]['appointmentId'] for r in results}) == 1
    assert len(env.db.appointments) == 1 and env.slots.items[slot]['booked_count'] == 1


def test_after_the_24h_idempotency_record_expires_there_is_still_no_second_booking(env):
    """Once DynamoDB's TTL removes the record, UNIQUE(patient_id, slot_id, active_flag) still allows one seat."""
    slot = env.add_slot(DOCTOR, CAMP, TOMORROW_9, capacity=3, minutes=60)
    assert env.book(slot, key='ttl-expired-key')[0] == 201
    env.idem.items.clear()
    again = env.book(slot, key='ttl-expired-key')
    assert again[0] == 409 and again[1]['code'] == 'ALREADY_BOOKED'
    assert len(env.db.appointments) == 1 and env.slots.items[slot]['booked_count'] == 1


def test_idempotency_key_reused_for_a_different_request_is_422(env):
    a = env.add_slot(DOCTOR, STD, TOMORROW_9)
    b = env.add_slot(DOCTOR2, STD2, TOMORROW_9 + timedelta(hours=3))
    assert env.book(a, key='reuse-key-123')[0] == 201
    status, body, _ = env.book(b, key='reuse-key-123')
    assert status == 422 and body['code'] == 'IDEMPOTENCY_KEY_REUSED'
    assert len(env.db.appointments) == 1


def test_a_rejected_request_replays_the_same_rejection(env):
    slot = env.add_slot(DOCTOR, STD, TOMORROW_9)
    env.add_patient(22, 'Ravi', 'Kumar')
    assert env.book(slot, user=22)[0] == 201
    first = env.book(slot, key='full-key-0001')
    assert first[0] == 409 and first[1]['code'] == 'SLOT_FULL'
    again = env.book(slot, key='full-key-0001')
    assert again[0] == 409 and again[2]['Idempotent-Replayed'] == 'true' and again[1] == first[1]


def test_crashed_attempt_is_taken_over_without_a_second_seat(env, monkeypatch):
    """Lambda died after taking the seat but before writing RDS. A retry with the same key finishes the SAME seat."""
    slot = env.add_slot(DOCTOR, STD, TOMORROW_9)
    original_confirm = MemoryStore.confirm
    monkeypatch.setattr(MemoryStore, 'confirm', lambda *a, **k: (_ for _ in ()).throw(SystemExit('crash')))
    with pytest.raises(SystemExit):
        env.book(slot, key='crash-key-0001')
    assert env.slots.items[slot]['booked_count'] == 1                      # seat held by the dead attempt
    record = env.idem.items['booking#21#crash-key-0001']
    assert record['status'] == 'IN_PROGRESS' and record['slot_version'] == 1
    record['lock_expires_at'] = int(time.time()) - 1                       # its lock runs out
    monkeypatch.setattr(MemoryStore, 'confirm', original_confirm)
    status, body, _ = env.book(slot, key='crash-key-0001')
    assert status == 201 and body['concurrency']['slotVersion'] == 1
    assert env.slots.items[slot]['booked_count'] == 1 and len(env.db.appointments) == 1


def test_idempotency_key_is_required_and_validated(env):
    slot = env.add_slot(DOCTOR, STD, TOMORROW_9)
    s, b, _ = env.call('POST', '/patient/appointments', body={'slotId': slot})
    assert s == 400 and b['code'] == 'IDEMPOTENCY_KEY_REQUIRED'
    assert env.book(slot, key='short')[1]['code'] == 'IDEMPOTENCY_KEY_INVALID'
    assert env.book(slot, key='bad key with spaces!')[1]['code'] == 'IDEMPOTENCY_KEY_INVALID'
    assert env.db.appointments == []


def test_keys_are_scoped_per_patient(env):
    """Two patients may happen to use the same key string: they get two different bookings."""
    slot = env.add_slot(DOCTOR, CAMP, TOMORROW_9, capacity=3, minutes=60)
    env.add_patient(22, 'Ravi', 'Kumar')
    a, b = env.book(slot, user=21, key='shared-key-01'), env.book(slot, user=22, key='shared-key-01')
    assert a[0] == b[0] == 201 and a[1]['appointmentId'] != b[1]['appointmentId']


# =============================================================================================== booking validation

def test_booking_happy_path_persists_publishes_and_reports(env):
    slot = env.add_slot(DOCTOR, STD, TOMORROW_9)
    status, body, _ = env.book(slot)
    assert status == 201 and body['status'] == 'CONFIRMED' and body['fee'] == '499.99'
    assert body['reference'] == 'MS-%06d' % body['appointmentId'] and body['patientName'] == 'Meera Iyer'
    assert body['localStart'] == '09:00' and body['doctor']['name'] == 'Dr. Asha Rao'
    assert body['cancellation']['tier'] == 'PARTIAL'          # 23.5 h before start at NOW
    assert body['concurrency'] == {'slotVersion': 1, 'attempts': 1, 'conflictRetries': 0, 'capacity': 1}
    row = env.live()[0]
    assert (row['status'], row['active_flag'], row['reschedule_count'], row['fee_amount']) == ('CONFIRMED', 1, 0, Decimal('499.99'))
    kind, detail, source, bus = next(e for e in env.events.sent if e[0] == 'AppointmentBooked')
    assert detail['appointmentId'] == body['appointmentId'] and detail['eventId'] and source == 'medislot.booking'
    assert env.slots.items[slot]['status'] == 'FULL' and env.slots.items[slot]['booking_refs'] == {'p21'}


@pytest.mark.parametrize('body, code', [
    ({'slotId': 'not-a-slot'}, 'VALIDATION'), ({'slotId': 42}, 'VALIDATION'), ({}, 'VALIDATION'),
    ({'slotId': '7#32#2026-11-21T03:30:00Z', 'extra': 1}, 'VALIDATION'),
    ({'slotId': '7#32#2026-11-21T03:30:00Z', 'familyMemberId': 4}, 'VALIDATION'),
    ({'slotId': '7#32#2026-11-21T03:30:00Z', 'reason': 'x'}, 'VALIDATION'),
])
def test_malformed_requests_are_400(env, body, code):
    s, b, _ = env.call('POST', '/patient/appointments', body=body, headers={'Idempotency-Key': uuid.uuid4().hex})
    assert s == 400 and b['code'] == code


def test_slot_must_exist_be_in_the_future_and_open(env):
    missing = '7#32#2026-11-25T03:30:00Z'
    status, body, _ = env.book(missing)
    assert status == 404 and body['code'] == 'SLOT_NOT_FOUND'
    past = env.add_slot(DOCTOR, STD, NOW - timedelta(minutes=30))
    assert env.book(past)[1]['code'] == 'SLOT_IN_PAST'
    starting_now = env.add_slot(DOCTOR, STD, NOW)
    assert env.book(starting_now)[0] == 422                                   # the exact start instant counts as started
    closed = env.add_slot(DOCTOR, STD, TOMORROW_9 + timedelta(hours=1), status='CLOSED')
    assert env.book(closed)[0:1] == (409,) and env.book(closed)[1]['code'] == 'SLOT_CLOSED'
    blocked = env.add_slot(DOCTOR, STD, TOMORROW_9 + timedelta(hours=2))
    env.slots.items[blocked]['manual_block'] = True
    assert env.book(blocked)[1]['code'] == 'SLOT_CLOSED'
    env.limits = {'minLeadMinutes': 120}
    soon = env.add_slot(DOCTOR, STD, NOW + timedelta(minutes=90))
    assert env.book(soon)[1]['code'] == 'SLOT_IN_PAST'
    assert env.db.appointments == []


def test_slot_id_must_match_the_slot_item(env):
    slot = env.add_slot(DOCTOR, STD, TOMORROW_9)
    env.slots.items[slot]['doctor_id'] = '999'
    assert env.book(slot)[1]['code'] == 'SLOT_INVALID'


def test_inactive_consultation_or_missing_profile_cannot_book(env):
    slot = env.add_slot(DOCTOR, STD, TOMORROW_9)
    env.db.offers[(DOCTOR, STD)]['type_active'] = False
    assert env.book(slot)[1]['code'] == 'NOT_BOOKABLE'
    env.db.offers[(DOCTOR, STD)]['type_active'] = True
    env.db.users[30] = {'is_active': True}                                   # account without a profile row
    assert env.book(slot, user=30)[1]['code'] == 'PROFILE_REQUIRED'
    env.db.users[21]['is_active'] = False
    assert env.book(slot)[1]['code'] == 'ACCOUNT_INACTIVE'


def test_doctor_token_cannot_book_and_patient_token_cannot_cancel_slots(env):
    slot = env.add_slot(DOCTOR, STD, TOMORROW_9)
    s, b, _ = env.call('POST', '/patient/appointments', role='doctor', user=DOCTOR, body={'slotId': slot},
                       headers={'Idempotency-Key': 'doctor-key-01'})
    assert s == 403
    s, b, _ = env.call('POST', '/doctor/me/slot-cancellations', role='patient', body={'slotId': slot, 'reason': 'x'})
    assert s == 403


# =============================================================================================== patient limits

def test_same_person_cannot_take_two_seats_in_one_slot(env):
    slot = env.add_slot(DOCTOR, CAMP, TOMORROW_9, capacity=5, minutes=60)
    assert env.book(slot)[0] == 201
    s, b, _ = env.book(slot)                                                 # a new key: a genuinely new request
    assert s == 409 and b['code'] == 'ALREADY_BOOKED'
    assert env.slots.items[slot]['booked_count'] == 1                         # precheck stopped it before the seat


def test_patient_limits_overlap_one_per_doctor_per_day_and_upcoming_cap(env):
    env.limits = {'maxUpcomingPerPatient': 2}
    assert env.book(env.add_slot(DOCTOR, STD, TOMORROW_9))[0] == 201
    clash = env.add_slot(DOCTOR2, STD2, TOMORROW_9 + timedelta(minutes=15))          # same time, other doctor
    assert env.book(clash)[1]['code'] == 'OVERLAPPING_APPOINTMENT'
    same_day = env.add_slot(DOCTOR, STD, TOMORROW_9 + timedelta(hours=5))             # same doctor, same local day
    assert env.book(same_day)[1]['code'] == 'ONE_PER_DOCTOR_PER_DAY'
    assert env.book(env.add_slot(DOCTOR, STD, TOMORROW_9 + timedelta(days=1)))[0] == 201
    s, b, _ = env.book(env.add_slot(DOCTOR2, STD2, TOMORROW_9 + timedelta(days=2)))
    assert b['code'] == 'PATIENT_LIMIT_REACHED' and len(env.live()) == 2
    assert all(item.get('booked_count', 0) <= 1 for item in env.slots.items.values())   # rejected before the seat


def test_one_per_doctor_per_day_uses_the_doctors_local_day(env):
    """23:00 IST on the 21st and 00:30 IST on the 22nd are different local days, even though both are 21st UTC."""
    late = datetime(2026, 11, 21, 17, 30, tzinfo=UTC)                                # 23:00 IST, 21 Nov
    after_midnight = datetime(2026, 11, 21, 19, 0, tzinfo=UTC)                       # 00:30 IST, 22 Nov
    env.add_slot(DOCTOR, STD, late)
    assert env.book(env.add_slot(DOCTOR, STD, late))[0] == 201
    sid, item = make_slot(DOCTOR, STD, after_midnight)
    item['local_date'] = '2026-11-22'
    env.slots.items[sid] = item
    assert env.book(sid)[0] == 201


# =============================================================================================== listing / isolation

def test_listing_detail_and_isolation(env):
    env.add_patient(22, 'Ravi', 'Kumar')
    a = env.book(env.add_slot(DOCTOR, STD, TOMORROW_9))[1]
    env.book(env.add_slot(DOCTOR, STD, TOMORROW_9 + timedelta(days=1)))
    theirs = env.book(env.add_slot(DOCTOR2, STD2, TOMORROW_9), user=22)[1]
    s, page, _ = env.call('GET', '/patient/appointments', query={'view': 'upcoming', 'limit': '1'})
    assert s == 200 and len(page['items']) == 1 and page['page']['hasMore'] and page['items'][0]['appointmentId'] == a['appointmentId']
    s, page2, _ = env.call('GET', '/patient/appointments', query={'view': 'upcoming', 'limit': '1',
                                                                  'cursor': page['page']['nextCursor']})
    assert s == 200 and page2['items'][0]['appointmentId'] != a['appointmentId'] and not page2['page']['hasMore']
    assert env.call('GET', '/patient/appointments', query={'view': 'past', 'cursor': page['page']['nextCursor']})[0] == 400
    assert env.call('GET', f"/patient/appointments/{a['appointmentId']}")[0] == 200
    # another patient's appointment: 404, identical to a made-up id - nothing leaks
    other = env.call('GET', f"/patient/appointments/{theirs['appointmentId']}")
    assert other[0] == 404 and 'Ravi' not in json.dumps(other[1])
    made_up = env.call('GET', '/patient/appointments/999999')
    assert (made_up[0], made_up[1]['code'], made_up[1]['message']) == (other[0], other[1]['code'], other[1]['message'])
    assert env.call('POST', f"/patient/appointments/{theirs['appointmentId']}/cancel", body={})[0] == 404
    assert env.call('GET', '/patient/appointments', query={'view': 'nope'})[0] == 400


def test_doctor_sees_only_their_own_bookings(env):
    env.book(env.add_slot(DOCTOR, STD, TOMORROW_9))
    env.book(env.add_slot(DOCTOR2, STD2, TOMORROW_9 + timedelta(hours=2)))
    s, b, _ = env.call('GET', '/doctor/me/appointments', role='doctor', user=DOCTOR, query={'from': '2026-11-21'})
    assert s == 200 and len(b['items']) == 1 and b['items'][0]['patientName'] == 'Meera Iyer'
    assert list(b['bySlot'].values()) == [['Meera Iyer']]


# =============================================================================================== refund tiers (pure)

START = datetime(2026, 11, 25, 4, 30, tzinfo=UTC)
TINY = timedelta(microseconds=1)
POL = bl.parse_policy(POLICY, 7)


@pytest.mark.parametrize('before, tier', [
    (timedelta(days=10), 'FULL'),
    (timedelta(hours=24), 'FULL'),                    # exactly at the 24 h cutoff instant: still FULL
    (timedelta(hours=24) - TINY, 'PARTIAL'),          # one microsecond later
    (timedelta(hours=23, minutes=59, seconds=59), 'PARTIAL'),
    (timedelta(hours=6), 'PARTIAL'),                  # exactly at the 6 h cutoff instant: still PARTIAL
    (timedelta(hours=6) - TINY, 'LATE'),
    (timedelta(minutes=1), 'LATE'),
    (TINY, 'LATE'),                                   # the last possible instant to cancel
    (timedelta(0), None),                             # at the start instant: started, not cancellable
    (-timedelta(minutes=5), None),
])
def test_refund_tier_every_boundary(before, tier):
    found = bl.refund_tier(POL, START, START - before)
    assert (found['tier'] if found else None) == tier


@pytest.mark.parametrize('paid, pct, expected', [
    ('499.99', 100, '499.99'), ('499.99', 50, '250.00'),   # 249.995 -> 250.00 (half-up on whole paise)
    ('333.33', 50, '166.67'), ('333.33', 0, '0.00'), ('0.01', 50, '0.01'), ('0.01', 49, '0.00'),
    ('1000', 75, '750.00'), ('0', 100, '0.00'), ('12345678.91', 33, '4074074.04'),
])
def test_refund_amount_is_exact_in_paise(paid, pct, expected):
    amount = bl.refund_amount(Decimal(paid), pct)
    assert str(amount) == expected and Decimal('0') <= amount <= Decimal(paid).quantize(Decimal('0.01'))


def test_fractional_hour_windows_have_exact_cutoffs():
    policy = bl.parse_policy({'version': 'x', 'tiers': [{'tier': 'FULL', 'minHoursBefore': 1.5, 'refundPercent': 100},
                                                        {'tier': 'NONE', 'minHoursBefore': 0, 'refundPercent': 0}]})
    assert bl.refund_tier(policy, START, START - timedelta(minutes=90))['tier'] == 'FULL'
    assert bl.refund_tier(policy, START, START - timedelta(minutes=90) + TINY)['tier'] == 'NONE'


@pytest.mark.parametrize('raw', [
    None, {}, {'tiers': []},
    {'tiers': [{'tier': 'FULL', 'minHoursBefore': 24, 'refundPercent': 100}]},                       # no 0 h tier
    {'tiers': [{'tier': 'A', 'minHoursBefore': 0, 'refundPercent': 0}]},                             # bad name
    {'tiers': [{'tier': 'EARLY', 'minHoursBefore': 24, 'refundPercent': 50},
               {'tier': 'LATE', 'minHoursBefore': 0, 'refundPercent': 80}]},                         # later pays more
    {'tiers': [{'tier': 'X1', 'minHoursBefore': 0, 'refundPercent': 0}, {'tier': 'X1', 'minHoursBefore': 5, 'refundPercent': 0}]},
    {'tiers': [{'tier': 'XX', 'minHoursBefore': 0, 'refundPercent': 101}]},
    {'tiers': [{'tier': 'XX', 'minHoursBefore': True, 'refundPercent': 0}]},
    {'tiers': [{'tier': 'XX', 'minHoursBefore': 0, 'refundPercent': 50.5}]},
    {'tiers': [{'tier': 'XX', 'minHoursBefore': 6, 'refundPercent': 9}, {'tier': 'YY', 'minHoursBefore': 6, 'refundPercent': 0},
               {'tier': 'ZZ', 'minHoursBefore': 0, 'refundPercent': 0}]},                           # duplicate window
    {'tiers': [{'tier': 'DOCTOR_CANCELLED', 'minHoursBefore': 0, 'refundPercent': 0}]},              # reserved name
])
def test_invalid_policies_are_rejected(raw):
    with pytest.raises(bl.PolicyError):
        bl.parse_policy(raw)


def test_policy_order_in_ssm_does_not_matter_and_version_is_recorded():
    shuffled = {'version': 'v9', 'tiers': list(reversed(POLICY['tiers']))}
    policy = bl.parse_policy(shuffled, 12)
    assert [t['tier'] for t in policy['tiers']] == ['FULL', 'PARTIAL', 'LATE'] and policy['version'] == 'v9@ssm-v12'


def test_refund_schedule_lists_the_last_instant_of_each_tier():
    sched = bl.refund_schedule(POL, START, Decimal('400.00'))
    assert sched[0] == {'tier': 'FULL', 'refundPercent': 100, 'refundAmount': '400.00',
                        'cancelBy': '2026-11-24T04:30:00Z', 'cancelBefore': None}
    assert sched[1]['cancelBy'] == '2026-11-24T22:30:00Z' and sched[2]['cancelBefore'] == '2026-11-25T04:30:00Z'


# =============================================================================================== cancellation via the API

def booked(env, start=START, fee='400.00'):
    env.db.offers[(DOCTOR, STD)]['fee_amount'] = Decimal(fee)
    slot = env.add_slot(DOCTOR, STD, start)
    s, b, _ = env.book(slot)
    assert s == 201, b
    return slot, b['appointmentId']


@pytest.mark.parametrize('before, tier, amount', [
    (timedelta(hours=24), 'FULL', '400.00'), (timedelta(hours=24) - TINY, 'PARTIAL', '200.00'),
    (timedelta(hours=6), 'PARTIAL', '200.00'), (timedelta(hours=6) - TINY, 'LATE', '0.00'),
    (TINY, 'LATE', '0.00'),
])
def test_cancel_at_exact_cutoff_instants_records_the_right_ledger_row(env, before, tier, amount):
    slot, appt = booked(env)
    env.now = START - before
    quote = env.call('GET', f'/patient/appointments/{appt}/refund-quote')[1]
    assert quote['tier'] == tier and quote['refundAmount'] == amount
    s, b, _ = env.call('POST', f'/patient/appointments/{appt}/cancel', body={'reason': 'Feeling better'})
    assert s == 200 and b['status'] == 'CANCELLED_BY_PATIENT' and b['refund']['tier'] == tier
    assert b['refund']['refundAmount'] == amount and b['refund']['reason'] == 'PATIENT_CANCEL'
    row = env.db.ledger[0]
    assert len(env.db.ledger) == 1 and str(row['refund_amount']) == amount and row['fee_amount'] == Decimal('400.00')
    assert env.db.appointments[0]['active_flag'] is None
    assert env.metric_sum('CancellationsByTier', Tier=tier) == 1


def test_cancel_after_start_is_refused_and_nothing_changes(env):
    slot, appt = booked(env)
    env.now = START
    s, b, _ = env.call('POST', f'/patient/appointments/{appt}/cancel', body={})
    assert s == 422 and b['code'] == 'APPOINTMENT_STARTED'
    assert env.db.ledger == [] and env.live()[0]['appointment_id'] == appt
    assert env.slots.items[slot]['booked_count'] == 1


def test_cancel_frees_the_seat_publishes_events_and_is_repeat_safe(env):
    slot, appt = booked(env)
    env.now = START - timedelta(days=2)
    first = env.call('POST', f'/patient/appointments/{appt}/cancel', body={})
    assert first[0] == 200 and first[1]['alreadyCancelled'] is False
    item = env.slots.items[slot]
    assert item['booked_count'] == 0 and 'booking_refs' not in item and item['status'] == 'OPEN'
    kinds = [t for t, *_ in env.events.sent]
    assert kinds.count('AppointmentCancelled') == 1 and kinds.count('SlotSeatFreed') == 1
    freed = next(d for t, d, *_ in env.events.sent if t == 'SlotSeatFreed')
    assert freed['_refStillOnSlot'] is False and freed['bookable'] is True and freed['cause'] == 'PATIENT_CANCELLED'
    second = env.call('POST', f'/patient/appointments/{appt}/cancel', body={})
    assert second[0] == 200 and second[1]['alreadyCancelled'] is True and second[1]['refund'] == first[1]['refund']
    assert len(env.db.ledger) == 1 and env.slots.items[slot]['booked_count'] == 0      # no double refund / release
    # the freed seat can be booked again
    env.add_patient(22, 'Ravi', 'Kumar')
    assert env.book(slot, user=22)[0] == 201


def test_cancellation_fails_closed_when_the_ssm_policy_is_broken(env):
    slot, appt = booked(env)
    env.policy = {'tiers': [{'tier': 'FULL', 'minHoursBefore': 24, 'refundPercent': 100}]}     # no 0 h tier
    s, b, _ = env.call('POST', f'/patient/appointments/{appt}/cancel', body={})
    assert s == 503 and b['code'] == 'REFUND_POLICY_INVALID'
    assert env.db.ledger == [] and len(env.live()) == 1
    assert env.call('GET', '/patient/appointments')[1]['refundPolicyAvailable'] is False      # listing still works


def test_refund_ledger_endpoint_totals(env):
    _, a1 = booked(env)
    _, a2 = booked(env, start=START + timedelta(days=1), fee='250.50')
    env.now = START - timedelta(hours=10)
    env.call('POST', f'/patient/appointments/{a1}/cancel', body={})              # PARTIAL 200.00
    env.call('POST', f'/patient/appointments/{a2}/cancel', body={})              # FULL 250.50
    s, b, _ = env.call('GET', '/patient/refunds')
    assert s == 200 and b['count'] == 2 and b['totalRefunded'] == '450.50'
    assert {r['tier'] for r in b['items']} == {'PARTIAL', 'FULL'}


# =============================================================================================== doctor cancellation

def test_doctor_cancels_a_slot_and_every_patient_is_refunded_in_full(env):
    slot = env.add_slot(DOCTOR, CAMP, START, capacity=4, minutes=60)
    for pid in (21, 22, 23):
        env.add_patient(pid, 'P', f'N{pid}')
        assert env.book(slot, user=pid)[0] == 201
    env.now = START - timedelta(hours=1)                                        # inside the patients' 0% window
    s, b, _ = env.call('POST', '/doctor/me/slot-cancellations', role='doctor', user=DOCTOR,
                       body={'slotId': slot, 'reason': 'Emergency surgery'})
    assert s == 200 and b['appointmentsCancelled'] == 3 and b['totalRefunded'] == '900.00'
    assert {(r['reason'], r['tier'], r['refund_percent']) for r in env.db.ledger} == {('DOCTOR_CANCEL', 'DOCTOR_CANCELLED', 100)}
    item = env.slots.items[slot]
    assert item['status'] == 'CLOSED' and item['manual_block'] and item['booked_count'] == 0
    assert env.metric_sum('CancellationsByTier', Tier='DOCTOR_CANCELLED') == 3
    kinds = [t for t, *_ in env.events.sent]
    assert kinds.count('AppointmentCancelled') == 3 and kinds.count('SlotSeatFreed') == 3 and 'SlotCancelled' in kinds
    assert all(d['bookable'] is False for t, d, *_ in env.events.sent if t == 'SlotSeatFreed')
    env.add_patient(24, 'Late', 'Comer')
    assert env.book(slot, user=24)[1]['code'] == 'SLOT_CLOSED'                    # nobody can take the seats back
    # repeating the cancellation refunds nobody twice
    again = env.call('POST', '/doctor/me/slot-cancellations', role='doctor', user=DOCTOR,
                     body={'slotId': slot, 'reason': 'Emergency surgery'})
    assert again[1]['appointmentsCancelled'] == 0 and len(env.db.ledger) == 3


def test_doctor_cannot_cancel_someone_elses_or_a_started_slot(env):
    slot = env.add_slot(DOCTOR2, STD2, START)
    s, b, _ = env.call('POST', '/doctor/me/slot-cancellations', role='doctor', user=DOCTOR, body={'slotId': slot, 'reason': 'nope'})
    assert s == 400
    mine = env.add_slot(DOCTOR, STD, NOW - timedelta(minutes=5))
    assert env.call('POST', '/doctor/me/slot-cancellations', role='doctor', user=DOCTOR,
                    body={'slotId': mine, 'reason': 'late'})[1]['code'] == 'SLOT_IN_PAST'
    assert env.call('POST', '/doctor/me/slot-cancellations', role='doctor', user=DOCTOR,
                    body={'slotId': mine})[0] == 400                              # a reason is required


def test_booking_racing_a_doctor_cancellation_is_rolled_back(env, monkeypatch):
    """The seat was taken, then the doctor cancelled before the booking reached RDS: the seat is given back."""
    slot = env.add_slot(DOCTOR, STD, START)
    original = MemoryStore.confirm

    def doctor_wins(self, b, lim, now, slot_cancelled):
        env.slots.items[b['slot_id']]['cancelled_by_doctor'] = True          # doctor closed it meanwhile
        return original(self, b, lim, now, slot_cancelled)
    monkeypatch.setattr(MemoryStore, 'confirm', doctor_wins)
    s, body, _ = env.book(slot)
    assert s == 409 and body['code'] == 'SLOT_CANCELLED'
    assert env.slots.items[slot]['booked_count'] == 0 and env.db.appointments == []


# =============================================================================================== events + metrics

def test_event_bus_down_does_not_fail_the_booking(monkeypatch):
    env = Env(monkeypatch, events_fail=True)
    env.add_patient(21)
    monkeypatch.setattr(bl.time, 'sleep', lambda s: None)
    slot = env.add_slot(DOCTOR, STD, TOMORROW_9)
    assert env.book(slot)[0] == 201 and len(env.live()) == 1
    assert env.metric_sum('EventPublishFailures') == 1


def test_seat_sweeper_frees_a_seat_whose_release_was_missed(env, monkeypatch):
    slot, appt = booked(env)
    env.now = START - timedelta(days=2)
    monkeypatch.setattr(bl, 'free_seat', lambda *a, **k: [])                   # Lambda stopped after COMMIT
    env.call('POST', f'/patient/appointments/{appt}/cancel', body={})
    assert env.slots.items[slot]['booked_count'] == 1
    monkeypatch.undo()
    for name, value in (('slots_table', lambda: env.slots), ('events_client', lambda: env.events),
                        ('make_store', lambda c: MemoryStore(env.db, c)), ('config', lambda: {}),
                        ('dbconn', lambda c: Conn()), ('clock', lambda: env.now), ('emit', lambda *a, **k: None),
                        ('metric', env._metric)):
        monkeypatch.setattr(bl, name, value)
    first = bl.handler({'action': 'release-pending'}, None)['body']
    assert first == {'checked': 1, 'released': 1} and env.slots.items[slot]['booked_count'] == 0
    assert [t for t, *_ in env.events.sent].count('SlotSeatFreed') == 1
    assert bl.handler({'action': 'release-pending'}, None)['body'] == {'checked': 1, 'released': 0}   # idempotent


def test_metric_lines_are_valid_embedded_metric_format(capsys, monkeypatch):
    monkeypatch.setenv('ENVIRONMENT', 'dev')
    bl.metric('CancellationsByTier', 1, Tier='FULL')
    doc = json.loads(capsys.readouterr().out.strip())
    spec = doc['_aws']['CloudWatchMetrics'][0]
    assert spec['Namespace'] == 'MediSlot/Booking' and spec['Dimensions'] == [['Environment', 'Tier']]
    assert spec['Metrics'] == [{'Name': 'CancellationsByTier', 'Unit': 'Count'}]
    assert doc['Tier'] == 'FULL' and doc['Environment'] == 'dev' and doc['CancellationsByTier'] == 1


def test_reconcile_reports_and_releases_an_orphan_seat(env):
    slot, _ = booked(env)
    env.slots.items[slot]['capacity'] = 2
    env.slots.items[slot]['status'] = 'OPEN'
    bl.reserve_seat(env.slots, slot, 'p99', NOW, bl.DEFAULT_LIMITS, {'retries': 0})   # seat with no appointment
    report = bl.handler({'action': 'reconcile-slot', 'slotId': slot}, None)['body']
    assert report['orphanTags'] == ['p99'] and report['released'] == [] and not report['consistent']
    fixed = bl.handler({'action': 'reconcile-slot', 'slotId': slot, 'releaseOrphans': True}, None)['body']
    assert fixed['released'] == ['p99'] and env.slots.items[slot]['booking_refs'] == {'p21'}


# =============================================================================================== SQL shape (real Store)

class RecordingCursor:
    def __init__(self, answers):
        self.sql, self.answers, self.lastrowid, self.rows = [], answers, 77, []
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def execute(self, sql, args=()):
        s = ' '.join(sql.split())
        self.sql.append((s, tuple(args or ())))
        self.rows = next((rows for key, rows in self.answers if key in s), [])
    def fetchone(self): return self.rows[0] if self.rows else None
    def fetchall(self): return self.rows


class RecordingConn:
    def __init__(self, cur): self.cur, self.commits, self.rollbacks = cur, 0, 0
    def cursor(self): return self.cur
    def commit(self): self.commits += 1
    def rollback(self): self.rollbacks += 1


def test_confirm_locks_only_the_callers_account_and_shares_the_doctor_row():
    """No accidental serialisation: RDS never takes a lock that two DIFFERENT patients would queue on."""
    cur = RecordingCursor([('FROM users WHERE user_id', [{'user_id': 21}]), ('FROM appointments a WHERE a.patient_id', [{}])])
    store = bl.Store(RecordingConn(cur))
    booking = {'patient_id': 21, 'doctor_id': 7, 'consultation_type_id': 32, 'slot_id': '7#32#2026-11-21T03:30:00Z',
               'starts_at': TOMORROW_9, 'ends_at': TOMORROW_9 + timedelta(minutes=30), 'local_date': date(2026, 11, 21),
               'timezone': 'Asia/Kolkata', 'fee': Decimal('499.99')}
    checked = []
    appointment_id = store.confirm(booking, bl.DEFAULT_LIMITS, NOW, lambda: checked.append(1) or False)
    locks = [(s, a) for s, a in cur.sql if 'FOR UPDATE' in s or 'SHARE MODE' in s]
    assert locks == [('SELECT user_id FROM users WHERE user_id = %s AND is_active = TRUE FOR UPDATE', (21,)),
                     ('SELECT user_id FROM doctors WHERE user_id = %s LOCK IN SHARE MODE', (7,))]
    assert checked == [1]                                   # slot re-checked for a doctor cancellation under the lock
    insert = next((s, a) for s, a in cur.sql if s.startswith('INSERT INTO appointments'))
    assert "'CONFIRMED', 1" in insert[0] and insert[1][-1] == '499.99' and appointment_id == 77
    assert store.conn.commits == 1


def test_doctor_cancellation_takes_the_doctor_row_exclusively_and_writes_the_ledger():
    cur = RecordingCursor([("status = 'CONFIRMED' FOR UPDATE", [{'appointment_id': 5, 'patient_id': 21, 'doctor_id': 7,
                                                                  'slot_id': 's', 'slot_start_utc': START.replace(tzinfo=None),
                                                                  'fee_amount': Decimal('300.00')}])])
    rows = bl.Store(RecordingConn(cur)).cancel_slot(7, 's', NOW)
    assert cur.sql[0] == ('SELECT user_id FROM doctors WHERE user_id = %s FOR UPDATE', (7,))
    ledger = next((s, a) for s, a in cur.sql if s.startswith('INSERT INTO refund_ledger'))
    assert "'DOCTOR_CANCEL'" in ledger[0] and ledger[1] == (5, 21, 'DOCTOR_CANCELLED', '300.00', '300.00')
    assert rows[0]['refundAmount'] == '300.00'


# =============================================================================================== doctor writers + OCC

def test_schedule_resync_never_overwrites_a_slot_booked_after_it_was_read():
    """The doctor's resync read the slot unbooked, then a patient booked it, then resync tried to rewrite it."""
    table = FakeTable()
    sid, item = make_slot(DOCTOR, STD, TOMORROW_9)
    table.items[sid] = dict(item)
    stale = dict(item)                                                    # what the resync read
    stats = {'retries': 0, 'attempts': 0}
    bl.reserve_seat(table, sid, 'ref-1', NOW, bl.DEFAULT_LIMITS, stats)  # a patient books in between
    rewritten = dict(item, capacity=2, version=1)
    assert doctor_slots._put_if_unchanged(table, rewritten, stale) is False
    assert doctor_slots._delete_if_unchanged(table, stale) is False
    assert table.items[sid]['booked_count'] == 1 and table.items[sid]['booking_refs'] == {'ref-1'}


def test_manual_close_bumps_the_version_so_an_inflight_booking_retries(env):
    slot = env.add_slot(DOCTOR, STD, TOMORROW_9)
    seen = env.slots.items[slot]['version']
    env.slots.update_item(Key={'doctor_id': str(DOCTOR), 'slot_id': slot},
                          UpdateExpression='SET manual_block = :t, version = if_not_exists(version, :zero) + :one',
                          ExpressionAttributeValues={':t': True, ':one': 1, ':zero': 0})
    assert env.slots.items[slot]['version'] == seen + 1
    assert env.book(slot)[1]['code'] == 'SLOT_CLOSED'


def test_release_is_idempotent_and_keeps_a_closed_slot_closed():
    table = FakeTable()
    sid, item = make_slot(DOCTOR, CAMP, TOMORROW_9, capacity=3)
    table.items[sid] = item
    for ref in ('a', 'b'):
        bl.reserve_seat(table, sid, ref, NOW, bl.DEFAULT_LIMITS, {'retries': 0})
    table.items[sid]['status'] = 'CLOSED'
    assert bl.release_seat(table, sid, 'a') is True and bl.release_seat(table, sid, 'a') is False
    assert table.items[sid]['booked_count'] == 1 and table.items[sid]['status'] == 'CLOSED'


# =============================================================================================== LIVE (deployed API)

API = os.environ.get('MEDISLOT_API_URL', '').rstrip('/')
PASSWORD = os.environ.get('MEDISLOT_CI_PASSWORD', '')
live = pytest.mark.skipif(not API or not PASSWORD, reason='set MEDISLOT_API_URL and MEDISLOT_CI_PASSWORD to run live')
LIVE_PARALLEL = int(os.environ.get('MEDISLOT_PARALLEL', '24'))


def http(method, path, token=None, **kw):
    import requests
    headers = kw.pop('headers', {})
    if token:
        headers['Authorization'] = 'Bearer ' + token
    return requests.request(method, API + path, headers=headers, timeout=40, **kw)


def sign_in(email):
    res = http('POST', '/auth/login', json={'email': email, 'password': PASSWORD})
    return res.json() if res.ok else None


def body_of(res):
    """JSON body, or {'message': <text>} when API Gateway answered with something that is not JSON."""
    try:
        data = res.json()
        return data if isinstance(data, dict) else {'message': str(data)[:200]}
    except ValueError:
        return {'message': res.text[:200]}


def label(status, data):
    """'409 SLOT_FULL'. Our Lambda always sends a code on errors; one WITHOUT a code came from API Gateway itself."""
    if status in (200, 201):
        return f'{status} OK'
    return f"{status} {data.get('code') or 'GATEWAY: ' + str(data.get('message'))}"


def probe_patient(i):
    """Long-lived probe patients, registered on the first run and reused (no junk accounts per run)."""
    email = f'ci-m5-patient-{i:02d}@medislot-ci.example'
    session = sign_in(email)
    if not session:
        reg = http('POST', '/auth/register/patient', json={
            'email': email, 'password': PASSWORD, 'firstName': 'Probe', 'lastName': f'Patient{chr(65 + i % 26)}',
            'phone': '9000000002', 'dateOfBirth': '1990-01-01', 'gender': 'Female'})
        assert reg.status_code in (201, 409), reg.text
        session = sign_in(email)
    assert session and session['role'] == 'patient', email
    return {'id': int(session['userId']), 'token': session['accessToken']}


@pytest.fixture(scope='module')
def camp_doctor():
    """Probe doctor offering a capacity-3 Health Camp every day 07:00-21:00 (registered once, then reused)."""
    email = 'ci-probe-camp@medislot-ci.example'
    session = sign_in(email)
    if not session:
        hospital = http('GET', '/auth/hospitals').json()['items'][0]
        reg = http('POST', '/auth/register/doctor', json={
            'email': email, 'password': PASSWORD, 'firstName': 'Camp', 'lastName': 'Ciprobe', 'phone': '9000000003',
            'specialization': 'General Medicine', 'licenseNumber': 'ZZ-904403', 'hospitalId': hospital['hospital_id'],
            'qualification': 'MBBS', 'experienceYears': 9})
        assert reg.status_code == 201, reg.text
        session = sign_in(email)
    token = session['accessToken']
    doctor_id = int(http('GET', '/doctor/me', token).json()['user_id'])
    listing = http('GET', '/doctor/me/consultation-types', token).json()
    camp = next(c for c in listing['catalog'] if c['code'] == 'HEALTH_CAMP')
    mine = next((t for t in listing['items'] if t['consultation_id'] == camp['consultation_id']), None)
    offer = {'consultation_id': camp['consultation_id'], 'capacity': 3, 'fee_amount': '300.00'}
    if mine:
        assert http('PUT', f"/doctor/me/consultation-types/{mine['consultation_type_id']}", token, json=offer).ok
        type_id = mine['consultation_type_id']
    else:
        made = http('POST', '/doctor/me/consultation-types', token, json=offer)
        assert made.status_code in (200, 201), made.text
        type_id = made.json()['consultationTypeId']
    week = [{'day_of_week': d, 'consultation_type_id': type_id, 'start_time': '07:00', 'end_time': '21:00'} for d in range(7)]
    assert http('PUT', '/doctor/me/availability', token, json={'patterns': week}).ok
    return {'id': doctor_id, 'token': token, 'typeId': type_id}


@pytest.fixture(scope='module')
def probe_patients(camp_doctor):
    """Sign in the probe patients and cancel anything a previous (failed) run left booked with the probe doctor."""
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        patients = list(pool.map(probe_patient, range(LIVE_PARALLEL)))
    leftovers = 0
    for p in patients:
        mine = http('GET', '/patient/appointments', p['token'], params={'view': 'upcoming', 'limit': 50})
        for a in body_of(mine).get('items', []):
            if a['doctor']['doctorId'] == camp_doctor['id']:
                http('POST', f"/patient/appointments/{a['appointmentId']}/cancel", p['token'], json={'reason': 'CI leftover'})
                leftovers += 1
    if leftovers:
        print(f'\nLIVE: cancelled {leftovers} leftover booking(s) from an earlier run')
    return patients


def empty_camp_slot(doctor):
    """A future capacity-3 slot of the probe doctor with all 3 seats free (a different one for each test)."""
    soon = (datetime.now(UTC) + timedelta(days=2)).date()
    days = http('GET', f"/doctors/{doctor['id']}/slots", params={
        'from': soon.isoformat(), 'to': (soon + timedelta(days=6)).isoformat(),
        'consultationTypeId': str(doctor['typeId'])}).json()['days']
    free = [s['slotId'] for d in days for s in d['slots'] if s['freeSeats'] == 3 and s['capacity'] == 3]
    assert free, 'no empty capacity-3 slot found for the probe doctor'
    return random.choice(free)


def warm_up(patients):
    """Same number of parallel authenticated calls first, so the burst below is not measuring Lambda cold starts.
    If THIS already fails, the account cannot run that many Lambdas at once (see the assertion message)."""
    def ping(p):
        res = http('GET', '/patient/appointments', p['token'], params={'view': 'upcoming', 'limit': 1})
        return label(res.status_code, body_of(res)) if res.status_code != 200 else None
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(patients)) as pool:
        return [x for x in pool.map(ping, patients) if x]


CONCURRENCY_HINT = ('API Gateway answered some requests itself (no "code" in the body), so they never reached the '
                    'booking logic. Usual causes: the account Lambda concurrency limit is lower than the number of '
                    'parallel requests (check: aws lambda get-account-settings -> ConcurrentExecutions; new accounts '
                    'often have 10) or a cold-start timeout. Details: ')


@pytest.mark.live
@live
def test_live_mandatory_1_parallel_bookings_exactly_3_succeed(camp_doctor, probe_patients):
    slot_id = empty_camp_slot(camp_doctor)
    cold = warm_up(probe_patients)
    assert not cold, CONCURRENCY_HINT + str(collections.Counter(cold))
    gate = threading.Barrier(len(probe_patients))

    def one(p):
        gate.wait()
        res = http('POST', '/patient/appointments', p['token'], json={'slotId': slot_id},
                   headers={'Idempotency-Key': f'live-{uuid.uuid4().hex}'})
        return p, res.status_code, body_of(res)

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(probe_patients)) as pool:
        results = list(pool.map(one, probe_patients))
    won = [(p, b) for p, s, b in results if s == 201]
    try:
        outcome = collections.Counter(label(s, b) for _, s, b in results)
        retries = sum((b.get('concurrency') or {}).get('conflictRetries', 0) if s == 201 else b.get('conflictRetries', 0)
                      for _, s, b in results)
        print(f'\nLIVE slot {slot_id}: {dict(outcome)}; conflict retries seen: {retries}')
        gateway = [k for k in outcome if 'GATEWAY' in k]
        assert not gateway, CONCURRENCY_HINT + str({k: outcome[k] for k in gateway})
        assert outcome == {'201 OK': 3, '409 SLOT_FULL': len(results) - 3}, dict(outcome)
        versions = [b['concurrency']['slotVersion'] for _, b in won]
        assert len(set(versions)) == 3, versions                         # each seat won at its own version
    finally:
        for p, b in won:                                                    # always free the seats again
            http('POST', f"/patient/appointments/{b['appointmentId']}/cancel", p['token'], json={'reason': 'CI cleanup'})


@pytest.mark.live
@live
def test_live_mandatory_2_idempotency_replay_returns_the_original_appointment(camp_doctor, probe_patients):
    p, slot_id = probe_patients[0], empty_camp_slot(camp_doctor)
    key = f'live-replay-{uuid.uuid4().hex}'
    first = http('POST', '/patient/appointments', p['token'], json={'slotId': slot_id}, headers={'Idempotency-Key': key})
    assert first.status_code == 201, first.text
    appointment_id = first.json()['appointmentId']
    try:
        for _ in range(3):
            again = http('POST', '/patient/appointments', p['token'], json={'slotId': slot_id},
                         headers={'Idempotency-Key': key})
            assert again.status_code == 201 and again.headers.get('Idempotent-Replayed') == 'true', again.text
            assert again.json()['appointmentId'] == appointment_id
        mine = http('GET', '/patient/appointments', p['token'], params={'view': 'upcoming', 'limit': 50}).json()['items']
        assert sum(1 for a in mine if a['slotId'] == slot_id) == 1                    # exactly one appointment
        quote = http('GET', f'/patient/appointments/{appointment_id}/refund-quote', p['token'])
        assert quote.status_code == 200 and quote.json()['tier'] in ('FULL', 'PARTIAL'), quote.text
    finally:
        done = http('POST', f'/patient/appointments/{appointment_id}/cancel', p['token'], json={})
        assert done.status_code == 200 and done.json()['refund']['tier'] in ('FULL', 'PARTIAL'), done.text


# =============================================================================================== dashboard (Flask)

@pytest.fixture
def web(monkeypatch):
    """The Flask dashboard with requests faked: it must reach booking ONLY through API Gateway paths."""
    os.environ.setdefault('FLASK_SECRET_KEY', 'test-secret')
    os.environ.setdefault('API_BASE_URL', 'https://api.example.test/dev')
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'dashboard'))
    import app as dashboard
    calls, answers = [], {}

    class Res:
        def __init__(self, status, data=None):
            self.status_code, self._data, self.ok = status, data or {}, status < 400
        def json(self): return self._data

    def request(method, url, headers=None, json=None, params=None, timeout=None):
        path = url.split('/dev', 1)[1]
        calls.append((method, path, json, dict(headers or {}), params))
        status, data = answers.get((method, path), (200, {}))
        return Res(status, data)

    def get(url, params=None, timeout=None, **kw):
        path = url.split('/dev', 1)[1]
        calls.append(('GET', path, None, {}, params))
        status, data = answers.get(('GET', path), (200, {}))
        return Res(status, data)

    monkeypatch.setattr(dashboard.requests, 'request', request)
    monkeypatch.setattr(dashboard.requests, 'get', get)
    client = dashboard.app.test_client()
    return dashboard, client, calls, answers


def login_as(client, role):
    with client.session_transaction() as s:
        s.update(token='a1', refresh_token='r1', token_exp=9e12, role=role)


BOOKED_BODY = {'appointmentId': 9, 'reference': 'MS-000009', 'startsAt': '2026-11-21T03:30:00Z', 'timezone': 'Asia/Kolkata',
               'doctor': {'name': 'Dr. Asha Rao'}}


def test_dashboard_book_sends_the_page_key_and_a_double_submit_reuses_it(web):
    dashboard, client, calls, answers = web
    login_as(client, 'patient')
    answers[('POST', '/patient/appointments')] = (201, BOOKED_BODY)
    form = {'action': 'book', 'slot_id': '7#32#2026-11-21T03:30:00Z', 'doctor_id': '7', 'idem_key': 'web-' + 'a' * 32}
    first = client.post('/patient/dashboard', data=form)
    answers[('POST', '/patient/appointments')] = (201, dict(BOOKED_BODY, replayed=True))
    second = client.post('/patient/dashboard', data=form)                    # Back + Book again
    sent = [c for c in calls if c[:2] == ('POST', '/patient/appointments')]
    assert len(sent) == 2 and sent[0][3]['Idempotency-Key'] == sent[1][3]['Idempotency-Key'] == 'web-' + 'a' * 32
    assert sent[0][2] == {'slotId': '7#32#2026-11-21T03:30:00Z'}
    assert 'tab=appointments' in first.headers['Location']
    page = client.get(second.headers['Location']).get_data(as_text=True)
    assert 'same booking sent twice' in page and 'MS-000009' in page


def test_dashboard_slot_full_goes_back_to_the_doctor_page(web):
    dashboard, client, calls, answers = web
    login_as(client, 'patient')
    answers[('POST', '/patient/appointments')] = (409, {'code': 'SLOT_FULL', 'message': 'This slot is fully booked.'})
    res = client.post('/patient/dashboard', data={'action': 'book', 'slot_id': '7#32#2026-11-21T03:30:00Z',
                                                  'doctor_id': '7', 'idem_key': 'web-' + 'b' * 32})
    assert res.headers['Location'].endswith('/patient/doctors/7')
    assert 'last seat a moment before you' in client.get('/patient/dashboard?tab=refunds').get_data(as_text=True)
    bad = client.post('/patient/dashboard', data={'action': 'book', 'slot_id': 'x', 'doctor_id': '7', 'idem_key': 'k'})
    assert len([c for c in calls if c[:2] == ('POST', '/patient/appointments')]) == 1 and bad.status_code == 302


def test_dashboard_appointments_tab_shows_refund_windows_and_cancels(web):
    dashboard, client, calls, answers = web
    login_as(client, 'patient')
    appt = {'appointmentId': 9, 'reference': 'MS-000009', 'status': 'CONFIRMED', 'startsAt': '2026-11-25T04:30:00Z',
            'endsAt': '2026-11-25T05:00:00Z', 'timezone': 'Asia/Kolkata', 'fee': '400.00',
            'patientName': 'Meera Iyer', 'doctor': {'name': 'Dr. Asha Rao', 'hospital': {'name': 'CARE'}},
            'consultation': {'label': 'Standard consult'},
            'cancellation': {'cancellable': True, 'tier': 'FULL', 'refundPercent': 100, 'refundAmount': '400.00',
                             'schedule': bl.refund_schedule(POL, START, Decimal('400.00'))}}
    answers[('GET', '/patient/me')] = (200, {'firstName': 'Meera', 'missingFields': []})
    answers[('GET', '/patient/appointments')] = (200, {'items': [appt]})
    page = client.get('/patient/dashboard?tab=appointments').get_data(as_text=True)
    assert 'MS-000009' in page and 'Wed 25 Nov 2026, 10:00 AM' in page and 'until Tue 24 Nov, 10:00 AM' in page
    assert not [c for c in calls if c[1] == '/doctors']                     # no doctor search on this tab
    answers[('POST', '/patient/appointments/9/cancel')] = (200, {'reference': 'MS-000009', 'alreadyCancelled': False,
                                                                 'refund': {'refundAmount': '400.00', 'refundPercent': 100, 'tier': 'FULL'}})
    res = client.post('/patient/dashboard', data={'action': 'cancel_appointment', 'appointment_id': '9'})
    assert 'tab=appointments' in res.headers['Location']
    assert '₹400.00 (100% - full refund)' in client.get('/patient/dashboard?tab=refunds').get_data(as_text=True)


def test_dashboard_doctor_cancel_slot_requires_a_reason(web):
    dashboard, client, calls, answers = web
    login_as(client, 'doctor')
    client.post('/doctor/dashboard', data={'action': 'cancel_slot', 'slot_id': '7#32#2026-11-21T03:30:00Z', 'reason': ''})
    assert not [c for c in calls if c[1] == '/doctor/me/slot-cancellations']
    answers[('POST', '/doctor/me/slot-cancellations')] = (200, {'message': 'Slot cancelled. 2 patient(s) refunded in full.'})
    res = client.post('/doctor/dashboard', data={'action': 'cancel_slot', 'slot_id': '7#32#2026-11-21T03:30:00Z',
                                                 'reason': 'Emergency surgery'})
    sent = [c for c in calls if c[1] == '/doctor/me/slot-cancellations'][0]
    assert sent[2] == {'slotId': '7#32#2026-11-21T03:30:00Z', 'reason': 'Emergency surgery'} and 'tab=schedule' in res.headers['Location']
