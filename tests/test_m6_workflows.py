"""Milestone 6: reschedule saga, waitlists, hold offers, notifications, reminders, no-show sweeper.

Re-uses the Milestone 5 harness (real threads, fake DynamoDB that evaluates the exact expression strings the Lambda
sends, in-memory RDS with the same unique key). Adds:

  * CompositeTable   fake DynamoDB table with a sort key, Query and GSIs (Waitlist, HoldOffers)
  * run_state_machine  a small Amazon States Language interpreter. It executes the REAL definition in
                       statemachines/reschedule.asl.json (Pass, Task, Retry, Catch, ResultSelector, ResultPath,
                       Succeed, Fail) and calls the booking Lambda handler for every Task, exactly like
                       Step Functions does with arn:aws:states:::lambda:invoke.

      MANDATORY TEST 3  test_mandatory_3_reschedule_saga_release_failure_patient_holds_exactly_one_appointment
      MANDATORY TEST 4  test_mandatory_4_waitlist_exclusivity_one_seat_three_patients_one_live_offer
"""
import concurrent.futures
import copy
import importlib.util
import json
import os
import threading
import time
import uuid
from datetime import timedelta

import pytest

import test_m5_booking as m5
from botocore.exceptions import ClientError

bl = m5.bl
wf = bl.workflows
ROOT = os.path.join(os.path.dirname(__file__), '..')
ASL_PATH = os.path.join(ROOT, 'statemachines', 'reschedule.asl.json')

_spec = importlib.util.spec_from_file_location('notifier_lambda', os.path.join(ROOT, 'lambdas', 'notifier', 'lambda_function.py'))
notifier = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(notifier)

NOW = m5.NOW                                     # 2026-11-20 04:00 UTC (09:30 IST)
DAY2 = NOW + timedelta(days=2)
RULES = dict(wf.DEFAULT_RULES)


# =============================================================================================== fake tables

class CompositeTable:
    """Fake DynamoDB table with hash + range key, Query (primary or GSI), per-item atomic conditional writes."""

    def __init__(self, hash_key, range_key=None, indexes=None):
        self.h, self.r, self.indexes = hash_key, range_key, indexes or {}
        self.items, self.lock = {}, threading.RLock()

    def _k(self, key):
        return (key[self.h], key[self.r]) if self.r else key[self.h]

    @staticmethod
    def _copy(item):
        return {k: (set(v) if isinstance(v, set) else v) for k, v in item.items()}

    def get_item(self, Key, ConsistentRead=False, **kw):
        with self.lock:
            item = self.items.get(self._k(Key))
            return {'Item': self._copy(item)} if item else {}

    def _check(self, item, cond, names, values):
        if cond and not m5.Cond(cond, names, values, item or {}).run():
            raise m5.ccf()

    def put_item(self, Item, ConditionExpression=None, ExpressionAttributeNames=None, ExpressionAttributeValues=None):
        m5.validate_placeholders(ConditionExpression, names=ExpressionAttributeNames, values=ExpressionAttributeValues)
        with self.lock:
            self._check(self.items.get(self._k(Item)), ConditionExpression, ExpressionAttributeNames,
                        ExpressionAttributeValues)
            self.items[self._k(Item)] = self._copy(Item)

    def update_item(self, Key, UpdateExpression, ConditionExpression=None, ExpressionAttributeNames=None,
                    ExpressionAttributeValues=None, **kw):
        m5.validate_placeholders(UpdateExpression, ConditionExpression, names=ExpressionAttributeNames,
                                 values=ExpressionAttributeValues)
        with self.lock:
            current = self.items.get(self._k(Key))
            self._check(current, ConditionExpression, ExpressionAttributeNames, ExpressionAttributeValues)
            item = self._copy(current) if current else dict(Key)
            m5.apply_update(item, UpdateExpression, ExpressionAttributeNames, ExpressionAttributeValues)
            self.items[self._k(Key)] = item

    def delete_item(self, Key, ConditionExpression=None, ExpressionAttributeNames=None, ExpressionAttributeValues=None):
        m5.validate_placeholders(ConditionExpression, names=ExpressionAttributeNames, values=ExpressionAttributeValues)
        with self.lock:
            self._check(self.items.get(self._k(Key)), ConditionExpression, ExpressionAttributeNames,
                        ExpressionAttributeValues)
            self.items.pop(self._k(Key), None)

    def query(self, KeyConditionExpression, ExpressionAttributeValues=None, ExpressionAttributeNames=None,
              IndexName=None, Limit=None, ConsistentRead=False, ExclusiveStartKey=None, **kw):
        assert isinstance(KeyConditionExpression, str), 'use string key conditions'
        if IndexName:
            assert not ConsistentRead, 'GSIs do not support consistent reads'
        hk, rk = self.indexes[IndexName] if IndexName else (self.h, self.r)
        m5.validate_placeholders(KeyConditionExpression, names=ExpressionAttributeNames, values=ExpressionAttributeValues)
        with self.lock:
            found = [self._copy(i) for i in self.items.values()
                     if hk in i and (not rk or rk in i)
                     and m5.Cond(KeyConditionExpression, ExpressionAttributeNames, ExpressionAttributeValues, i).run()]
        found.sort(key=lambda i: i.get(rk, '') if rk else '')
        return {'Items': found[:Limit] if Limit else found}


class FakeSNS:
    def __init__(self, fail_times=0):
        self.messages, self.fail_times, self.lock = [], fail_times, threading.Lock()

    def publish(self, **kw):
        with self.lock:
            if self.fail_times:
                self.fail_times -= 1
                raise RuntimeError('SNS unavailable')
            self.messages.append(kw)
            return {'MessageId': uuid.uuid4().hex}


# =============================================================================================== ASL interpreter

def jget(data, path):
    if path == '$':
        return data
    cur = data
    for part in path[2:].split('.'):
        cur = cur[part]
    return cur


def jset(data, path, value):
    if path is None:
        return data
    if path == '$':
        return value
    out = copy.deepcopy(data)
    cur = out
    parts = path[2:].split('.')
    for part in parts[:-1]:
        cur = cur.setdefault(part, {})
    cur[parts[-1]] = value
    return out


def resolve_params(template, data):
    if isinstance(template, dict):
        out = {}
        for key, value in template.items():
            if key.endswith('.$'):
                out[key[:-2]] = jget(data, value)
            else:
                out[key] = resolve_params(value, data)
        return out
    return template


def error_matches(patterns, name):
    return 'States.ALL' in patterns or name in patterns


def run_state_machine(definition, event, invoke):
    """Execute an ASL definition. Returns (status, output, history) with history = [(state, outcome)]."""
    state_name, data, history = definition['StartAt'], event, []
    while True:
        state = definition['States'][state_name]
        kind = state['Type']
        if kind == 'Pass':
            data = jset(data, state.get('ResultPath', '$'), resolve_params(state['Parameters'], data)) \
                if 'Parameters' in state else data
            history.append((state_name, 'pass'))
        elif kind == 'Task':
            params = resolve_params(state['Parameters'], data)
            attempts, caught = {}, None
            while True:
                try:
                    payload = invoke(params['Payload'])
                    result = {'Payload': payload, 'StatusCode': 200}
                    if 'ResultSelector' in state:
                        result = resolve_params(state['ResultSelector'], result)
                    data = jset(data, state.get('ResultPath', '$'), result)
                    history.append((state_name, 'ok'))
                    break
                except Exception as exc:
                    name = type(exc).__name__
                    index = next((i for i, r in enumerate(state.get('Retry', []))
                                  if error_matches(r['ErrorEquals'], name)), None)
                    if index is not None and attempts.get(index, 0) < state['Retry'][index].get('MaxAttempts', 3):
                        attempts[index] = attempts.get(index, 0) + 1
                        history.append((state_name, f'retry:{name}'))
                        continue
                    catch = next((c for c in state.get('Catch', []) if error_matches(c['ErrorEquals'], name)), None)
                    history.append((state_name, f'error:{name}'))
                    if not catch:
                        return 'FAILED', {'Error': name, 'Cause': str(exc)}, history
                    error = {'Error': name, 'Cause': json.dumps({'errorMessage': str(exc), 'errorType': name})}
                    data = jset(data, catch.get('ResultPath', '$'), error)
                    caught = catch['Next']
                    break
            if caught:
                state_name = caught
                continue
        elif kind == 'Succeed':
            history.append((state_name, 'succeed'))
            return 'SUCCEEDED', data, history
        elif kind == 'Fail':
            history.append((state_name, 'fail'))
            return 'FAILED', {'Error': state['Error'], 'Cause': state.get('Cause')}, history
        else:
            raise AssertionError(f'unsupported state type {kind}')
        if state.get('End'):
            return 'SUCCEEDED', data, history
        state_name = state['Next']


def load_definition(function_arn='arn:aws:lambda:ap-south-1:000000000000:function:medislot-dev-booking'):
    with open(ASL_PATH) as fh:
        return json.loads(fh.read().replace('${BookingFunctionArn}', function_arn))


# =============================================================================================== in-memory RDS (M6 methods)

class MemoryStore6(m5.MemoryStore):
    def _limit_problem_ex(self, b, lim, now, exclude):
        n = bl.naive(now)
        day_start, day_end = [bl.naive(x) for x in bl.local_day_bounds(b['local_date'], b['timezone'])]
        mine = [a for a in self._live() if a['patient_id'] == b['patient_id'] and a['appointment_id'] != exclude
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

    def reschedule_precheck(self, b, lim, now, appointment_id):
        with self.db.lock:
            return self._limit_problem_ex(b, lim, now, appointment_id)

    def move_appointment(self, appointment_id, from_slot, b, lim, now, slot_cancelled):
        with self.db.row_lock(('user', b['patient_id'])):
            with self.db.lock:
                a = next((a for a in self.db.appointments if a['appointment_id'] == appointment_id
                          and a['patient_id'] == b['patient_id']), None)
                if a and a['slot_id'] == b['slot_id']:
                    return 'already'
                if not a or a['status'] != 'CONFIRMED' or a['slot_id'] != from_slot:
                    raise bl.ApiError(409, 'APPOINTMENT_CHANGED', 'changed')
                if slot_cancelled():
                    raise bl.ApiError(409, 'SLOT_CANCELLED', 'cancelled')
                problem = self._limit_problem_ex(b, lim, now, appointment_id)
                if problem:
                    raise problem
                a.update(slot_id=b['slot_id'], slot_start_utc=bl.naive(b['starts_at']),
                         slot_end_utc=bl.naive(b['ends_at']), reschedule_count=a['reschedule_count'] + 1)
                return 'moved'

    def unmove_appointment(self, appointment_id, to_slot, original):
        with self.db.lock:
            a = next((a for a in self.db.appointments if a['appointment_id'] == appointment_id), None)
            if not a or a['slot_id'] != to_slot:
                return 'already'
            a.update(slot_id=original['slot_id'], slot_start_utc=bl.naive(original['starts_at']),
                     slot_end_utc=bl.naive(original['ends_at']), reschedule_count=max(a['reschedule_count'], 1) - 1)
            return 'reverted'

    def patient_lock(self, patient_id):
        return self.db.row_lock(('user', patient_id))

    def upcoming_confirmed(self, start, end, limit=1000):
        with self.db.lock:
            return [self._joined(a) for a in sorted(self.db.appointments, key=lambda a: a['slot_start_utc'])
                    if a['status'] == 'CONFIRMED' and bl.naive(start) < a['slot_start_utc'] <= bl.naive(end)][:limit]

    def stale_confirmed(self, cutoff, limit=200):
        with self.db.lock:
            return [self._joined(a) for a in self.db.appointments
                    if a['status'] == 'CONFIRMED' and a['slot_end_utc'] <= bl.naive(cutoff)][:limit]

    def mark_no_show(self, appointment_id, cutoff):
        with self.db.lock:
            a = next((a for a in self.db.appointments if a['appointment_id'] == appointment_id), None)
            if a and a['status'] == 'CONFIRMED' and a['slot_end_utc'] <= bl.naive(cutoff):
                a.update(status='NO_SHOW', active_flag=None)
                return True
            return False


# =============================================================================================== environment

class Events6(m5.FakeEvents):
    """EventBridge: records every event; RescheduleRequested starts the saga (like the EventBridge -> Step Functions
    rule); SlotSeatFreed is queued for the waitlist-promotions consumer; notification events for the notifier."""

    def __init__(self, env):
        super().__init__(env.slots)
        self.env, self.executions, self.promotion_queue = env, [], []

    def put_events(self, Entries):
        result = super().put_events(Entries)
        for e in Entries:
            event = {'detail-type': e['DetailType'], 'source': e['Source'], 'detail': json.loads(e['Detail'])}
            if e['DetailType'] == 'RescheduleRequested':
                status, output, history = run_state_machine(load_definition(), event, self.env.invoke)
                self.executions.append({'status': status, 'output': output, 'history': history})
            elif e['DetailType'] == 'SlotSeatFreed':
                self.promotion_queue.append(event)
        return result


class Env6(m5.Env):
    def __init__(self, monkeypatch, rules=None):
        super().__init__(monkeypatch)
        self.waitlist = CompositeTable('slot_id', 'waitlist_entry', {'GSI_PatientWaitlists': ('patient_id', 'joined_at')})
        self.offers = CompositeTable('slot_id', 'offer_id', {'GSI_OfferExpiry': ('status', 'expires_at')})
        self.notify_log = m5.FakeTable(key='dispatch_key')
        self.events = Events6(self)
        self.rules = dict(RULES, **(rules or {}))
        mp = monkeypatch
        mp.setattr(bl, 'events_client', lambda: self.events)
        mp.setattr(bl, 'make_store', lambda conn: MemoryStore6(self.db, conn))
        mp.setattr(wf, 'waitlist_table', lambda: self.waitlist)
        mp.setattr(wf, 'offers_table', lambda: self.offers)
        mp.setattr(wf, 'notification_log_table', lambda: self.notify_log)
        mp.setattr(wf, 'rules', lambda: dict(self.rules))
        mp.setattr(wf, 'RESCHEDULE_WAIT_SECONDS', 0.5)
        mp.setattr(wf.time, 'sleep', lambda s: None)

    def invoke(self, payload):
        """Step Functions lambda:invoke -> the booking Lambda handler (errors propagate, as in AWS)."""
        return bl.handler(copy.deepcopy(payload), None)

    def drain_promotions(self):
        while self.events.promotion_queue:
            event = self.events.promotion_queue.pop(0)
            result = bl.handler({'Records': [{'messageId': uuid.uuid4().hex, 'body': json.dumps(event)}]}, None)
            assert result['batchItemFailures'] == []

    def offers_by_status(self, status='PENDING'):
        with self.offers.lock:
            return [dict(o) for o in self.offers.items.values() if o['status'] == status]

    def sent(self, kind):
        return [d for t, d, *_ in self.events.sent if t == kind]

    def slot(self, slot_id):
        return self.slots.items[slot_id]


@pytest.fixture
def env6(monkeypatch):
    e = Env6(monkeypatch)
    for pid in (21, 22, 23, 24, 25, 26, 27):
        e.add_patient(pid)
    return e


def booked(env, slot_id, user=21):
    status, body, _ = env.book(slot_id, user=user)
    assert status == 201, body
    return body


def full_slot_with_waitlist(env, patients=(22, 23, 24), capacity=1):
    slot = env.add_slot(m5.DOCTOR, m5.STD, DAY2, capacity=capacity)
    holders = [21] + [p for p in (25, 26, 27)][:capacity - 1]
    appointments = [booked(env, slot, user=p) for p in holders]
    for pid in patients:
        status, body, _ = env.call('POST', '/patient/waitlists', user=pid, body={'slotId': slot})
        assert status == 201, body
    return slot, appointments


# =============================================================================================== MANDATORY TEST 3

@pytest.mark.parametrize('run', range(3))
def test_mandatory_3_reschedule_saga_release_failure_patient_holds_exactly_one_appointment(env6, monkeypatch, run):
    """A failure is forced while the saga releases the ORIGINAL seat (after the target seat was claimed and the
    appointment moved). The real state machine must run its compensation branch, and afterwards the patient holds
    exactly one appointment - the original one - and exactly one seat."""
    monkeypatch.setenv('FAULT_INJECTION', 'enabled')
    original = env6.add_slot(m5.DOCTOR, m5.STD, DAY2)
    target = env6.add_slot(m5.DOCTOR, m5.STD, DAY2 + timedelta(hours=2))
    appointment = booked(env6, original)

    status, body, _ = env6.call('POST', f"/patient/appointments/{appointment['appointmentId']}/reschedule",
                                body={'slotId': target}, headers={'X-Fault-Injection': 'release'})

    assert status == 409 and body['status'] == 'COMPENSATED', body
    assert 'SeatReleaseFailed' in body['reason']
    execution = env6.events.executions[-1]
    path = [name for name, outcome in execution['history'] if outcome in ('ok', 'succeed', 'fail', 'pass')]
    assert path == ['Prepare', 'ClaimTargetSeat', 'CommitReschedule', 'CompensateRevertAppointment',
                    'CompensateReleaseTargetSeat', 'RecordCompensated', 'RescheduleCompensated']
    assert [o for n, o in execution['history'] if n == 'ReleaseOriginalSeat'] == \
        ['retry:SeatReleaseFailed', 'retry:SeatReleaseFailed', 'error:SeatReleaseFailed']   # retried, then compensated
    live = env6.live()
    assert len(live) == 1                                                    # exactly ONE appointment
    assert live[0]['appointment_id'] == appointment['appointmentId'] and live[0]['slot_id'] == original
    assert live[0]['reschedule_count'] == 0
    ref = bl.seat_ref(21)
    assert ref in env6.slot(original)['booking_refs'] and env6.slot(original)['booked_count'] == 1
    assert ref not in env6.slot(target).get('booking_refs', set()) and env6.slot(target)['booked_count'] == 0
    assert env6.metric_sum('ReschedulesCompensated') == 1
    assert env6.sent('AppointmentRescheduled') == []
    assert bl.reconcile_slot(MemoryStore6(env6.db), original)['consistent']
    assert bl.reconcile_slot(MemoryStore6(env6.db), target)['consistent']
    # the lock is released: a new reschedule of the same appointment is possible again
    status, body, _ = env6.call('POST', f"/patient/appointments/{appointment['appointmentId']}/reschedule",
                                body={'slotId': target})
    assert status == 200 and body['status'] == 'SUCCEEDED', body


def test_saga_failure_at_commit_releases_the_target_seat_only(env6, monkeypatch):
    monkeypatch.setenv('FAULT_INJECTION', 'enabled')
    original = env6.add_slot(m5.DOCTOR, m5.STD, DAY2)
    target = env6.add_slot(m5.DOCTOR, m5.STD, DAY2 + timedelta(hours=2))
    appointment = booked(env6, original)
    status, body, _ = env6.call('POST', f"/patient/appointments/{appointment['appointmentId']}/reschedule",
                                body={'slotId': target}, headers={'X-Fault-Injection': 'commit'})
    assert status == 409 and body['status'] == 'COMPENSATED'
    path = [n for n, o in env6.events.executions[-1]['history'] if o in ('ok', 'fail')]
    assert 'CompensateRevertAppointment' not in path and 'CompensateReleaseTargetSeat' in path
    assert [a['slot_id'] for a in env6.live()] == [original]
    assert env6.slot(target)['booked_count'] == 0 and env6.slot(original)['booked_count'] == 1


def test_fault_injection_header_is_ignored_unless_enabled(env6, monkeypatch):
    monkeypatch.delenv('FAULT_INJECTION', raising=False)
    original = env6.add_slot(m5.DOCTOR, m5.STD, DAY2)
    target = env6.add_slot(m5.DOCTOR, m5.STD, DAY2 + timedelta(hours=2))
    appointment = booked(env6, original)
    status, body, _ = env6.call('POST', f"/patient/appointments/{appointment['appointmentId']}/reschedule",
                                body={'slotId': target}, headers={'X-Fault-Injection': 'release'})
    assert status == 200 and body['status'] == 'SUCCEEDED'


def test_reschedule_success_moves_appointment_and_seats_and_publishes(env6):
    original = env6.add_slot(m5.DOCTOR, m5.STD, DAY2)
    target = env6.add_slot(m5.DOCTOR, m5.STD, DAY2 + timedelta(hours=2))
    appointment = booked(env6, original)
    status, body, _ = env6.call('POST', f"/patient/appointments/{appointment['appointmentId']}/reschedule",
                                body={'slotId': target})
    assert status == 200 and body['status'] == 'SUCCEEDED'
    assert body['appointment']['slotId'] == target and body['appointment']['rescheduleCount'] == 1
    assert env6.events.executions[-1]['status'] == 'SUCCEEDED'
    assert env6.slot(original)['booked_count'] == 0 and env6.slot(target)['booked_count'] == 1
    moved = env6.sent('AppointmentRescheduled')
    assert len(moved) == 1 and moved[0]['fromSlotId'] == original and moved[0]['rescheduleCount'] == 1
    assert [d['slotId'] for d in env6.sent('SlotSeatFreed')] == [original]
    status, again, _ = env6.call('GET', f"/patient/reschedules/{body['requestId']}")
    assert status == 200 and again['status'] == 'SUCCEEDED'
    assert env6.call('GET', f"/patient/reschedules/{body['requestId']}", user=22)[0] == 404     # not yours


def test_reschedule_rules_cutoff_attempt_limit_and_same_consultation(env6):
    env6.rules['maxReschedules'] = 1
    near = env6.add_slot(m5.DOCTOR, m5.STD, NOW + timedelta(hours=5))            # inside the 6-hour cutoff
    a_near = booked(env6, near)
    other = env6.add_slot(m5.DOCTOR, m5.STD, DAY2)
    s, b, _ = env6.call('POST', f"/patient/appointments/{a_near['appointmentId']}/reschedule", body={'slotId': other})
    assert s == 422 and b['code'] == 'RESCHEDULE_CUTOFF'

    first = env6.add_slot(m5.DOCTOR, m5.STD, DAY2 + timedelta(days=1))
    second = env6.add_slot(m5.DOCTOR, m5.STD, DAY2 + timedelta(days=1, hours=2))
    a = booked(env6, first, user=22)
    assert env6.call('POST', f"/patient/appointments/{a['appointmentId']}/reschedule", user=22,
                     body={'slotId': second})[0] == 200
    s, b, _ = env6.call('POST', f"/patient/appointments/{a['appointmentId']}/reschedule", user=22,
                        body={'slotId': first})
    assert s == 422 and b['code'] == 'RESCHEDULE_LIMIT'

    camp = env6.add_slot(m5.DOCTOR, m5.CAMP, DAY2 + timedelta(days=3), capacity=3, minutes=60)
    c = booked(env6, env6.add_slot(m5.DOCTOR, m5.STD, DAY2 + timedelta(days=4)), user=23)
    s, b, _ = env6.call('POST', f"/patient/appointments/{c['appointmentId']}/reschedule", user=23,
                        body={'slotId': camp})
    assert s == 422 and b['code'] == 'DIFFERENT_CONSULTATION'
    s, b, _ = env6.call('POST', f"/patient/appointments/{c['appointmentId']}/reschedule", user=24,
                        body={'slotId': camp})
    assert s == 404                                                           # someone else's appointment


def test_reschedule_into_a_full_slot_is_rejected_and_nothing_changes(env6):
    original = env6.add_slot(m5.DOCTOR, m5.STD, DAY2)
    target = env6.add_slot(m5.DOCTOR, m5.STD, DAY2 + timedelta(hours=2))
    appointment = booked(env6, original)
    booked(env6, target, user=22)
    s, b, _ = env6.call('POST', f"/patient/appointments/{appointment['appointmentId']}/reschedule",
                        body={'slotId': target})
    assert s == 409 and b['code'] == 'SLOT_FULL'                             # refused before the saga starts
    assert env6.events.executions == []


def test_saga_claim_race_lost_ends_rejected_without_compensation(env6):
    original = env6.add_slot(m5.DOCTOR, m5.STD, DAY2)
    target = env6.add_slot(m5.DOCTOR, m5.STD, DAY2 + timedelta(hours=2))
    appointment = booked(env6, original)
    real_put = env6.events.put_events

    def someone_books_first(Entries):
        if any(e['DetailType'] == 'RescheduleRequested' for e in Entries):
            booked(env6, target, user=22)                                     # between validation and the saga
        return real_put(Entries)

    env6.events.put_events = someone_books_first
    s, b, _ = env6.call('POST', f"/patient/appointments/{appointment['appointmentId']}/reschedule",
                        body={'slotId': target})
    assert s == 409 and b['status'] == 'REJECTED' and 'SLOT_FULL' in b['reason']
    assert [a['slot_id'] for a in env6.live() if a['patient_id'] == 21] == [original]
    assert env6.metric_sum('ReschedulesRejected') == 1


def test_saga_steps_are_idempotent_when_step_functions_retries_them(env6):
    original = env6.add_slot(m5.DOCTOR, m5.STD, DAY2)
    target = env6.add_slot(m5.DOCTOR, m5.STD, DAY2 + timedelta(hours=2))
    appointment = booked(env6, original)
    calls, once = [], env6.invoke

    def twice(payload):                                                       # every task delivered twice
        calls.append(payload['step'])
        once(payload)
        return once(payload)

    env6.invoke = twice
    s, b, _ = env6.call('POST', f"/patient/appointments/{appointment['appointmentId']}/reschedule",
                        body={'slotId': target})
    assert s == 200 and b['status'] == 'SUCCEEDED'
    assert env6.slot(target)['booked_count'] == 1 and env6.slot(original)['booked_count'] == 0
    assert [a['reschedule_count'] for a in env6.live()] == [1]
    assert len(env6.sent('AppointmentRescheduled')) == 1


# =============================================================================================== MANDATORY TEST 4

@pytest.mark.parametrize('run', range(3))
def test_mandatory_4_waitlist_exclusivity_one_seat_three_patients_one_live_offer(env6, run):
    """One seat is freed while three patients wait. Many promoters run at once (duplicate SQS deliveries, the
    sweeper, a manual promote). At every moment at most ONE offer is live, and the held seat cannot be booked by
    anyone else. The seat then passes down the line: expiry -> next patient, decline -> next patient, accept."""
    slot, (first,) = full_slot_with_waitlist(env6, patients=(22, 23, 24))
    live_seen, stop = [], threading.Event()

    def monitor():                                                            # samples continuously
        while not stop.is_set():
            live_seen.append(len(env6.offers_by_status('PENDING')))

    watcher = threading.Thread(target=monitor)
    watcher.start()
    try:
        assert env6.call('POST', f"/patient/appointments/{first['appointmentId']}/cancel", body={})[0] == 200
        assert len(env6.events.promotion_queue) == 1                          # SlotSeatFreed -> waitlist queue
        event = env6.events.promotion_queue.pop()
        with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:   # 12 promoters race for one seat
            barrier = threading.Barrier(12)

            def promoter(i):
                barrier.wait()
                if i % 3 == 0:
                    return bl.handler({'action': 'promote', 'slotId': slot}, None)
                return bl.handler({'Records': [{'messageId': str(i), 'body': json.dumps(event)}]}, None)

            list(pool.map(promoter, range(12)))
        pending = env6.offers_by_status('PENDING')
        assert len(pending) == 1 and pending[0]['patient_id'] == '22'         # first in line
        item = env6.slot(slot)
        assert item['held_count'] == 1 and item['booked_count'] == 0 and item['hold_offer_id'] == pending[0]['offer_id']

        # the held seat is invisible to normal booking: nobody else can take it
        s, b, _ = env6.book(slot, user=25)
        assert s == 409 and b['code'] == 'SLOT_FULL'
        s, b, _ = env6.book(slot, user=22)                                    # not even the offered patient
        assert s == 409 and b['code'] == 'OFFER_PENDING'
        s, b, _ = env6.call('POST', '/patient/waitlists', user=26, body={'slotId': slot})
        assert s == 201                                                       # the slot counts as full

        # offer 1 expires -> exactly one new offer, for patient 23 (sweepers + promoters racing again)
        env6.now = NOW + timedelta(minutes=RULES['holdMinutes'] + 1)
        with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
            list(pool.map(lambda i: bl.handler({'action': 'offer-sweep'} if i % 2 else
                                               {'action': 'promote', 'slotId': slot}, None), range(6)))
        pending = env6.offers_by_status('PENDING')
        assert len(pending) == 1 and pending[0]['patient_id'] == '23'
        assert len(env6.offers_by_status('EXPIRED')) == 1 and env6.metric_sum('OffersExpired') == 1

        # patient 23 declines -> patient 24
        token = wf.offer_token(slot, pending[0]['offer_id'])
        s, b, _ = env6.call('POST', f'/patient/offers/{token}/decline', user=23)
        assert s == 200 and b['nextOffer'] is True
        pending = env6.offers_by_status('PENDING')
        assert len(pending) == 1 and pending[0]['patient_id'] == '24'

        # patient 24 accepts -> confirmed appointment, seat booked, no hold, no live offer
        token = wf.offer_token(slot, pending[0]['offer_id'])
        s, b, _ = env6.call('POST', f'/patient/offers/{token}/accept', user=24)
        assert s == 201 and b['status'] == 'CONFIRMED' and b['slotId'] == slot
    finally:
        stop.set()
        watcher.join()
    assert max(live_seen) == 1                                                # NEVER two live offers
    item = env6.slot(slot)
    assert item['booked_count'] == 1 and item['held_count'] == 0 and 'hold_offer_id' not in item
    assert env6.offers_by_status('PENDING') == []
    assert [a['patient_id'] for a in env6.live() if a['slot_id'] == slot] == [24]
    assert env6.metric_sum('WaitlistPromotions') == 3 and env6.metric_sum('OffersAccepted') == 1
    assert bl.reconcile_slot(MemoryStore6(env6.db), slot)['consistent']
    assert len(env6.sent('HoldOfferIssued')) == 3 and len(env6.sent('HoldOfferExpired')) == 1


# =============================================================================================== waitlists

def test_waitlist_cap_of_five_is_enforced_even_under_parallel_joins(env6):
    slots = []
    for i in range(7):
        sid = env6.add_slot(m5.DOCTOR2, m5.STD2, DAY2 + timedelta(hours=i))
        env6.slots.items[sid].update(booked_count=1, status='FULL', booking_refs={'p99'})
        slots.append(sid)
    with concurrent.futures.ThreadPoolExecutor(max_workers=7) as pool:
        results = list(pool.map(lambda s: env6.call('POST', '/patient/waitlists', user=22, body={'slotId': s}), slots))
    codes = sorted(r[0] for r in results)
    assert codes == [201] * 5 + [422] * 2
    assert all(r[1]['code'] == 'WAITLIST_LIMIT' for r in results if r[0] == 422)
    s, b, _ = env6.call('GET', '/patient/waitlists', user=22)
    assert s == 200 and len(b['items']) == 5 and b['cap'] == 5
    left = b['items'][0]['waitlistId']
    assert env6.call('DELETE', f'/patient/waitlists/{left}', user=22)[0] == 200
    joined = [r for r in results if r[0] == 201]
    missing = next(sid for sid in slots if sid not in {r[1]['slotId'] for r in joined})
    assert env6.call('POST', '/patient/waitlists', user=22, body={'slotId': missing})[0] == 201


def test_join_rules_free_seats_own_booking_duplicate(env6):
    slot = env6.add_slot(m5.DOCTOR, m5.STD, DAY2)
    s, b, _ = env6.call('POST', '/patient/waitlists', user=22, body={'slotId': slot})
    assert s == 409 and b['code'] == 'SEATS_AVAILABLE'
    booked(env6, slot, user=21)
    s, b, _ = env6.call('POST', '/patient/waitlists', user=21, body={'slotId': slot})
    assert s == 409 and b['code'] == 'ALREADY_BOOKED'
    assert env6.call('POST', '/patient/waitlists', user=22, body={'slotId': slot})[0] == 201
    s, b, _ = env6.call('POST', '/patient/waitlists', user=22, body={'slotId': slot})
    assert s == 200 and b['joined'] is False                                 # joining twice is harmless
    s, b, _ = env6.call('GET', '/patient/waitlists', user=22)
    assert b['items'][0]['position'] == 1


def test_doctor_cancelling_a_slot_withdraws_its_live_offer(env6):
    slot, (first,) = full_slot_with_waitlist(env6, patients=(22,))
    env6.call('POST', f"/patient/appointments/{first['appointmentId']}/cancel", body={})
    env6.drain_promotions()
    assert len(env6.offers_by_status('PENDING')) == 1
    s, b, _ = env6.call('POST', '/doctor/me/slot-cancellations', role='doctor', user=m5.DOCTOR,
                        body={'slotId': slot, 'reason': 'Doctor unavailable'})
    assert s == 200
    assert env6.offers_by_status('PENDING') == [] and len(env6.offers_by_status('WITHDRAWN')) == 1
    item = env6.slot(slot)
    assert item['held_count'] == 0 and 'hold_offer_id' not in item and item['status'] == 'CLOSED'


def test_accept_after_expiry_is_refused_and_seat_moves_on(env6):
    slot, (first,) = full_slot_with_waitlist(env6, patients=(22, 23))
    env6.call('POST', f"/patient/appointments/{first['appointmentId']}/cancel", body={})
    env6.drain_promotions()
    offer = env6.offers_by_status('PENDING')[0]
    env6.now = NOW + timedelta(minutes=RULES['holdMinutes'] + 1)
    s, b, _ = env6.call('POST', f"/patient/offers/{wf.offer_token(slot, offer['offer_id'])}/accept", user=22)
    assert s == 410 and b['code'] == 'OFFER_EXPIRED'
    bl.handler({'action': 'offer-sweep'}, None)
    assert [o['patient_id'] for o in env6.offers_by_status('PENDING')] == ['23']
    s, b, _ = env6.call('POST', f"/patient/offers/{wf.offer_token(slot, offer['offer_id'])}/accept", user=23)
    assert s == 404                                                           # not your offer


def test_an_interrupted_acceptance_is_recovered_by_the_sweeper(env6):
    slot, (first,) = full_slot_with_waitlist(env6, patients=(22,))
    env6.call('POST', f"/patient/appointments/{first['appointmentId']}/cancel", body={})
    env6.drain_promotions()
    offer = env6.offers_by_status('PENDING')[0]
    env6.offers.items[(slot, offer['offer_id'])]['status'] = 'ACCEPTING'      # Lambda died right after claiming
    env6.now = NOW + timedelta(minutes=RULES['holdMinutes'] + 10)
    summary = bl.handler({'action': 'offer-sweep'}, None)['body']
    assert summary['recoveredAcceptances'] == 1
    item = env6.slot(slot)
    assert item['held_count'] == 0 and 'hold_offer_id' not in item and bl.seat_ref(22) not in item.get('booking_refs', set())


# =============================================================================================== notifications

def notifier_env(monkeypatch, sns=None):
    log_table = m5.FakeTable(key='dispatch_key')
    sns = sns or FakeSNS()
    monkeypatch.setattr(notifier, 'log_table', lambda: log_table)
    monkeypatch.setattr(notifier, 'sns', lambda: sns)
    monkeypatch.setattr(notifier, 'metric', lambda *a, **k: None)
    monkeypatch.setenv('NOTIFICATION_TOPIC_ARN', 'arn:aws:sns:ap-south-1:000000000000:medislot-dev-notifications')
    return log_table, sns


def sqs(*events):
    return {'Records': [{'messageId': uuid.uuid4().hex, 'body': json.dumps(e)} for e in events]}


def test_all_eight_notification_events_render_and_send(monkeypatch):
    _, sns = notifier_env(monkeypatch)
    base = {'appointmentId': 42, 'patientId': 21, 'patientName': 'Meera Iyer', 'doctorName': 'Dr. Asha Rao',
            'startsAt': '2026-11-22T03:30:00Z', 'timezone': 'Asia/Kolkata', 'reference': 'MS-000042',
            'fromStartsAt': '2026-11-21T03:30:00Z', 'rescheduleCount': 1, 'offerId': 'abc.' + 'f' * 32,
            'expiresAt': '2026-11-21T04:00:00Z', 'refund': {'refundAmount': '500.00', 'refundPercent': 100}}
    assert set(notifier.TEMPLATES) == set(wf.NOTIFICATION_EVENTS) and len(wf.NOTIFICATION_EVENTS) == 8
    result = notifier.handler(sqs(*[{'detail-type': k, 'detail': base} for k in wf.NOTIFICATION_EVENTS]), None)
    assert result == {'batchItemFailures': []} and len(sns.messages) == 8
    booked_msg = next(m for m in sns.messages if m['MessageAttributes']['eventType']['StringValue'] == 'AppointmentBooked')
    assert 'Sun 22 Nov 2026, 9:00 AM' in booked_msg['Message'] and 'Dr. Asha Rao' in booked_msg['Message']
    assert booked_msg['MessageAttributes']['patientId']['StringValue'] == '21'


def test_exactly_once_dispatch_duplicates_and_sns_failures(monkeypatch):
    log_table, sns = notifier_env(monkeypatch, FakeSNS(fail_times=1))
    event = {'detail-type': 'AppointmentBooked', 'detail': {'appointmentId': 7, 'patientId': 21,
                                                            'startsAt': '2026-11-22T03:30:00Z'}}
    first = notifier.handler(sqs(event), None)                                # SNS down: claim released, retried
    assert len(first['batchItemFailures']) == 1 and sns.messages == [] and log_table.items == {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:        # redelivered 8 times at once
        results = list(pool.map(lambda _: notifier.handler(sqs(event), None), range(8)))
    assert len(sns.messages) == 1                                             # sent exactly once
    assert log_table.items['AppointmentBooked#7']['status'] == 'SENT'
    assert sum(len(r['batchItemFailures']) for r in results) <= 7             # in-flight duplicates retried later
    assert notifier.handler(sqs(event), None) == {'batchItemFailures': []} and len(sns.messages) == 1


def test_reminder_dispatcher_run_twice_sends_each_reminder_once(env6, monkeypatch):
    _, sns = notifier_env(monkeypatch)
    soon = booked(env6, env6.add_slot(m5.DOCTOR, m5.STD, NOW + timedelta(minutes=90)), user=21)      # 2 h window
    tomorrow = booked(env6, env6.add_slot(m5.DOCTOR2, m5.STD2, NOW + timedelta(hours=20)), user=22)  # 24 h window
    booked(env6, env6.add_slot(m5.DOCTOR, m5.STD, NOW + timedelta(days=3)), user=23)               # not yet
    env6.events.sent.clear()
    for _ in range(2):                                                        # the mentor triggers it twice
        bl.handler({'action': 'reminders'}, None)
    reminders = [(t, d) for t, d, *_ in env6.events.sent if t.startswith('AppointmentReminder')]
    assert sorted((t, d['appointmentId']) for t, d in reminders) == sorted(
        [('AppointmentReminder2h', soon['appointmentId']), ('AppointmentReminder24h', tomorrow['appointmentId'])])
    # even if both runs had published, the notifier sends each one once
    events = [{'detail-type': t, 'detail': d} for t, d in reminders] * 2
    notifier.handler(sqs(*events), None)
    assert len(sns.messages) == 2
    # later the 24 h appointment enters the 2 h window: one more (different) reminder
    env6.now = NOW + timedelta(hours=19)
    bl.handler({'action': 'reminders'}, None)
    assert sum(1 for t, d, *_ in env6.events.sent if t == 'AppointmentReminder2h') == 2


def test_no_show_sweeper_marks_stale_confirmed_once(env6):
    a = booked(env6, env6.add_slot(m5.DOCTOR, m5.STD, NOW + timedelta(hours=1)), user=21)
    b = booked(env6, env6.add_slot(m5.DOCTOR2, m5.STD2, NOW + timedelta(hours=3)), user=22)
    env6.now = NOW + timedelta(hours=1, minutes=30 + RULES['noShowGraceMinutes'] + 1)   # a ended > grace ago
    first = bl.handler({'action': 'no-show-sweep'}, None)['body']
    second = bl.handler({'action': 'no-show-sweep'}, None)['body']
    assert first['marked'] == 1 and second['marked'] == 0
    status = {x['appointment_id']: x['status'] for x in env6.db.appointments}
    assert status[a['appointmentId']] == 'NO_SHOW' and status[b['appointmentId']] == 'CONFIRMED'
    assert len(env6.sent('AppointmentNoShow')) == 1


def test_state_machine_definition_is_valid_and_has_the_compensation_branch():
    asl = load_definition()
    states = asl['States']
    assert asl['StartAt'] in states
    for name, state in states.items():
        for target in [state.get('Next')] + [c['Next'] for c in state.get('Catch', [])]:
            assert target is None or target in states, (name, target)
        if state['Type'] == 'Task':
            assert state['Parameters']['FunctionName'].startswith('arn:aws:lambda:')
    release = states['ReleaseOriginalSeat']
    assert release['Catch'][0]['Next'] == 'CompensateRevertAppointment'
    assert states['CompensateRevertAppointment']['Next'] == 'CompensateReleaseTargetSeat'
