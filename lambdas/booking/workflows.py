"""MediSlot Milestone 6: reschedule saga steps, waitlists, hold offers, sweepers, reminders and no-shows.

This module lives next to lambda_function.py in the booking Lambda zip and re-uses its seat code (optimistic
concurrency on the Slots table, the RDS Store, publish(), metric()). lambda_function calls bind() with itself,
so every helper below is reached as B.<name> (and tests that monkeypatch the booking module also patch here).

  Patient routes (JWT authorizer, patient tokens only)
    POST   /patient/appointments/{appointmentId}/reschedule   {"slotId"}  -> Step Functions saga (sync wait <= 20 s)
    GET    /patient/reschedules/{requestId}                   saga outcome (PENDING | SUCCEEDED | COMPENSATED | REJECTED)
    GET    /patient/waitlists                                 my active waitlists + pending hold offers
    POST   /patient/waitlists                                 {"slotId"}  join (slot must be full; at most N active)
    DELETE /patient/waitlists/{waitlistId}                    leave (a pending offer is declined)
    POST   /patient/offers/{offerId}/accept                   hold offer -> confirmed appointment
    POST   /patient/offers/{offerId}/decline                  give the held seat to the next patient

  Direct invocations (never reachable through API Gateway)
    {"action": "saga", "step": ..., "request": {...}}   Step Functions tasks (claim/commit/release/finalize + compensation)
    {"action": "offer-sweep"}                          EventBridge Scheduler, every minute: expire offers, promote next
    {"action": "reminders"}                            EventBridge Scheduler, every 15 min: 24 h and 2 h reminders
    {"action": "no-show-sweep"}                        EventBridge Scheduler, every 15 min: stale CONFIRMED -> NO_SHOW
    {"action": "promote", "slotId": ...}               manual: offer a free seat to the first waitlisted patient
    SQS records (waitlist-promotions queue)            SlotSeatFreed events -> promote()

Read docs/DESIGN_NOTES.md (sections "Reschedule saga" and "Waitlist exclusivity") for the reasoning.
"""
import base64
import json
import os
import time
import uuid
from datetime import date, timedelta
from decimal import Decimal

import pymysql
from botocore.exceptions import ClientError

B = None                      # the booking lambda_function module (set by bind)


def bind(module):
    global B
    B = module


# =============================================================================================== rules (SSM)

DEFAULT_RULES = {
    'rescheduleCutoffHours': 6,      # no reschedule within 6 hours of the appointment start
    'maxReschedules': 2,             # successful reschedules allowed per appointment
    'waitlistCap': 5,                # active waitlists one patient may be on at the same time
    'holdMinutes': 15,               # how long a hold offer keeps the seat for the patient
    'reminderHours': [24, 2],        # reminders before the appointment start
    'noShowGraceMinutes': 60,        # CONFIRMED this long after the slot ENDED -> NO_SHOW
}
RULE_BOUNDS = {'rescheduleCutoffHours': (0, 72), 'maxReschedules': (0, 10), 'waitlistCap': (1, 50),
               'holdMinutes': (1, 24 * 60), 'noShowGraceMinutes': (0, 7 * 24 * 60)}
RESCHEDULE_WAIT_SECONDS = 20         # the API waits this long for the saga before answering 202
RESCHEDULE_LOCK_SECONDS = 15 * 60    # one saga per appointment at a time (lock self-expires if a run is lost)
REQUEST_TTL_SECONDS = 14 * 24 * 3600
NOTIFY_MARK_TTL_SECONDS = 30 * 24 * 3600
TERMINAL = ('SUCCEEDED', 'COMPENSATED', 'REJECTED', 'NEEDS_ATTENTION')
NOTIFICATION_EVENTS = ('AppointmentBooked', 'AppointmentCancelled', 'AppointmentRescheduled', 'HoldOfferIssued',
                       'HoldOfferExpired', 'AppointmentReminder24h', 'AppointmentReminder2h', 'AppointmentNoShow')


def rules():
    """Workflow rules from SSM /medislot/<env>/booking/workflow-rules (JSON), bounded; defaults when unreadable."""
    out = dict(DEFAULT_RULES)
    try:
        raw, _ = B.ssm_json(f'{B.param_prefix()}/workflow-rules')
    except Exception as exc:                          # missing parameter: run with the documented defaults
        B.emit('warning', 'workflow_rules_default', None, error=type(exc).__name__)
        raw = {}
    for key, (low, high) in RULE_BOUNDS.items():
        try:
            value = int(raw.get(key, out[key]))
            if low <= value <= high:
                out[key] = value
        except (TypeError, ValueError, AttributeError):
            pass
    hours = raw.get('reminderHours') if isinstance(raw, dict) else None
    if isinstance(hours, list) and hours and all(isinstance(h, int) and 0 < h <= 168 for h in hours):
        out['reminderHours'] = sorted(set(hours), reverse=True)
    return out


# =============================================================================================== tables

_tables = {}


def _table(key, env_name):
    if key not in _tables:
        import boto3
        _tables[key] = boto3.resource('dynamodb').Table(os.environ[env_name])
    return _tables[key]


def waitlist_table():
    return _table('waitlist', 'WAITLIST_TABLE')


def offers_table():
    return _table('offers', 'HOLD_OFFERS_TABLE')


def notification_log_table():
    return _table('notify', 'NOTIFICATION_LOG_TABLE')


def requests_table():
    """Saga request records + per-appointment locks live in the Idempotency table (it already has TTL)."""
    return B.idempotency_table()


def now_epoch(now):
    return int(now.timestamp())


def token(slot_id):
    """URL-safe id for a slot (slot ids contain '#' and ':')."""
    return base64.urlsafe_b64encode(slot_id.encode()).decode().rstrip('=')


def untoken(value):
    try:
        raw = str(value or '')
        slot_id = base64.urlsafe_b64decode(raw + '=' * (-len(raw) % 4)).decode()
    except Exception:
        raise B.ApiError(400, 'INVALID_ID', 'Unknown waitlist or offer id.')
    if not B.SLOT_ID.match(slot_id):
        raise B.ApiError(400, 'INVALID_ID', 'Unknown waitlist or offer id.')
    return slot_id


def offer_token(slot_id, offer_id):
    return f'{token(slot_id)}.{offer_id}'


def parse_offer_token(value):
    raw = str(value or '')
    if raw.count('.') != 1:
        raise B.ApiError(400, 'INVALID_ID', 'Unknown offer id.')
    slot_part, offer_id = raw.split('.')
    if not offer_id.isalnum() or len(offer_id) != 32:
        raise B.ApiError(400, 'INVALID_ID', 'Unknown offer id.')
    return untoken(slot_part), offer_id


def slot_problem_for_waitlist(slot, now):
    if not slot:
        return B.ApiError(404, 'SLOT_NOT_FOUND', 'Slot not found.')
    if B.parse_instant(slot['start_at']) <= now:
        return B.ApiError(422, 'SLOT_IN_PAST', 'This slot has already started.')
    if slot.get('status') == B.CLOSED or slot.get('manual_block') or slot.get('cancelled_by_doctor'):
        return B.ApiError(409, 'SLOT_CLOSED', 'The doctor is not taking bookings for this slot.')
    return None


def counts(slot):
    return (int(slot.get('capacity', 1) or 1), int(slot.get('booked_count', 0) or 0),
            int(slot.get('held_count', 0) or 0))


def version_guard(item, values):
    if 'version' in item:
        values[':seen'] = int(item.get('version', 0) or 0)
        return 'version = :seen'
    return 'attribute_not_exists(version)'


# =============================================================================================== reschedule (API)

class SeatUnavailable(Exception):
    """Saga: the target seat could not be claimed (full, closed, started). Nothing to compensate."""


class CommitRejected(Exception):
    """Saga: RDS refused to move the appointment (rules, appointment changed). Target seat must be given back."""


class SeatReleaseFailed(Exception):
    """Saga: the original seat could not be released after the move. Triggers the compensation branch."""


def reschedule(store, event, params, cid):
    """POST /patient/appointments/{appointmentId}/reschedule  {"slotId": "<target slot>"}"""
    patient_id = B.caller(event, 'patient')
    appointment_id = B.positive_id(params.get('appointmentId'), 'appointmentId')
    data = B.json_body(event)
    unknown = sorted(set(data) - {'slotId'})
    if unknown:
        raise B.ApiError(400, 'VALIDATION', 'Unknown field(s): ' + ', '.join(unknown))
    target = data.get('slotId')
    match = B.SLOT_ID.match(target) if isinstance(target, str) and len(target) <= 100 else None
    if not match:
        raise B.ApiError(400, 'VALIDATION', 'slotId must be a slot id from GET /doctors/{doctorId}/slots')
    now, rule, lim = B.clock(), rules(), B.limits()
    row = store.appointment(patient_id, appointment_id)
    if not row:
        raise B.ApiError(404, 'APPOINTMENT_NOT_FOUND', 'Appointment not found.')
    if row['status'] != B.CONFIRMED:
        raise B.ApiError(409, 'NOT_RESCHEDULABLE', 'Only a confirmed appointment can be rescheduled.')
    starts = B.utc(row['slot_start_utc'])
    if starts - now < timedelta(hours=rule['rescheduleCutoffHours']):
        raise B.ApiError(422, 'RESCHEDULE_CUTOFF', f"Appointments cannot be rescheduled within "
                                                   f"{rule['rescheduleCutoffHours']} hours of their start.",
                         cutoffHours=rule['rescheduleCutoffHours'])
    if int(row.get('reschedule_count') or 0) >= rule['maxReschedules']:
        raise B.ApiError(422, 'RESCHEDULE_LIMIT', f"This appointment has already been rescheduled "
                                                  f"{rule['maxReschedules']} time(s), the maximum.",
                         maxReschedules=rule['maxReschedules'])
    if target == row['slot_id']:
        raise B.ApiError(409, 'SAME_SLOT', 'The appointment is already in that slot.')
    if int(match.group(1)) != int(row['doctor_id']) or int(match.group(2)) != int(row['consultation_type_id']):
        raise B.ApiError(422, 'DIFFERENT_CONSULTATION', 'Pick another slot of the same doctor and consultation type.')
    slot = B.read_slot(B.slots_table(), target)
    if not slot:
        raise B.ApiError(404, 'SLOT_NOT_FOUND', 'Slot not found.')
    if B.seat_ref(patient_id) in (slot.get('booking_refs') or set()):
        raise B.ApiError(409, 'ALREADY_BOOKED', 'You already have a seat (or a pending waitlist offer) in that slot.')
    problem = B.seat_problem(slot, now, lim['minLeadMinutes'])
    if problem:
        raise problem
    booking = target_booking(row, slot, patient_id)
    problem = store.reschedule_precheck(booking, lim, now, appointment_id)
    if problem:
        raise problem

    inject = None
    if os.environ.get('FAULT_INJECTION') == 'enabled':          # dev only: lets the mentor force a saga failure
        inject = str(B.headers_of(event).get('x-fault-injection') or '').strip().lower() or None
        if inject not in (None, 'release', 'commit'):
            raise B.ApiError(400, 'VALIDATION', 'X-Fault-Injection must be "release" or "commit"')

    request_id = uuid.uuid4().hex
    table, epoch = requests_table(), now_epoch(now)
    lock_key = f'reschedule-lock#{appointment_id}'
    try:
        table.put_item(Item={'idempotency_key': lock_key, 'request_id': request_id, 'status': 'LOCKED',
                             'lock_expires_at': epoch + RESCHEDULE_LOCK_SECONDS,
                             'expires_at': epoch + RESCHEDULE_LOCK_SECONDS},
                       ConditionExpression='attribute_not_exists(idempotency_key) OR lock_expires_at < :now',
                       ExpressionAttributeValues={':now': epoch})
    except ClientError as exc:
        if not B.conditional_failed(exc):
            raise
        raise B.ApiError(409, 'RESCHEDULE_IN_PROGRESS', 'A reschedule of this appointment is already running.',
                         headers={'Retry-After': '5'})
    detail = {'requestId': request_id, 'appointmentId': appointment_id, 'patientId': patient_id,
              'doctorId': int(row['doctor_id']), 'fromSlotId': row['slot_id'], 'toSlotId': target,
              'fromStartsAt': B.iso(starts), 'fromEndsAt': B.iso(B.utc(row['slot_end_utc'])),
              'toStartsAt': slot['start_at'], 'toEndsAt': slot['end_at'], 'requestedAt': B.iso(now)}
    if inject:
        detail['inject'] = inject
    table.put_item(Item={'idempotency_key': f'reschedule#{request_id}', 'status': 'PENDING', 'patient_id': patient_id,
                         'appointment_id': appointment_id, 'request': json.dumps(detail), 'created_at': epoch,
                         'expires_at': epoch + REQUEST_TTL_SECONDS})
    sent = B.publish([('RescheduleRequested', detail)])
    if not sent:
        finish_request(detail, 'REJECTED', reason='The reschedule could not be started. Nothing was changed.')
        raise B.ApiError(503, 'RESCHEDULE_UNAVAILABLE', 'The reschedule could not be started. Nothing was changed; '
                                                        'please try again.', headers={'Retry-After': '5'})
    B.emit('info', 'reschedule_requested', cid, requestId=request_id, appointmentId=appointment_id, toSlotId=target)
    record = wait_for_request(request_id, RESCHEDULE_WAIT_SECONDS)
    return request_response(store, record or {'status': 'PENDING', 'request': json.dumps(detail)}, patient_id)


def target_booking(row, slot, patient_id):
    return {'patient_id': patient_id, 'doctor_id': int(row['doctor_id']),
            'consultation_type_id': int(row['consultation_type_id']), 'slot_id': slot['slot_id'],
            'starts_at': B.parse_instant(slot['start_at']), 'ends_at': B.parse_instant(slot['end_at']),
            'local_date': date.fromisoformat(slot['local_date']),
            'timezone': slot.get('timezone') or row.get('timezone') or 'Asia/Kolkata',
            'fee': Decimal(str(row['fee_amount']))}


def load_request(request_id):
    return requests_table().get_item(Key={'idempotency_key': f'reschedule#{request_id}'},
                                     ConsistentRead=True).get('Item')


def wait_for_request(request_id, timeout, poll=0.4, sleep=None):
    sleep = sleep or time.sleep
    deadline = time.time() + timeout
    while True:
        record = load_request(request_id)
        if record and record.get('status') in TERMINAL:
            return record
        if time.time() >= deadline:
            return record
        sleep(poll)


def request_response(store, record, patient_id):
    detail = json.loads(record.get('request') or '{}')
    status = record.get('status', 'PENDING')
    out = {'requestId': detail.get('requestId'), 'status': status, 'appointmentId': detail.get('appointmentId'),
           'fromSlotId': detail.get('fromSlotId'), 'toSlotId': detail.get('toSlotId')}
    if record.get('reason'):
        out['reason'] = record['reason']
    if status == 'SUCCEEDED':
        row = store.appointment(patient_id, int(detail['appointmentId']))
        out['appointment'] = B.appointment_payload(row, B.safe_policy(), B.clock()) if row else None
        out['message'] = 'Appointment rescheduled.'
        return 200, out
    if status == 'PENDING':
        out['message'] = 'The reschedule is still running. Check GET /patient/reschedules/{requestId}.'
        return 202, out
    out['message'] = {'COMPENSATED': 'The reschedule failed part-way and was rolled back. Your original appointment '
                                     'is unchanged.',
                      'REJECTED': 'The reschedule was not possible. Your original appointment is unchanged.',
                      'NEEDS_ATTENTION': 'The reschedule failed and is being checked by our team.'}[status]
    return 409, out


def reschedule_status(store, event, params, cid):
    """GET /patient/reschedules/{requestId}"""
    patient_id = B.caller(event, 'patient')
    request_id = str(params.get('requestId') or '')
    if not request_id.isalnum() or len(request_id) != 32:
        raise B.ApiError(400, 'INVALID_ID', 'Unknown reschedule request.')
    record = load_request(request_id)
    if not record or int(record.get('patient_id', -1)) != patient_id:
        raise B.ApiError(404, 'NOT_FOUND', 'Unknown reschedule request.')
    status, payload = request_response(store, record, patient_id)
    return (200 if status == 202 else status), payload


def finish_request(detail, status, reason=None, extra=None):
    """Write the saga outcome and drop the per-appointment lock (only if this request still owns it)."""
    table = requests_table()
    values = {':s': status, ':at': int(time.time())}
    expression = 'SET #s = :s, finished_at = :at'
    if reason:
        expression += ', reason = :r'
        values[':r'] = str(reason)[:500]
    for key, value in (extra or {}).items():
        expression += f', {key} = :{key}'
        values[f':{key}'] = value
    table.update_item(Key={'idempotency_key': f"reschedule#{detail['requestId']}"}, UpdateExpression=expression,
                      ExpressionAttributeNames={'#s': 'status'}, ExpressionAttributeValues=values)
    try:
        table.delete_item(Key={'idempotency_key': f"reschedule-lock#{detail['appointmentId']}"},
                          ConditionExpression='request_id = :rid',
                          ExpressionAttributeValues={':rid': detail['requestId']})
    except ClientError as exc:
        if not B.conditional_failed(exc):
            raise


def mark_progress(detail, **flags):
    names, values, sets = {}, {}, []
    for i, (key, value) in enumerate(flags.items()):
        names[f'#f{i}'], values[f':v{i}'] = key, value
        sets.append(f'#f{i} = :v{i}')
    requests_table().update_item(Key={'idempotency_key': f"reschedule#{detail['requestId']}"},
                                 UpdateExpression='SET ' + ', '.join(sets),
                                 ExpressionAttributeNames=names, ExpressionAttributeValues=values)


def error_text(error):
    """Step Functions passes {"Error", "Cause"}; Cause of a Lambda error is a JSON string with errorMessage."""
    if not isinstance(error, dict):
        return None
    cause = error.get('Cause')
    try:
        cause = json.loads(cause).get('errorMessage', cause)
    except (TypeError, ValueError, AttributeError):
        pass
    return f"{error.get('Error')}: {cause}" if cause else error.get('Error')


# =============================================================================================== reschedule (saga steps)

def saga_step(store, event):
    """One Task of the Step Functions state machine statemachines/reschedule.asl.json. Every step is idempotent:
    Step Functions retries a step whose Lambda timed out, and the step must then do nothing twice."""
    step, detail = event.get('step'), event.get('request') or {}
    if not detail.get('requestId'):
        raise ValueError('saga step without request')
    record = load_request(detail['requestId']) or {}
    ref, table, now = B.seat_ref(detail['patientId']), B.slots_table(), B.clock()
    B.emit('info', 'saga_step', detail['requestId'], step=step, appointmentId=detail.get('appointmentId'))

    if step == 'claim':                               # 1. take a seat in the target slot (optimistic concurrency)
        if record.get('claimed'):
            return {'step': step, 'claimed': True, 'repeat': True}
        try:
            seat = B.reserve_seat(table, detail['toSlotId'], ref, now, B.limits(), {'retries': 0, 'attempts': 0})
        except B.ApiError as exc:
            adopt = exc.code == 'ALREADY_BOOKED' and store.active_for(detail['patientId'], detail['toSlotId']) is None
            if not adopt:                             # our own earlier attempt took the seat, then the Lambda died
                raise SeatUnavailable(f'{exc.code}: {exc.message}')
            seat = {'version': None}
        mark_progress(detail, claimed=True)
        return {'step': step, 'claimed': True, 'slotVersion': seat['version']}

    if step == 'commit':                              # 2. move the appointment in RDS (the system of record)
        if detail.get('inject') == 'commit':
            raise CommitRejected('Injected fault: the database refused the move (X-Fault-Injection: commit).')
        target = B.read_slot(table, detail['toSlotId']) or {}
        booking = {'patient_id': detail['patientId'], 'doctor_id': detail['doctorId'], 'slot_id': detail['toSlotId'],
                   'starts_at': B.parse_instant(detail['toStartsAt']), 'ends_at': B.parse_instant(detail['toEndsAt']),
                   'local_date': date.fromisoformat(target.get('local_date') or
                                                                           detail['toStartsAt'][:10]),
                   'timezone': target.get('timezone') or 'Asia/Kolkata'}
        try:
            outcome = store.move_appointment(detail['appointmentId'], detail['fromSlotId'], booking, B.limits(), now,
                                             lambda: bool((B.read_slot(table, detail['toSlotId']) or {})
                                                          .get('cancelled_by_doctor')))
        except B.ApiError as exc:
            B.rollback(store.conn)
            raise CommitRejected(f'{exc.code}: {exc.message}')
        except pymysql.err.MySQLError as exc:
            B.rollback(store.conn)
            raise CommitRejected(f'database: {type(exc).__name__}')
        mark_progress(detail, committed=True)
        return {'step': step, 'moved': outcome}

    if step == 'release':                             # 3. give the ORIGINAL seat back
        if detail.get('inject') == 'release':
            B.metric('SagaInjectedFaults', 1, Step='release')
            raise SeatReleaseFailed('Injected fault: releasing the original seat failed (X-Fault-Injection: release).')
        try:
            released = B.release_seat(table, detail['fromSlotId'], ref)
        except Exception as exc:
            raise SeatReleaseFailed(f'{type(exc).__name__}: {exc}')
        mark_progress(detail, released=True)
        return {'step': step, 'released': released}

    if step == 'finalize':                            # 4. success: record, events, metric, unlock
        if record.get('status') == 'SUCCEEDED':
            return {'step': step, 'repeat': True}
        row = store.appointment(detail['patientId'], detail['appointmentId'])
        events = [B.appointment_event('AppointmentRescheduled', row, fromSlotId=detail['fromSlotId'],
                                      fromStartsAt=detail['fromStartsAt'], toSlotId=detail['toSlotId'],
                                      rescheduleCount=int(row.get('reschedule_count') or 0),
                                      requestId=detail['requestId'])] if row else []
        events += [('SlotSeatFreed', {'appointmentId': detail['appointmentId'], 'patientId': detail['patientId'],
                                     'doctorId': detail['doctorId'], 'slotId': detail['fromSlotId'],
                                     'startsAt': detail['fromStartsAt'], 'bookable': True, 'cause': 'RESCHEDULED_AWAY',
                                     'occurredAt': B.iso(now)})]
        finish_request(detail, 'SUCCEEDED')
        B.publish(events)
        B.metric('ReschedulesSucceeded', 1)
        return {'step': step, 'status': 'SUCCEEDED'}

    if step == 'revert':                              # compensation: put the appointment back in its original slot
        booking = {'slot_id': detail['fromSlotId'], 'starts_at': B.parse_instant(detail['fromStartsAt']),
                   'ends_at': B.parse_instant(detail['fromEndsAt'])}
        outcome = store.unmove_appointment(detail['appointmentId'], detail['toSlotId'], booking)
        mark_progress(detail, reverted=True)
        return {'step': step, 'reverted': outcome}

    if step == 'release-target':                      # compensation: give the claimed target seat back
        released = B.release_seat(table, detail['toSlotId'], ref)
        if released:
            B.publish([('SlotSeatFreed', {'appointmentId': detail['appointmentId'], 'patientId': detail['patientId'],
                                          'doctorId': detail['doctorId'], 'slotId': detail['toSlotId'],
                                          'startsAt': detail['toStartsAt'], 'bookable': True,
                                          'cause': 'RESCHEDULE_COMPENSATED', 'occurredAt': B.iso(now)})])
        mark_progress(detail, targetReleased=True)
        return {'step': step, 'released': released}

    if step == 'compensated':
        if record.get('status') != 'COMPENSATED':
            finish_request(detail, 'COMPENSATED', reason=error_text(event.get('error')))
            B.metric('ReschedulesCompensated', 1)
        return {'step': step, 'status': 'COMPENSATED'}

    if step == 'rejected':
        if record.get('claimed') and not record.get('committed'):
            B.release_seat(table, detail['toSlotId'], ref)   # the claim worked but its reply was lost: undo it
        if record.get('status') != 'REJECTED':
            finish_request(detail, 'REJECTED', reason=error_text(event.get('error')))
            B.metric('ReschedulesRejected', 1)
        return {'step': step, 'status': 'REJECTED'}

    if step == 'stuck':                               # compensation itself failed: alarm + manual runbook step
        finish_request(detail, 'NEEDS_ATTENTION', reason=error_text(event.get('error') or event.get('stuckError')))
        B.metric('RescheduleCompensationFailed', 1)
        B.emit('error', 'reschedule_compensation_failed', detail['requestId'], detail=json.dumps(detail))
        return {'step': step, 'status': 'NEEDS_ATTENTION'}

    raise ValueError(f'unknown saga step {step!r}')


# =============================================================================================== waitlists

def member_key(patient_id):
    return f'patient#{int(patient_id)}'


def memberships(patient_id):
    """The patient's waitlist memberships (strongly consistent: the cap must count every one)."""
    out, kwargs = [], {'KeyConditionExpression': 'slot_id = :p', 'ConsistentRead': True,
                       'ExpressionAttributeValues': {':p': member_key(patient_id)}}
    while True:
        page = waitlist_table().query(**kwargs)
        out.extend(page.get('Items', []))
        if not page.get('LastEvaluatedKey'):
            return out
        kwargs['ExclusiveStartKey'] = page['LastEvaluatedKey']


def entries(slot_id):
    """Waitlist entries of one slot, oldest first (sort key = joined_at#patient)."""
    out, kwargs = [], {'KeyConditionExpression': 'slot_id = :s', 'ConsistentRead': True,
                       'ExpressionAttributeValues': {':s': slot_id}}
    while True:
        page = waitlist_table().query(**kwargs)
        out.extend(page.get('Items', []))
        if not page.get('LastEvaluatedKey'):
            return sorted(out, key=lambda e: e['waitlist_entry'])
        kwargs['ExclusiveStartKey'] = page['LastEvaluatedKey']


def remove_entry(slot_id, patient_id, entry_key=None):
    table = waitlist_table()
    if entry_key is None:
        member = table.get_item(Key={'slot_id': member_key(patient_id), 'waitlist_entry': slot_id},
                                ConsistentRead=True).get('Item')
        entry_key = (member or {}).get('entry_key')
    if entry_key:
        table.delete_item(Key={'slot_id': slot_id, 'waitlist_entry': entry_key})
    table.delete_item(Key={'slot_id': member_key(patient_id), 'waitlist_entry': slot_id})


def join_waitlist(store, event, params, cid):
    """POST /patient/waitlists  {"slotId"}"""
    patient_id = B.caller(event, 'patient')
    data = B.json_body(event)
    slot_id = data.get('slotId')
    if not (isinstance(slot_id, str) and len(slot_id) <= 100 and B.SLOT_ID.match(slot_id)):
        raise B.ApiError(400, 'VALIDATION', 'slotId must be a slot id from GET /doctors/{doctorId}/slots')
    now, rule = B.clock(), rules()
    slot = B.read_slot(B.slots_table(), slot_id)
    problem = slot_problem_for_waitlist(slot, now)
    if problem:
        raise problem
    if B.seat_ref(patient_id) in (slot.get('booking_refs') or set()):
        raise B.ApiError(409, 'ALREADY_BOOKED', 'You already have a seat (or a pending offer) in this slot.')
    capacity, booked, held = counts(slot)
    if booked + held < capacity:
        raise B.ApiError(409, 'SEATS_AVAILABLE', 'This slot still has free seats. Book it directly.')
    patient = store.patient(patient_id)
    if not patient or not patient['is_active']:
        raise B.ApiError(403, 'ACCOUNT_INACTIVE', 'This account is closed.')
    if not patient.get('profile_id'):
        raise B.ApiError(422, 'PROFILE_REQUIRED', 'Create your patient profile before joining a waitlist.')
    with store.patient_lock(patient_id):              # serialises joins of ONE patient: the cap cannot be raced
        mine = memberships(patient_id)
        existing = next((m for m in mine if m['waitlist_entry'] == slot_id), None)
        if existing:
            return 200, waitlist_view(existing, now, joined=False)
        active = [m for m in mine if B.parse_instant(m['starts_at']) > now]
        if len(active) >= rule['waitlistCap']:
            raise B.ApiError(422, 'WAITLIST_LIMIT', f"You are already on {rule['waitlistCap']} waitlists, the "
                                                    f"maximum. Leave one first.", waitlistCap=rule['waitlistCap'])
        joined = B.iso(now)
        entry_key = f'{now.strftime("%Y-%m-%dT%H:%M:%S.%f")}Z#{int(patient_id)}'
        waitlist_table().put_item(Item={'slot_id': slot_id, 'waitlist_entry': entry_key,
                                        'patient_id': str(patient_id), 'joined_at': joined,
                                        'doctor_id': str(slot['doctor_id']), 'starts_at': slot['start_at'],
                                        'state': 'WAITING'},
                                  ConditionExpression='attribute_not_exists(waitlist_entry)')
        member = {'slot_id': member_key(patient_id), 'waitlist_entry': slot_id, 'entry_key': entry_key,
                  'doctor_id': str(slot['doctor_id']), 'consultation_type_id': str(slot['consultation_type_id']),
                  'starts_at': slot['start_at'], 'ends_at': slot['end_at'], 'timezone': slot.get('timezone'),
                  'local_date': slot.get('local_date'), 'member_since': joined}
        waitlist_table().put_item(Item=member)
    B.metric('WaitlistJoins', 1)
    B.publish([('WaitlistJoined', {'slotId': slot_id, 'patientId': patient_id, 'doctorId': int(slot['doctor_id']),
                                   'startsAt': slot['start_at'], 'occurredAt': joined})])
    B.emit('info', 'waitlist_joined', cid, slotId=slot_id, patientId=str(patient_id))
    return 201, waitlist_view(member, now, joined=True)


def waitlist_view(member, now, joined=None):
    slot_id = member['waitlist_entry']
    line = [e for e in entries(slot_id) if e.get('state') in ('WAITING', 'OFFERED')]
    position = next((i + 1 for i, e in enumerate(line) if e['waitlist_entry'] == member.get('entry_key')), None)
    out = {'waitlistId': token(slot_id), 'slotId': slot_id, 'doctorId': int(member['doctor_id']),
           'consultationTypeId': int(member.get('consultation_type_id') or 0), 'startsAt': member['starts_at'],
           'endsAt': member.get('ends_at'), 'timezone': member.get('timezone'), 'localDate': member.get('local_date'),
           'joinedAt': member.get('member_since'), 'position': position, 'waiting': len(line), 'offer': None}
    if joined is not None:
        out['joined'] = joined
    if member.get('offer_id'):
        offer = offers_table().get_item(Key={'slot_id': slot_id, 'offer_id': member['offer_id']},
                                        ConsistentRead=True).get('Item')
        if offer and offer.get('status') == 'PENDING' and int(offer['expires_at']) > now_epoch(now):
            out['offer'] = {'offerId': offer_token(slot_id, offer['offer_id']), 'status': 'PENDING',
                            'expiresAt': offer['expires_iso'],
                            'secondsLeft': int(offer['expires_at']) - now_epoch(now)}
    return out


def list_waitlists(store, event, params, cid):
    """GET /patient/waitlists"""
    patient_id = B.caller(event, 'patient')
    now = B.clock()
    items = []
    for member in memberships(patient_id):
        if B.parse_instant(member['starts_at']) <= now:   # the slot has started: the waitlist is over
            remove_entry(member['waitlist_entry'], patient_id, member.get('entry_key'))
            continue
        view = waitlist_view(member, now)
        offer = store.offer(view['doctorId'], view['consultationTypeId']) or {}
        view['doctor'] = {'name': f"Dr. {offer.get('first_name', '')} {offer.get('last_name', '')}".strip()}
        view['consultation'] = {'label': offer.get('label'), 'fee': B.money(offer.get('fee_amount'))}
        items.append(view)
    items.sort(key=lambda v: v['startsAt'])
    return 200, {'items': items, 'cap': rules()['waitlistCap'], 'pendingOffers': sum(1 for v in items if v['offer'])}


def leave_waitlist(store, event, params, cid):
    """DELETE /patient/waitlists/{waitlistId}"""
    patient_id = B.caller(event, 'patient')
    slot_id = untoken(params.get('waitlistId'))
    member = waitlist_table().get_item(Key={'slot_id': member_key(patient_id), 'waitlist_entry': slot_id},
                                       ConsistentRead=True).get('Item')
    if not member:
        raise B.ApiError(404, 'NOT_ON_WAITLIST', 'You are not on the waitlist for this slot.')
    if member.get('offer_id'):
        close_offer(slot_id, member['offer_id'], 'DECLINED', patient_id)
    remove_entry(slot_id, patient_id, member.get('entry_key'))
    B.metric('WaitlistLeaves', 1)
    if member.get('offer_id'):
        promote(slot_id)
    return 200, {'waitlistId': token(slot_id), 'slotId': slot_id, 'left': True}


# =============================================================================================== hold offers

def hold_seat(table, slot, offer_id, ref, expires_iso):
    """Hold one free seat for one patient. Fails (ConditionalCheckFailed) if ANY offer is already live on the slot:
    that condition is what makes offers exclusive (one live offer per slot at any moment)."""
    capacity, booked, held = counts(slot)
    values = {':one': 1, ':zero': 0, ':next': int(slot.get('version', 0) or 0) + 1, ':oid': offer_id, ':ref': ref,
              ':refset': {ref}, ':exp': expires_iso, ':closed': B.CLOSED,
              ':status': B.FULL if booked + held + 1 >= capacity else B.OPEN, ':limit': capacity - booked}
    guard = version_guard(slot, values)
    table.update_item(
        Key=B.slot_key_for(slot['slot_id']),
        UpdateExpression='SET held_count = if_not_exists(held_count, :zero) + :one, version = :next, #s = :status, '
                         'hold_offer_id = :oid, hold_ref = :ref, hold_expires_at = :exp ADD booking_refs :refset',
        ConditionExpression=guard + ' AND attribute_not_exists(hold_offer_id) AND #s <> :closed'
                                    ' AND attribute_not_exists(manual_block)'
                                    ' AND (attribute_not_exists(held_count) OR held_count < :limit)'
                                    ' AND (attribute_not_exists(booking_refs) OR NOT contains(booking_refs, :ref))',
        ExpressionAttributeNames={'#s': 'status'}, ExpressionAttributeValues=values)


def release_hold(slot_id, offer_id, max_attempts=8):
    """Give a held seat back (offer expired, declined, withdrawn or failed). Idempotent. True = released."""
    table = B.slots_table()
    for attempt in range(1, max_attempts + 1):
        item = B.read_slot(table, slot_id)
        if not item or item.get('hold_offer_id') != offer_id:
            return False
        ref = item.get('hold_ref')
        stays_closed = item.get('status') == B.CLOSED or item.get('manual_block')
        values = {':one': 1, ':next': int(item.get('version', 0) or 0) + 1, ':oid': offer_id,
                  ':status': B.CLOSED if stays_closed else B.OPEN}
        update = ('SET held_count = held_count - :one, version = :next, #s = :status '
                  'REMOVE hold_offer_id, hold_ref, hold_expires_at')
        if ref:
            values[':refset'] = {ref}
            update += ' DELETE booking_refs :refset'
        guard = version_guard(item, values)
        try:
            table.update_item(
                Key=B.slot_key_for(slot_id),
                UpdateExpression=update,
                ConditionExpression=guard + ' AND hold_offer_id = :oid',
                ExpressionAttributeNames={'#s': 'status'}, ExpressionAttributeValues=values)
            return True
        except ClientError as exc:
            if not B.conditional_failed(exc):
                raise
            time.sleep(B.backoff_delay(attempt, 15, 300))
    raise RuntimeError(f'could not release hold {offer_id} on {slot_id}')


def convert_hold(slot_id, offer_id, max_attempts=8):
    """Accepted offer: the held seat becomes a booked seat (the patient's seat tag is already on the slot)."""
    table = B.slots_table()
    for attempt in range(1, max_attempts + 1):
        item = B.read_slot(table, slot_id)
        if not item or item.get('hold_offer_id') != offer_id:
            return False
        values = {':one': 1, ':zero': 0, ':next': int(item.get('version', 0) or 0) + 1, ':oid': offer_id}
        guard = version_guard(item, values)
        try:
            table.update_item(
                Key=B.slot_key_for(slot_id),
                UpdateExpression='SET held_count = held_count - :one, '
                                 'booked_count = if_not_exists(booked_count, :zero) + :one, version = :next '
                                 'REMOVE hold_offer_id, hold_ref, hold_expires_at',
                ConditionExpression=guard + ' AND hold_offer_id = :oid', ExpressionAttributeValues=values)
            return True
        except ClientError as exc:
            if not B.conditional_failed(exc):
                raise
            time.sleep(B.backoff_delay(attempt, 15, 300))
    raise RuntimeError(f'could not convert hold {offer_id} on {slot_id}')


def promote(slot_id, now=None, max_attempts=6):
    """Offer a free seat of this slot to the first patient in line. Safe to call any number of times, from any
    number of places at once (SQS consumer, sweeper, accept/decline): at most ONE offer can be live per slot."""
    now = now or B.clock()
    table, rule = B.slots_table(), rules()
    for _ in range(max_attempts):
        slot = B.read_slot(table, slot_id)
        if slot_problem_for_waitlist(slot, now):
            return {'slotId': slot_id, 'promoted': False, 'reason': 'slot not bookable'}
        if slot.get('hold_offer_id'):
            return {'slotId': slot_id, 'promoted': False, 'reason': 'an offer is already live'}
        capacity, booked, held = counts(slot)
        if booked + held >= capacity:
            return {'slotId': slot_id, 'promoted': False, 'reason': 'no free seat'}
        refs = slot.get('booking_refs') or set()
        entry = None
        for candidate in entries(slot_id):
            if candidate.get('state') != 'WAITING':
                continue
            if B.seat_ref(candidate['patient_id']) in refs:      # booked this slot meanwhile: leave the line
                remove_entry(slot_id, candidate['patient_id'], candidate['waitlist_entry'])
                continue
            entry = candidate
            break
        if not entry:
            return {'slotId': slot_id, 'promoted': False, 'reason': 'waitlist empty'}
        offer_id = uuid.uuid4().hex
        expires = now + timedelta(minutes=rule['holdMinutes'])
        offers = offers_table()
        # 1. the offer item first, as ISSUING (not yet acceptable): if we stop after this line, the sweeper
        #    removes it at its expiry and no seat is stuck
        offers.put_item(Item={'slot_id': slot_id, 'offer_id': offer_id, 'patient_id': entry['patient_id'],
                              'status': 'ISSUING', 'expires_at': now_epoch(expires), 'expires_iso': B.iso(expires),
                              'doctor_id': str(slot['doctor_id']), 'starts_at': slot['start_at'],
                              'entry_key': entry['waitlist_entry'], 'created_at': B.iso(now)},
                        ConditionExpression='attribute_not_exists(offer_id)')
        # 2. then the seat, guarded by "no live offer on this slot" + the slot version
        try:
            hold_seat(table, slot, offer_id, B.seat_ref(entry['patient_id']), B.iso(expires))
        except ClientError as exc:
            if not B.conditional_failed(exc):
                raise
            offers.delete_item(Key={'slot_id': slot_id, 'offer_id': offer_id})     # lost the race: try again
            continue
        # 3. only the promoter that won the seat makes its offer live (PENDING = the patient may accept it)
        offers.update_item(Key={'slot_id': slot_id, 'offer_id': offer_id}, UpdateExpression='SET #s = :p',
                           ConditionExpression='#s = :i', ExpressionAttributeNames={'#s': 'status'},
                           ExpressionAttributeValues={':p': 'PENDING', ':i': 'ISSUING'})
        waitlist_table().update_item(Key={'slot_id': slot_id, 'waitlist_entry': entry['waitlist_entry']},
                                     UpdateExpression='SET #st = :o, offer_id = :oid',
                                     ExpressionAttributeNames={'#st': 'state'},
                                     ExpressionAttributeValues={':o': 'OFFERED', ':oid': offer_id})
        waitlist_table().update_item(Key={'slot_id': member_key(entry['patient_id']), 'waitlist_entry': slot_id},
                                     UpdateExpression='SET offer_id = :oid, offer_expires_at = :exp',
                                     ExpressionAttributeValues={':oid': offer_id, ':exp': B.iso(expires)})
        B.metric('WaitlistPromotions', 1)
        B.publish([('HoldOfferIssued', {'offerId': offer_token(slot_id, offer_id), 'slotId': slot_id,
                                        'patientId': int(entry['patient_id']), 'doctorId': int(slot['doctor_id']),
                                        'startsAt': slot['start_at'], 'timezone': slot.get('timezone'),
                                        'expiresAt': B.iso(expires), 'holdMinutes': rule['holdMinutes'],
                                        'occurredAt': B.iso(now)})])
        B.emit('info', 'hold_offer_issued', None, slotId=slot_id, offerId=offer_id, patientId=entry['patient_id'])
        return {'slotId': slot_id, 'promoted': True, 'offerId': offer_id, 'patientId': int(entry['patient_id'])}
    return {'slotId': slot_id, 'promoted': False, 'reason': 'contention, retry later'}


def close_offer(slot_id, offer_id, status, patient_id=None, now=None, require_expired=False, from_status='PENDING'):
    """PENDING -> status (EXPIRED / DECLINED / WITHDRAWN / FAILED), then give the held seat back. True if we closed it."""
    now = now or B.clock()
    values = {':p': from_status, ':s': status, ':ttl': now_epoch(now) + 3 * 24 * 3600, ':at': B.iso(now)}
    condition = '#s = :p'
    if require_expired:
        condition += ' AND expires_at <= :now'
        values[':now'] = now_epoch(now)
    if patient_id is not None:
        condition += ' AND patient_id = :pid'
        values[':pid'] = str(patient_id)
    try:
        offers_table().update_item(Key={'slot_id': slot_id, 'offer_id': offer_id},
                                   UpdateExpression='SET #s = :s, expires_at = :ttl, closed_at = :at',
                                   ConditionExpression=condition, ExpressionAttributeNames={'#s': 'status'},
                                   ExpressionAttributeValues=values)
    except ClientError as exc:
        if not B.conditional_failed(exc):
            raise
        return False
    release_hold(slot_id, offer_id)
    return True


def withdraw_offers(slot_id):
    """The doctor closed/cancelled the slot: a live offer on it is withdrawn and its seat released."""
    slot = B.read_slot(B.slots_table(), slot_id) or {}
    offer_id = slot.get('hold_offer_id')
    if not offer_id:
        return False
    offer = offers_table().get_item(Key={'slot_id': slot_id, 'offer_id': offer_id}, ConsistentRead=True).get('Item')
    closed = close_offer(slot_id, offer_id, 'WITHDRAWN')
    if offer:
        remove_entry(slot_id, offer['patient_id'], offer.get('entry_key'))
    return closed


def load_offer(slot_id, offer_id):
    return offers_table().get_item(Key={'slot_id': slot_id, 'offer_id': offer_id}, ConsistentRead=True).get('Item')


def accept_offer(store, event, params, cid):
    """POST /patient/offers/{offerId}/accept"""
    patient_id = B.caller(event, 'patient')
    slot_id, offer_id = parse_offer_token(params.get('offerId'))
    now, table = B.clock(), B.slots_table()
    offer = load_offer(slot_id, offer_id)
    if not offer or offer.get('patient_id') != str(patient_id):
        raise B.ApiError(404, 'OFFER_NOT_FOUND', 'Offer not found.')
    if offer['status'] == 'ACCEPTED':
        row = store.active_for(patient_id, slot_id)
        if row:
            return 200, dict(B.appointment_payload(row, B.safe_policy(), now), replayed=True)
    if offer['status'] != 'PENDING' or int(offer['expires_at']) <= now_epoch(now):
        raise B.ApiError(410, 'OFFER_EXPIRED', 'This offer has expired. The seat went to the next patient in line.')
    slot = B.read_slot(table, slot_id)
    if not slot or slot.get('hold_offer_id') != offer_id:
        close_offer(slot_id, offer_id, 'WITHDRAWN', now=now)
        raise B.ApiError(410, 'OFFER_EXPIRED', 'This offer is no longer valid.')
    if slot_problem_for_waitlist(slot, now):
        withdraw_offers(slot_id)
        raise B.ApiError(409, 'SLOT_CLOSED', 'The doctor is no longer taking bookings for this slot.')
    try:                                              # claim the offer: the sweeper can no longer expire it
        offers_table().update_item(Key={'slot_id': slot_id, 'offer_id': offer_id},
                                   UpdateExpression='SET #s = :a, accepted_at = :at',
                                   ConditionExpression='#s = :p AND expires_at > :now',
                                   ExpressionAttributeNames={'#s': 'status'},
                                   ExpressionAttributeValues={':a': 'ACCEPTING', ':p': 'PENDING', ':at': B.iso(now),
                                                              ':now': now_epoch(now)})
    except ClientError as exc:
        if not B.conditional_failed(exc):
            raise
        raise B.ApiError(410, 'OFFER_EXPIRED', 'This offer has expired.')
    doctor_id, type_id = int(slot['doctor_id']), int(slot['consultation_type_id'])
    bookable = store.offer(doctor_id, type_id)
    booking = {'patient_id': patient_id, 'doctor_id': doctor_id, 'consultation_type_id': type_id, 'slot_id': slot_id,
               'starts_at': B.parse_instant(slot['start_at']), 'ends_at': B.parse_instant(slot['end_at']),
               'local_date': date.fromisoformat(slot['local_date']),
               'timezone': slot.get('timezone') or 'Asia/Kolkata',
               'fee': Decimal(str((bookable or {}).get('fee_amount') or '0'))}

    def failed(problem):
        offers_table().update_item(Key={'slot_id': slot_id, 'offer_id': offer_id}, UpdateExpression='SET #s = :f',
                                   ExpressionAttributeNames={'#s': 'status'}, ExpressionAttributeValues={':f': 'FAILED'})
        release_hold(slot_id, offer_id)
        remove_entry(slot_id, patient_id, offer.get('entry_key'))
        promote(slot_id)
        return problem

    if not bookable or not bookable['type_active'] or not bookable['doctor_active']:
        raise failed(B.ApiError(409, 'NOT_BOOKABLE', 'The doctor no longer offers this consultation.'))
    try:
        appointment_id = store.confirm(booking, B.limits(), now,
                                       lambda: bool((B.read_slot(table, slot_id) or {}).get('cancelled_by_doctor')))
    except B.ApiError as problem:
        B.rollback(store.conn)
        raise failed(problem)
    except pymysql.err.IntegrityError:
        B.rollback(store.conn)
        raise failed(B.ApiError(409, 'ALREADY_BOOKED', 'You already have a seat in this slot.'))
    convert_hold(slot_id, offer_id)
    offers_table().update_item(Key={'slot_id': slot_id, 'offer_id': offer_id},
                               UpdateExpression='SET #s = :a, expires_at = :ttl, appointment_id = :aid',
                               ExpressionAttributeNames={'#s': 'status'},
                               ExpressionAttributeValues={':a': 'ACCEPTED', ':ttl': now_epoch(now) + 3 * 24 * 3600,
                                                          ':aid': int(appointment_id)})
    remove_entry(slot_id, patient_id, offer.get('entry_key'))
    B.metric('OffersAccepted', 1)
    B.metric('BookingsConfirmed', 1)
    row = store.active_for(patient_id, slot_id)
    B.publish([B.appointment_event('AppointmentBooked', row, consultationTypeId=type_id, fee=B.money(booking['fee']),
                                   via='WAITLIST_OFFER')])
    promote(slot_id)                                  # more free seats (capacity > 1)? next patient in line
    B.emit('info', 'hold_offer_accepted', cid, slotId=slot_id, offerId=offer_id, appointmentId=appointment_id)
    return 201, B.appointment_payload(row, B.safe_policy(), now)


def decline_offer(store, event, params, cid):
    """POST /patient/offers/{offerId}/decline"""
    patient_id = B.caller(event, 'patient')
    slot_id, offer_id = parse_offer_token(params.get('offerId'))
    offer = load_offer(slot_id, offer_id)
    if not offer or offer.get('patient_id') != str(patient_id):
        raise B.ApiError(404, 'OFFER_NOT_FOUND', 'Offer not found.')
    if not close_offer(slot_id, offer_id, 'DECLINED', patient_id):
        raise B.ApiError(410, 'OFFER_EXPIRED', 'This offer is no longer pending.')
    remove_entry(slot_id, patient_id, offer.get('entry_key'))
    B.metric('OffersDeclined', 1)
    result = promote(slot_id)
    return 200, {'offerId': offer_token(slot_id, offer_id), 'status': 'DECLINED', 'nextOffer': result.get('promoted')}


def expire_offers(now=None, limit=200, store=None):
    """Scheduler (every minute): PENDING offers past their expiry -> EXPIRED, seat released, next patient offered."""
    now = now or B.clock()
    kwargs = {'IndexName': 'GSI_OfferExpiry', 'KeyConditionExpression': '#s = :p AND expires_at <= :now',
              'ExpressionAttributeNames': {'#s': 'status'},
              'ExpressionAttributeValues': {':p': 'PENDING', ':now': now_epoch(now)}, 'Limit': limit}
    due = offers_table().query(**kwargs).get('Items', [])
    abandoned = offers_table().query(**dict(kwargs, ExpressionAttributeValues={':p': 'ISSUING',
                                                                               ':now': now_epoch(now)})).get('Items', [])
    for offer in abandoned:                           # a promoter stopped between writing the offer and making it live
        if close_offer(offer['slot_id'], offer['offer_id'], 'ABANDONED', now=now, require_expired=True,
                       from_status='ISSUING'):
            promote(offer['slot_id'], now)
    expired, promoted = 0, 0
    for offer in due:
        if not close_offer(offer['slot_id'], offer['offer_id'], 'EXPIRED', now=now, require_expired=True):
            continue                                  # accepted / declined / expired by a parallel sweeper
        expired += 1
        remove_entry(offer['slot_id'], offer['patient_id'], offer.get('entry_key'))
        B.metric('OffersExpired', 1)
        B.publish([('HoldOfferExpired', {'offerId': offer_token(offer['slot_id'], offer['offer_id']),
                                         'slotId': offer['slot_id'], 'patientId': int(offer['patient_id']),
                                         'doctorId': int(offer['doctor_id']), 'startsAt': offer.get('starts_at'),
                                         'occurredAt': B.iso(now)})])
        promoted += bool(promote(offer['slot_id'], now).get('promoted'))
    recovered = recover_acceptances(now, limit, store)
    summary = {'due': len(due), 'expired': expired, 'promoted': promoted, 'recoveredAcceptances': recovered}
    B.emit('info', 'offers_swept', None, **summary)
    return summary


def recover_acceptances(now, limit=50, store=None):
    """An acceptance that stopped half-way (status ACCEPTING, Lambda gone) must not keep the seat held forever.
    Five minutes after the offer's expiry: if the appointment exists the hold becomes a booked seat, otherwise
    the held seat is released and offered to the next patient."""
    kwargs = {'IndexName': 'GSI_OfferExpiry', 'KeyConditionExpression': '#s = :a AND expires_at <= :cut',
              'ExpressionAttributeNames': {'#s': 'status'},
              'ExpressionAttributeValues': {':a': 'ACCEPTING', ':cut': now_epoch(now) - 300}, 'Limit': limit}
    stuck = offers_table().query(**kwargs).get('Items', [])
    if not stuck:
        return 0
    conn = None
    try:
        if store is None:
            conn = B.dbconn(B.config())
            store = B.make_store(conn)
        for offer in stuck:
            row = store.active_for(int(offer['patient_id']), offer['slot_id'])
            final = 'ACCEPTED' if row else 'FAILED'
            if row:
                convert_hold(offer['slot_id'], offer['offer_id'])
            else:
                release_hold(offer['slot_id'], offer['offer_id'])
            offers_table().update_item(Key={'slot_id': offer['slot_id'], 'offer_id': offer['offer_id']},
                                       UpdateExpression='SET #s = :f, expires_at = :ttl',
                                       ExpressionAttributeNames={'#s': 'status'},
                                       ExpressionAttributeValues={':f': final, ':ttl': now_epoch(now) + 3 * 24 * 3600})
            if not row:
                promote(offer['slot_id'], now)
    finally:
        if conn is not None:
            B.release(conn)
    return len(stuck)


def handle_sqs(event):
    """waitlist-promotions queue: each message is an EventBridge SlotSeatFreed event. Partial batch failures."""
    failures = []
    for record in event.get('Records', []):
        try:
            message = json.loads(record.get('body') or '{}')
            detail = message.get('detail') or {}
            if message.get('detail-type') == 'SlotSeatFreed' and detail.get('bookable', True) and detail.get('slotId'):
                promote(detail['slotId'])
        except Exception as exc:
            B.emit('error', 'waitlist_promotion_failed', None, messageId=record.get('messageId'),
                   errorType=type(exc).__name__, error=str(exc)[:300])
            failures.append({'itemIdentifier': record.get('messageId')})
    return {'batchItemFailures': failures}


# =============================================================================================== reminders + no-shows

def dispatch_reminders(store, now=None):
    """Scheduler (every 15 min). One reminder per appointment per window, however often this runs:
    a conditional put of 'queued#<kind>#<appointment>' in the NotificationLog table guards the publish, and the
    notifier Lambda checks its own dispatch key again (two independent guards)."""
    now = now or B.clock()
    windows = rules()['reminderHours']                          # e.g. [24, 2]
    rows = store.upcoming_confirmed(now, now + timedelta(hours=max(windows)))
    log, queued, skipped = notification_log_table(), 0, 0
    for row in rows:
        hours_left = (B.utc(row['slot_start_utc']) - now).total_seconds() / 3600
        window = min(h for h in windows if hours_left <= h)    # the tightest window it is inside
        kind = f'AppointmentReminder{window}h'
        key = f"queued#{kind}#{row['appointment_id']}#{B.iso(B.utc(row['slot_start_utc']))}"
        try:
            log.put_item(Item={'dispatch_key': key, 'status': 'QUEUED', 'queued_at': B.iso(now),
                               'expires_at': now_epoch(now) + NOTIFY_MARK_TTL_SECONDS},
                         ConditionExpression='attribute_not_exists(dispatch_key)')
        except ClientError as exc:
            if not B.conditional_failed(exc):
                raise
            skipped += 1
            continue
        if B.publish([B.appointment_event(kind, row, window=f'{window}h')]):
            queued += 1
        else:                                         # not sent: let the next run try again
            log.delete_item(Key={'dispatch_key': key})
    summary = {'checked': len(rows), 'queued': queued, 'alreadyQueued': skipped}
    B.metric('RemindersQueued', queued)
    B.emit('info', 'reminders_dispatched', None, **summary)
    return summary


def sweep_no_shows(store, now=None, limit=200):
    """Scheduler (every 15 min): CONFIRMED appointments whose slot ended more than noShowGraceMinutes ago and that
    the doctor never marked -> NO_SHOW (the doctor can still correct it to COMPLETED). Idempotent."""
    now = now or B.clock()
    cutoff = now - timedelta(minutes=rules()['noShowGraceMinutes'])
    marked, events = 0, []
    for row in store.stale_confirmed(cutoff, limit):
        if store.mark_no_show(row['appointment_id'], cutoff):
            marked += 1
            events.append(B.appointment_event('AppointmentNoShow', dict(row, status='NO_SHOW'), markedBy='SWEEPER'))
    B.publish(events)
    if marked:
        B.metric('NoShowsMarked', marked)
    summary = {'marked': marked, 'cutoff': B.iso(cutoff)}
    B.emit('info', 'no_shows_swept', None, **summary)
    return summary
