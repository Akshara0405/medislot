"""MediSlot notifier Lambda (Milestone 6): EventBridge -> SQS -> this Lambda -> SNS.

Runs OUTSIDE the VPC (it needs SNS and DynamoDB only, no RDS), triggered by the notifications SQS queue with
ReportBatchItemFailures. Every message is one EventBridge event from the medislot.booking source.

Exactly-once dispatch
  Each event maps to a business dispatch key (e.g. AppointmentBooked#42, AppointmentReminder24h#42#<start>,
  HoldOfferIssued#<offer>). Before publishing, the key is CLAIMED in the NotificationLog table with a conditional
  put. Already SENT -> skipped (a duplicate from EventBridge, SQS redelivery, a publish retry or a scheduler that
  ran twice). The claim is released if SNS fails, so the retry can send it. A claim left SENDING by a Lambda that
  died is taken over after CLAIM_TIMEOUT_SECONDS.

The eight notification events (docs/DESIGN_NOTES.md, "Notifications"):
  AppointmentBooked, AppointmentCancelled, AppointmentRescheduled, HoldOfferIssued, HoldOfferExpired,
  AppointmentReminder24h, AppointmentReminder2h, AppointmentNoShow
"""
import json
import logging
import os
import time
from datetime import datetime, timezone
from functools import lru_cache
from zoneinfo import ZoneInfo

import boto3
from botocore.exceptions import ClientError

log = logging.getLogger()
log.setLevel(logging.INFO)

if os.environ.get('AWS_XRAY_DAEMON_ADDRESS'):        # Lambda with Active tracing: SNS + DynamoDB subsegments
    try:
        from aws_xray_sdk.core import patch
        patch(['boto3', 'botocore'])
    except Exception:
        log.warning('xray_patch_skipped')

NAMESPACE = 'MediSlot/Booking'
CLAIM_TIMEOUT_SECONDS = 300
LOG_TTL_SECONDS = 90 * 24 * 3600

TEMPLATES = {
    'AppointmentBooked': ('Appointment confirmed',
                          'Your appointment {reference} with {doctor} is confirmed for {when}.'),
    'AppointmentCancelled': ('Appointment cancelled',
                             'Your appointment {reference} on {when} was cancelled{by}.{refund}'),
    'AppointmentRescheduled': ('Appointment rescheduled',
                               'Your appointment {reference} with {doctor} has moved from {from_when} to {when}.'),
    'HoldOfferIssued': ('A seat is waiting for you',
                        'A seat opened up for {when}. It is held for you until {expires}. '
                        'Accept it under Waitlists in MediSlot before then.'),
    'HoldOfferExpired': ('Waitlist offer expired',
                         'Your held seat for {when} was not accepted in time and has gone to the next patient.'),
    'AppointmentReminder24h': ('Appointment tomorrow',
                               'Reminder: your appointment {reference} with {doctor} is at {when} (in about 24 hours).'),
    'AppointmentReminder2h': ('Appointment in 2 hours',
                              'Reminder: your appointment {reference} with {doctor} starts at {when} (in about 2 hours).'),
    'AppointmentNoShow': ('Missed appointment',
                          'Your appointment {reference} on {when} was marked as missed (no-show). Contact the clinic '
                          'if this is a mistake.'),
}


@lru_cache(maxsize=None)
def client(name):
    return boto3.client(name)


@lru_cache(maxsize=1)
def log_table():
    return boto3.resource('dynamodb').Table(os.environ['NOTIFICATION_LOG_TABLE'])


def sns():
    return client('sns')


def metric(name, value, **dims):
    """Embedded Metric Format: totals per Environment, and per Environment + EventType."""
    base = {'Environment': os.environ.get('ENVIRONMENT', 'dev')}
    sets = [['Environment']] + ([['Environment'] + list(dims)] if dims else [])
    print(json.dumps({'_aws': {'Timestamp': int(time.time() * 1000), 'CloudWatchMetrics': [
        {'Namespace': NAMESPACE, 'Dimensions': sets, 'Metrics': [{'Name': name, 'Unit': 'Count'}]}]},
        **base, **{k: str(v) for k, v in dims.items()}, name: value}))


def emit(level, message, **fields):
    getattr(log, level)(json.dumps({'level': level.upper(), 'message': message, **fields}, default=str))


def dispatch_key(kind, detail):
    """The business identity of one notification: two deliveries of the same thing get the same key."""
    if kind in ('HoldOfferIssued', 'HoldOfferExpired'):
        return f"{kind}#{detail['offerId']}"
    if kind == 'AppointmentRescheduled':
        return f"{kind}#{detail['appointmentId']}#{detail.get('rescheduleCount', 0)}"
    if kind.startswith('AppointmentReminder'):
        return f"{kind}#{detail['appointmentId']}#{detail.get('startsAt')}"
    return f"{kind}#{detail['appointmentId']}"


def local_time(iso_value, tz_name):
    if not iso_value:
        return 'the scheduled time'
    try:
        value = datetime.fromisoformat(str(iso_value).replace('Z', '+00:00'))
        return value.astimezone(ZoneInfo(tz_name or 'Asia/Kolkata')).strftime('%a %d %b %Y, %I:%M %p').replace(' 0', ' ')
    except Exception:
        return str(iso_value)


def render(kind, detail):
    subject, body = TEMPLATES[kind]
    tz_name = detail.get('timezone') or 'Asia/Kolkata'
    refund = detail.get('refund') or {}
    text = body.format(
        reference=detail.get('reference') or f"MS-{int(detail.get('appointmentId') or 0):06d}",
        doctor=detail.get('doctorName') or 'your doctor', when=local_time(detail.get('startsAt'), tz_name),
        from_when=local_time(detail.get('fromStartsAt'), tz_name),
        expires=local_time(detail.get('expiresAt'), tz_name),
        by=' by the doctor' if detail.get('cancelledBy') == 'DOCTOR' else '',
        refund=(f" Refund: INR {refund.get('refundAmount')} ({refund.get('refundPercent')}%)." if refund else ''))
    greeting = f"Hello {detail['patientName']},\n\n" if detail.get('patientName') else ''
    return f'MediSlot: {subject}', greeting + text + '\n\n- MediSlot'


def claim(key, kind, detail, now):
    """True = we own this dispatch now. False = already sent (duplicate)."""
    table = log_table()
    item = {'dispatch_key': key, 'status': 'SENDING', 'event_type': kind, 'claimed_at': now,
            'patient_id': str(detail.get('patientId')), 'expires_at': now + LOG_TTL_SECONDS}
    try:
        table.put_item(Item=item, ConditionExpression='attribute_not_exists(dispatch_key)')
        return True
    except ClientError as exc:
        if exc.response.get('Error', {}).get('Code') != 'ConditionalCheckFailedException':
            raise
    found = table.get_item(Key={'dispatch_key': key}, ConsistentRead=True).get('Item') or {}
    if found.get('status') == 'SENT':
        return False
    if int(found.get('claimed_at', 0)) > now - CLAIM_TIMEOUT_SECONDS:
        raise RuntimeError(f'dispatch {key} is being sent by another invocation; retry later')
    try:                                              # the previous claimer died: take the claim over
        table.update_item(Key={'dispatch_key': key}, UpdateExpression='SET claimed_at = :now',
                          ConditionExpression='#s = :sending AND claimed_at = :old',
                          ExpressionAttributeNames={'#s': 'status'},
                          ExpressionAttributeValues={':now': now, ':sending': 'SENDING', ':old': found['claimed_at']})
        return True
    except ClientError as exc:
        if exc.response.get('Error', {}).get('Code') != 'ConditionalCheckFailedException':
            raise
        raise RuntimeError(f'dispatch {key} was taken over by another invocation; retry later')


def dispatch(event):
    """Returns 'sent', 'duplicate' or 'ignored'. Raises when it must be retried (SQS redelivers it)."""
    kind, detail = event.get('detail-type'), event.get('detail') or {}
    if kind not in TEMPLATES:
        return 'ignored'
    key, now = dispatch_key(kind, detail), int(time.time())
    if not claim(key, kind, detail, now):
        metric('NotificationDuplicatesSkipped', 1, EventType=kind)
        emit('info', 'notification_duplicate_skipped', dispatchKey=key)
        return 'duplicate'
    subject, message = render(kind, detail)
    try:
        result = sns().publish(
            TopicArn=os.environ['NOTIFICATION_TOPIC_ARN'], Subject=subject[:100], Message=message,
            MessageAttributes={'eventType': {'DataType': 'String', 'StringValue': kind},
                               'patientId': {'DataType': 'String', 'StringValue': str(detail.get('patientId'))}})
    except Exception:
        log_table().delete_item(Key={'dispatch_key': key})        # give the claim back so the retry can send it
        raise
    log_table().update_item(Key={'dispatch_key': key}, UpdateExpression='SET #s = :sent, message_id = :m, sent_at = :t',
                            ExpressionAttributeNames={'#s': 'status'},
                            ExpressionAttributeValues={':sent': 'SENT', ':m': result.get('MessageId', ''), ':t': now})
    metric('NotificationsDispatched', 1, EventType=kind)
    emit('info', 'notification_sent', dispatchKey=key, eventType=kind, messageId=result.get('MessageId'))
    return 'sent'


def handler(event, context):
    failures, outcome = [], {'sent': 0, 'duplicate': 0, 'ignored': 0}
    for record in (event or {}).get('Records', []):
        try:
            result = dispatch(json.loads(record.get('body') or '{}'))
            outcome[result] += 1
        except Exception as exc:
            emit('error', 'notification_failed', messageId=record.get('messageId'), errorType=type(exc).__name__,
                 error=str(exc)[:300])
            metric('NotificationFailures', 1)
            failures.append({'itemIdentifier': record.get('messageId')})
    emit('info', 'notification_batch', **outcome, failed=len(failures))
    return {'batchItemFailures': failures}
