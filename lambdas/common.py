"""Shared helpers copied into every Lambda zip: config, MySQL connection, logging, small HTTP helpers.

There is NO SQL schema code here any more. Tables are created once per deployment by
lambdas/authentication/migrate.py from schema/schema.sql, so a request only runs its own queries.
"""
import base64
import functools
import hashlib
import json
import logging
import os
import time
import traceback
import uuid
from functools import lru_cache

import boto3
import pymysql
from boto3.dynamodb.conditions import Key

log = logging.getLogger()
log.setLevel(logging.INFO)


# --------------------------------------------------------------------------- AWS clients (created once per container)

@lru_cache(maxsize=None)
def client(name):
    return boto3.client(name)


@lru_cache(maxsize=None)
def resource(name):
    return boto3.resource(name)


def table(name):
    return resource('dynamodb').Table(name)


def auth_table():
    return table(f"MediSlot-{os.environ.get('ENVIRONMENT', 'dev')}-AuthTokens")


@lru_cache(maxsize=64)
def get_param(name, decrypt=False):
    return client('ssm').get_parameter(Name=name, WithDecryption=decrypt)['Parameter']['Value']


@lru_cache(maxsize=1)
def config():
    env = os.environ.get('ENVIRONMENT', 'dev')
    p = f'/medislot/{env}'
    return {'env': env, 'prefix': p, 'jwt': get_param(f'{p}/auth/jwt-secret', True),
            'dbhost': get_param(f'{p}/db/host'), 'dbport': int(get_param(f'{p}/db/port')),
            'dbname': get_param(f'{p}/db/name'), 'dbuser': get_param(f'{p}/db/username'),
            'dbpass': get_param(f'{p}/db/password', True)}


# --------------------------------------------------------------------------- DynamoDB Slots table (shared)
#
# Partition key: doctor_id (S)   Sort key: slot_id (S, "<doctor>#<type>#<start UTC>")
# LSI_DoctorDate: doctor_id + local_date, so one Query returns a doctor's slots for any date range.
# The doctor Lambda, the materializer and the shared GET /doctors/{doctorId}/slots endpoint all read it
# through query_slots(), so there is exactly one way to fetch slots.

SLOTS_DATE_INDEX = 'LSI_DoctorDate'


def slot_key(doctor_id, slot_id):
    """Primary key of one item in the Slots table."""
    return {'doctor_id': str(doctor_id), 'slot_id': str(slot_id)}


def slot_key_for(slot_id):
    """Primary key from a slot id alone: every slot id starts with its doctor ("<doctor>#<type>#<start>")."""
    doctor_id = str(slot_id).split('#', 1)[0]
    if not doctor_id.isdigit():
        raise ValueError(f'slot id does not start with a doctor id: {slot_id!r}')
    return slot_key(doctor_id, slot_id)


def query_slots(table, doctor_id, first_day, last_day=None):
    """Every slot of one doctor whose local date is in [first_day, last_day] (one day when last_day is None).

    One Query on the doctor's partition (LSI on local_date), following pagination.
    Sorted by local date, then start time.
    """
    first = first_day if isinstance(first_day, str) else first_day.isoformat()
    last = first if last_day is None else (last_day if isinstance(last_day, str) else last_day.isoformat())
    date_cond = Key('local_date').eq(first) if first == last else Key('local_date').between(first, last)
    kwargs = {'IndexName': SLOTS_DATE_INDEX,
              'KeyConditionExpression': Key('doctor_id').eq(str(doctor_id)) & date_cond}
    items = []
    while True:
        page = table.query(**kwargs)
        items.extend(page.get('Items', []))
        if not page.get('LastEvaluatedKey'):
            break
        kwargs['ExclusiveStartKey'] = page['LastEvaluatedKey']
    return sorted(items, key=lambda i: (str(i.get('local_date', '')), str(i.get('start_at', ''))))


# --------------------------------------------------------------------------- optional bearer token (public routes)

def bearer_token(event):
    headers = {str(k).lower(): v for k, v in ((event or {}).get('headers') or {}).items()}
    raw = str(headers.get('authorization') or '').strip()
    return raw[7:].strip() if raw.lower().startswith('bearer ') else (raw or None)


def verify_access_token(token, secret):
    """Same checks as the API authorizer (signature, expiry, role, logout denylist). Returns the claims.

    Used by public routes that answer differently for a signed-in caller. Raises ValueError when the token is bad.
    """
    import jwt                       # only the Lambdas that verify tokens ship PyJWT
    try:
        claims = jwt.decode(token, secret, algorithms=['HS256'],
                            options={'require': ['exp', 'iat', 'sub', 'jti', 'role']})
    except jwt.PyJWTError as exc:
        raise ValueError(f'invalid token: {type(exc).__name__}')
    if claims['role'] not in ('patient', 'doctor'):
        raise ValueError('unknown role')
    now = int(time.time())
    item = auth_table().get_item(Key={'token_key': 'JTI#' + str(claims['jti']), 'token_type': 'DENYLIST'}).get('Item')
    if item and int(item.get('expires_at', 0)) > now:
        raise ValueError('token revoked (logged out)')
    return claims


# --------------------------------------------------------------------------- MySQL

_conn = None


def dbconn(c):
    """Return an open connection. A warm Lambda re-uses the one it opened before (saves a TCP + login per request)."""
    global _conn
    if _conn is not None:
        try:
            _conn.ping(reconnect=True)
            return _conn
        except Exception:
            _conn = None
    _conn = pymysql.connect(host=c['dbhost'], port=c['dbport'], user=c['dbuser'], password=c['dbpass'],
                            database=c['dbname'], connect_timeout=5, read_timeout=8, write_timeout=8,
                            cursorclass=pymysql.cursors.DictCursor, autocommit=False)
    return _conn


def release(conn):
    """Call in `finally` instead of close(): ends any open transaction but keeps the connection for the next request."""
    global _conn
    if conn is None:
        return
    try:
        conn.rollback()           # never leave a transaction (or its old snapshot) open between requests
    except Exception:
        try:
            conn.close()
        except Exception:
            pass
        if conn is _conn:
            _conn = None


# --------------------------------------------------------------------------- logging (CloudWatch)

def emit(level, message, correlation_id=None, **fields):
    """One JSON line per event, so CloudWatch Logs Insights can filter on any field."""
    payload = {'level': level.upper(), 'message': message, 'correlationId': correlation_id or str(uuid.uuid4()),
               **{k: v for k, v in fields.items() if v is not None}}
    getattr(log, level if level in ('info', 'warning', 'error') else 'info')(json.dumps(payload, default=str))


def logged(function):
    """Decorator for every Lambda handler: logs one line for EVERY request, whether it succeeds or fails.

    status < 400 -> INFO  "request_succeeded"
    status 4xx   -> WARNING "request_rejected" (with the message sent to the client)
    status 5xx or an exception -> ERROR "request_failed" (with the error type and stack trace)
    """
    def decorate(handler):
        @functools.wraps(handler)
        def wrapper(event, context):
            event = event or {}
            started = time.time()
            cid = correlation(event)
            who = (event.get('requestContext') or {}).get('authorizer') or {}
            route = (f"{event.get('httpMethod', '')} {event.get('resource') or event.get('path', '')}".strip()
                     or f"invoke:{event.get('action') or 'scheduled'}")
            base = {'function': function, 'route': route, 'userId': who.get('userId'), 'role': who.get('role'),
                    'requestId': getattr(context, 'aws_request_id', None)}
            try:
                result = handler(event, context)
            except Exception as exc:
                emit('error', 'request_failed', cid, **base, status=500, errorType=type(exc).__name__,
                     error=str(exc)[:500], stack=traceback.format_exc()[-3000:],
                     durationMs=int((time.time() - started) * 1000))
                raise
            status = result.get('statusCode') if isinstance(result, dict) else None
            status = status if isinstance(status, int) else 200
            fields = dict(base, status=status, durationMs=int((time.time() - started) * 1000))
            if status >= 500:
                emit('error', 'request_failed', cid, **fields, reason=_message(result))
            elif status >= 400:
                emit('warning', 'request_rejected', cid, **fields, reason=_message(result))
            else:
                emit('info', 'request_succeeded', cid, **fields)
            return result
        return wrapper
    return decorate


def _message(result):
    try:
        data = result.get('body')
        data = json.loads(data) if isinstance(data, str) else data
        return str(data.get('message'))[:300] if isinstance(data, dict) else None
    except Exception:
        return None


# --------------------------------------------------------------------------- HTTP helpers

def response(status, body):
    return {'statusCode': status, 'headers': {'content-type': 'application/json', 'cache-control': 'no-store'},
            'body': json.dumps(body, default=str)}


def token_hash(raw):
    return hashlib.sha256(raw.encode()).hexdigest()


def body(event):
    try:
        value = event.get('body') or '{}'
        if event.get('isBase64Encoded'):
            value = base64.b64decode(value).decode()
        return json.loads(value)
    except Exception:
        return None


def correlation(event):
    headers = {str(k).lower(): v for k, v in (event.get('headers') or {}).items()}
    return (headers.get('x-correlation-id') or event.get('correlationId')
            or (event.get('requestContext') or {}).get('requestId') or str(uuid.uuid4()))
