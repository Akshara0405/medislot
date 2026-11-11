"""Milestone 4 patient Lambda: search SQL + keyset cursor, slot views, profile validation, patient isolation."""
import importlib.util
import itertools
import json
import os
import sys
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

import pytest

os.environ.setdefault('AWS_DEFAULT_REGION', 'ap-south-1')
ROOT = os.path.join(os.path.dirname(__file__), '..', 'lambdas')
sys.path.insert(0, ROOT)
_spec = importlib.util.spec_from_file_location('patient_lambda', os.path.join(ROOT, 'patient', 'lambda_function.py'))
pl = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(pl)


# ----------------------------------------------------------------- fakes

class Cur:
    """Answers the handful of queries the patient Lambda makes; records everything it is asked."""

    def __init__(self, profile=None, doctors=None, total=0):
        self.profile, self.doctors, self.total = profile, doctors or [], total
        self.sql, self.rows, self.rowcount = [], [], 0

    def __enter__(self): return self
    def __exit__(self, *a): return False

    def execute(self, sql, args=()):
        s = ' '.join(sql.split())
        self.sql.append((s, list(args or ())))
        if s.startswith('SELECT u.user_id, u.email'):
            self.rows = [self.profile] if self.profile else []
        elif s.startswith('SELECT COUNT(*) AS total'):
            self.rows = [{'total': self.total}]
        elif s.startswith('SELECT d.user_id, d.first_name') and 'LIMIT' in s:
            self.rows = self.doctors
        elif s.startswith('UPDATE patients'):
            self.rowcount = 0 if (self.profile or {}).get('stale') else 1
            if self.rowcount:
                self.profile = dict(self.profile, profile_version=self.profile['profile_version'] + 1)
        else:
            self.rows = []

    def fetchone(self): return self.rows[0] if self.rows else None
    def fetchall(self): return self.rows


class Conn:
    def __init__(self, cur): self.cur, self.commits = cur, 0
    def cursor(self): return self.cur
    def commit(self): self.commits += 1
    def rollback(self): pass
    def close(self): pass


PROFILE = {'user_id': 21, 'email': 'meera@example.com', 'profile_id': 21, 'first_name': 'Meera', 'last_name': 'Iyer',
           'phone': '9876543210', 'date_of_birth': date(1994, 5, 2), 'gender': 'Female', 'address': None,
           'city': 'Hyderabad', 'blood_group': None, 'emergency_contact_name': None, 'emergency_contact_phone': None,
           'profile_version': 3, 'updated_at': datetime(2026, 11, 5, 9, 0), 'created_at': datetime(2026, 11, 2, 9, 0)}


def run(monkeypatch, method, path, *, cur=None, role='patient', user='21', body=None, query=None, headers=None):
    cur = cur or Cur(profile=dict(PROFILE))
    conn = Conn(cur)
    monkeypatch.setattr(pl, 'config', lambda: {})
    monkeypatch.setattr(pl, 'dbconn', lambda c: conn)
    monkeypatch.setattr(pl, 'emit', lambda *a, **k: None)
    event = {'httpMethod': method, 'path': path, 'queryStringParameters': query, 'headers': headers or {},
             'body': json.dumps(body) if body is not None else None,
             'requestContext': {'authorizer': {'role': role, 'userId': user, 'jti': 'j-1'} if role else {}}}
    res = pl.handler(event, None)
    return res['statusCode'], json.loads(res['body']), res['headers'], cur


# ----------------------------------------------------------------- search

def test_every_filter_combination_builds_one_indexed_query():
    filters = {'name': ['rao'], 'hospitalId': 3, 'specialization': 'Cardiology', 'city': 'Hyderabad'}
    for size in range(5):
        for combo in itertools.combinations(filters, size):
            chosen = {k: filters[k] for k in combo}
            count_sql, count_args, page_sql, page_args = pl.search_sql(chosen, None, 10)
            assert page_sql.endswith('ORDER BY d.last_name, d.first_name, d.user_id LIMIT %s')
            assert page_args[-1] == 11                       # limit + 1 to detect another page
            assert ('d.specialization = %s' in page_sql) == ('specialization' in chosen)
            assert ('d.hospital_id = %s' in page_sql) == ('hospitalId' in chosen)
            assert ('h.city = %s' in page_sql) == ('city' in chosen)
            assert ('d.last_name LIKE %s' in page_sql) == ('name' in chosen)
            assert page_sql.count('%s') == len(page_args) and count_sql.count('%s') == len(count_args)


def test_name_filter_strips_dr_and_uses_prefix_like():
    f = pl.normalise_filters({'name': 'Dr.  Asha  Rao'})
    assert f == {'name': ['asha', 'rao']}
    _, _, sql, args = pl.search_sql(f)
    assert args[:4] == ['asha%', 'asha%', 'rao%', 'rao%']   # prefix only: no leading wildcard, so indexes stay usable


@pytest.mark.parametrize('query, message', [
    ({'name': 'R<script>'}, 'name may contain'), ({'name': 'a b c d'}, 'three words'),
    ({'hospitalId': 'x'}, 'hospitalId'), ({'hospitalId': '0'}, 'hospitalId'), ({'limit': '0'}, 'limit'),
    ({'limit': '51'}, 'limit'), ({'limit': 'ten'}, 'limit'), ({'cursor': '%%%'}, 'cursor'),
])
def test_bad_search_input_is_rejected(monkeypatch, query, message):
    status, out, _, _ = run(monkeypatch, 'GET', '/doctors', query=query, role=None)
    assert status == 400 and message in out['message']


def doctor_row(uid, last, first='Asha'):
    return {'user_id': uid, 'first_name': first, 'last_name': last, 'specialization': 'Cardiology',
            'qualification': 'MBBS, MD', 'experience_years': 9, 'languages': 'English, Telugu', 'photo_key': None,
            'hospital_id': 3, 'hospital_name': 'CARE Hospitals', 'city': 'Hyderabad',
            'fee_from': Decimal('450.00'), 'type_labels': 'Quick consult|Standard consult'}


def test_search_is_public_and_returns_keyset_cursor(monkeypatch):
    cur = Cur(doctors=[doctor_row(5, 'Rao'), doctor_row(9, 'Reddy'), doctor_row(12, 'Sharma')], total=7)
    status, out, headers, _ = run(monkeypatch, 'GET', '/doctors', cur=cur, role=None,
                                  query={'specialization': 'Cardiology', 'limit': '2'})
    assert status == 200 and out['totalMatches'] == 7 and headers['x-correlation-id']
    assert [d['doctorId'] for d in out['items']] == [5, 9]             # the third row only proves hasMore
    assert out['page'] == {'limit': 2, 'returned': 2, 'hasMore': True, 'nextCursor': out['page']['nextCursor']}
    assert out['items'][0]['feeFrom'] == '450.00' and out['items'][0]['consultationTypes'] == ['Quick consult', 'Standard consult']
    assert 'phone' not in out['items'][0] and 'license_number' not in json.dumps(out)

    # Page 2: the cursor turns into a "strictly after (Reddy, Asha, 9)" condition.
    cur2 = Cur(doctors=[doctor_row(12, 'Sharma')], total=7)
    status, out2, _, cur2 = run(monkeypatch, 'GET', '/doctors', cur=cur2, role=None,
                                query={'specialization': 'Cardiology', 'limit': '2', 'cursor': out['page']['nextCursor']})
    page_sql, page_args = [x for x in cur2.sql if 'LIMIT' in x[0]][0]
    assert status == 200 and 'd.last_name > %s' in page_sql and page_args[1:6] == ['Reddy', 'Reddy', 'Asha', 'Asha', 9]
    assert out2['page']['hasMore'] is False and out2['page']['nextCursor'] is None


def test_cursor_cannot_be_reused_with_other_filters(monkeypatch):
    token = pl.encode_cursor(doctor_row(9, 'Reddy'), {'city': 'Hyderabad'})
    status, out, _, _ = run(monkeypatch, 'GET', '/doctors', role=None, query={'city': 'Chennai', 'cursor': token})
    assert status == 400 and 'different search' in out['message']


# ----------------------------------------------------------------- slots

def test_seat_view_counts_free_seats_and_hides_past_or_unknown():
    types = {'41': pl.type_payload({'consultation_type_id': 41, 'code': 'HEALTH_CAMP', 'label': 'Health camp',
                                    'duration_min': 60, 'fee_amount': Decimal('200'), 'capacity': 6,
                                    'min_capacity': 2, 'max_capacity': 10})}
    assert types['41']['groupSession'] is True and types['41']['seatsPerSlot'] == 6
    tz, now = pl.zone('Asia/Kolkata'), datetime(2026, 11, 6, 3, 0, tzinfo=timezone.utc)
    slot = {'slot_id': '5#41#2026-11-07T04:30:00Z', 'consultation_type_id': '41', 'start_at': '2026-11-07T04:30:00Z',
            'end_at': '2026-11-07T05:30:00Z', 'capacity': Decimal(6), 'booked_count': Decimal(2),
            'held_count': Decimal(1), 'status': 'OPEN'}
    view = pl.seat_view(slot, types, tz, now)
    assert view['freeSeats'] == 3 and view['status'] == 'AVAILABLE' and view['localStart'] == '10:00'
    assert view['fee'] == '200.00' and view['capacity'] == 6
    assert pl.seat_view(dict(slot, booked_count=5), types, tz, now)['status'] == 'FULL'
    assert pl.seat_view(dict(slot, status='CLOSED'), types, tz, now)['freeSeats'] == 0
    assert pl.seat_view(dict(slot, consultation_type_id='gone'), types, tz, now) is None
    assert pl.seat_view(slot, types, tz, datetime(2026, 11, 8, tzinfo=timezone.utc)) is None


class SlotCur(Cur):
    def execute(self, sql, args=()):
        s = ' '.join(sql.split())
        if 'FROM doctors d JOIN users u' in s and 'h.address' in s:
            self.rows = [{'user_id': 5, 'first_name': 'Asha', 'last_name': 'Rao', 'specialization': 'Cardiology',
                          'qualification': 'MD', 'experience_years': 9, 'languages': None, 'photo_key': None,
                          'timezone': 'Asia/Kolkata', 'hospital_id': 3, 'hospital_name': 'CARE', 'city': 'Hyderabad',
                          'address': 'Banjara Hills'}]
        elif 'FROM consultation_types' in s:
            self.rows = [{'consultation_type_id': 30, 'code': 'STANDARD', 'label': 'Standard consult',
                          'duration_min': 30, 'fee_amount': Decimal('450.00'), 'capacity': 1,
                          'min_capacity': 1, 'max_capacity': 1}]
        else:
            super().execute(sql, args)


@pytest.mark.parametrize('query, message', [
    ({'from': '2026-13-01'}, 'YYYY-MM-DD'), ({'from': '2026-11-10', 'to': '2026-11-09'}, 'on or after'),
    ({'consultationTypeId': 'nope'}, 'consultationTypeId'), ({'includeFull': 'maybe'}, 'includeFull'),
])
def test_slot_range_validation(monkeypatch, query, message):
    monkeypatch.setattr(pl, 'slots_table', lambda: None)
    status, out, _, _ = run(monkeypatch, 'GET', '/doctors/5/slots', cur=SlotCur(), role=None, query=query)
    assert status == 400 and message in out['message']


def test_slot_range_longer_than_window_rejected(monkeypatch):
    today = date.today()
    status, out, _, _ = run(monkeypatch, 'GET', '/doctors/5/slots', cur=SlotCur(), role=None,
                            query={'from': today.isoformat(), 'to': (today + timedelta(days=40)).isoformat()})
    assert status == 400 and 'at most 30 days' in out['message']


def test_slots_grouped_by_day_with_free_seat_totals(monkeypatch):
    tomorrow = datetime.now(pl.zone('Asia/Kolkata')).date() + timedelta(days=1)

    class Table:
        def query(self, IndexName, KeyConditionExpression, **kw):
            assert IndexName == 'GSI_DoctorDate'
            if KeyConditionExpression._values[1] != f'5#{tomorrow.isoformat()}':
                return {'Items': []}
            base = {'consultation_type_id': '30', 'capacity': 1, 'held_count': 0}
            return {'Items': [
                dict(base, slot_id='a', start_at=f'{tomorrow}T03:30:00Z', end_at=f'{tomorrow}T04:00:00Z', booked_count=0, status='OPEN'),
                dict(base, slot_id='b', start_at=f'{tomorrow}T04:00:00Z', end_at=f'{tomorrow}T04:30:00Z', booked_count=1, status='FULL'),
                dict(base, slot_id='c', start_at=f'{tomorrow}T04:30:00Z', end_at=f'{tomorrow}T05:00:00Z', booked_count=0, status='CLOSED'),
            ]}
    monkeypatch.setattr(pl, 'slots_table', lambda: Table())
    status, out, _, _ = run(monkeypatch, 'GET', '/doctors/5/slots', cur=SlotCur(), role=None,
                            query={'from': tomorrow.isoformat(), 'to': tomorrow.isoformat()})
    day = out['days'][0]
    assert status == 200 and [s['slotId'] for s in day['slots']] == ['a', 'b']      # CLOSED is hidden
    assert day['openSlots'] == 1 and day['freeSeats'] == 1 and out['summary']['bookableSlots'] == 1
    status, out, _, _ = run(monkeypatch, 'GET', '/doctors/5/slots', cur=SlotCur(), role=None,
                            query={'from': tomorrow.isoformat(), 'to': tomorrow.isoformat(), 'includeFull': 'false'})
    assert [s['slotId'] for s in out['days'][0]['slots']] == ['a']


def test_unknown_doctor_is_404(monkeypatch):
    status, out, _, _ = run(monkeypatch, 'GET', '/doctors/999', cur=Cur(), role=None)
    assert status == 404 and out['message'] == 'Doctor not found'
    assert run(monkeypatch, 'GET', '/doctors/abc', cur=Cur(), role=None)[0] == 400


# ----------------------------------------------------------------- profile CRUD + isolation

def test_get_me_returns_etag_and_completeness(monkeypatch):
    status, out, headers, _ = run(monkeypatch, 'GET', '/patient/me')
    assert status == 200 and headers['ETag'] == '"v3"' and out['version'] == 3
    assert out['dateOfBirth'] == '1994-05-02' and out['missingFields'] == ['emergencyContactPhone']


def test_patient_cannot_read_another_patient(monkeypatch):
    status, out, _, cur = run(monkeypatch, 'GET', '/patient/profiles/22')
    assert status == 403 and out['message'] == 'Patients can only read their own profile'
    assert not any('FROM users u LEFT JOIN patients' in s for s, _ in cur.sql)   # no read of the other row at all
    status, out, _, _ = run(monkeypatch, 'GET', '/patient/profiles/21')
    assert status == 200 and out['patientId'] == 21


def test_doctor_token_cannot_use_patient_routes_even_if_authorizer_is_bypassed(monkeypatch):
    for method, path in (('GET', '/patient/me'), ('PUT', '/patient/me'), ('GET', '/patient/profiles/21')):
        assert run(monkeypatch, method, path, role='doctor', body={'city': 'X'})[0] == 403


def test_update_validates_and_bumps_version(monkeypatch):
    status, out, headers, cur = run(monkeypatch, 'PUT', '/patient/me',
                                    body={'city': ' Hyderabad ', 'bloodGroup': 'ab+', 'version': 3})
    sql, args = [x for x in cur.sql if x[0].startswith('UPDATE patients')][0]
    assert 'profile_version = profile_version + 1' in sql and sql.endswith('AND profile_version = %s')
    assert args == ['Hyderabad', 'AB+', 21, 3] and status == 200 and headers['ETag'] == '"v4"'


def test_stale_version_gets_412_with_latest_copy(monkeypatch):
    cur = Cur(profile=dict(PROFILE, stale=True))
    status, out, _, _ = run(monkeypatch, 'PUT', '/patient/me', cur=cur, body={'city': 'Pune'},
                            headers={'If-Match': 'W/"v2"'})
    assert status == 412 and out['currentVersion'] == 3 and out['current']['city'] == 'Hyderabad'


@pytest.mark.parametrize('body, message', [
    ({}, 'at least one field'), ({'email': 'x@y.z'}, 'cannot be changed'), ({'favouriteColour': 'red'}, 'Unknown field'),
    ({'firstName': ''}, 'required'), ({'phone': '12'}, '7 to 15 digits'), ({'dateOfBirth': '2999-01-01'}, 'future'),
    ({'dateOfBirth': '1800-01-01'}, '120 years'), ({'gender': 'robot'}, 'gender must be'),
    ({'bloodGroup': 'C+'}, 'bloodGroup'), ({'address': '<b>x</b>'}, 'address'), ({'city': ['a']}, 'must be text'),
])
def test_update_rejects_bad_fields(monkeypatch, body, message):
    status, out, _, _ = run(monkeypatch, 'PUT', '/patient/me', body=body)
    assert status == 400 and message in out['message']


def test_create_requires_core_fields_and_refuses_duplicates(monkeypatch):
    status, out, _, _ = run(monkeypatch, 'POST', '/patient/me', body={'firstName': 'Meera'})
    assert status == 400 and 'lastName' in out['message'] and 'phone' in out['message']
    status, out, _, _ = run(monkeypatch, 'POST', '/patient/me',
                            body={'firstName': 'Meera', 'lastName': 'Iyer', 'phone': '9876543210'})
    assert status == 409


def test_delete_needs_confirmation_then_revokes_tokens(monkeypatch):
    assert run(monkeypatch, 'DELETE', '/patient/me', body={})[0] == 400

    class Tokens:
        def __init__(self): self.put, self.deleted = [], []
        def put_item(self, Item): self.put.append(Item)
        def get_item(self, Key): return {'Item': {'user_id': '21'}}
        def delete_item(self, Key): self.deleted.append(Key)
    tokens = Tokens()
    monkeypatch.setattr(pl, 'auth_table', lambda: tokens)
    status, out, _, cur = run(monkeypatch, 'DELETE', '/patient/me', body={'confirm': 'DELETE', 'refreshToken': 'r1'})
    assert status == 200 and out['deleted'] is True
    assert any(s.startswith('DELETE FROM patients') for s, _ in cur.sql)
    assert any(s.startswith('UPDATE users SET is_active = FALSE') for s, _ in cur.sql)
    assert tokens.put[0]['token_key'] == 'JTI#j-1' and tokens.deleted[0]['token_type'] == 'REFRESH'


def test_unknown_route_and_wrong_method(monkeypatch):
    assert run(monkeypatch, 'GET', '/patient/elsewhere')[0] == 404
    assert run(monkeypatch, 'PATCH', '/patient/me')[0] == 405
