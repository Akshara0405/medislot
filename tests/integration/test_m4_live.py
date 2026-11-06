"""Milestone 4 integration tests against the DEPLOYED API (API Gateway -> Lambda -> RDS / DynamoDB).

Run by the deploy job after the stacks are updated. Locally:
    MEDISLOT_API_URL=https://xxxx.execute-api.ap-south-1.amazonaws.com/dev \
    MEDISLOT_CI_PASSWORD=... pytest -v tests/integration

Two long-lived "CI probe" doctors are reused on every run (registered once, then signed in), so repeated runs do
not fill the mentor's search results with junk. Patients are created per run and deleted at the end.
"""
import itertools
import os
import uuid
from datetime import date, timedelta

import pytest
import requests

API = os.environ.get('MEDISLOT_API_URL', '').rstrip('/')
PASSWORD = os.environ.get('MEDISLOT_CI_PASSWORD', '')
pytestmark = pytest.mark.skipif(not API or not PASSWORD,
                                reason='set MEDISLOT_API_URL and MEDISLOT_CI_PASSWORD to run live tests')
RUN = uuid.uuid4().hex[:8]
PROBES = [  # (email, first, last, specialization, licence)
    ('ci-probe-anand@medislot-ci.example', 'Anand', 'Ciprobe', 'Cardiology', 'ZZ-904401'),
    ('ci-probe-bhavna@medislot-ci.example', 'Bhavna', 'Ciprobe', 'Dermatology', 'ZZ-904402'),
]


def call(method, path, token=None, **kw):
    headers = kw.pop('headers', {})
    if token:
        headers['Authorization'] = 'Bearer ' + token
    return requests.request(method, API + path, headers=headers, timeout=30, **kw)


def login(email, password=PASSWORD):
    res = call('POST', '/auth/login', json={'email': email, 'password': password})
    return res.json() if res.ok else None


@pytest.fixture(scope='module')
def hospitals():
    items = call('GET', '/auth/hospitals').json()['items']
    assert items, 'no active hospitals seeded'
    return items


@pytest.fixture(scope='module')
def probe_doctors(hospitals):
    """Register (first run) or sign in the two probe doctors, then give each a fee and a full week of slots."""
    doctors = []
    for i, (email, first, last, spec, licence) in enumerate(PROBES):
        hospital = hospitals[min(i, len(hospitals) - 1)]
        session = login(email)
        if not session:
            reg = call('POST', '/auth/register/doctor', json={
                'email': email, 'password': PASSWORD, 'firstName': first, 'lastName': last, 'phone': '9000000000',
                'specialization': spec, 'licenseNumber': licence, 'hospitalId': hospital['hospital_id'],
                'qualification': 'MBBS', 'experienceYears': 5})
            assert reg.status_code == 201, reg.text
            session = login(email)
        assert session and session['role'] == 'doctor'
        token = session['accessToken']
        me = call('GET', '/doctor/me', token).json()
        types = call('GET', '/doctor/me/consultation-types', token).json()['items']
        standard = next((t for t in types if t['name'] == 'STANDARD'), None)
        body = {'name': 'STANDARD', 'duration_min': 30, 'capacity': 1, 'fee_amount': '450.00'}
        if standard:
            assert call('PUT', f"/doctor/me/consultation-types/{standard['consultation_type_id']}", token,
                        json=body).ok
            type_id = standard['consultation_type_id']
        else:
            created = call('POST', '/doctor/me/consultation-types', token, json=body)
            assert created.status_code == 201, created.text
            type_id = created.json()['consultationTypeId']
        week = [{'day_of_week': d, 'consultation_type_id': type_id, 'start_time': '20:00', 'end_time': '21:00'}
                for d in range(7)]
        saved = call('PUT', '/doctor/me/availability', token, json={'patterns': week})
        assert saved.ok, saved.text
        doctors.append({'id': int(me['user_id']), 'token': token, 'first': first, 'last': last, 'spec': spec,
                        'hospital': hospital, 'type_id': type_id})
    return doctors


def new_patient(tag):
    email = f'ci-{RUN}-{tag}@medislot-ci.example'
    reg = call('POST', '/auth/register/patient', json={
        'email': email, 'password': PASSWORD, 'firstName': 'Ci', 'lastName': f'Patient{tag.upper()}',
        'phone': '9000000001', 'dateOfBirth': '1995-01-15', 'gender': 'Female'})
    assert reg.status_code == 201, reg.text
    session = login(email)
    return {'id': int(session['userId']), 'token': session['accessToken'], 'refresh': session['refreshToken'],
            'email': email}


@pytest.fixture(scope='module')
def patients():
    a, b = new_patient('a'), new_patient('b')
    yield a, b
    for p in (a, b):        # clean up: erase both test patients
        call('DELETE', '/patient/me', p['token'], json={'confirm': 'DELETE', 'refreshToken': p['refresh']})


# ----------------------------------------------------------------- profile CRUD

def test_profile_requires_a_token():
    assert call('GET', '/patient/me').status_code == 401


def test_profile_read_update_and_optimistic_concurrency(patients):
    a, _ = patients
    me = call('GET', '/patient/me', a['token'])
    assert me.status_code == 200 and me.json()['email'] == a['email'] and me.headers.get('ETag')
    version = me.json()['version']
    upd = call('PUT', '/patient/me', a['token'], json={'city': 'Hyderabad', 'bloodGroup': 'o+', 'version': version})
    assert upd.status_code == 200 and upd.json()['bloodGroup'] == 'O+' and upd.json()['version'] == version + 1
    stale = call('PUT', '/patient/me', a['token'], json={'city': 'Pune'}, headers={'If-Match': f'"v{version}"'})
    assert stale.status_code == 412 and stale.json()['currentVersion'] == version + 1
    assert call('PUT', '/patient/me', a['token'], json={'email': 'x@y.z'}).status_code == 400
    assert call('PUT', '/patient/me', a['token'], json={'dateOfBirth': '2999-01-01'}).status_code == 400
    assert call('POST', '/patient/me', a['token'],
                json={'firstName': 'A', 'lastName': 'B', 'phone': '9000000001'}).status_code == 409


# ----------------------------------------------------------------- isolation

def test_patient_cannot_read_another_patients_profile(patients):
    a, b = patients
    denied = call('GET', f"/patient/profiles/{b['id']}", a['token'])
    assert denied.status_code == 403
    assert b['email'] not in denied.text and 'PatientB' not in denied.text
    assert call('GET', f"/patient/profiles/{a['id']}", a['token']).status_code == 200
    # Even a made-up id gets the same 403, so nobody can probe which patient ids exist.
    assert call('GET', '/patient/profiles/999999999', a['token']).status_code == 403


def test_doctor_token_is_rejected_on_patient_routes(probe_doctors):
    assert call('GET', '/patient/me', probe_doctors[0]['token']).status_code == 403


def test_patient_token_is_rejected_on_doctor_routes(patients):
    assert call('GET', '/doctor/me', patients[0]['token']).status_code == 403


# ----------------------------------------------------------------- search

def search(**params):
    res = call('GET', '/doctors', params=params)
    assert res.status_code == 200, res.text
    return res.json()


def test_every_filter_combination_finds_the_probe_doctor(probe_doctors):
    doc = probe_doctors[0]
    values = {'name': doc['last'], 'hospitalId': doc['hospital']['hospital_id'], 'specialization': doc['spec'],
              'city': doc['hospital']['city']}
    for size in range(1, 5):
        for combo in itertools.combinations(values, size):
            params = {k: values[k] for k in combo}
            params['limit'] = 50
            found, cursor = [], None
            while True:
                page = search(**params, **({'cursor': cursor} if cursor else {}))
                found += [d['doctorId'] for d in page['items']]
                cursor = page['page']['nextCursor']
                if not cursor:
                    break
            assert doc['id'] in found, f'probe doctor missing for filters {combo}'


def test_filters_narrow_results(probe_doctors):
    a, b = probe_doctors
    ids = [d['doctorId'] for d in search(name=f"Dr {a['first']} {a['last']}")['items']]
    assert a['id'] in ids and b['id'] not in ids
    spec_ids = [d['doctorId'] for d in search(name=a['last'], specialization=a['spec'])['items']]
    assert a['id'] in spec_ids and b['id'] not in spec_ids
    assert search(name=a['last'], city='No Such City Anywhere')['totalMatches'] == 0
    hospital_prefix = a['hospital']['name'][:4]
    assert a['id'] in [d['doctorId'] for d in search(name=a['last'], hospital=hospital_prefix)['items']]


def test_keyset_pagination_walks_every_page_once(probe_doctors):
    first = search(name='Ciprobe', limit=1)
    assert first['totalMatches'] >= 2 and first['page']['hasMore'] is True
    second = search(name='Ciprobe', limit=1, cursor=first['page']['nextCursor'])
    assert second['items'][0]['doctorId'] != first['items'][0]['doctorId']
    names = [(d['lastName'], d['firstName']) for d in first['items'] + second['items']]
    assert names == sorted(names, key=lambda n: (n[0].lower(), n[1].lower()))


@pytest.mark.parametrize('params', [{'limit': 0}, {'limit': 51}, {'hospitalId': 'abc'}, {'cursor': 'not-a-cursor'},
                                    {'name': 'a b c d e'}])
def test_bad_search_parameters_are_400(params):
    assert call('GET', '/doctors', params=params).status_code == 400


def test_cursor_from_another_search_is_rejected(probe_doctors):
    page = search(name='Ciprobe', limit=1)
    res = call('GET', '/doctors', params={'name': 'Ciprobe', 'city': 'Elsewhere', 'limit': 1,
                                          'cursor': page['page']['nextCursor']})
    assert res.status_code == 400


# ----------------------------------------------------------------- public profile + slots

def test_public_profile_has_types_and_fees_but_no_private_data(probe_doctors):
    doc = probe_doctors[0]
    res = call('GET', f"/doctors/{doc['id']}")
    assert res.status_code == 200
    data = res.json()
    standard = next(t for t in data['consultationTypes'] if t['code'] == 'STANDARD')
    assert standard['fee'] == '450.00' and standard['durationMinutes'] == 30 and standard['seatsPerSlot'] == 1
    for private in ('phone', 'license_number', 'licenseNumber', 'email'):
        assert private not in data
    assert call('GET', '/doctors/999999999').status_code == 404


def test_slots_show_free_seats_for_a_date_range(probe_doctors):
    doc = probe_doctors[0]
    start = date.today() + timedelta(days=1)
    res = call('GET', f"/doctors/{doc['id']}/slots", params={'from': start.isoformat(),
                                                             'to': (start + timedelta(days=6)).isoformat()})
    assert res.status_code == 200, res.text
    data = res.json()
    assert len(data['days']) == 7
    slots = [s for d in data['days'] for s in d['slots']]
    assert slots, 'probe doctor should have 20:00-21:00 slots every day'
    assert all(0 <= s['freeSeats'] <= s['capacity'] for s in slots)
    assert any(s['freeSeats'] == s['capacity'] == 1 for s in slots)
    assert data['summary']['freeSeats'] == sum(d['freeSeats'] for d in data['days'])


@pytest.mark.parametrize('params', [{'from': 'yesterday'}, {'from': '2030-01-10', 'to': '2030-01-01'},
                                    {'from': date.today().isoformat(),
                                     'to': (date.today() + timedelta(days=45)).isoformat()},
                                    {'consultationTypeId': 'not-a-type'}])
def test_bad_slot_ranges_are_400(probe_doctors, params):
    assert call('GET', f"/doctors/{probe_doctors[0]['id']}/slots", params=params).status_code == 400


# ----------------------------------------------------------------- delete (runs last in this module)

def test_zz_deleted_patient_can_no_longer_sign_in():
    p = new_patient('gone')
    res = call('DELETE', '/patient/me', p['token'], json={'confirm': 'DELETE', 'refreshToken': p['refresh']})
    assert res.status_code == 200
    assert login(p['email']) is None
    assert call('GET', '/patient/me', p['token']).status_code in (401, 403)       # access token was revoked
    assert call('POST', '/auth/refresh', json={'refreshToken': p['refresh']}).status_code == 401
