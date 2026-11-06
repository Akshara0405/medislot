"""Patient side of the Flask dashboard: register -> sign in -> search -> slots -> profile, all through the API."""
import os
import sys
from urllib.parse import parse_qs, urlparse

os.environ.setdefault('FLASK_SECRET_KEY', 'test-secret')
os.environ.setdefault('API_BASE_URL', 'https://api.example.test/dev')
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'dashboard'))
import app as dashboard  # noqa: E402

FAKE_PASSWORD = 'Test-only-' + 'pw-5678'  # gitleaks:allow


class Res:
    def __init__(self, status, data=None):
        self.status_code, self._data, self.ok = status, data or {}, status < 400

    def json(self):
        return self._data


FILTERS = {'specializations': ['Cardiology'], 'cities': ['Hyderabad'],
           'hospitals': [{'hospitalId': 3, 'name': 'CARE Hospitals', 'city': 'Hyderabad'}]}
ME = {'patientId': 21, 'email': 'meera@example.com', 'firstName': 'Meera', 'lastName': 'Iyer', 'phone': '9876543210',
      'city': 'Hyderabad', 'version': 3, 'missingFields': ['emergencyContactPhone'], 'profileComplete': False,
      'updatedAt': '2026-11-05T09:00:00'}
DOCTOR = {'doctorId': 5, 'name': 'Dr. Asha Rao', 'firstName': 'Asha', 'lastName': 'Rao',
          'specialization': 'Cardiology', 'qualification': 'MD', 'experienceYears': 9,
          'hospital': {'hospitalId': 3, 'name': 'CARE Hospitals', 'city': 'Hyderabad'}, 'feeFrom': '450.00',
          'consultationTypes': ['Standard consult'], 'acceptingBookings': True, 'photoUrl': None}


def fake_api(monkeypatch, *, role='patient', put_status=200, search_more=False):
    calls = []
    dashboard._filter_cache.update(at=0.0, data=None)

    def post(url, json=None, headers=None, timeout=None):
        path = url.split('/dev', 1)[1]
        calls.append(('POST', path, json))
        if path == '/auth/register/patient':
            return Res(201, {'userId': '21'})
        if path == '/auth/login':
            return Res(200, {'accessToken': 'a1', 'refreshToken': 'r1', 'expiresIn': 900, 'role': role})
        if path == '/auth/logout':
            return Res(200)
        raise AssertionError(path)

    def get(url, params=None, timeout=None, **kw):
        path = url.split('/dev', 1)[1]
        calls.append(('GET', path, params))
        if path == '/doctors/filters':
            return Res(200, FILTERS)
        if path == '/doctors':
            return Res(200, {'items': [DOCTOR], 'totalMatches': 3,
                             'page': {'limit': 8, 'returned': 1, 'hasMore': search_more,
                                      'nextCursor': 'CUR2' if search_more else None}})
        if path == '/doctors/5':
            return Res(200, dict(DOCTOR, timezone='Asia/Kolkata', consultationTypes=[
                {'consultationTypeId': 't30', 'label': 'Standard consult', 'fee': '450.00', 'durationMinutes': 30,
                 'seatsPerSlot': 1, 'groupSession': False}], nextOpenSlot=None, bookingStatus='NO_OPEN_SLOTS_SOON'))
        if path == '/doctors/5/slots':
            return Res(200, {'summary': {'bookableSlots': 1, 'freeSeats': 1}, 'notices': [], 'days': [
                {'date': params['from'], 'closed': False, 'fullyBooked': False, 'openSlots': 1, 'freeSeats': 1,
                 'slots': [{'localStart': '09:00', 'localEnd': '09:30', 'label': 'Standard consult',
                            'durationMinutes': 30, 'fee': '450.00', 'capacity': 1, 'freeSeats': 1}]}]})
        if path == '/doctors/404':
            return Res(404, {'message': 'Doctor not found'})
        raise AssertionError(path)

    def request(method, url, headers=None, json=None, params=None, timeout=None):
        path = url.split('/dev', 1)[1]
        calls.append((method, path, json))
        if (method, path) == ('GET', '/patient/me'):
            return Res(200, ME)
        if (method, path) == ('PUT', '/patient/me'):
            return Res(put_status, {'message': 'conflict'} if put_status == 412 else ME)
        if (method, path) == ('DELETE', '/patient/me'):
            return Res(200, {'deleted': True})
        raise AssertionError((method, path))

    monkeypatch.setattr(dashboard.requests, 'post', post)
    monkeypatch.setattr(dashboard.requests, 'get', get)
    monkeypatch.setattr(dashboard.requests, 'request', request)
    return calls


def register_form(**extra):
    data = {'first_name': 'Meera', 'last_name': 'Iyer', 'email': 'Meera@Example.com', 'phone': '98765 43210',
            'password': FAKE_PASSWORD, 'confirm_password': FAKE_PASSWORD, 'date_of_birth': '1994-05-02',
            'gender': 'Female', 'city': 'Hyderabad', 'blood_group': 'O+', 'emergency_contact_phone': '9000000001'}
    data.update(extra)
    return data


def signed_in(client, role='patient'):
    with client.session_transaction() as s:
        s.update(token='a1', refresh_token='r1', token_exp=9e12, role=role)


def test_registration_saves_extra_fields_then_sends_patient_to_login(monkeypatch):
    calls = fake_api(monkeypatch)
    client = dashboard.app.test_client()
    res = client.post('/patient/register', data=register_form())
    assert res.status_code == 302 and '/patient/login?email=meera@example.com' in res.headers['Location']
    sequence = [(m, p) for m, p, _ in calls if p != '/doctors/filters']
    assert sequence == [('POST', '/auth/register/patient'), ('POST', '/auth/login'), ('PUT', '/patient/me'),
                        ('POST', '/auth/logout')]
    put_body = [b for m, p, b in calls if m == 'PUT'][0]
    assert put_body == {'city': 'Hyderabad', 'bloodGroup': 'O+', 'emergencyContactPhone': '9000000001'}
    with client.session_transaction() as s:
        assert 'token' not in s


def test_registration_requires_mandatory_fields_without_calling_api(monkeypatch):
    calls = fake_api(monkeypatch)
    client = dashboard.app.test_client()
    for bad in ({'city': ''}, {'gender': 'Robot'}, {'date_of_birth': '2999-01-01'}, {'phone': '12'},
                {'confirm_password': 'different-1'}):
        res = client.post('/patient/register', data=register_form(**bad))
        assert res.status_code == 400
    assert not [c for c in calls if c[1].startswith('/auth')]


def test_doctor_account_cannot_sign_in_to_patient_portal(monkeypatch):
    calls = fake_api(monkeypatch, role='doctor')
    client = dashboard.app.test_client()
    res = client.post('/patient/login', data={'email': 'doc@example.com', 'password': FAKE_PASSWORD})
    assert res.status_code == 200 and 'doctor account' in res.get_data(as_text=True)
    assert ('POST', '/auth/logout') in [(m, p) for m, p, _ in calls]
    with client.session_transaction() as s:
        assert 'token' not in s


def test_patient_pages_need_a_patient_session(monkeypatch):
    fake_api(monkeypatch)
    client = dashboard.app.test_client()
    assert '/patient/login' in client.get('/patient/dashboard').headers['Location']
    signed_in(client, role='doctor')
    assert '/patient/login' in client.get('/patient/dashboard').headers['Location']
    assert '/patient/login' in client.get('/patient/doctors/5').headers['Location']


def test_first_visit_searches_patient_city_and_builds_pagination(monkeypatch):
    calls = fake_api(monkeypatch, search_more=True)
    client = dashboard.app.test_client()
    signed_in(client)
    page = client.get('/patient/dashboard').get_data(as_text=True)
    search = [p for m, path, p in calls if path == '/doctors'][0]
    assert search['city'] == 'Hyderabad' and search['limit'] == 8
    assert 'Dr. Asha Rao' in page and 'from ₹450.00' in page and 'Showing doctors in your city first' in page
    next_href = [x for x in page.split('"') if 'cursor=CUR2' in x][0]
    qs = parse_qs(urlparse(next_href.replace('&amp;', '&')).query)
    assert qs['cursor'] == ['CUR2'] and qs['trail'] == ['-'] and qs['city'] == ['Hyderabad']


def test_explicit_filters_are_forwarded(monkeypatch):
    calls = fake_api(monkeypatch)
    client = dashboard.app.test_client()
    signed_in(client)
    client.get('/patient/dashboard?tab=find&searched=1&name=rao&specialization=Cardiology&hospital_id=3&city=')
    search = [p for m, path, p in calls if path == '/doctors'][-1]
    assert search == {'name': 'rao', 'specialization': 'Cardiology', 'hospitalId': '3', 'limit': 8}   # blanks dropped


def test_doctor_page_shows_fees_and_seat_cards(monkeypatch):
    calls = fake_api(monkeypatch)
    client = dashboard.app.test_client()
    signed_in(client)
    page = client.get('/patient/doctors/5?free=1').get_data(as_text=True)
    assert '₹450.00' in page and '9:00 AM' in page and '1 of 1 seat free' in page
    slots_params = [p for m, path, p in calls if path == '/doctors/5/slots'][0]
    assert slots_params['includeFull'] == 'false'
    assert '/patient/dashboard' in client.get('/patient/doctors/404').headers['Location']


def test_profile_conflict_shows_warning(monkeypatch):
    calls = fake_api(monkeypatch, put_status=412)
    client = dashboard.app.test_client()
    signed_in(client)
    res = client.post('/patient/dashboard', data={'action': 'profile', 'version': '3', 'first_name': 'Meera',
                                                  'last_name': 'Iyer', 'phone': '9876543210', 'city': 'Pune'})
    assert res.status_code == 302 and 'tab=profile' in res.headers['Location']
    put_body = [b for m, p, b in calls if m == 'PUT'][0]
    assert put_body['version'] == 3 and put_body['city'] == 'Pune'
    assert 'changed in another tab' in client.get(res.headers['Location']).get_data(as_text=True)


def test_delete_account_needs_typed_confirmation(monkeypatch):
    calls = fake_api(monkeypatch)
    client = dashboard.app.test_client()
    signed_in(client)
    client.post('/patient/dashboard', data={'action': 'delete_account', 'confirm': 'delete'})
    assert not [c for c in calls if c[0] == 'DELETE']
    res = client.post('/patient/dashboard', data={'action': 'delete_account', 'confirm': 'DELETE'})
    assert res.headers['Location'].endswith('/')
    assert [b for m, p, b in calls if m == 'DELETE'][0] == {'confirm': 'DELETE', 'refreshToken': 'r1'}
    with client.session_transaction() as s:
        assert 'token' not in s
