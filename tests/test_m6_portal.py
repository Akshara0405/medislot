"""Milestone 6/7 web portal: waitlists + hold offers, reschedule, full-slot "Join waitlist", doctor week view, guards."""
import os
import sys

os.environ.setdefault('FLASK_SECRET_KEY', 'test-secret')
os.environ.setdefault('API_BASE_URL', 'https://api.example.test/dev')
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'dashboard'))
import app as dashboard  # noqa: E402

SLOT = '5#30#2026-12-01T03:30:00Z'
TOKEN = 'NSMzMCMyMDI2LTEyLTAxVDAzOjMwOjAwWg'
OFFER = TOKEN + '.' + 'a' * 32


class Res:
    def __init__(self, status, data=None):
        self.status_code, self._data, self.ok = status, data or {}, status < 400

    def json(self):
        return self._data


APPOINTMENT = {'appointmentId': 9, 'reference': 'MS-000009', 'status': 'CONFIRMED', 'slotId': SLOT,
               'startsAt': '2026-12-01T03:30:00Z', 'endsAt': '2026-12-01T04:00:00Z', 'timezone': 'Asia/Kolkata',
               'rescheduleCount': 0, 'fee': '450.00',
               'doctor': {'doctorId': 5, 'name': 'Dr. Asha Rao', 'hospital': {'name': 'CARE Hospitals'}},
               'consultation': {'consultationTypeId': 30, 'label': 'Standard consult'}}


def fake(monkeypatch, responses):
    calls = []
    dashboard._filter_cache.update(at=0.0, data=None)

    def request(method, url, headers=None, json=None, params=None, timeout=None):
        path = url.split('/dev', 1)[1]
        calls.append((method, path, json))
        if (method, path) == ('GET', '/patient/me'):
            return Res(200, {'firstName': 'Meera', 'missingFields': []})
        return responses[(method, path)]

    def get(url, params=None, timeout=None, **kw):
        path = url.split('/dev', 1)[1]
        calls.append(('GET', path, params))
        if path == '/doctors/filters':
            return Res(200, {'specializations': [], 'cities': [], 'hospitals': []})
        return responses[('GET', path)]

    monkeypatch.setattr(dashboard.requests, 'request', request)
    monkeypatch.setattr(dashboard.requests, 'get', get)
    return calls


def signed_in(client, role='patient'):
    with client.session_transaction() as s:
        s.update(token='a1', refresh_token='r1', token_exp=9e12, role=role, user_id='21')


def test_waitlists_tab_shows_position_and_held_seat(monkeypatch):
    fake(monkeypatch, {('GET', '/patient/waitlists'): Res(200, {'cap': 5, 'pendingOffers': 1, 'items': [
        {'waitlistId': TOKEN, 'slotId': SLOT, 'startsAt': '2026-12-01T03:30:00Z', 'timezone': 'Asia/Kolkata',
         'position': 1, 'waiting': 3, 'doctor': {'name': 'Dr. Asha Rao'}, 'consultation': {'label': 'Standard consult'},
         'offer': {'offerId': OFFER, 'expiresAt': '2026-11-30T04:15:00Z', 'secondsLeft': 600}}]})})
    client = dashboard.app.test_client()
    signed_in(client)
    page = client.get('/patient/dashboard?tab=waitlists').get_data(as_text=True)
    assert 'Seat held for you until' in page and '10 min left' in page and 'Accept and book' in page
    assert 'Waitlists</a>' in page                                           # top-bar link


def test_accept_offer_books_and_goes_to_appointments(monkeypatch):
    calls = fake(monkeypatch, {('POST', f'/patient/offers/{OFFER}/accept'): Res(201, APPOINTMENT)})
    client = dashboard.app.test_client()
    signed_in(client)
    res = client.post('/patient/dashboard', data={'action': 'accept_offer', 'token': OFFER})
    assert res.status_code == 302 and 'tab=appointments' in res.headers['Location']
    assert ('POST', f'/patient/offers/{OFFER}/accept', None) in calls


def test_join_waitlist_from_a_full_slot(monkeypatch):
    calls = fake(monkeypatch, {('POST', '/patient/waitlists'): Res(201, {'position': 2})})
    client = dashboard.app.test_client()
    signed_in(client)
    res = client.post('/patient/dashboard', data={'action': 'join_waitlist', 'slot_id': SLOT, 'doctor_id': '5'})
    assert 'tab=waitlists' in res.headers['Location']
    assert ('POST', '/patient/waitlists', {'slotId': SLOT}) in calls


def test_reschedule_page_lists_free_slots_and_posts(monkeypatch):
    other = '5#30#2026-12-02T03:30:00Z'
    calls = fake(monkeypatch, {
        ('GET', '/patient/appointments/9'): Res(200, dict(APPOINTMENT)),
        ('GET', '/doctors/5/slots'): Res(200, {'days': [{'date': '2026-12-02', 'slots': [
            {'slotId': other, 'localStart': '09:00', 'freeSeats': 1, 'capacity': 1},
            {'slotId': SLOT, 'localStart': '09:00', 'freeSeats': 1, 'capacity': 1}]}]}),
        ('POST', '/patient/appointments/9/reschedule'): Res(200, {'status': 'SUCCEEDED', 'appointment': dict(
            APPOINTMENT, slotId=other, startsAt='2026-12-02T03:30:00Z')})})
    client = dashboard.app.test_client()
    signed_in(client)
    page = client.get('/patient/appointments/9/reschedule').get_data(as_text=True)
    assert 'Reschedule MS-000009' in page and page.count('Move here') == 1   # the current slot is not offered
    res = client.post('/patient/dashboard', data={'action': 'reschedule', 'appointment_id': '9', 'slot_id': other})
    assert 'tab=appointments' in res.headers['Location']
    assert ('POST', '/patient/appointments/9/reschedule', {'slotId': other}) in calls


def test_compensated_reschedule_is_explained(monkeypatch):
    fake(monkeypatch, {('POST', '/patient/appointments/9/reschedule'): Res(409, {
        'status': 'COMPENSATED', 'message': 'The reschedule failed part-way and was rolled back.'})})
    client = dashboard.app.test_client()
    signed_in(client)
    client.post('/patient/dashboard', data={'action': 'reschedule', 'appointment_id': '9', 'slot_id': SLOT})
    with client.session_transaction() as s:
        assert any('rolled back' in m for _, m in s['_flashes'])


def test_route_guards_both_directions(monkeypatch):
    fake(monkeypatch, {})
    client = dashboard.app.test_client()
    signed_in(client, role='patient')
    assert '/doctor/login' in client.get('/doctor/dashboard').headers['Location']
    signed_in(client, role='doctor')
    assert '/patient/login' in client.get('/patient/dashboard?tab=waitlists').headers['Location']
    assert '/patient/login' in client.get('/patient/appointments/9/reschedule').headers['Location']


def test_doctor_week_view_starts_on_monday(monkeypatch):
    seen = {}

    def request(method, url, headers=None, json=None, params=None, timeout=None):
        path = url.split('/dev', 1)[1]
        if path == '/doctor/me':
            return Res(200, {'user_id': 7, 'first_name': 'Asha'})
        if path.endswith('/slots'):
            seen['slots'] = params
            return Res(200, {'view': 'owner', 'days': []})
        if path == '/doctor/me/appointments':
            seen['appointments'] = params
        return Res(200, {'items': []})

    monkeypatch.setattr(dashboard.requests, 'request', request)
    monkeypatch.setattr(dashboard.requests, 'get', lambda *a, **k: Res(200, {'items': []}))
    client = dashboard.app.test_client()
    signed_in(client, role='doctor')
    with client.session_transaction() as s:
        s['user_id'] = '7'
    page = client.get('/doctor/dashboard?tab=schedule&week=2026-12-03').get_data(as_text=True)   # a Thursday
    assert seen['slots'] == {'from': '2026-11-30', 'to': '2026-12-06'}
    assert seen['appointments']['days'] == 7 and 'Week of 30 Nov - 06 Dec 2026' in page
