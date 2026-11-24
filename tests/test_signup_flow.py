"""Doctor sign-up: the account is created, the photo is uploaded, then the doctor is sent to the login page."""
import io
import os
import sys

os.environ.setdefault('FLASK_SECRET_KEY', 'test-secret')
os.environ.setdefault('API_BASE_URL', 'https://api.example.test/dev')
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'dashboard'))
import app as dashboard  # noqa: E402

# Made-up value used only in these tests, never a real account. The comment tells the gitleaks scan in CI so.
FAKE_PASSWORD = 'Test-only-' + 'pw-1234'  # gitleaks:allow


class Res:
    def __init__(self, status, data=None):
        self.status_code, self._data, self.ok = status, data or {}, status < 400

    def json(self):
        return self._data


def form(**extra):
    data = {'first_name': 'Asha', 'last_name': 'Rao', 'email': 'Asha@Example.com', 'phone': '9876543210',
            'password': FAKE_PASSWORD, 'confirm_password': FAKE_PASSWORD, 'specialization': 'Cardiology',
            'license_number': 'TS-123456', 'hospital_id': '1', 'qualification': 'MBBS', 'experience_years': '4',
            'photo': (io.BytesIO(b'\xff\xd8\xffphoto'), 'me.jpg', 'image/jpeg')}
    data.update(extra)
    return data


def fake_api(monkeypatch, photo_ok=True):
    calls = []

    def post(url, json=None, headers=None, timeout=None):
        path = url.split('/dev', 1)[1]
        calls.append(path)
        if path == '/auth/register/doctor':
            return Res(201, {'userId': '7'})
        if path == '/auth/login':
            return Res(200, {'accessToken': 'a1', 'refreshToken': 'r1', 'expiresIn': 900, 'role': 'doctor'})
        if path == '/auth/logout':
            assert headers['Authorization'] == 'Bearer a1' and json == {'refreshToken': 'r1'}
            return Res(200)
        if path == '/auth/hospitals':
            return Res(200, {'items': []})
        raise AssertionError(path)

    def request(method, url, headers=None, json=None, params=None, timeout=None):
        path = url.split('/dev', 1)[1]
        calls.append(path)
        if path == '/auth/hospitals':
            return Res(200, {'items': []})
        assert path == '/doctor/me/photo' and headers['Authorization'] == 'Bearer a1'
        return Res(200, {'uploadUrl': 'https://s3.example.test/upload'}) if photo_ok else Res(500, {'message': 'down'})

    def put(url, data=None, headers=None, timeout=None):
        calls.append('S3 PUT')
        return Res(200)

    monkeypatch.setattr(dashboard.requests, 'post', post)
    monkeypatch.setattr(dashboard.requests, 'request', request)
    monkeypatch.setattr(dashboard.requests, 'put', put)
    monkeypatch.setattr(dashboard.requests, 'get', lambda *a, **k: Res(200, {'items': []}))
    return calls


def test_signup_goes_to_login_page_not_dashboard(monkeypatch):
    calls = fake_api(monkeypatch)
    client = dashboard.app.test_client()
    res = client.post('/doctor/signup', data=form(), content_type='multipart/form-data')
    assert res.status_code == 302
    assert '/doctor/login' in res.headers['Location'] and 'email=asha@example.com' in res.headers['Location']
    assert calls == ['/auth/register/doctor', '/auth/login', '/doctor/me/photo', 'S3 PUT', '/auth/logout']
    with client.session_transaction() as s:
        assert 'token' not in s and 'refresh_token' not in s and 'role' not in s
    # The dashboard is not reachable until the doctor signs in.
    assert '/doctor/login' in client.get('/doctor/dashboard').headers['Location']
    page = client.get(res.headers['Location']).get_data(as_text=True)
    assert 'value="asha@example.com"' in page and 'Please sign in to continue' in page


def test_signup_with_failed_photo_still_goes_to_login_and_revokes_tokens(monkeypatch):
    calls = fake_api(monkeypatch, photo_ok=False)
    client = dashboard.app.test_client()
    res = client.post('/doctor/signup', data=form(), content_type='multipart/form-data')
    assert res.status_code == 302 and '/doctor/login' in res.headers['Location']
    assert calls[-1] == '/auth/logout'
    with client.session_transaction() as s:
        assert 'token' not in s

def test_schedule_tab_reads_the_shared_slots_endpoint_with_the_doctor_token(monkeypatch):
    """The doctor dashboard no longer has its own schedule API: it calls GET /doctors/{id}/slots like patients do."""
    seen = {}

    def request(method, url, headers=None, json=None, params=None, timeout=None):
        path = url.split('/dev', 1)[1]
        if path == '/doctor/me':
            return Res(200, {'user_id': 7, 'timezone': 'Asia/Kolkata', 'first_name': 'Asha', 'last_name': 'Rao'})
        if path in ('/doctor/me/availability', '/doctor/me/blackouts'):
            return Res(200, {'items': []})
        if path == '/doctor/me/consultation-types':
            return Res(200, {'items': [{'consultation_type_id': 30, 'consultation_id': 2, 'name': 'STANDARD',
                                        'label': 'Standard consult', 'duration_min': 30, 'fee_amount': '450.00',
                                        'capacity': 1, 'min_capacity': 1, 'max_capacity': 1, 'is_active': 1}],
                             'catalog': []})
        if path == '/doctors/7/slots':
            seen.update(params=params, auth=headers.get('Authorization'))
            return Res(200, {'view': 'owner', 'days': [{'date': params['from'], 'slots': [
                {'slotId': '7#30#x', 'localDate': params['from'], 'consultationTypeId': 30, 'status': 'CLOSED',
                 'booked': 0, 'held': 0, 'capacity': 1, 'manualBlock': True, 'localStart': '09:00',
                 'localEnd': '09:30', 'label': 'Standard consult'}]}]})
        raise AssertionError(path)

    monkeypatch.setattr(dashboard.requests, 'request', request)
    monkeypatch.setattr(dashboard.requests, 'get', lambda *a, **k: Res(200, {'items': []}))
    dashboard._hospital_cache.update(at=0.0, items=[])
    client = dashboard.app.test_client()
    with client.session_transaction() as s:
        s.update(token='a1', role='doctor', user_id='7', token_exp=9999999999)
    page = client.get('/doctor/dashboard?tab=schedule&date=2026-12-01').get_data(as_text=True)
    assert seen == {'params': {'from': '2026-12-01', 'to': '2026-12-30'}, 'auth': 'Bearer a1'}
    assert '9:00 AM' in page and 'Re-open' in page and '7#30#x' in page


def test_doctor_id_falls_back_to_the_token_subject_for_older_sessions():
    import base64 as b64
    import json as js
    payload = b64.urlsafe_b64encode(js.dumps({'sub': '42'}).encode()).decode().rstrip('=')
    with dashboard.app.test_request_context():
        dashboard.session['token'] = 'h.' + payload + '.s'
        assert dashboard.session_user_id() == '42'
