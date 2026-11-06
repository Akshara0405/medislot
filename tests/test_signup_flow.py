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