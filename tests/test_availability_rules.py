"""Rules enforced by the doctor Lambda: no overlapping patterns, fixed capacity per type."""
import importlib.util, json, os, sys
import pytest
os.environ.setdefault('AWS_DEFAULT_REGION', 'ap-south-1')
ROOT = os.path.join(os.path.dirname(__file__), '..', 'lambdas')
sys.path.insert(0, os.path.join(ROOT, 'doctor'))
sys.path.insert(0, ROOT)
# Every Lambda is called lambda_function.py, so load the doctor one under its own name.
_spec = importlib.util.spec_from_file_location('doctor_lambda', os.path.join(ROOT, 'doctor', 'lambda_function.py'))
lf = importlib.util.module_from_spec(_spec); _spec.loader.exec_module(lf)

TYPES = {
    'quick':    {'name': 'QUICK',       'duration_min': 15},
    'detailed': {'name': 'DETAILED',    'duration_min': 30},   # 30 min so 09:00-09:30 is a valid length
    'camp':     {'name': 'HEALTH_CAMP', 'duration_min': 60},
}


@pytest.fixture(autouse=True)
def no_dynamodb(monkeypatch):
    """These tests have no DynamoDB; tests that care about bookings override this stub themselves."""
    monkeypatch.setattr(lf, 'find_booking_conflicts', lambda cur, d, p: [])


class Cur:
    def __init__(self): self.rows = []; self.saved = []; self.types_written = []
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def execute(self, sql, params=()):
        s = ' '.join(sql.split())
        if s.startswith('SELECT') and 'FROM consultation_types' in s and 'is_active=1' in s:
            self.rows = [TYPES[params[0]]] if params[0] in TYPES else []
        elif s.startswith('INSERT INTO availability_patterns'): self.saved.append(params)
        elif s.startswith('INSERT INTO consultation_types'): self.types_written.append(params)
        elif s.startswith('UPDATE consultation_types'): self.types_written.append(params); self.rowcount = 1
        else: self.rows = []
    def fetchone(self): return self.rows[0] if self.rows else None
    def fetchall(self): return self.rows


class Conn:
    def __init__(self): self.c = Cur()
    def cursor(self): return self.c
    def commit(self): pass
    def close(self): pass


def call(monkeypatch, method, path, payload):
    conn = Conn()
    monkeypatch.setattr(lf, 'config', lambda: {})
    monkeypatch.setattr(lf, 'dbconn', lambda c: conn)
    monkeypatch.setattr(lf, 'sync', lambda cur, doctor_id: {})
    event = {'requestContext': {'authorizer': {'role': 'doctor', 'userId': '7'}}, 'httpMethod': method,
             'path': path, 'body': json.dumps(payload)}
    res = lf.handler(event, None)
    return res['statusCode'], json.loads(res['body']), conn.c


def pattern(tid, start, end, day=0):
    return {'day_of_week': day, 'consultation_type_id': tid, 'start_time': start, 'end_time': end}


def put_patterns(monkeypatch, patterns):
    return call(monkeypatch, 'PUT', '/doctor/me/availability', {'patterns': patterns})


def test_quick_then_detailed_overlap_is_rejected_with_clear_message(monkeypatch):
    code, out, cur = put_patterns(monkeypatch, [pattern('quick', '09:00', '09:15'), pattern('detailed', '09:00', '09:30')])
    assert code == 400 and cur.saved == []
    assert 'overlap' in out['message'].lower()
    assert 'Quick' in out['message'] and 'Detailed' in out['message']      # says which two clash
    assert '09:00' in out['message'] and '09:30' in out['message']


def test_partial_overlap_is_rejected(monkeypatch):
    code, out, _ = put_patterns(monkeypatch, [pattern('quick', '09:00', '09:15'), pattern('detailed', '09:10', '09:40')])
    assert code == 400 and 'overlap' in out['message'].lower()


def test_back_to_back_and_other_days_are_allowed(monkeypatch):
    code, _, cur = put_patterns(monkeypatch, [pattern('quick', '09:00', '09:15'), pattern('detailed', '09:15', '09:45'),
                                              pattern('detailed', '09:00', '09:30', day=1)])
    assert code == 200 and len(cur.saved) == 3


def test_same_type_overlap_is_rejected_too(monkeypatch):
    code, _, _ = put_patterns(monkeypatch, [pattern('quick', '09:00', '10:00'), pattern('quick', '09:30', '10:30')])
    assert code == 400


def test_pattern_end_only_counts_whole_slots(monkeypatch):
    # 09:00-09:34 with a 15-minute type makes 09:00 and 09:15 slots and ends at 09:30, so 09:30-10:00 may follow.
    code, out, cur = put_patterns(monkeypatch, [pattern('quick', '09:00', '09:34'), pattern('detailed', '09:30', '10:00')])
    assert code == 200 and len(cur.saved) == 2, out
    # ...but a start inside the last whole slot still clashes.
    code, out, _ = put_patterns(monkeypatch, [pattern('quick', '09:00', '09:34'), pattern('detailed', '09:25', '09:55')])
    assert code == 400 and 'overlap' in out['message'].lower() and '09:30' in out['message']


def test_exact_duplicate_pattern_says_it_already_exists(monkeypatch):
    code, out, cur = put_patterns(monkeypatch, [pattern('quick', '09:00', '09:30'), pattern('quick', '09:00', '09:30')])
    assert code == 409 and cur.saved == []
    assert 'already exists' in out['message'].lower() and 'Monday' in out['message'] and 'Nothing was saved' in out['message']


def test_long_range_hides_a_later_overlap_no_more(monkeypatch):
    # 09:00-12:00 swallows both later rows, not just the one next to it.
    code, _, _ = put_patterns(monkeypatch, [pattern('quick', '09:00', '12:00'), pattern('quick', '09:15', '09:30'), pattern('detailed', '10:00', '10:30')])
    assert code == 400


def type_payload(name, capacity, duration=15):
    return {'name': name, 'duration_min': duration, 'capacity': capacity, 'fee_amount': '500'}


def test_normal_types_must_have_capacity_exactly_1(monkeypatch):
    for name in ('QUICK', 'STANDARD', 'DETAILED'):
        for bad in (0, 2, 3, 10):
            code, out, cur = call(monkeypatch, 'POST', '/doctor/me/consultation-types', type_payload(name, bad))
            assert code == 400 and cur.types_written == [], (name, bad)
            assert 'capacity' in out['message'].lower() and '1' in out['message']
        code, _, _ = call(monkeypatch, 'POST', '/doctor/me/consultation-types', type_payload(name, 1))
        assert code == 201


def test_health_camp_capacity_range(monkeypatch):
    for bad in (1, 0, 11, 50):
        code, out, _ = call(monkeypatch, 'POST', '/doctor/me/consultation-types', type_payload('HEALTH_CAMP', bad, 60))
        assert code == 400 and 'health camp' in out['message'].lower()
    for good in (2, 3, 5, 10):
        code, _, _ = call(monkeypatch, 'POST', '/doctor/me/consultation-types', type_payload('HEALTH_CAMP', good, 60))
        assert code == 201


def test_saving_is_blocked_when_new_slots_would_overlap_a_booked_one(monkeypatch):
    monkeypatch.setattr(lf, 'find_booking_conflicts', lambda cur, d, p: ['Monday 09 Nov 09:00-09:15 overlaps an existing booked appointment at 09:00-09:30'])
    code, out, cur = put_patterns(monkeypatch, [pattern('quick', '09:00', '09:30')])
    assert code == 409 and cur.saved == [] and 'Nothing was saved' in out['message'] and 'booked appointment' in out['message']


def test_if_the_booking_check_breaks_nothing_is_saved(monkeypatch):
    """Fail closed: a broken conflict check must never let a save through (it used to save silently)."""
    def boom(cur, d, p): raise RuntimeError('DynamoDB unavailable')
    monkeypatch.setattr(lf, 'find_booking_conflicts', boom)
    code, out, cur = put_patterns(monkeypatch, [pattern('quick', '09:00', '09:30')])
    assert code == 503 and cur.saved == [] and 'nothing was saved' in out['message'].lower()
