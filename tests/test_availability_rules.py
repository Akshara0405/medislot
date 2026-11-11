"""Rules enforced by the doctor Lambda: no overlapping patterns, capacity limits from the consultations table."""
import importlib.util, json, os, sys
import pytest
os.environ.setdefault('AWS_DEFAULT_REGION', 'ap-south-1')
ROOT = os.path.join(os.path.dirname(__file__), '..', 'lambdas')
sys.path.insert(0, os.path.join(ROOT, 'doctor'))
sys.path.insert(0, ROOT)
# Every Lambda is called lambda_function.py, so load the doctor one under its own name.
_spec = importlib.util.spec_from_file_location('doctor_lambda', os.path.join(ROOT, 'doctor', 'lambda_function.py'))
lf = importlib.util.module_from_spec(_spec); _spec.loader.exec_module(lf)

# The doctor's active consultation types (ids are AUTO_INCREMENT integers) and the consultations table.
TYPE_IDS = {'quick': 1, 'detailed': 2, 'camp': 3}
TYPES = [
    {'consultation_type_id': 1, 'name': 'QUICK', 'label': 'Quick consult', 'duration_min': 15},
    {'consultation_type_id': 2, 'name': 'DETAILED', 'label': 'Detailed consult', 'duration_min': 30},  # 30 so 09:00-09:30 fits
    {'consultation_type_id': 3, 'name': 'HEALTH_CAMP', 'label': 'Health camp', 'duration_min': 60},
]
CONSULTATIONS = {
    'QUICK': {'consultation_id': 1, 'code': 'QUICK', 'label': 'Quick consult', 'duration_min': 15, 'min_capacity': 1, 'max_capacity': 1},
    'STANDARD': {'consultation_id': 2, 'code': 'STANDARD', 'label': 'Standard consult', 'duration_min': 30, 'min_capacity': 1, 'max_capacity': 1},
    'DETAILED': {'consultation_id': 3, 'code': 'DETAILED', 'label': 'Detailed consult', 'duration_min': 60, 'min_capacity': 1, 'max_capacity': 1},
    'HEALTH_CAMP': {'consultation_id': 4, 'code': 'HEALTH_CAMP', 'label': 'Health camp', 'duration_min': 60, 'min_capacity': 2, 'max_capacity': 10},
}


@pytest.fixture(autouse=True)
def no_dynamodb(monkeypatch):
    """These tests have no DynamoDB; tests that care about bookings override this stub themselves."""
    monkeypatch.setattr(lf, 'find_booking_conflicts', lambda cur, d, p: [])


class Cur:
    def __init__(self): self.rows = []; self.saved = []; self.types_written = []; self.lastrowid = 0; self.rowcount = 0
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def execute(self, sql, params=()):
        s = ' '.join(sql.split())
        if 'FROM consultation_types t JOIN consultations c' in s and 't.is_active = 1' in s:
            self.rows = list(TYPES)
        elif s.startswith('SELECT * FROM consultations WHERE code'):
            self.rows = [CONSULTATIONS[params[0]]] if params[0] in CONSULTATIONS else []
        elif s.startswith('INSERT INTO consultation_types'):
            self.types_written.append(params); self.lastrowid = 10 + len(self.types_written)
        elif s.startswith('UPDATE consultation_types'):
            self.types_written.append(params); self.rowcount = 1
        else:
            self.rows = []            # no existing patterns, no existing type rows
    def executemany(self, sql, rows):
        if sql.startswith('INSERT INTO availability_patterns'):
            self.saved.extend(rows)
    def fetchone(self): return self.rows[0] if self.rows else None
    def fetchall(self): return self.rows


class Conn:
    def __init__(self): self.c = Cur()
    def cursor(self): return self.c
    def commit(self): pass
    def rollback(self): pass
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
    return {'day_of_week': day, 'consultation_type_id': TYPE_IDS[tid], 'start_time': start, 'end_time': end}


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


def type_payload(name, capacity):
    return {'name': name, 'capacity': capacity, 'fee_amount': '500'}


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
        code, out, _ = call(monkeypatch, 'POST', '/doctor/me/consultation-types', type_payload('HEALTH_CAMP', bad))
        assert code == 400 and 'health camp' in out['message'].lower()
    for good in (2, 3, 5, 10):
        code, _, _ = call(monkeypatch, 'POST', '/doctor/me/consultation-types', type_payload('HEALTH_CAMP', good))
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


def test_unknown_consultation_and_wrong_duration_are_rejected(monkeypatch):
    code, out, _ = call(monkeypatch, 'POST', '/doctor/me/consultation-types', type_payload('MASSAGE', 1))
    assert code == 400 and 'Unknown consultation' in out['message']
    code, out, _ = call(monkeypatch, 'POST', '/doctor/me/consultation-types', dict(type_payload('QUICK', 1), duration_min=60))
    assert code == 400 and '15 minutes' in out['message']


def test_new_type_returns_its_integer_id(monkeypatch):
    code, out, cur = call(monkeypatch, 'POST', '/doctor/me/consultation-types', type_payload('STANDARD', 1))
    assert code == 201 and out['consultationTypeId'] == 11
    assert cur.types_written[0][:2] == (7, 2)          # doctor 7, consultation_id of STANDARD


def test_non_numeric_ids_are_rejected(monkeypatch):
    code, out, _ = call(monkeypatch, 'PUT', '/doctor/me/consultation-types/1f0c-uuid', type_payload('QUICK', 1))
    assert code == 400 and 'whole number' in out['message']
    code, out, _ = put_patterns(monkeypatch, [{'day_of_week': 0, 'consultation_type_id': 'abc', 'start_time': '09:00', 'end_time': '10:00'}])
    assert code == 400 and 'Unknown or inactive' in out['message']
