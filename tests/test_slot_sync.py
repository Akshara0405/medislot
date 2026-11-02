import os, sys
from datetime import date, timedelta, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lambdas', 'doctor'))
import slots

DOC = 7
T30, T15, T60 = 'type-30', 'type-15', 'type-60'


class FakeTable:
    def __init__(self): self.items = {}
    def query(self, IndexName, KeyConditionExpression, **kw):
        key = KeyConditionExpression._values[1]
        return {'Items': sorted((dict(i) for i in self.items.values() if i['doctor_date'] == key), key=lambda i: i['start_at'])}
    def batch_writer(self):
        t = self
        class W:
            def __enter__(s): return s
            def __exit__(s, *a): return False
            def put_item(s, Item): t.items[Item['slot_id']] = dict(Item)
            def delete_item(s, Key): t.items.pop(Key['slot_id'], None)
        return W()


class FakeCur:
    def __init__(self, patterns, blackouts=(), tz='Asia/Kolkata'):
        self.patterns, self.blackouts, self.tz, self.rows = patterns, list(blackouts), tz, []
    def execute(self, sql, params=None):
        s = ' '.join(sql.split())
        if 'FROM doctors' in s: self.rows = [{'tz': self.tz}]
        elif 'FROM availability_patterns' in s: self.rows = self.patterns
        elif 'FROM blackout_dates' in s: self.rows = [{'blackout_date': date.fromisoformat(d)} for d in self.blackouts]
    def fetchone(self): return self.rows[0] if self.rows else None
    def fetchall(self): return self.rows


def pat(day, start, end, tid, dur, cap=1):
    return {'day_of_week': day, 'start_time': start, 'end_time': end, 'consultation_type_id': tid, 'duration_min': dur, 'capacity': cap}


def test_9_to_1_is_split_by_consultation_length():
    for tid, dur, expected in ((T15, 15, 16), (T30, 30, 8), (T60, 60, 4)):
        out = list(slots.generate_slots(DOC, tid, date(2026, 11, 2), 'Asia/Kolkata', time(9), time(13), dur, 1))
        assert len(out) == expected
        assert out[0]['start_at'] == '2026-11-02T03:30:00Z'          # 09:00 IST
        assert out[-1]['end_at'] == '2026-11-02T07:30:00Z'           # 13:00 IST


def test_window_is_30_days_and_only_matching_weekdays():
    monday = date(2026, 11, 2)
    table, cur = FakeTable(), FakeCur([pat(0, time(9), time(13), T30, 30)])
    r = slots.resync_slots(cur, DOC, table=table, today=monday)
    mondays = [monday + timedelta(days=7 * i) for i in range(5) if (monday + timedelta(days=7 * i)) < monday + timedelta(days=30)]
    assert r['created'] == 8 * len(mondays) == len(table.items)
    assert {i['local_date'] for i in table.items.values()} == {d.isoformat() for d in mondays}


def test_blackout_removes_open_slots_and_removal_restores_them():
    monday = date(2026, 11, 2)
    table = FakeTable(); pats = [pat(0, time(9), time(13), T30, 30)]
    slots.resync_slots(FakeCur(pats), DOC, table=table, today=monday)
    before = len(table.items)
    r = slots.resync_slots(FakeCur(pats, ['2026-11-09']), DOC, table=table, today=monday)
    assert r['removed'] == 8 and len(table.items) == before - 8
    assert not [i for i in table.items.values() if i['local_date'] == '2026-11-09']
    r = slots.resync_slots(FakeCur(pats), DOC, table=table, today=monday)
    assert r['created'] == 8 and len(table.items) == before


def test_booked_slots_survive_blackout_and_pattern_changes():
    monday = date(2026, 11, 2)
    table = FakeTable(); pats = [pat(0, time(9), time(13), T30, 30)]
    slots.resync_slots(FakeCur(pats), DOC, table=table, today=monday)
    booked = next(i for i in table.items.values() if i['local_date'] == '2026-11-09')
    booked['booked_count'] = 1
    r = slots.resync_slots(FakeCur(pats, ['2026-11-09']), DOC, table=table, today=monday)
    assert booked['slot_id'] in table.items and r['keptBooked'] == 1 and r['removed'] == 7
    slots.resync_slots(FakeCur([]), DOC, table=table, today=monday)          # all availability removed
    assert list(table.items) == [booked['slot_id']]


def test_resync_is_idempotent():
    monday = date(2026, 11, 2); table = FakeTable(); cur = FakeCur([pat(2, time(10), time(12), T60, 60)])
    slots.resync_slots(cur, DOC, table=table, today=monday)
    r = slots.resync_slots(cur, DOC, table=table, today=monday)
    assert (r['created'], r['updated'], r['removed']) == (0, 0, 0)
