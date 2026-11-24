import os, sys
from datetime import date, timedelta, time
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lambdas'))
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lambdas', 'doctor'))
import common
import slots

DOC = 7
T30, T15, T60 = 'type-30', 'type-15', 'type-60'


def parse_key_condition(cond):
    """(doctor_id, first_date, last_date) from Key('doctor_id').eq(..) & Key('local_date').eq/between(..)."""
    doctor, dates = cond._values
    assert doctor._values[0].name == 'doctor_id' and dates._values[0].name == 'local_date'
    values = dates._values[1:]
    return doctor._values[1], values[0], values[-1]


class FakeTable:
    """Slots table keyed by (doctor_id, slot_id) with the LSI_DoctorDate index on (doctor_id, local_date)."""
    def __init__(self): self.items = {}; self.queries = []
    def _check_key(self, Key):
        assert set(Key) == {'doctor_id', 'slot_id'} and Key['slot_id'].startswith(Key['doctor_id'] + '#')
    def update_item(self, Key, ExpressionAttributeValues, **kw):
        self._check_key(Key); self.items[Key['slot_id']]['status'] = ExpressionAttributeValues[':s']
    def query(self, IndexName, KeyConditionExpression, **kw):
        assert IndexName == 'LSI_DoctorDate'
        doctor, first, last = parse_key_condition(KeyConditionExpression)
        self.queries.append((doctor, first, last))
        rows = [dict(i) for i in self.items.values() if i['doctor_id'] == doctor and first <= i['local_date'] <= last]
        return {'Items': sorted(rows, key=lambda i: i['start_at'])}
    def batch_writer(self):
        t = self
        class W:
            def __enter__(s): return s
            def __exit__(s, *a): return False
            def put_item(s, Item): t.items[Item['slot_id']] = dict(Item)
            def delete_item(s, Key): t._check_key(Key); t.items.pop(Key['slot_id'], None)
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


def pat(day, start, end, tid, dur, cap=1, name='STANDARD'):
    # min/max seats come from the consultations table: 2-10 for a health camp, exactly 1 otherwise.
    low, high = (2, 10) if name == 'HEALTH_CAMP' else (1, 1)
    return {'day_of_week': day, 'start_time': start, 'end_time': end, 'consultation_type_id': tid, 'duration_min': dur,
            'capacity': cap, 'type_name': name, 'min_capacity': low, 'max_capacity': high}


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


def test_blackout_closes_open_slots_and_removal_reopens_them():
    monday = date(2026, 11, 2)
    table = FakeTable(); pats = [pat(0, time(9), time(13), T30, 30)]
    slots.resync_slots(FakeCur(pats), DOC, table=table, today=monday)
    before = len(table.items)
    r = slots.resync_slots(FakeCur(pats, ['2026-11-09']), DOC, table=table, today=monday)
    day = [i for i in table.items.values() if i['local_date'] == '2026-11-09']
    assert len(table.items) == before and r['removed'] == 0 and r['closed'] == 8
    assert len(day) == 8 and {i['status'] for i in day} == {'CLOSED'}
    assert {i['status'] for i in table.items.values() if i['local_date'] != '2026-11-09'} == {'OPEN'}
    r = slots.resync_slots(FakeCur(pats), DOC, table=table, today=monday)
    assert r['reopened'] == 8 and {i['status'] for i in table.items.values()} == {'OPEN'}


def test_blackout_on_a_day_never_synced_creates_closed_slots():
    monday = date(2026, 11, 2); table = FakeTable()
    slots.resync_slots(FakeCur([pat(0, time(9), time(11), T30, 30)], ['2026-11-09']), DOC, table=table, today=monday)
    assert {i['status'] for i in table.items.values() if i['local_date'] == '2026-11-09'} == {'CLOSED'}


def test_booked_slot_is_closed_not_deleted_and_keeps_its_booking():
    monday = date(2026, 11, 2)
    table = FakeTable(); pats = [pat(0, time(9), time(13), T30, 30)]
    slots.resync_slots(FakeCur(pats), DOC, table=table, today=monday)
    booked = next(i for i in table.items.values() if i['local_date'] == '2026-11-09')
    booked['booked_count'] = 1
    r = slots.resync_slots(FakeCur(pats, ['2026-11-09']), DOC, table=table, today=monday)
    assert table.items[booked['slot_id']]['status'] == 'CLOSED' and table.items[booked['slot_id']]['booked_count'] == 1
    assert r['removed'] == 0 and r['closed'] == 8
    slots.resync_slots(FakeCur(pats), DOC, table=table, today=monday)       # blackout removed
    assert table.items[booked['slot_id']]['status'] == 'FULL' and table.items[booked['slot_id']]['booked_count'] == 1


def test_booked_slots_survive_pattern_changes():
    monday = date(2026, 11, 2)
    table = FakeTable(); pats = [pat(0, time(9), time(13), T30, 30)]
    slots.resync_slots(FakeCur(pats), DOC, table=table, today=monday)
    booked = next(i for i in table.items.values() if i['local_date'] == '2026-11-09')
    booked['booked_count'] = 1
    r = slots.resync_slots(FakeCur([]), DOC, table=table, today=monday)          # all availability removed
    assert list(table.items) == [booked['slot_id']] and r['keptBooked'] == 1


def test_capacity_is_1_for_normal_types_even_if_the_database_row_says_otherwise():
    monday = date(2026, 11, 2); table = FakeTable()
    bad = [pat(0, time(9), time(10), T30, 30, cap=5, name='STANDARD'), pat(0, time(10), time(10, 15), T15, 15, cap=3, name='QUICK')]
    slots.resync_slots(FakeCur(bad), DOC, table=table, today=monday)
    assert {i['capacity'] for i in table.items.values()} == {1}


def test_health_camp_keeps_its_group_capacity():
    monday = date(2026, 11, 2); table = FakeTable()
    slots.resync_slots(FakeCur([pat(0, time(9), time(10), T60, 60, cap=4, name='HEALTH_CAMP')]), DOC, table=table, today=monday)
    assert {i['capacity'] for i in table.items.values()} == {4}


def test_resync_is_idempotent():
    monday = date(2026, 11, 2); table = FakeTable(); cur = FakeCur([pat(2, time(10), time(12), T60, 60)])
    slots.resync_slots(cur, DOC, table=table, today=monday)
    r = slots.resync_slots(cur, DOC, table=table, today=monday)
    assert (r['created'], r['updated'], r['removed']) == (0, 0, 0)


def _booked_standard_on_9_nov():
    monday = date(2026, 11, 2); table = FakeTable()
    std = [pat(0, time(9), time(9, 30), 'std', 30, name='STANDARD')]
    slots.resync_slots(FakeCur(std), DOC, table=table, today=monday)
    booked = next(i for i in table.items.values() if i['local_date'] == '2026-11-09')
    booked['booked_count'] = 1
    return monday, table, booked


def test_quick_over_a_booked_standard_is_reported_as_a_conflict():
    monday, table, booked = _booked_standard_on_9_nov()
    quick = [{'day_of_week': 0, 'start_time': '09:00', 'end_time': '09:30', 'consultation_type_id': 'quick', 'duration_min': 15}]
    problems = slots.find_booking_conflicts(FakeCur([]), DOC, quick, table=table, today=monday)
    assert problems and 'Monday 09 Nov 09:00-09:15 overlaps an existing booked appointment at 09:00-09:30' in problems[0]
    # keeping the same standard pattern is not a conflict
    same = [{'day_of_week': 0, 'start_time': '09:00', 'end_time': '09:30', 'consultation_type_id': 'std', 'duration_min': 30}]
    assert slots.find_booking_conflicts(FakeCur([]), DOC, same, table=table, today=monday) == []
    # a later, non-overlapping time is fine
    later = [{'day_of_week': 0, 'start_time': '09:30', 'end_time': '10:00', 'consultation_type_id': 'quick', 'duration_min': 15}]
    assert slots.find_booking_conflicts(FakeCur([]), DOC, later, table=table, today=monday) == []


def test_resync_never_creates_a_slot_on_top_of_a_kept_booking():
    monday, table, booked = _booked_standard_on_9_nov()
    r = slots.resync_slots(FakeCur([pat(0, time(9), time(9, 30), 'quick', 15, name='QUICK')]), DOC, table=table, today=monday)
    day = [i for i in table.items.values() if i['local_date'] == '2026-11-09']
    assert [i['slot_id'] for i in day] == [booked['slot_id']] and r['skippedOverlap'] == 2


def test_old_overlapping_patterns_never_produce_overlapping_slots():
    monday = date(2026, 11, 2); table = FakeTable()
    both = [pat(0, time(9), time(9, 30), 'std', 30, name='STANDARD'), pat(0, time(9), time(9, 30), 'quick', 15, name='QUICK')]
    # simulate slots an older version already wrote for both patterns
    for p in both:
        for item in slots.generate_slots(DOC, p['consultation_type_id'], date(2026, 11, 9), 'Asia/Kolkata', p['start_time'], p['end_time'], p['duration_min'], 1):
            table.items[item['slot_id']] = item
    assert len(table.items) == 3
    slots.resync_slots(FakeCur(both), DOC, table=table, today=monday)
    day = sorted((i for i in table.items.values() if i['local_date'] == '2026-11-09'), key=lambda i: i['start_at'])
    assert all(a['end_at'] <= b['start_at'] for a, b in zip(day, day[1:]))


def test_materializer_will_not_create_slots_over_a_booked_one():
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'lambdas', 'materializer'))
    import core
    std = list(core.generate_slots(DOC, 'std', date(2026, 11, 9), 'Asia/Kolkata', time(9), time(9, 30), 30, 1))
    std[0]['booked_count'] = 1
    quick = list(core.generate_slots(DOC, 'quick', date(2026, 11, 9), 'Asia/Kolkata', time(9), time(10), 15, 1))
    create, there, skipped = core.choose_non_overlapping(quick, std)
    assert skipped == 2 and [c['start_at'] for c in create] == ['2026-11-09T04:00:00Z', '2026-11-09T04:15:00Z']


def test_items_are_partitioned_by_doctor():
    item = next(slots.generate_slots(DOC, T30, date(2026, 11, 2), 'Asia/Kolkata', time(9), time(10), 30, 1))
    assert item['doctor_id'] == str(DOC) and item['slot_id'] == f'{DOC}#{T30}#2026-11-02T03:30:00Z'
    assert 'doctor_date' not in item and item['local_date'] == '2026-11-02'


class PagedTable:
    """Returns the doctor's rows two per page, newest first, to prove query_slots follows pages and sorts."""
    def __init__(self, items): self.items = items; self.calls = []
    def query(self, IndexName, KeyConditionExpression, ExclusiveStartKey=None, **kw):
        assert IndexName == common.SLOTS_DATE_INDEX
        doctor, first, last = parse_key_condition(KeyConditionExpression)
        self.calls.append((doctor, first, last))
        rows = sorted((i for i in self.items if i['doctor_id'] == doctor and first <= i['local_date'] <= last),
                      key=lambda i: i['start_at'], reverse=True)
        start = ExclusiveStartKey or 0
        page = {'Items': [dict(r) for r in rows[start:start + 2]]}
        if start + 2 < len(rows):
            page['LastEvaluatedKey'] = start + 2
        return page


def test_query_slots_reads_a_30_day_range_in_one_paginated_query():
    first = date(2026, 11, 3)
    source = [{'doctor_id': '7', 'local_date': (first + timedelta(days=i)).isoformat(),
               'start_at': f'{(first + timedelta(days=i)).isoformat()}T04:00:00Z'} for i in range(30)]
    source.append({'doctor_id': '8', 'local_date': '2026-11-05', 'start_at': '2026-11-05T04:00:00Z'})  # other doctor
    table = PagedTable(source)
    result = common.query_slots(table, DOC, first, first + timedelta(days=29))
    assert {c for c in table.calls} == {('7', '2026-11-03', '2026-12-02')} and len(table.calls) == 15    # pages
    assert len(result) == 30 and result[0]['local_date'] == '2026-11-03' and result[-1]['local_date'] == '2026-12-02'
    one_day = common.query_slots(PagedTable(source), DOC, '2026-11-05')
    assert [i['local_date'] for i in one_day] == ['2026-11-05']


def test_single_slot_can_be_closed_and_reopened_without_touching_the_rest_of_the_day():
    monday = date(2026, 11, 2)
    pats = [pat(0, time(9), time(11), T30, 30)]
    table = FakeTable()
    slots.resync_slots(FakeCur(pats), DOC, table=table, today=monday)
    nine = f'{DOC}#{T30}#2026-11-02T03:30:00Z'                       # 09:00-09:30 IST
    assert table.items[nine]['status'] == 'OPEN'
    table.items[nine]['manual_block'] = True                          # what the slot-blocks route sets
    slots.resync_slots(FakeCur(pats), DOC, table=table, today=monday)
    day = [i for i in table.items.values() if i['local_date'] == '2026-11-02']
    assert table.items[nine]['status'] == 'CLOSED' and table.items[nine]['manual_block'] is True
    assert sorted(i['status'] for i in day) == ['CLOSED', 'OPEN', 'OPEN', 'OPEN']
    slots.resync_slots(FakeCur(pats), DOC, table=table, today=monday)  # still closed after another sync
    assert table.items[nine]['status'] == 'CLOSED'
    del table.items[nine]['manual_block']                             # route removes the flag on re-open
    slots.resync_slots(FakeCur(pats), DOC, table=table, today=monday)
    assert table.items[nine]['status'] == 'OPEN'


def test_manually_closed_booked_slot_keeps_its_booking():
    monday = date(2026, 11, 2)
    pats = [pat(0, time(9), time(11), T30, 30)]
    table = FakeTable()
    slots.resync_slots(FakeCur(pats), DOC, table=table, today=monday)
    nine = f'{DOC}#{T30}#2026-11-02T03:30:00Z'
    table.items[nine].update(booked_count=1, status='FULL', manual_block=True)
    slots.resync_slots(FakeCur(pats), DOC, table=table, today=monday)
    assert table.items[nine]['status'] == 'CLOSED' and table.items[nine]['booked_count'] == 1
    del table.items[nine]['manual_block']
    slots.resync_slots(FakeCur(pats), DOC, table=table, today=monday)
    assert table.items[nine]['status'] == 'FULL' and table.items[nine]['booked_count'] == 1
