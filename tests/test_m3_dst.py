from datetime import date,time
from lambdas.materializer.core import generate_slots

def test_fall_back_produces_two_distinct_0130_slots():
    slots=list(generate_slots('1','a',date(2026,11,1),'America/New_York',time(1,0),time(2,0),30,1))
    starts=[x['start_at'] for x in slots]
    assert '2026-11-01T05:00:00Z' in starts
    assert '2026-11-01T06:00:00Z' in starts
    assert '2026-11-01T06:30:00Z' in starts
    assert len(starts)==len(set(starts))

def test_spring_forward_skips_nonexistent_local_times():
    slots=list(generate_slots('1','a',date(2027,3,14),'America/New_York',time(2,0),time(3,0),30,1))
    assert slots==[]

def test_stable_ids():
    args=('1','a',date(2026,11,1),'America/New_York',time(1,0),time(2,0),30,1)
    assert [x['slot_id'] for x in generate_slots(*args)]==[x['slot_id'] for x in generate_slots(*args)]
