from datetime import datetime, date, time, timedelta, timezone
from zoneinfo import ZoneInfo

def valid_instants(naive,tz):
    result={}
    for fold in (0,1):
        aware=naive.replace(tzinfo=tz,fold=fold)
        utc=aware.astimezone(timezone.utc)
        if utc.astimezone(tz).replace(tzinfo=None)==naive:
            result[utc]=utc
    return sorted(result)

def generate_slots(doctor_id,consultation_type_id,day,timezone_name,start,end,duration,capacity,status='OPEN'):
    tz=ZoneInfo(timezone_name)
    cursor=datetime.combine(day,start); finish=datetime.combine(day,end)
    while cursor+timedelta(minutes=duration)<=finish:
        end_local=cursor+timedelta(minutes=duration)
        for start_utc in valid_instants(cursor,tz):
            # End must represent the same physical duration, and match the requested local wall clock end.
            end_utc=start_utc+timedelta(minutes=duration)
            if end_utc.astimezone(tz).replace(tzinfo=None)!=end_local:
                continue
            start_str=start_utc.isoformat().replace('+00:00','Z')
            end_str=end_utc.isoformat().replace('+00:00','Z')
            yield {'slot_id':f'{doctor_id}#{consultation_type_id}#{start_str}', 'doctor_date':f'{doctor_id}#{day.isoformat()}', 'doctor_id':str(doctor_id), 'consultation_type_id':str(consultation_type_id), 'start_at':start_str,'end_at':end_str,'timezone':timezone_name,'local_date':day.isoformat(),'capacity':capacity,'booked_count':0,'held_count':0,'version':0,'status':status}
        cursor=end_local


def choose_non_overlapping(items, existing):
    """Split generated slots into (to_create, already_there, skipped_overlap).

    A slot is skipped when it overlaps any slot already stored for that doctor/day (a booking must never be
    double-booked) or one chosen earlier in this run. Earliest start first, longer slot first on a tie.
    """
    taken = list(existing)
    ids = {e['slot_id'] for e in existing}
    create, there, skipped = [], 0, 0
    for item in sorted(sorted(items, key=lambda i: i['end_at'], reverse=True), key=lambda i: i['start_at']):
        if item['slot_id'] in ids:
            there += 1
        elif any(item['start_at'] < t['end_at'] and t['start_at'] < item['end_at'] for t in taken):
            skipped += 1
        else:
            create.append(item); taken.append(item)
    return create, there, skipped
