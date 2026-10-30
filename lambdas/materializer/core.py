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

def generate_slots(doctor_id,consultation_type_id,day,timezone_name,start,end,duration,capacity):
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
            yield {'slot_id':f'{doctor_id}#{consultation_type_id}#{start_str}', 'doctor_date':f'{doctor_id}#{day.isoformat()}', 'doctor_id':str(doctor_id), 'consultation_type_id':str(consultation_type_id), 'start_at':start_str,'end_at':end_str,'timezone':timezone_name,'local_date':day.isoformat(),'capacity':capacity,'booked_count':0,'held_count':0,'version':0,'status':'OPEN'}
        cursor=end_local
