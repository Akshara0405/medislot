import os
from datetime import date,datetime,timedelta
from botocore.exceptions import ClientError
import boto3
from common import config,dbconn
from core import generate_slots

def handler(event,context):
    first=date.fromisoformat(event['date']) if event and event.get('date') else datetime.now().date()
    days=int(event.get('days',30)) if event else 30
    if not 1<=days<=30:raise ValueError('days must be 1..30')
    conn=dbconn(config()); table=boto3.resource('dynamodb').Table(os.environ['SLOTS_TABLE'])
    created=existing=0
    try:
        with conn.cursor() as cur:
            cur.execute("""SELECT d.user_id doctor_id,COALESCE(d.timezone,'Asia/Kolkata') timezone, a.day_of_week,a.start_time,a.end_time,t.consultation_type_id,t.duration_min,t.capacity FROM availability_patterns a JOIN doctors d ON d.user_id=a.doctor_id JOIN consultation_types t ON t.consultation_type_id=a.consultation_type_id WHERE t.is_active=1""")
            patterns=cur.fetchall()
            cur.execute('SELECT doctor_id,blackout_date FROM blackout_dates')
            blocked={(str(x['doctor_id']),str(x['blackout_date'])) for x in cur.fetchall()}
        for offset in range(days):
            day=first+timedelta(days=offset)
            for p in patterns:
                if p['day_of_week']!=day.weekday() or (str(p['doctor_id']),day.isoformat()) in blocked:continue
                for item in generate_slots(p['doctor_id'],p['consultation_type_id'],day,p['timezone'],(datetime.min+p['start_time']).time() if isinstance(p['start_time'],timedelta) else p['start_time'],(datetime.min+p['end_time']).time() if isinstance(p['end_time'],timedelta) else p['end_time'],p['duration_min'],p['capacity']):
                    try:table.put_item(Item=item,ConditionExpression='attribute_not_exists(slot_id)');created+=1
                    except ClientError as exc:
                        if exc.response['Error']['Code']=='ConditionalCheckFailedException':existing+=1
                        else:raise
        return {'created':created,'alreadyExisting':existing,'startDate':first.isoformat(),'days':days}
    finally:conn.close()
