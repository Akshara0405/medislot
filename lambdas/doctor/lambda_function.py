import json, os, uuid, re
from datetime import date, datetime
from decimal import Decimal
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
import boto3
from botocore.exceptions import ClientError
from common import config, dbconn, response, body

NAMES={'QUICK':15,'STANDARD':30,'DETAILED':60,'HEALTH_CAMP':60}

def fail(code,message): return response(code,{'message':message})
def validate_timezone(value):
    try: ZoneInfo(value); return True
    except (ZoneInfoNotFoundError,KeyError,TypeError,ValueError): return False

def jsonable(obj):
    if isinstance(obj,(datetime,date)): return obj.isoformat()
    if isinstance(obj,Decimal): return str(obj)
    return obj

def handler(event,context):
    ctx=event.get('requestContext',{}).get('authorizer',{})
    if ctx.get('role')!='doctor' or not ctx.get('userId'): return fail(403,'Doctor role required')
    doctor_id=ctx['userId']; method=event.get('httpMethod','GET'); path=event.get('path','').rstrip('/')
    data=body(event) if method in ('POST','PUT','PATCH') else {}
    if data is None or not isinstance(data,dict): return fail(400,'Invalid JSON object')
    c=config(); conn=dbconn(c)
    try:
        with conn.cursor() as cur:
            if path.endswith('/doctor/me'):
                if method=='GET':
                    cur.execute('SELECT * FROM doctors WHERE user_id=%s',(doctor_id,)); item=cur.fetchone()
                    return response(200,{k:jsonable(v) for k,v in item.items()}) if item else fail(404,'Profile not found')
                if method=='PUT':
                    fields={'first_name':'first_name','last_name':'last_name','phone':'phone','specialization':'specialization','license_number':'license_number','hospital_id':'hospital_id','qualification':'qualification','experience_years':'experience_years','timezone':'timezone','languages':'languages'}
                    updates={fields[k]:v for k,v in data.items() if k in fields}
                    if not updates:return fail(400,'No editable fields supplied')
                    if 'timezone' in updates and not validate_timezone(updates['timezone']):return fail(400,'Invalid IANA timezone')
                    if 'experience_years' in updates and (not isinstance(updates['experience_years'],int) or not 0<=updates['experience_years']<=80):return fail(400,'Invalid experience')
                    if 'hospital_id' in updates:
                        cur.execute('SELECT hospital_id FROM hospitals WHERE hospital_id=%s AND is_active=1',(updates['hospital_id'],))
                        if not cur.fetchone():return fail(400,'Hospital not found')
                    cur.execute('UPDATE doctors SET '+','.join(k+'=%s' for k in updates)+' WHERE user_id=%s',(*updates.values(),doctor_id)); conn.commit()
                    return response(200,{'message':'Profile updated'})
            if path.endswith('/doctor/me/photo') and method=='POST':
                mime=data.get('contentType','')
                if mime not in ('image/jpeg','image/png','image/webp'):return fail(400,'Unsupported photo content type')
                bucket=os.environ['PHOTO_BUCKET']; key=f'doctors/{doctor_id}/{uuid.uuid4()}'
                url=boto3.client('s3').generate_presigned_url('put_object',Params={'Bucket':bucket,'Key':key,'ContentType':mime},ExpiresIn=300)
                cur.execute('UPDATE doctors SET photo_key=%s WHERE user_id=%s',(key,doctor_id));conn.commit()
                return response(200,{'uploadUrl':url,'photoKey':key,'expiresIn':300,'contentType':mime})
            if '/consultation-types' in path:
                ident=path.split('/consultation-types/',1)[-1] if '/consultation-types/' in path else None
                if method=='GET' and ident is None:
                    cur.execute('SELECT * FROM consultation_types WHERE doctor_id=%s ORDER BY name',(doctor_id,));return response(200,{'items':[{k:jsonable(v) for k,v in x.items()} for x in cur.fetchall()]})
                if method in ('POST','PUT'):
                    name=data.get('name'); duration=data.get('duration_min'); capacity=data.get('capacity'); fee=data.get('fee_amount')
                    if name not in NAMES or duration not in (15,30,60) or not isinstance(capacity,int) or (not (2<=capacity<=10) if name=='HEALTH_CAMP' else capacity!=1):return fail(400,'Invalid consultation type, duration or capacity')
                    try:
                        amount=Decimal(str(fee)); assert amount>=0 and amount.is_finite()
                    except (ValueError,AssertionError,TypeError):return fail(400,'Invalid fee')
                    if method=='POST':
                        ident=str(uuid.uuid4());cur.execute('INSERT INTO consultation_types(consultation_type_id,doctor_id,name,duration_min,fee_amount,capacity,is_active) VALUES(%s,%s,%s,%s,%s,%s,1)',(ident,doctor_id,name,duration,str(amount),capacity));status=201
                    else:
                        if not ident:return fail(400,'Type ID required')
                        cur.execute('UPDATE consultation_types SET name=%s,duration_min=%s,fee_amount=%s,capacity=%s WHERE consultation_type_id=%s AND doctor_id=%s',(name,duration,str(amount),capacity,ident,doctor_id));status=200
                        if not cur.rowcount:return fail(404,'Type not found')
                    conn.commit();return response(status,{'consultationTypeId':ident})
                if method=='DELETE' and ident:
                    cur.execute('UPDATE consultation_types SET is_active=0 WHERE consultation_type_id=%s AND doctor_id=%s',(ident,doctor_id));conn.commit();return response(200,{'deactivated':bool(cur.rowcount)})
            if path.endswith('/doctor/me/availability'):
                if method=='GET':
                    cur.execute('SELECT * FROM availability_patterns WHERE doctor_id=%s ORDER BY day_of_week,start_time',(doctor_id,));return response(200,{'items':[{k:str(v) if k in ('start_time','end_time') else jsonable(v) for k,v in x.items()} for x in cur.fetchall()]})
                if method=='PUT':
                    patterns=data.get('patterns')
                    if not isinstance(patterns,list) or len(patterns)>100:return fail(400,'patterns must be a list of at most 100 entries')
                    for x in patterns:
                        if not isinstance(x,dict) or type(x.get('day_of_week')) is not int or not 0<=x['day_of_week']<=6:return fail(400,'Invalid weekday (Monday=0)')
                        try:
                            start=datetime.strptime(x['start_time'],'%H:%M').time();end=datetime.strptime(x['end_time'],'%H:%M').time()
                            if start>=end:raise ValueError()
                        except (ValueError,KeyError,TypeError):return fail(400,'Invalid local time range')
                        cur.execute('SELECT 1 FROM consultation_types WHERE consultation_type_id=%s AND doctor_id=%s AND is_active=1',(x.get('consultation_type_id'),doctor_id))
                        if not cur.fetchone():return fail(400,'Unknown or inactive consultation type')
                    cur.execute('DELETE FROM availability_patterns WHERE doctor_id=%s',(doctor_id,))
                    for x in patterns:cur.execute('INSERT INTO availability_patterns(pattern_id,doctor_id,consultation_type_id,day_of_week,start_time,end_time) VALUES(%s,%s,%s,%s,%s,%s)',(str(uuid.uuid4()),doctor_id,x['consultation_type_id'],x['day_of_week'],x['start_time'],x['end_time']))
                    conn.commit();return response(200,{'patternsSaved':len(patterns)})
            if '/blackouts' in path:
                ident=path.split('/blackouts/',1)[-1] if '/blackouts/' in path else None
                if method=='GET' and not ident:
                    cur.execute('SELECT * FROM blackout_dates WHERE doctor_id=%s ORDER BY blackout_date',(doctor_id,));return response(200,{'items':[{k:jsonable(v) for k,v in x.items()} for x in cur.fetchall()]})
                if method=='POST' and not ident:
                    try: date.fromisoformat(data['blackout_date'])
                    except (KeyError,ValueError,TypeError):return fail(400,'Invalid blackout date')
                    ident=str(uuid.uuid4());cur.execute('INSERT INTO blackout_dates(blackout_id,doctor_id,blackout_date,reason) VALUES(%s,%s,%s,%s)',(ident,doctor_id,data['blackout_date'],str(data.get('reason',''))[:200]));conn.commit();return response(201,{'blackoutId':ident})
                if method=='DELETE' and ident:
                    cur.execute('DELETE FROM blackout_dates WHERE blackout_id=%s AND doctor_id=%s',(ident,doctor_id));conn.commit();return response(200,{'deleted':bool(cur.rowcount)})
            if path.endswith('/doctor/me/schedule') and method=='GET':
                day=(event.get('queryStringParameters') or {}).get('date')
                try: date.fromisoformat(day)
                except (ValueError,TypeError):return fail(400,'date must be YYYY-MM-DD')
                table=boto3.resource('dynamodb').Table(os.environ['SLOTS_TABLE'])
                from boto3.dynamodb.conditions import Key
                items=table.query(IndexName='GSI_DoctorDate',KeyConditionExpression=Key('doctor_date').eq(f'{doctor_id}#{day}'))['Items']
                return response(200,{'date':day,'slots':[{k:jsonable(v) for k,v in x.items()} for x in items]})
            return fail(404,'Unknown doctor endpoint')
    except Exception as exc:
        import pymysql
        if isinstance(exc,pymysql.IntegrityError):return fail(409,'Duplicate or conflicting record')
        raise
    finally:conn.close()
