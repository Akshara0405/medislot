import json, os, uuid, logging, boto3, pymysql, hashlib, time
from functools import lru_cache
from botocore.exceptions import ClientError
log = logging.getLogger()
log.setLevel(logging.INFO)
ssm = boto3.client("ssm")
ddb = boto3.resource("dynamodb")
def emit(level, message, correlation_id=None, **fields):
    payload = {"level": level, "message": message, "correlationId": correlation_id or str(uuid.uuid4()), **fields}
    getattr(log, level if level in ("info","warning","error") else "info")(json.dumps(payload))
def response(status, body):
    return {"statusCode": status, "headers": {"content-type":"application/json", "cache-control":"no-store"}, "body": json.dumps(body)}
@lru_cache(maxsize=64)
def get_param(name, decrypt=False):
    return ssm.get_parameter(Name=name, WithDecryption=decrypt)["Parameter"]["Value"]
def config():
    env=os.environ.get("ENVIRONMENT","dev")
    return {"env":env,"prefix":f"/medislot/{env}","jwt":get_param(f"/medislot/{env}/auth/jwt-secret",True),
            "dbhost":get_param(f"/medislot/{env}/db/host"),"dbport":int(get_param(f"/medislot/{env}/db/port")),
            "dbname":get_param(f"/medislot/{env}/db/name"),"dbuser":get_param(f"/medislot/{env}/db/username"),
            "dbpass":get_param(f"/medislot/{env}/db/password",True)}
_M3_COLUMNS = [
    ("doctors", "timezone", "VARCHAR(64) NOT NULL DEFAULT 'Asia/Kolkata'"),
    ("doctors", "languages", "VARCHAR(200) NULL"),
    ("doctors", "photo_key", "VARCHAR(300) NULL"),
]
_M3_TABLES = [
    """CREATE TABLE IF NOT EXISTS consultation_types (
      consultation_type_id CHAR(36) PRIMARY KEY,
      doctor_id BIGINT UNSIGNED NOT NULL,
      name ENUM('QUICK','STANDARD','DETAILED','HEALTH_CAMP') NOT NULL,
      duration_min SMALLINT NOT NULL,
      fee_amount DECIMAL(10,2) NOT NULL,
      capacity TINYINT NOT NULL,
      is_active BOOLEAN NOT NULL DEFAULT TRUE,
      UNIQUE KEY uq_doctor_type (doctor_id, name),
      FOREIGN KEY (doctor_id) REFERENCES doctors(user_id)
    ) ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS availability_patterns (
      pattern_id CHAR(36) PRIMARY KEY,
      doctor_id BIGINT UNSIGNED NOT NULL,
      consultation_type_id CHAR(36) NOT NULL,
      day_of_week TINYINT NOT NULL,
      start_time TIME NOT NULL,
      end_time TIME NOT NULL,
      KEY ix_doctor_weekday (doctor_id, day_of_week),
      FOREIGN KEY (doctor_id) REFERENCES doctors(user_id),
      FOREIGN KEY (consultation_type_id) REFERENCES consultation_types(consultation_type_id)
    ) ENGINE=InnoDB""",
    """CREATE TABLE IF NOT EXISTS blackout_dates (
      blackout_id CHAR(36) PRIMARY KEY,
      doctor_id BIGINT UNSIGNED NOT NULL,
      blackout_date DATE NOT NULL,
      reason VARCHAR(200),
      UNIQUE KEY uq_doctor_blackout (doctor_id, blackout_date),
      FOREIGN KEY (doctor_id) REFERENCES doctors(user_id)
    ) ENGINE=InnoDB""",
]
SEED_HOSPITALS = [
    ("MediSlot General Hospital", "Hyderabad", "Main Road, Hyderabad"),
    ("Apollo Hospitals", "Hyderabad", "Jubilee Hills, Hyderabad"),
    ("Yashoda Hospitals", "Hyderabad", "Secunderabad, Hyderabad"),
    ("KIMS Hospitals", "Hyderabad", "Secunderabad, Hyderabad"),
    ("CARE Hospitals", "Hyderabad", "Banjara Hills, Hyderabad"),
    ("Continental Hospitals", "Hyderabad", "Gachibowli, Hyderabad"),
    ("AIG Hospitals", "Hyderabad", "Gachibowli, Hyderabad"),
    ("Nizam's Institute of Medical Sciences", "Hyderabad", "Punjagutta, Hyderabad"),
    ("Osmania General Hospital", "Hyderabad", "Afzalgunj, Hyderabad"),
]
_m3_ready = False
def ensure_milestone3_schema(cur):
    """Idempotent Milestone 3 schema (mirrors schema/milestone3.sql). Runs once per warm Lambda."""
    global _m3_ready
    if _m3_ready:
        return
    for tbl, col, ddl in _M3_COLUMNS:
        cur.execute("""SELECT 1 FROM information_schema.COLUMNS
                       WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME=%s AND COLUMN_NAME=%s""", (tbl, col))
        if not cur.fetchone():
            try:
                cur.execute(f"ALTER TABLE {tbl} ADD COLUMN {col} {ddl}")
            except pymysql.err.OperationalError as exc:
                if exc.args[0] != 1060:  # 1060 = duplicate column: another Lambda added it first
                    raise
    for statement in _M3_TABLES:
        cur.execute(statement)
    # Doctor sign-up shows these in a dropdown. INSERT IGNORE keeps this safe to re-run;
    # edit or extend the list freely (hospitals are unique on name + city).
    cur.executemany("INSERT IGNORE INTO hospitals (name, city, address) VALUES (%s,%s,%s)", SEED_HOSPITALS)
    _m3_ready = True
def dbconn(c):
    conn=pymysql.connect(host=c["dbhost"],port=c["dbport"],user=c["dbuser"],password=c["dbpass"],
                         database=c["dbname"],connect_timeout=5,read_timeout=8,write_timeout=8,
                         cursorclass=pymysql.cursors.DictCursor,autocommit=False)
    with conn.cursor() as cur:
        cur.execute("""CREATE TABLE IF NOT EXISTS users (
          user_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
          email VARCHAR(254) NOT NULL UNIQUE,
          password_hash VARCHAR(255) NOT NULL,
          role ENUM('patient','doctor') NOT NULL,
          full_name VARCHAR(150) NULL,
          is_active BOOLEAN NOT NULL DEFAULT TRUE,
          created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
          updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6)
        ) ENGINE=InnoDB""")
        # Keep the legacy column for backward compatibility, but stop requiring or populating it.
        cur.execute("""SELECT IS_NULLABLE FROM information_schema.COLUMNS
                       WHERE TABLE_SCHEMA=DATABASE() AND TABLE_NAME='users' AND COLUMN_NAME='full_name'""")
        legacy_name = cur.fetchone()
        if legacy_name and legacy_name["IS_NULLABLE"] == "NO":
            cur.execute("ALTER TABLE users MODIFY full_name VARCHAR(150) NULL")
        cur.execute("""CREATE TABLE IF NOT EXISTS hospitals (
          hospital_id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
          name VARCHAR(150) NOT NULL,
          address VARCHAR(500) NULL,
          city VARCHAR(100) NULL,
          is_active BOOLEAN NOT NULL DEFAULT TRUE,
          created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
          UNIQUE KEY uq_hospitals_name_city (name, city),
          KEY ix_hospitals_city (city, is_active)
        ) ENGINE=InnoDB""")
        cur.execute("""CREATE TABLE IF NOT EXISTS patients (
          user_id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
          first_name VARCHAR(75) NOT NULL,
          last_name VARCHAR(75) NOT NULL,
          phone VARCHAR(25) NOT NULL,
          date_of_birth DATE NULL,
          gender VARCHAR(30) NULL,
          address VARCHAR(500) NULL,
          city VARCHAR(100) NULL,
          blood_group VARCHAR(3) NULL,
          emergency_contact_name VARCHAR(150) NULL,
          emergency_contact_phone VARCHAR(25) NULL,
          profile_version INT UNSIGNED NOT NULL DEFAULT 1,
          created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
          updated_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
          CONSTRAINT fk_patients_user FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
        ) ENGINE=InnoDB""")
        cur.execute("""CREATE TABLE IF NOT EXISTS doctors (
          user_id BIGINT UNSIGNED NOT NULL PRIMARY KEY,
          first_name VARCHAR(75) NOT NULL,
          last_name VARCHAR(75) NOT NULL,
          phone VARCHAR(25) NOT NULL,
          specialization VARCHAR(120) NOT NULL,
          license_number VARCHAR(32) NOT NULL UNIQUE,
          hospital_id BIGINT UNSIGNED NOT NULL,
          qualification VARCHAR(150) NOT NULL,
          experience_years SMALLINT UNSIGNED NOT NULL DEFAULT 0,
          created_at DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6),
          -- Milestone 4 search indexes (see docs/M4_SEARCH_INDEXES.md). InnoDB appends the primary key
          -- (user_id) to every secondary index, so each one already ends in the keyset tie-breaker.
          KEY ix_doctors_name (last_name, first_name),
          KEY ix_doctors_first_name (first_name),
          KEY ix_doctors_spec_name (specialization, last_name, first_name),
          KEY ix_doctors_hospital_name (hospital_id, last_name, first_name),
          CONSTRAINT fk_doctors_user FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
          CONSTRAINT fk_doctors_hospital FOREIGN KEY (hospital_id) REFERENCES hospitals(hospital_id)
        ) ENGINE=InnoDB""")
        ensure_milestone3_schema(cur)
    conn.commit()
    return conn
def table(name):
    return ddb.Table(name)
def auth_table():
    return table(f"MediSlot-{os.environ.get('ENVIRONMENT','dev')}-AuthTokens")
def token_hash(raw):
    return hashlib.sha256(raw.encode()).hexdigest()
def body(event):
    try:
        value=event.get("body") or "{}"
        if event.get("isBase64Encoded"): import base64; value=base64.b64decode(value).decode()
        return json.loads(value)
    except Exception: return None
def correlation(event):
    return (event.get("headers") or {}).get("x-correlation-id") or (event.get("requestContext") or {}).get("requestId") or str(uuid.uuid4())
