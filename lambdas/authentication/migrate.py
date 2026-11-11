"""One-off database migration, run by the deployment pipeline, never by an API request.

    aws lambda invoke --function-name medislot-dev-authentication --payload '{"action":"migrate"}' out.json

What it does, in order (every step is safe to repeat):
  1. Databases made by the old code kept consultation types, patterns and blackouts with UUID
     (CHAR(36)) ids. Those three tables are renamed to legacy_* so the new INT tables can be created.
  2. Runs schema/schema.sql: creates any missing table and the seed rows (consultations, hospitals).
  3. Adds columns/indexes that older databases are missing (tables created before Milestone 3/4).
  4. Copies the legacy_* rows into the new tables with fresh AUTO_INCREMENT ids, checks the counts,
     then drops the legacy_* tables.
"""
import os
import re
import time

from common import emit

HERE = os.path.dirname(os.path.abspath(__file__))
SCHEMA_PATHS = [os.path.join(HERE, '..', '..', 'schema', 'schema.sql'),        # in the repository
                os.path.join(HERE, 'schema.sql')]                              # inside the Lambda zip

LEGACY = ['consultation_types', 'availability_patterns', 'blackout_dates']

# (table, column, definition) for databases created before these columns were part of schema.sql.
UPGRADE_COLUMNS = [
    ('doctors', 'timezone', "VARCHAR(64) NOT NULL DEFAULT 'Asia/Kolkata'"),
    ('doctors', 'languages', 'VARCHAR(200) NULL'),
    ('doctors', 'photo_key', 'VARCHAR(300) NULL'),
    ('patients', 'city', 'VARCHAR(100) NULL AFTER address'),
    ('patients', 'blood_group', 'VARCHAR(3) NULL AFTER city'),
    ('patients', 'emergency_contact_name', 'VARCHAR(150) NULL AFTER blood_group'),
    ('patients', 'emergency_contact_phone', 'VARCHAR(25) NULL AFTER emergency_contact_name'),
    ('patients', 'profile_version', 'INT UNSIGNED NOT NULL DEFAULT 1 AFTER emergency_contact_phone'),
    ('patients', 'updated_at', 'DATETIME(6) NOT NULL DEFAULT CURRENT_TIMESTAMP(6) '
                               'ON UPDATE CURRENT_TIMESTAMP(6) AFTER created_at'),
]
UPGRADE_INDEXES = [
    ('doctors', 'ix_doctors_name', 'last_name, first_name'),
    ('doctors', 'ix_doctors_first_name', 'first_name'),
    ('doctors', 'ix_doctors_spec_name', 'specialization, last_name, first_name'),
    ('doctors', 'ix_doctors_hospital_name', 'hospital_id, last_name, first_name'),
    ('hospitals', 'ix_hospitals_city', 'city, is_active'),
]


def load_statements(path=None):
    """schema.sql split into single statements (full-line '--' comments removed)."""
    path = path or next(p for p in SCHEMA_PATHS if os.path.exists(p))
    with open(path, encoding='utf-8-sig') as handle:          # -sig: ignore a BOM some Windows editors add
        text = handle.read().replace('\r\n', '\n').replace('\r', '\n')   # Windows / old Mac line endings
    text = re.sub(r'/\*.*?\*/', '', text, flags=re.S)                    # /* block comments */
    text = '\n'.join(line for line in text.splitlines() if not line.strip().startswith(('--', '#')))
    return [s.strip() for s in re.split(r';[ \t]*(?:\n|$)', text) if s.strip()]


def _scalar(cur, sql, args=()):
    cur.execute(sql, args)
    row = cur.fetchone()
    return None if not row else list(row.values())[0]


def table_exists(cur, name):
    return bool(_scalar(cur, 'SELECT COUNT(*) AS n FROM information_schema.TABLES '
                             'WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s', (name,)))


def column_type(cur, table, column):
    return _scalar(cur, 'SELECT DATA_TYPE FROM information_schema.COLUMNS '
                        'WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = %s AND COLUMN_NAME = %s', (table, column))


def rename_uuid_tables(cur):
    """Step 1. Returns True when old UUID tables were found and moved aside."""
    if column_type(cur, 'consultation_types', 'consultation_type_id') not in ('char', 'varchar'):
        return False
    cur.execute('RENAME TABLE ' + ', '.join(f'{t} TO legacy_{t}' for t in LEGACY))
    return True


def upgrade_old_tables(cur):
    """Step 3. Bring tables created by older code up to the shape in schema.sql."""
    changes = []
    for table, column, definition in UPGRADE_COLUMNS:
        if column_type(cur, table, column) is None:
            cur.execute(f'ALTER TABLE {table} ADD COLUMN {column} {definition}')
            changes.append(f'{table}.{column}')
    for table, name, columns in UPGRADE_INDEXES:
        if not _scalar(cur, 'SELECT COUNT(*) AS n FROM information_schema.STATISTICS WHERE TABLE_SCHEMA = DATABASE() '
                            'AND TABLE_NAME = %s AND INDEX_NAME = %s', (table, name)):
            cur.execute(f'CREATE INDEX {name} ON {table} ({columns})')
            changes.append(f'{table}.{name}')
    # Milestone 2 had a required users.full_name; names now live in patients/doctors.
    cur.execute("SELECT IS_NULLABLE FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() "
                "AND TABLE_NAME = 'users' AND COLUMN_NAME = 'full_name'")
    row = cur.fetchone()
    if row and row['IS_NULLABLE'] == 'NO':
        cur.execute('ALTER TABLE users MODIFY full_name VARCHAR(150) NULL')
        changes.append('users.full_name nullable')
    return changes


def copy_legacy_rows(cur):
    """Step 4. Copy UUID rows into the INT tables, then drop the legacy tables. Returns counts copied."""
    if not table_exists(cur, 'legacy_consultation_types'):
        return None
    cur.execute('SELECT consultation_id, code FROM consultations')
    by_code = {r['code']: r['consultation_id'] for r in cur.fetchall()}
    new_type_id = {}
    # The copy is one transaction, so either nothing or everything was copied by an earlier run
    # (one that stopped after the COMMIT but before the DROP).
    if not _scalar(cur, 'SELECT COUNT(*) AS n FROM consultation_types'):
        cur.execute('SELECT * FROM legacy_consultation_types ORDER BY doctor_id, name')
        for t in cur.fetchall():
            cur.execute('INSERT INTO consultation_types (doctor_id, consultation_id, fee_amount, capacity, is_active) '
                        'VALUES (%s, %s, %s, %s, %s)',
                        (t['doctor_id'], by_code[t['name']], t['fee_amount'], t['capacity'], t['is_active']))
            new_type_id[t['consultation_type_id']] = cur.lastrowid
        cur.execute('SELECT * FROM legacy_availability_patterns ORDER BY doctor_id, day_of_week, start_time')
        for p in cur.fetchall():
            if new_type_id.get(p['consultation_type_id']):
                cur.execute('INSERT INTO availability_patterns (doctor_id, consultation_type_id, day_of_week, '
                            'start_time, end_time) VALUES (%s, %s, %s, %s, %s)',
                            (p['doctor_id'], new_type_id[p['consultation_type_id']], p['day_of_week'],
                             p['start_time'], p['end_time']))
        cur.execute('INSERT INTO blackout_dates (doctor_id, blackout_date, reason) '
                    'SELECT doctor_id, blackout_date, reason FROM legacy_blackout_dates ORDER BY doctor_id, blackout_date')
    counts = {t: (_scalar(cur, f'SELECT COUNT(*) AS n FROM legacy_{t}'), _scalar(cur, f'SELECT COUNT(*) AS n FROM {t}'))
              for t in LEGACY}
    if any(new < old for old, new in counts.values()):
        raise RuntimeError(f'Legacy copy incomplete, legacy tables kept for inspection: {counts}')
    return counts


def drop_legacy(cur):
    for t in ['availability_patterns', 'blackout_dates', 'consultation_types']:     # children first
        cur.execute(f'DROP TABLE IF EXISTS legacy_{t}')


def run(conn):
    started = time.time()
    with conn.cursor() as cur:
        renamed = rename_uuid_tables(cur)
        statements = load_statements()
        for sql in statements:
            cur.execute(sql)
        conn.commit()
        changes = upgrade_old_tables(cur)
        copied = copy_legacy_rows(cur)
        conn.commit()
        if copied is not None:
            drop_legacy(cur)
    summary = {'statements': len(statements), 'renamedUuidTables': renamed, 'upgraded': changes,
               'legacyRowsCopied': {t: c[1] for t, c in copied.items()} if copied else None,
               'durationMs': int((time.time() - started) * 1000)}
    emit('info', 'database_migrated', None, **summary)
    return summary