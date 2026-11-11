"""Database migration (lambdas/authentication/migrate.py): schema.sql parsing and the UUID -> INT upgrade."""
import os
import sys
from decimal import Decimal

ROOT = os.path.join(os.path.dirname(__file__), '..', 'lambdas')
sys.path.insert(0, ROOT)
sys.path.insert(0, os.path.join(ROOT, 'authentication'))
import migrate  # noqa: E402


def test_schema_sql_has_no_uuid_or_enum_ids_and_splits_cleanly():
    statements = migrate.load_statements()
    creates = [s for s in statements if s.startswith('CREATE TABLE')]
    assert len(creates) == 8
    text = '\n'.join(statements)
    assert 'CHAR(36)' not in text and "ENUM('QUICK'" not in text
    for table in ('users', 'hospitals', 'consultations', 'consultation_types', 'availability_patterns', 'blackout_dates'):
        create = next(s for s in creates if f'EXISTS {table} (' in s)
        assert 'AUTO_INCREMENT' in create, table
    assert 'INSERT IGNORE' not in text                          # INSERT IGNORE burns AUTO_INCREMENT numbers
    assert sum(1 for s in statements if s.startswith('INSERT INTO consultations')) == 4


class OldDatabase:
    """Pretends to be a MySQL database created by the old code (UUID ids, ENUM consultation names)."""

    def __init__(self):
        self.ran, self.rows, self.lastrowid, self.inserted = [], [], 0, {}
        self.tables = {'consultation_types', 'availability_patterns', 'blackout_dates', 'doctors', 'patients', 'users'}
        self.legacy = {
            'consultation_types': [
                {'consultation_type_id': 'aaaa-1111', 'doctor_id': 7, 'name': 'STANDARD', 'duration_min': 30,
                 'fee_amount': Decimal('450.00'), 'capacity': 1, 'is_active': 1},
                {'consultation_type_id': 'bbbb-2222', 'doctor_id': 7, 'name': 'HEALTH_CAMP', 'duration_min': 60,
                 'fee_amount': Decimal('100.00'), 'capacity': 6, 'is_active': 0}],
            'availability_patterns': [
                {'pattern_id': 'p-1', 'doctor_id': 7, 'consultation_type_id': 'aaaa-1111', 'day_of_week': 0,
                 'start_time': '09:00:00', 'end_time': '13:00:00'}],
            'blackout_dates': [{'blackout_id': 'b-1', 'doctor_id': 7, 'blackout_date': '2026-12-25', 'reason': 'Xmas'}],
        }

    def __enter__(self): return self
    def __exit__(self, *a): return False

    def execute(self, sql, args=()):
        s = ' '.join(sql.split())
        self.ran.append(s)
        self.rows = []
        if 'information_schema.COLUMNS' in s and 'DATA_TYPE' in s:
            if args == ('consultation_types', 'consultation_type_id') and 'legacy_consultation_types' not in self.tables:
                self.rows = [{'DATA_TYPE': 'char'}]
            elif args[0] in ('doctors', 'patients') and args[1] != 'updated_at':
                self.rows = [{'DATA_TYPE': 'varchar'}]               # only patients.updated_at is missing
        elif s.startswith('RENAME TABLE'):
            for t in ('consultation_types', 'availability_patterns', 'blackout_dates'):
                self.tables.discard(t)
                self.tables.add(f'legacy_{t}')
        elif s.startswith('CREATE TABLE IF NOT EXISTS'):
            self.tables.add(s.split()[5])
        elif 'information_schema.TABLES' in s:
            self.rows = [{'n': int(args[0] in self.tables)}]
        elif 'information_schema.STATISTICS' in s:
            self.rows = [{'n': 1}]
        elif 'IS_NULLABLE' in s:
            self.rows = [{'IS_NULLABLE': 'NO'}]
        elif s.startswith('SELECT consultation_id, code FROM consultations'):
            self.rows = [{'consultation_id': i, 'code': c} for i, c in
                         enumerate(['QUICK', 'STANDARD', 'DETAILED', 'HEALTH_CAMP'], start=1)]
        elif s.startswith('SELECT * FROM legacy_'):
            self.rows = self.legacy[s.split()[3][len('legacy_'):]]
        elif s.startswith('SELECT COUNT(*) AS n FROM legacy_'):
            self.rows = [{'n': len(self.legacy[s.split()[-1][len('legacy_'):]])}]
        elif s.startswith('SELECT COUNT(*) AS n FROM'):
            self.rows = [{'n': len(self.inserted.get(s.split()[-1], []))}]
        elif s.startswith('INSERT INTO') and 'legacy' not in s and 'consultations' not in s.split('(')[0] \
                and 'hospitals' not in s.split('(')[0]:
            table = s.split()[2]
            self.inserted.setdefault(table, []).append(args)
            self.lastrowid = len(self.inserted[table])
        elif s.startswith('INSERT INTO blackout_dates') and 'legacy' in s:
            self.inserted['blackout_dates'] = list(self.legacy['blackout_dates'])

    def fetchone(self): return self.rows[0] if self.rows else None
    def fetchall(self): return self.rows


class Conn:
    def __init__(self): self.cur, self.commits = OldDatabase(), 0
    def cursor(self): return self.cur
    def commit(self): self.commits += 1


def test_old_uuid_tables_are_converted_without_losing_rows(monkeypatch):
    monkeypatch.setattr(migrate, 'emit', lambda *a, **k: None)
    conn = Conn()
    summary = migrate.run(conn)
    ran = conn.cur.ran
    assert summary['renamedUuidTables'] is True
    assert any(s.startswith('RENAME TABLE consultation_types TO legacy_consultation_types') for s in ran)
    assert summary['upgraded'] == ['patients.updated_at', 'users.full_name nullable']
    # New integer ids: STANDARD -> consultation 2, HEALTH_CAMP -> 4; the pattern points at the new type id 1.
    types = conn.cur.inserted['consultation_types']
    assert [t[:2] for t in types] == [(7, 2), (7, 4)] and types[1][4] == 0       # inactive stays inactive
    assert conn.cur.inserted['availability_patterns'] == [(7, 1, 0, '09:00:00', '13:00:00')]
    assert summary['legacyRowsCopied'] == {'consultation_types': 2, 'availability_patterns': 1, 'blackout_dates': 1}
    drops = [s for s in ran if s.startswith('DROP TABLE')]
    assert drops == ['DROP TABLE IF EXISTS legacy_availability_patterns', 'DROP TABLE IF EXISTS legacy_blackout_dates',
                     'DROP TABLE IF EXISTS legacy_consultation_types']
