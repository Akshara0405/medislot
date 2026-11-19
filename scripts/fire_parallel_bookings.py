#!/usr/bin/env python3
"""Fire N truly parallel bookings at ONE slot of the deployed MediSlot API and report what happened.

For the Milestone 5 checkpoint: anyone can run this independently of the test suite.

    python scripts/fire_parallel_bookings.py --api https://xxxx.execute-api.ap-south-1.amazonaws.com/dev \\
        --password 'SomePassw0rd' --patients 24 --doctor 42            # picks a slot of doctor 42 with free seats
    python scripts/fire_parallel_bookings.py ... --slot '42#7#2026-11-21T03:30:00Z'
    python scripts/fire_parallel_bookings.py ... --same-key            # every request re-uses ONE Idempotency-Key
    python scripts/fire_parallel_bookings.py ... --keep                # do not cancel the winners afterwards

Patients are accounts <prefix>-NN@medislot-load.example. They are registered on first use and re-used after that,
so every request comes from a DIFFERENT patient (the realistic case). All requests wait on a barrier and are
released at the same instant.

What to look for
  * exactly `capacity` requests get 201, all others 409 SLOT_FULL
  * "conflict retries" > 0: requests really collided on the slot's version and were retried, i.e. contention was
    handled by optimistic concurrency, not avoided by some accidental serialisation
  * then open the CloudWatch dashboard MediSlot-<env>-Booking: BookingConflictRetries rises by the same amount
"""
import argparse
import collections
import concurrent.futures
import statistics
import sys
import threading
import time
import uuid
from datetime import date, timedelta

import requests


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument('--api', required=True, help='API base URL including the stage, e.g. .../dev')
    ap.add_argument('--password', required=True, help='password for the load-test patients (min 10 chars, letter + digit)')
    ap.add_argument('--patients', type=int, default=24)
    ap.add_argument('--prefix', default='load', help='email prefix for the load-test patients')
    ap.add_argument('--slot', help='slot id to hammer')
    ap.add_argument('--doctor', type=int, help='doctor id: pick the first future slot with the most free seats')
    ap.add_argument('--same-key', action='store_true', help='all requests from ONE patient with ONE Idempotency-Key')
    ap.add_argument('--keep', action='store_true', help='keep the bookings (default: cancel winners afterwards)')
    args = ap.parse_args()
    api = args.api.rstrip('/')
    if not args.slot and not args.doctor:
        sys.exit('give --slot or --doctor')

    def call(method, path, token=None, **kw):
        headers = kw.pop('headers', {})
        if token:
            headers['Authorization'] = 'Bearer ' + token
        return requests.request(method, api + path, headers=headers, timeout=40, **kw)

    def patient(i):
        email = f'{args.prefix}-{i:03d}@medislot-load.example'
        res = call('POST', '/auth/login', json={'email': email, 'password': args.password})
        if not res.ok:
            reg = call('POST', '/auth/register/patient', json={
                'email': email, 'password': args.password, 'firstName': 'Load', 'lastName': f'Tester{chr(65 + i % 26)}',
                'phone': '9000000009', 'dateOfBirth': '1991-06-01', 'gender': 'Other'})
            if reg.status_code not in (201, 409):
                sys.exit(f'could not register {email}: {reg.status_code} {reg.text}')
            res = call('POST', '/auth/login', json={'email': email, 'password': args.password})
        if not res.ok:
            sys.exit(f'could not sign in {email}: {res.status_code} {res.text}')
        return {'email': email, 'token': res.json()['accessToken']}

    slot = args.slot
    if not slot:
        start = date.today() + timedelta(days=1)
        days = call('GET', f'/doctors/{args.doctor}/slots',
                    params={'from': start.isoformat(), 'to': (start + timedelta(days=6)).isoformat(),
                            'includeFull': 'false'}).json().get('days', [])
        candidates = [s for d in days for s in d['slots'] if s['freeSeats'] > 0]
        if not candidates:
            sys.exit('that doctor has no free seats in the next week')
        best = max(candidates, key=lambda s: s['freeSeats'])          # first (earliest) slot with the most seats
        slot = best['slotId']
        print(f'slot {slot}  ({best["label"]}, {best["freeSeats"]} of {best["capacity"]} seats free)')

    count = 1 if args.same_key else args.patients
    print(f'signing in {count} patient(s) ...')
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        people = list(pool.map(patient, range(count)))
    if args.same_key:
        people = people * args.patients
    shared_key = 'load-' + uuid.uuid4().hex
    gate = threading.Barrier(len(people))

    def fire(p):
        key = shared_key if args.same_key else 'load-' + uuid.uuid4().hex
        gate.wait()
        started = time.perf_counter()
        res = call('POST', '/patient/appointments', p['token'], json={'slotId': slot}, headers={'Idempotency-Key': key})
        ms = (time.perf_counter() - started) * 1000
        try:
            data = res.json()
        except ValueError:
            data = {'message': res.text[:200]}
        return p, res.status_code, data, ms, res.headers.get('Idempotent-Replayed') == 'true'

    print(f'firing {len(people)} parallel booking requests at the same instant ...')
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(people)) as pool:
        results = list(pool.map(fire, people))

    outcome = collections.Counter(f"{s} {d.get('code') or ('BOOKED' if s == 201 else '')}".strip() for _, s, d, _, _ in results)
    retries = sum((d.get('concurrency') or {}).get('conflictRetries', 0) if s == 201 else d.get('conflictRetries', 0)
                  for _, s, d, _, _ in results)
    winners = [(p, d) for p, s, d, _, replay in results if s == 201 and not replay]
    replays = sum(1 for *_, replay in results if replay)
    latencies = sorted(ms for *_, ms, _ in results)
    print('\nresult')
    for key, n in sorted(outcome.items()):
        print(f'  {key:<28} {n}')
    print(f'  idempotent replays           {replays}')
    print(f'  distinct appointments        {len({d["appointmentId"] for _, s, d, _, _ in results if s == 201})}')
    print(f'  conflict retries (OCC)       {retries}')
    print(f'  slot versions won            {sorted(d["concurrency"]["slotVersion"] for _, d in winners)}')
    print(f'  latency ms p50 / p95 / max   {statistics.median(latencies):.0f} / '
          f'{latencies[int(len(latencies) * .95) - 1]:.0f} / {latencies[-1]:.0f}')
    if not args.keep:
        for p, d in winners:
            call('POST', f'/patient/appointments/{d["appointmentId"]}/cancel', p['token'], json={'reason': 'load test cleanup'})
        print(f'\ncancelled {len(winners)} booking(s) again (use --keep to keep them)')


if __name__ == '__main__':
    main()
