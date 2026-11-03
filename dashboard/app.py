import json
import os
import re
import time
import logging
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo
from decimal import Decimal, InvalidOperation

import requests
from flask import Flask, render_template, request, redirect, session, url_for, flash

app = Flask(__name__)
app.secret_key = os.environ['FLASK_SECRET_KEY']
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SECURE=os.environ.get('COOKIE_SECURE', 'false').lower() == 'true',
    SESSION_COOKIE_SAMESITE='Lax',
    MAX_CONTENT_LENGTH=5 * 1024 * 1024,
)
API = os.environ['API_BASE_URL'].rstrip('/')
ALLOWED_PHOTO_TYPES = {'image/jpeg', 'image/png', 'image/webp'}

SPECIALIZATIONS = [
    'Allergy & Immunology', 'Anaesthesiology', 'Cardiology', 'Dermatology', 'Diabetology & Endocrinology',
    'Emergency Medicine', 'ENT (Otorhinolaryngology)', 'Gastroenterology', 'General Medicine', 'General Surgery',
    'Gynaecology & Obstetrics', 'Nephrology', 'Neurology', 'Neurosurgery', 'Oncology', 'Ophthalmology',
    'Orthopaedics', 'Paediatrics', 'Psychiatry', 'Pulmonology', 'Radiology', 'Rheumatology', 'Urology',
    'Dentistry', 'Physiotherapy', 'Other',
]

_hospital_cache = {'at': 0.0, 'items': []}

def get_hospitals():
    """Load active hospitals; cache successful and empty responses briefly."""
    if time.time() - _hospital_cache['at'] < 30:
        return _hospital_cache['items']
    try:
        res = requests.get(API + '/auth/hospitals', timeout=15)
        if not res.ok:
            logging.getLogger(__name__).error('Hospital API returned HTTP %s: %s', res.status_code, res.text[:500])
            return []
        payload = res.json()
        # Accept both decoded API Gateway proxy JSON and a stringified body.
        if isinstance(payload, dict) and isinstance(payload.get('body'), str):
            payload = json.loads(payload['body'])
        items = payload.get('items', []) if isinstance(payload, dict) else []
        if not isinstance(items, list):
            raise ValueError('Hospital API response did not contain an items list')
        valid = [h for h in items if isinstance(h, dict) and h.get('hospital_id') and h.get('name')]
        _hospital_cache.update(at=time.time(), items=valid)
        if not valid:
            logging.getLogger(__name__).warning('Hospital API returned no active hospitals')
        return valid
    except (requests.RequestException, ValueError, TypeError) as exc:
        logging.getLogger(__name__).exception('Could not load hospitals: %s', type(exc).__name__)
        return []


def specialization_options(current=''):
    """The fixed list, plus a doctor's older free-text value so it is not silently lost."""
    current = (current or '').strip()
    return SPECIALIZATIONS if not current or current in SPECIALIZATIONS else [current] + SPECIALIZATIONS


def render_signup(values, status=200):
    return render_template('signup.html', values=values, hospitals=get_hospitals(),
                           specializations=specialization_options(values.get('specialization', ''))), status


def api_request(method, path, *, payload=None, params=None, token=None, **kwargs):
    headers = kwargs.pop('headers', {})
    access_token = token or session.get('token')
    if access_token:
        headers['Authorization'] = 'Bearer ' + access_token
    return requests.request(method, API + path, headers=headers, json=payload,
                            params=params, timeout=20, **kwargs)


def api_message(response, fallback):
    try:
        return response.json().get('message') or fallback
    except (ValueError, AttributeError):
        return fallback


@app.get('/')
def home():
    if session.get('token') and session.get('role') == 'doctor':
        return redirect(url_for('dashboard'))
    return render_template('home.html')


@app.route('/doctor/signup', methods=['GET', 'POST'])
@app.route('/doctor/register', methods=['GET', 'POST'])
def signup():
    if request.method == 'GET':
        return render_signup({})

    photo = request.files.get('photo')
    required = ['first_name', 'last_name', 'email', 'phone', 'password', 'confirm_password',
                'specialization', 'license_number', 'hospital_id', 'qualification', 'experience_years']
    missing = [field for field in required if not request.form.get(field, '').strip()]
    if missing or not photo or not photo.filename:
        flash('Complete every required field and choose a profile photo.', 'error')
        return render_signup(request.form, 400)
    if request.form['password'] != request.form['confirm_password']:
        flash('The passwords do not match.', 'error')
        return render_signup(request.form, 400)
    if photo.mimetype not in ALLOWED_PHOTO_TYPES:
        flash('Upload a JPG, PNG or WEBP profile photo.', 'error')
        return render_signup(request.form, 400)
    try:
        experience = int(request.form['experience_years'])
        hospital_id = int(request.form['hospital_id'])
        if not 0 <= experience <= 80 or hospital_id <= 0:
            raise ValueError()
    except ValueError:
        flash('Choose a hospital and enter experience between 0 and 80 years.', 'error')
        return render_signup(request.form, 400)

    payload = {
        'firstName': request.form['first_name'].strip(),
        'lastName': request.form['last_name'].strip(),
        'email': request.form['email'].strip().lower(),
        'phone': request.form['phone'].strip(),
        'password': request.form['password'],
        'specialization': request.form['specialization'].strip(),
        'licenseNumber': request.form['license_number'].strip().upper(),
        'hospitalId': hospital_id,
        'qualification': request.form['qualification'].strip(),
        'experienceYears': experience,
    }
    try:
        registered = requests.post(API + '/auth/register/doctor', json=payload, timeout=20)
        if registered.status_code != 201:
            flash(api_message(registered, 'Doctor registration failed. Please check your details.'), 'error')
            return render_signup(request.form, registered.status_code)

        logged_in = requests.post(API + '/auth/login', json={
            'email': payload['email'], 'password': payload['password']
        }, timeout=20)
        if not logged_in.ok or logged_in.json().get('role') != 'doctor':
            flash('Your account was created. Please sign in to finish setting up your profile photo.', 'success')
            return redirect(url_for('login'))

        token = logged_in.json()['accessToken']
        presign = api_request('POST', '/doctor/me/photo', payload={'contentType': photo.mimetype}, token=token)
        if not presign.ok:
            flash('Your account was created, but photo upload could not be started. Sign in and try again after the doctor photo API is deployed. ' + api_message(presign, ''), 'warning')
            return redirect(url_for('login'))
        upload = requests.put(presign.json()['uploadUrl'], data=photo.read(),
                              headers={'Content-Type': photo.mimetype}, timeout=30)
        if not upload.ok:
            flash('Your account was created, but the photo upload did not complete. Sign in and upload it again.', 'warning')
            return redirect(url_for('login'))

        session['token'] = token
        session['role'] = 'doctor'
        flash('Welcome to MediSlot. Your doctor profile has been created.', 'success')
        return redirect(url_for('dashboard'))
    except requests.RequestException:
        flash('MediSlot could not reach the API. Please try again in a moment.', 'error')
        return render_signup(request.form, 502)


@app.route('/doctor/login', methods=['GET', 'POST'])
def login():
    if request.method == 'POST':
        try:
            response = requests.post(API + '/auth/login', json={
                'email': request.form.get('email', '').strip(),
                'password': request.form.get('password', ''),
            }, timeout=20)
            if response.ok and response.json().get('role') == 'doctor':
                session.clear()
                session['token'] = response.json()['accessToken']
                session['role'] = 'doctor'
                return redirect(url_for('dashboard'))
            flash('We could not sign you in as a doctor. Check your details and try again.', 'error')
        except requests.RequestException:
            flash('MediSlot could not reach the API. Please try again.', 'error')
    return render_template('login.html')


@app.route('/doctor/logout')
def logout():
    session.clear()
    return redirect(url_for('home'))


TABS = [('profile', 'Profile overview'), ('types', 'Consultation types'), ('availability', 'Availability'),
        ('schedule', 'Schedule'), ('blackouts', 'Blackouts')]
TAB_KEYS = {key for key, _ in TABS}
CAMP_MIN_CAPACITY, CAMP_MAX_CAPACITY = 2, 10   # keep in step with lambdas/doctor/lambda_function.py
TYPE_DURATION = {'QUICK': 15, 'STANDARD': 30, 'DETAILED': 60, 'HEALTH_CAMP': 60}
ID_PATTERN = re.compile(r'^[0-9a-fA-F-]{36}$')


def api_items(response):
    """Return the 'items' list from an API response, or [] when the call failed."""
    try:
        return response.json().get('items', []) if response.ok else []
    except ValueError:
        return []


def hhmm(value):
    """'9:00:00' or '09:00:00' -> '09:00' for <input type=time>."""
    parts = str(value).split(':')
    try:
        return '%02d:%02d' % (int(parts[0]), int(parts[1]))
    except (ValueError, IndexError):
        return str(value)


def slot_note(result):
    """Turn the API's slot-sync summary into a short sentence for the flash message."""
    try:
        data = result.json()
    except ValueError:
        return '', None
    sync = data.get('slotSync') or {}
    parts = []
    if sync.get('created'):
        parts.append('%d slot%s added' % (sync['created'], '' if sync['created'] == 1 else 's'))
    if sync.get('removed'):
        parts.append('%d open slot%s removed' % (sync['removed'], '' if sync['removed'] == 1 else 's'))
    if sync.get('closed'):
        parts.append('%d slot%s marked closed' % (sync['closed'], '' if sync['closed'] == 1 else 's'))
    if sync.get('reopened'):
        parts.append('%d slot%s re-opened' % (sync['reopened'], '' if sync['reopened'] == 1 else 's'))
    if sync.get('skippedOverlap'):
        parts.append('%d slot%s skipped (overlap a booking)' % (sync['skippedOverlap'], '' if sync['skippedOverlap'] == 1 else 's'))
    if sync.get('keptBooked'):
        parts.append('%d booked slot%s kept' % (sync['keptBooked'], '' if sync['keptBooked'] == 1 else 's'))
    note = (' Next %d days: %s.' % (sync.get('days', 30), ', '.join(parts))) if parts else ''
    return note, data.get('slotSyncError')


def outcome(result, ok_message, fallback):
    if result is not None and result.ok:
        note, warning = slot_note(result)
        flash(ok_message + note, 'success')
        if warning:
            flash(warning, 'warning')
    else:
        flash(api_message(result, fallback) if result is not None else fallback, 'error')


def handle_dashboard_post():
    action = request.form.get('action', '')
    tab = 'profile'
    try:
        if action == 'profile':
            payload = {k: request.form[k] for k in (
                'first_name', 'last_name', 'phone', 'specialization', 'qualification', 'timezone', 'languages'
            ) if k in request.form}
            if request.form.get('hospital_id', '').isdigit():
                payload['hospital_id'] = int(request.form['hospital_id'])
            outcome(api_request('PUT', '/doctor/me', payload=payload), 'Profile saved.', 'Could not save your profile.')

        elif action == 'photo':
            photo = request.files.get('photo')
            if not photo or not photo.filename or photo.mimetype not in ALLOWED_PHOTO_TYPES:
                flash('Choose a JPG, PNG or WEBP photo.', 'error')
            else:
                presign = api_request('POST', '/doctor/me/photo', payload={'contentType': photo.mimetype})
                if not presign.ok:
                    flash(api_message(presign, 'Could not start the photo upload.'), 'error')
                else:
                    upload = requests.put(presign.json()['uploadUrl'], data=photo.read(),
                                          headers={'Content-Type': photo.mimetype}, timeout=30)
                    flash('Profile photo updated.' if upload.ok else 'The photo upload did not complete. Please try again.',
                          'success' if upload.ok else 'error')

        elif action == 'availability':
            tab = 'availability'
            try:
                patterns = json.loads(request.form['patterns'])
                if not isinstance(patterns, list):
                    raise ValueError()
            except (ValueError, KeyError):
                flash('Availability data is invalid.', 'error')
            else:
                outcome(api_request('PUT', '/doctor/me/availability', payload={'patterns': patterns}),
                        'Availability saved.', 'Could not save availability.')

        elif action in ('add_type', 'save_type'):
            tab = 'types'
            name = request.form.get('name', '')
            try:
                if name not in TYPE_DURATION:
                    raise ValueError('Choose a valid consultation type.')
                try:
                    capacity = int(request.form.get('capacity') or 1)
                except ValueError:
                    raise ValueError('Capacity must be a whole number.')
                if name == 'HEALTH_CAMP':
                    if not CAMP_MIN_CAPACITY <= capacity <= CAMP_MAX_CAPACITY:
                        raise ValueError('A health camp needs a capacity between %d and %d.' % (CAMP_MIN_CAPACITY, CAMP_MAX_CAPACITY))
                else:
                    capacity = 1      # one patient at a time; whatever was typed is ignored
                try:
                    fee = Decimal(request.form.get('fee_amount', '').strip())
                except InvalidOperation:
                    raise ValueError('Enter a valid fee.')
                if not fee.is_finite() or fee < 0:
                    raise ValueError('Enter a valid fee.')
            except ValueError as exc:
                flash(str(exc), 'error')
            else:
                payload = {'name': name, 'duration_min': TYPE_DURATION[name], 'capacity': capacity,
                           'fee_amount': str(fee)}
                if action == 'save_type':
                    type_id = request.form.get('type_id', '')
                    if not ID_PATTERN.match(type_id):
                        flash('Unknown consultation type.', 'error')
                    else:
                        outcome(api_request('PUT', '/doctor/me/consultation-types/' + type_id, payload=payload),
                                'Consultation type saved.', 'Could not save the consultation type.')
                else:
                    # A deactivated type of the same name is revived instead of duplicated.
                    existing = next((t for t in api_items(api_request('GET', '/doctor/me/consultation-types'))
                                     if t.get('name') == name), None)
                    if existing:
                        outcome(api_request('PUT', '/doctor/me/consultation-types/' + existing['consultation_type_id'],
                                            payload=payload), 'Consultation type saved.',
                                'Could not save the consultation type.')
                    else:
                        outcome(api_request('POST', '/doctor/me/consultation-types', payload=payload),
                                'Consultation type added.', 'Could not add the consultation type.')

        elif action == 'delete_type':
            tab = 'types'
            type_id = request.form.get('type_id', '')
            if ID_PATTERN.match(type_id):
                outcome(api_request('DELETE', '/doctor/me/consultation-types/' + type_id),
                        'Consultation type removed.', 'Could not remove the consultation type.')

        elif action == 'add_blackout':
            tab = 'blackouts'
            day = request.form.get('blackout_date', '')
            try:
                date.fromisoformat(day)
            except ValueError:
                flash('Choose a valid date.', 'error')
            else:
                outcome(api_request('POST', '/doctor/me/blackouts',
                                    payload={'blackout_date': day, 'reason': request.form.get('reason', '').strip()}),
                        'Blackout date added.', 'Could not add the blackout date.')

        elif action == 'delete_blackout':
            tab = 'blackouts'
            blackout_id = request.form.get('blackout_id', '')
            if ID_PATTERN.match(blackout_id):
                outcome(api_request('DELETE', '/doctor/me/blackouts/' + blackout_id),
                        'Blackout date removed.', 'Could not remove the blackout date.')
    except requests.RequestException:
        flash('MediSlot could not reach the API. Please try again.', 'error')
    return redirect(url_for('dashboard', tab=tab))


def decorate_slots(slots, type_labels, tz_name):
    """Show slot times in the doctor's own timezone, with the consultation type name."""
    try:
        tz = ZoneInfo(tz_name or 'Asia/Kolkata')
    except Exception:
        tz = ZoneInfo('Asia/Kolkata')
    rows = []
    for slot in slots:
        try:
            start = datetime.fromisoformat(str(slot['start_at']).replace('Z', '+00:00')).astimezone(tz)
            end = datetime.fromisoformat(str(slot['end_at']).replace('Z', '+00:00')).astimezone(tz)
        except (KeyError, ValueError):
            continue
        rows.append(dict(slot, start_local=start.strftime('%I:%M %p').lstrip('0'), end_local=end.strftime('%I:%M %p').lstrip('0'),
                         type_label=type_labels.get(slot.get('consultation_type_id'), 'Consultation')))
    return rows


@app.route('/doctor/dashboard', methods=['GET', 'POST'])
def dashboard():
    if 'token' not in session or session.get('role') != 'doctor':
        return redirect(url_for('login'))
    if request.method == 'POST':
        return handle_dashboard_post()

    active = request.args.get('tab', 'profile')
    if active not in TAB_KEYS:
        active = 'profile'
    day = request.args.get('date') or date.today().isoformat()
    try:
        day_obj = date.fromisoformat(day)
    except ValueError:
        day_obj = date.today()
        day = day_obj.isoformat()
    try:
        profile = api_request('GET', '/doctor/me')
        if profile.status_code == 401:
            session.clear()
            return redirect(url_for('login'))
        availability = api_request('GET', '/doctor/me/availability')
        schedule = api_request('GET', '/doctor/me/schedule', params={'start_date': day, 'days': 30})
        all_types = api_items(api_request('GET', '/doctor/me/consultation-types'))
        types = [t for t in all_types if t.get('is_active')]
        blackouts = api_items(api_request('GET', '/doctor/me/blackouts'))
    except requests.RequestException:
        flash('Unable to load your dashboard from the API.', 'error')
        profile = availability = schedule = None
        all_types, types, blackouts = [], [], []

    active_ids = {t['consultation_type_id'] for t in types}
    patterns = [{'consultation_type_id': p.get('consultation_type_id'), 'day_of_week': p.get('day_of_week'),
                 'start_time': hhmm(p.get('start_time')), 'end_time': hhmm(p.get('end_time'))}
                for p in (api_items(availability) if availability is not None else [])
                if p.get('consultation_type_id') in active_ids]
    profile_data = profile.json() if profile is not None and profile.ok else {}
    type_labels = {t['consultation_type_id']: '%s · %s min' % (str(t.get('name', '')).replace('_', ' ').title(), t.get('duration_min'))
                   for t in all_types}
    slots = schedule.json().get('slots', []) if schedule is not None and schedule.ok else []
    decorated = decorate_slots(slots, type_labels, profile_data.get('timezone'))
    slots_by_day = {}
    for slot in decorated:
        slots_by_day.setdefault(slot.get('local_date') or day, []).append(slot)
    blackout_map = {str(b.get('blackout_date')): b.get('reason') for b in blackouts}
    schedule_days = [
        {'date': (day_obj + timedelta(days=offset)).isoformat(),
         'slots': slots_by_day.get((day_obj + timedelta(days=offset)).isoformat(), []),
         'blocked': (day_obj + timedelta(days=offset)).isoformat() in blackout_map,
         'reason': blackout_map.get((day_obj + timedelta(days=offset)).isoformat())}
        for offset in range(30)
    ]
    return render_template(
        'dashboard.html',
        tabs=TABS, active=active, day=day, types=types, blackouts=blackouts, patterns=patterns,
        profile=profile_data,
        schedule=decorated, schedule_days=schedule_days,
        schedule_error=(schedule is not None and not schedule.ok),
        prev_day=(day_obj - timedelta(days=30)).isoformat(), next_day=(day_obj + timedelta(days=30)).isoformat(),
        type_names=list(TYPE_DURATION),
        camp_min=CAMP_MIN_CAPACITY, camp_max=CAMP_MAX_CAPACITY,
        hospitals=get_hospitals(),
        specializations=specialization_options(profile_data.get('specialization', '')),
        type_options=[{'id': t['consultation_type_id'], 'duration': t.get('duration_min'),
                       'label': '%s · %s min' % (str(t.get('name', '')).replace('_', ' ').title(), t.get('duration_min'))}
                      for t in types],
    )


@app.errorhandler(413)
def file_too_large(_error):
    return render_signup({}, 413)