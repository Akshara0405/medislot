import base64
import json
import logging
import os
import re
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from zoneinfo import ZoneInfo

import requests
from flask import Flask, flash, g, redirect, render_template, request, session, url_for

app = Flask(__name__)
app.secret_key = os.environ['FLASK_SECRET_KEY']
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SECURE=os.environ.get('COOKIE_SECURE', 'false').lower() == 'true',
    SESSION_COOKIE_SAMESITE='Lax',
    MAX_CONTENT_LENGTH=5 * 1024 * 1024,
    # The API refresh token lives 7 days, so the dashboard session may too (a plain session cookie dies with the browser).
    PERMANENT_SESSION_LIFETIME=timedelta(days=7),
)
API = os.environ['API_BASE_URL'].rstrip('/')
ALLOWED_PHOTO_TYPES = {'image/jpeg', 'image/png', 'image/webp'}


# ----------------------------------------------------------------------------- logging
# Every page request is logged (success or failure). On the EC2 instance the lines also go to the
# CloudWatch log group /medislot/<env>/dashboard (created by web-stack.yaml) through watchtower.
log = logging.getLogger('medislot.dashboard')
log.setLevel(logging.INFO)
if not log.handlers:
    log.addHandler(logging.StreamHandler())
    try:
        import boto3
        import watchtower
        region = (re.search(r'execute-api\.([a-z0-9-]+)\.amazonaws', API) or [None, 'ap-south-1'])[1]
        log.addHandler(watchtower.CloudWatchLogHandler(
            log_group_name=f"/medislot/{os.environ.get('ENVIRONMENT', 'dev')}/dashboard", create_log_group=False,
            boto3_client=boto3.client('logs', region_name=region)))
    except Exception:          # tests and local runs: console only
        pass


@app.before_request
def _start_timer():
    g.started = time.time()


@app.after_request
def _log_request(resp):
    if request.endpoint != 'static':
        level = logging.ERROR if resp.status_code >= 500 else logging.WARNING if resp.status_code >= 400 else logging.INFO
        log.log(level, json.dumps({'message': 'page_request', 'method': request.method, 'path': request.path,
                                   'status': resp.status_code, 'role': session.get('role'),
                                   'durationMs': int((time.time() - g.get('started', time.time())) * 1000)}))
    return resp


@app.errorhandler(Exception)
def _crash(exc):
    from werkzeug.exceptions import HTTPException
    if isinstance(exc, HTTPException):
        return exc
    log.exception(json.dumps({'message': 'page_failed', 'method': request.method, 'path': request.path,
                              'errorType': type(exc).__name__}))
    return 'Something went wrong. Please try again.', 500

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
    return render_template('register.html', role='doctor', values=values, hospitals=get_hospitals(),
                           specializations=specialization_options(values.get('specialization', ''))), status


def store_login(data, role='doctor'):
    """Keep the access token, its expiry and the refresh token so the session can renew itself."""
    session['token'] = data['accessToken']
    if data.get('refreshToken'):
        session['refresh_token'] = data['refreshToken']
    session['token_exp'] = time.time() + int(data.get('expiresIn') or 900)
    session['role'] = role
    if str(data.get('userId', '')).isdigit():
        session['user_id'] = str(data['userId'])
    session.permanent = True


def session_user_id():
    """The signed-in user's id: saved at login, or read from the access token (sessions from before that change).

    Only used to build a URL. The API verifies the token itself and decides what the caller may see.
    """
    if str(session.get('user_id', '')).isdigit():
        return session['user_id']
    try:
        part = str(session.get('token', '')).split('.')[1]
        sub = str(json.loads(base64.urlsafe_b64decode(part + '=' * (-len(part) % 4))).get('sub', ''))
    except (IndexError, ValueError, TypeError, AttributeError):
        return None
    if sub.isdigit():
        session['user_id'] = sub
        return sub
    return None


def end_signup_login(data):
    """Revoke the short-lived tokens used during sign-up to upload the photo. Best effort: never blocks sign-up."""
    try:
        requests.post(API + '/auth/logout', json={'refreshToken': data.get('refreshToken')},
                      headers={'Authorization': 'Bearer ' + data['accessToken']}, timeout=10)
    except (requests.RequestException, KeyError, TypeError):
        pass


def refresh_access_token():
    """Swap the refresh token for a new access token. False means the doctor must sign in again."""
    refresh_token = session.get('refresh_token')
    if not refresh_token:
        return False
    try:
        renewed = requests.post(API + '/auth/refresh', json={'refreshToken': refresh_token}, timeout=20)
        if not renewed.ok:
            return False
        data = renewed.json()
        session['token'] = data['accessToken']
        session['token_exp'] = time.time() + int(data.get('expiresIn') or 900)
        session.permanent = True
        return True
    except (requests.RequestException, ValueError, KeyError):
        return False


def api_request(method, path, *, payload=None, params=None, token=None, **kwargs):
    extra_headers = kwargs.pop('headers', {})

    def send():
        headers = dict(extra_headers)
        access_token = token or session.get('token')
        if access_token:
            headers['Authorization'] = 'Bearer ' + access_token
        return requests.request(method, API + path, headers=headers, json=payload,
                                params=params, timeout=20, **kwargs)

    own_session = token is None
    # Renew a minute before the 15-minute access token runs out, instead of failing and logging the doctor out.
    if own_session and session.get('refresh_token') and time.time() > session.get('token_exp', 0) - 60:
        refresh_access_token()
    result = send()
    if own_session and result.status_code == 401 and refresh_access_token():
        result = send()
    return result


def api_message(response, fallback):
    try:
        return response.json().get('message') or fallback
    except (ValueError, AttributeError):
        return fallback


@app.get('/')
def home():
    if session.get('token') and session.get('role') == 'doctor':
        return redirect(url_for('dashboard'))
    if session.get('token') and session.get('role') == 'patient':
        return redirect(url_for('patient_dashboard'))
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

        # Sign in for a moment ONLY to upload the photo (that API needs a doctor token).
        # These tokens are never stored in the browser session: the doctor signs in themselves next.
        logged_in = requests.post(API + '/auth/login', json={
            'email': payload['email'], 'password': payload['password']
        }, timeout=20)
        if not logged_in.ok or logged_in.json().get('role') != 'doctor':
            flash('Your account was created. Please sign in to finish setting up your profile photo.', 'success')
            return redirect(url_for('login', email=payload['email']))

        login_data = logged_in.json()
        token = login_data['accessToken']
        try:
            presign = api_request('POST', '/doctor/me/photo', payload={'contentType': photo.mimetype}, token=token)
            if not presign.ok:
                flash('Your account was created, but photo upload could not be started. Sign in and upload your photo '
                      'from your profile. ' + api_message(presign, ''), 'warning')
                return redirect(url_for('login', email=payload['email']))
            upload = requests.put(presign.json()['uploadUrl'], data=photo.read(),
                                  headers={'Content-Type': photo.mimetype}, timeout=30)
            if not upload.ok:
                flash('Your account was created, but the photo upload did not complete. Sign in and upload it again '
                      'from your profile.', 'warning')
                return redirect(url_for('login', email=payload['email']))
        finally:
            end_signup_login(login_data)

        # Do NOT sign the new doctor in: send them to the login page with their email filled in.
        session.pop('token', None); session.pop('refresh_token', None); session.pop('token_exp', None); session.pop('role', None)
        flash('Your doctor account has been created. Please sign in to continue.', 'success')
        return redirect(url_for('login', email=payload['email']))
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
                store_login(response.json())
                return redirect(url_for('dashboard'))
            flash('We could not sign you in as a doctor. Check your details and try again.', 'error')
        except requests.RequestException:
            flash('MediSlot could not reach the API. Please try again.', 'error')
    # After sign-up the email arrives as ?email=..., so the doctor only types the password.
    email = request.form.get('email') or request.args.get('email', '')
    return render_template('login.html', role='doctor', email=email.strip()[:254])


@app.route('/doctor/logout')
def logout():
    session.clear()
    return redirect(url_for('home'))


TABS = [('profile', 'Profile overview'), ('types', 'Consultation types'), ('availability', 'Availability'),
        ('schedule', 'Schedule'), ('blackouts', 'Blackouts')]
TAB_KEYS = {key for key, _ in TABS} | {'cancelled'}


def money_total(amounts):
    """Sum of '450.00'-style amounts (None and bad values are skipped), formatted with two decimals."""
    total = Decimal('0')
    for value in amounts:
        try:
            total += Decimal(str(value))
        except (InvalidOperation, TypeError, ValueError):
            pass
    return f'{total:.2f}'
ID_PATTERN = re.compile(r'^[1-9][0-9]{0,18}$')     # every database id is an AUTO_INCREMENT integer
# The status dropdown on each booked appointment in the doctor's Schedule tab.
APPOINTMENT_STATUS_CHOICES = {'CONFIRMED': 'Confirmed', 'COMPLETED': 'Completed', 'NO_SHOW': 'No-show',
                              'CANCELLED': 'Cancelled'}
APPOINTMENT_STATUS_LABELS = {'CONFIRMED': 'Confirmed', 'COMPLETED': 'Completed', 'NO_SHOW': 'No-show',
                             'CANCELLED_BY_PATIENT': 'Cancelled by patient', 'CANCELLED_BY_DOCTOR': 'Cancelled by you'}


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
    elif result is not None and result.status_code == 401:
        # Access and refresh tokens are both gone: say so, then the dashboard sends the doctor to the login page.
        session.clear()
        flash('Your session has expired. Please sign in again.', 'warning')
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
            # Capacity limits and durations live in the consultations table; the API checks them.
            tab = 'types'
            try:
                capacity = int(request.form.get('capacity') or 1)
                fee = Decimal(request.form.get('fee_amount', '').strip())
                if not fee.is_finite() or fee < 0:
                    raise InvalidOperation()
            except (ValueError, InvalidOperation):
                flash('Enter a whole-number capacity and a valid fee.', 'error')
            else:
                payload = {'capacity': capacity, 'fee_amount': str(fee)}
                if action == 'save_type':
                    type_id = request.form.get('type_id', '')
                    if not ID_PATTERN.match(type_id):
                        flash('Unknown consultation type.', 'error')
                    else:
                        outcome(api_request('PUT', '/doctor/me/consultation-types/' + type_id, payload=payload),
                                'Consultation type saved.', 'Could not save the consultation type.')
                elif not ID_PATTERN.match(request.form.get('consultation_id', '')):
                    flash('Choose a valid consultation type.', 'error')
                else:
                    # Same as before: a type the doctor already offers is updated instead of added twice.
                    # (The API itself re-activates a type the doctor removed earlier.)
                    payload['consultation_id'] = int(request.form['consultation_id'])
                    existing = next((t for t in api_items(api_request('GET', '/doctor/me/consultation-types'))
                                     if t.get('consultation_id') == payload['consultation_id'] and t.get('is_active')), None)
                    if existing:
                        outcome(api_request('PUT', '/doctor/me/consultation-types/%s' % existing['consultation_type_id'],
                                            payload=payload), 'Consultation type saved.', 'Could not save the consultation type.')
                    else:
                        outcome(api_request('POST', '/doctor/me/consultation-types', payload=payload),
                                'Consultation type added.', 'Could not add the consultation type.')

        elif action == 'delete_type':
            tab = 'types'
            type_id = request.form.get('type_id', '')
            if ID_PATTERN.match(type_id):
                outcome(api_request('DELETE', '/doctor/me/consultation-types/' + type_id),
                        'Consultation type removed.', 'Could not remove the consultation type.')

        elif action == 'toggle_slot':
            tab = 'schedule'
            slot_id = request.form.get('slot_id', '')
            if slot_id and len(slot_id) <= 190 and '#' in slot_id:
                blocked = request.form.get('blocked') == '1'
                result = api_request('POST', '/doctor/me/slot-blocks', payload={'slot_id': slot_id, 'blocked': blocked})
                if result is not None and result.ok and blocked and result.json().get('hasBooking'):
                    flash('Slot closed. It already has a booking, which stays in place.', 'success')
                else:
                    outcome(result, 'Slot closed.' if blocked else 'Slot re-opened.', 'Could not update the slot.')
            else:
                flash('Invalid slot.', 'error')

        elif action == 'cancel_slot':
            # Milestone 5: the doctor cancels a slot; every booked patient is refunded in full by the API.
            tab = 'schedule'
            slot_id, reason = request.form.get('slot_id', ''), request.form.get('reason', '').strip()
            if not slot_id or len(slot_id) > 190 or '#' not in slot_id:
                flash('Invalid slot.', 'error')
            elif len(reason) < 3:
                flash('Give patients a short reason (at least 3 characters) for cancelling.', 'error')
            else:
                result = api_request('POST', '/doctor/me/slot-cancellations', payload={'slotId': slot_id, 'reason': reason[:300]})
                if result.ok:
                    flash(api_message(result, 'Slot cancelled.'), 'success')
                else:
                    outcome(result, '', 'Could not cancel the slot.')

        elif action == 'set_status':
            # Doctor changes one appointment: Confirmed / Completed / Cancelled (cancelled = 100% refund).
            tab = 'schedule'
            appointment_id, status = request.form.get('appointment_id', ''), request.form.get('status', '')
            if not ID_PATTERN.match(appointment_id) or status not in APPOINTMENT_STATUS_CHOICES:
                flash('Choose a valid appointment status.', 'error')
            else:
                result = api_request('POST', f'/doctor/me/appointments/{appointment_id}/status',
                                     payload={'status': status})
                if result.ok:
                    flash(api_message(result, 'Appointment status updated.'), 'success')
                else:
                    outcome(result, '', 'Could not change the appointment status.')

        elif action == 'add_blackout':
            tab = 'blackouts'
            day = request.form.get('blackout_date', '')
            reason = request.form.get('reason', '').strip()
            try:
                date.fromisoformat(day)
            except ValueError:
                flash('Choose a valid date.', 'error')
            else:
                added = api_request('POST', '/doctor/me/blackouts', payload={'blackout_date': day, 'reason': reason})
                # 409 = the date was already a blackout: still run the cancellation, so adding it again is the retry.
                if added.ok or added.status_code == 409:
                    if added.ok:
                        outcome(added, 'Blackout date added.', 'Could not add the blackout date.')
                    else:
                        flash(api_message(added, f'{day} is already a blackout date.'), 'warning')
                    cancel_bookings_on(day, reason)
                else:
                    outcome(added, 'Blackout date added.', 'Could not add the blackout date.')

        elif action == 'delete_blackout':
            tab = 'blackouts'
            blackout_id = request.form.get('blackout_id', '')
            if ID_PATTERN.match(blackout_id):
                outcome(api_request('DELETE', '/doctor/me/blackouts/' + blackout_id),
                        'Blackout date removed.', 'Could not remove the blackout date.')
    except requests.RequestException:
        flash('MediSlot could not reach the API. Please try again.', 'error')
    # Come back to the same schedule view (one chosen date, or the 30-day window) after a change.
    back = {}
    for key in ('date', 'start'):
        value = request.form.get('return_' + key, '')
        try:
            back[key] = date.fromisoformat(value).isoformat()
        except ValueError:
            pass
    return redirect(url_for('dashboard', tab=tab, **back))


def cancel_bookings_on(day, reason=''):
    """After a blackout: cancel (100% refund) every booked appointment on that date. Flashes the outcome."""
    text = re.sub(r'[<>]', '', reason or '').strip()
    payload = {'date': day, 'reason': ('Doctor unavailable: ' + text)[:300] if text else
               f'The doctor is unavailable on {day}'}
    result = api_request('POST', '/doctor/me/day-cancellations', payload=payload)
    if result.ok:
        data = result.json()
        if data.get('appointmentsCancelled'):
            flash(f"{data['appointmentsCancelled']} booked appointment(s) on {day} were cancelled automatically and "
                  f"refunded in full (₹{data.get('totalRefunded')}).", 'success')
        if data.get('skippedStarted'):
            flash(f"{data['skippedStarted']} slot(s) on {day} had already started and were left as they are.", 'warning')
    elif result.status_code == 401:
        outcome(result, '', '')
    else:
        flash(api_message(result, 'The blackout is saved, but booked appointments on that date could not be '
                                  'cancelled.') + ' Add the same date again to retry.', 'warning')


def decorate_slots(slots_payload, type_labels):
    """Flatten the shared GET /doctors/{id}/slots answer (owner view) into the rows the Schedule tab shows.

    Times come back already in the doctor's timezone (localStart/localEnd); only the 12-hour format is added here.
    """
    rows = []
    for day in (slots_payload or {}).get('days', []):
        for slot in day.get('slots', []):
            rows.append({
                'slot_id': slot.get('slotId'), 'local_date': slot.get('localDate') or day.get('date'),
                'start_at': slot.get('startsAt'), 'end_at': slot.get('endsAt'),
                'consultation_type_id': slot.get('consultationTypeId'), 'status': slot.get('status'),
                'booked_count': int(slot.get('booked') or 0), 'held_count': int(slot.get('held') or 0),
                'capacity': int(slot.get('capacity') or 1), 'manual_block': bool(slot.get('manualBlock')),
                'start_local': twelve_hour(slot.get('localStart')), 'end_local': twelve_hour(slot.get('localEnd')),
                'type_label': type_labels.get(str(slot.get('consultationTypeId'))) or slot.get('label') or 'Consultation',
            })
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
    # Schedule tab: ?date=YYYY-MM-DD shows ONLY that date (the date picker); ?start=YYYY-MM-DD (or nothing) shows
    # the 30-day window starting there. Past dates work too: their slots are shown as completed / ended.
    single = bool(request.args.get('date'))
    try:
        day_obj = date.fromisoformat(request.args.get('date') or request.args.get('start') or '')
    except ValueError:
        day_obj, single = date.today(), False
    day = day_obj.isoformat()
    span = 1 if single else 30
    try:
        # The five reads do not depend on each other, so they run at the same time (one API round trip, not five).
        if session.get('refresh_token') and time.time() > session.get('token_exp', 0) - 60:
            refresh_access_token()
        token = session.get('token')
        # Slots come from the same endpoint patients use; with the doctor's own token it returns the owner view.
        doctor_id = session_user_id() or '0'
        calls = {'profile': ('/doctor/me', None), 'availability': ('/doctor/me/availability', None),
                 'schedule': (f'/doctors/{doctor_id}/slots',
                              {'from': day, 'to': (day_obj + timedelta(days=span - 1)).isoformat()}),
                 'types': ('/doctor/me/consultation-types', None), 'blackouts': ('/doctor/me/blackouts', None),
                 'appointments': ('/doctor/me/appointments',
                                  {'from': day, 'days': span, 'includeCancelled': 'true'})}
        if active == 'cancelled':     # Cancelled tab: the last 30 days and the next 30 days (the API's max window)
            today = date.today()
            calls['cancelled_past'] = ('/doctor/me/appointments', {'from': (today - timedelta(days=30)).isoformat(),
                                                                   'days': 30, 'includeCancelled': 'true'})
            calls['cancelled_next'] = ('/doctor/me/appointments', {'from': today.isoformat(), 'days': 30,
                                                                   'includeCancelled': 'true'})
        with ThreadPoolExecutor(max_workers=len(calls)) as pool:
            futures = {k: pool.submit(api_request, 'GET', path, params=params, token=token)
                       for k, (path, params) in calls.items()}
            got = {k: f.result() for k, f in futures.items()}
        profile, availability, schedule = got['profile'], got['availability'], got['schedule']
        if profile.status_code == 401:
            session.clear()
            flash('Your session has expired. Please sign in again.', 'warning')
            return redirect(url_for('login'))
        all_types = api_items(got['types'])
        catalog = got['types'].json().get('catalog', []) if got['types'].ok else []
        types = [t for t in all_types if t.get('is_active')]
        blackouts = api_items(got['blackouts'])
        appointments = api_items(got['appointments'])
        cancelled, cancelled_error = [], None
        for key in ('cancelled_past', 'cancelled_next'):
            if key in got:
                if not got[key].ok:
                    cancelled_error = api_message(got[key], 'Some cancelled appointments could not be loaded.')
                cancelled += [a for a in api_items(got[key]) if str(a.get('status', '')).startswith('CANCELLED')]
    except requests.RequestException:
        flash('Unable to load your dashboard from the API.', 'error')
        profile = availability = schedule = None
        all_types, types, blackouts, catalog, appointments = [], [], [], [], []
        cancelled, cancelled_error = [], 'MediSlot could not reach the API.'
    # Newest cancellation first; one row per appointment.
    cancelled = list({a['appointmentId']: decorate_appointment(a) for a in cancelled}.values())
    cancelled.sort(key=lambda a: str(a.get('updatedAt') or ''), reverse=True)
    cancelled_refunded = money_total(a.get('refund', {}).get('refundAmount') for a in cancelled)
    appts_by_slot = {}
    for a in appointments:            # every appointment of the slot (confirmed, completed and cancelled)
        if not str(a.get('appointmentId', '')).isdigit():
            continue
        status = a.get('status')
        appts_by_slot.setdefault(a.get('slotId'), []).append({
            'id': a['appointmentId'], 'reference': a.get('reference'), 'name': a.get('patientName') or 'Patient',
            'status': status, 'status_label': APPOINTMENT_STATUS_LABELS.get(status, status),
            'choice': 'CANCELLED' if str(status).startswith('CANCELLED') else status,
            'cancelled': str(status).startswith('CANCELLED')})

    active_ids = {t['consultation_type_id'] for t in types}
    patterns = [{'consultation_type_id': p.get('consultation_type_id'), 'day_of_week': p.get('day_of_week'),
                 'start_time': hhmm(p.get('start_time')), 'end_time': hhmm(p.get('end_time'))}
                for p in (api_items(availability) if availability is not None else [])
                if p.get('consultation_type_id') in active_ids]
    profile_data = profile.json() if profile is not None and profile.ok else {}
    type_labels = {str(t['consultation_type_id']): '%s · %s min' % (str(t.get('name', '')).replace('_', ' ').title(), t.get('duration_min'))
                   for t in all_types}
    schedule_ok = schedule is not None and schedule.ok and schedule.json().get('view') == 'owner'
    decorated = decorate_slots(schedule.json() if schedule_ok else None, type_labels)
    now_z = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
    for slot in decorated:                       # who booked it (Milestone 5) and whether it can still be cancelled
        slot['appointments'] = appts_by_slot.get(slot.get('slot_id'), [])
        slot['patients'] = [a['name'] for a in slot['appointments'] if not a['cancelled']]
        slot['future'] = str(slot.get('start_at', '')) > now_z
        slot['ended'] = bool(slot.get('end_at')) and str(slot.get('end_at')) <= now_z
        # A slot that has ended with a patient in it is "completed"; one nobody booked has simply ended.
        slot['completed'] = slot['ended'] and (int(slot.get('booked_count') or 0) > 0 or
                                               any(a['status'] in ('COMPLETED', 'NO_SHOW') for a in slot['appointments']))
    slots_by_day = {}
    for slot in decorated:
        slots_by_day.setdefault(slot.get('local_date') or day, []).append(slot)
    blackout_map = {str(b.get('blackout_date')): b.get('reason') for b in blackouts}
    # Only days that actually have slots (or are blackout days) are shown.
    schedule_days = []
    for offset in range(span):
        d = day_obj + timedelta(days=offset)
        iso = d.isoformat()
        schedule_days.append({
            'date': iso, 'weekday': d.strftime('%a'), 'daynum': d.day, 'month': d.strftime('%b'),
            'slots': slots_by_day.get(iso, []),
            'blocked': iso in blackout_map, 'reason': blackout_map.get(iso),
        })
    slot_total = sum(len(g['slots']) for g in schedule_days)
    open_total = sum(1 for g in schedule_days for s in g['slots']
                     if s.get('status') != 'CLOSED' and not s['ended']
                     and int(s.get('booked_count') or 0) < int(s.get('capacity') or 1))
    completed_total = sum(1 for g in schedule_days for s in g['slots'] if s['completed'])
    # One chosen date is always shown (even when empty); the 30-day window only lists days with slots.
    busy_days = schedule_days if single else [g for g in schedule_days if g['slots'] or g['blocked']]
    step = timedelta(days=span)
    nav_key = 'date' if single else 'start'
    return render_template(
        'doctor.html',
        tabs=TABS, active=active, day=day, busy_days=busy_days, slot_total=slot_total, open_total=open_total,
        completed_total=completed_total, types=types, blackouts=blackouts, patterns=patterns,
        profile=profile_data, single_day=single, is_past_day=day_obj < date.today(),
        day_label=day_obj.strftime('%A %d %B %Y'), nav_key=nav_key,
        status_choices=APPOINTMENT_STATUS_CHOICES, cancelled=cancelled, cancelled_error=cancelled_error,
        cancelled_refunded=cancelled_refunded,
        schedule=decorated, schedule_days=schedule_days,
        schedule_error=(schedule is not None and not schedule_ok),
        prev_day=(day_obj - step).isoformat(), next_day=(day_obj + step).isoformat(),
        catalog=catalog,
        hospitals=get_hospitals(),
        specializations=specialization_options(profile_data.get('specialization', '')),
        type_options=[{'id': t['consultation_type_id'], 'duration': t.get('duration_min'),
                       'label': '%s · %s min' % (str(t.get('name', '')).replace('_', ' ').title(), t.get('duration_min'))}
                      for t in types],
    )


# =====================================================================================================
# Patient portal (Milestone 4): register, sign in, find doctors, browse slots, manage own profile.
# Every call goes through API Gateway; the dashboard never talks to RDS or DynamoDB itself.
# =====================================================================================================
GENDER_OPTIONS = ['Female', 'Male', 'Non-binary', 'Other', 'Prefer not to say']
BLOOD_GROUP_OPTIONS = ['A+', 'A-', 'B+', 'B-', 'AB+', 'AB-', 'O+', 'O-']
PATIENT_TABS = [('find', 'Find a doctor'), ('appointments', 'My appointments'), ('cancelled', 'Cancelled'),
                ('refunds', 'Refunds'), ('profile', 'My profile')]
IDEM_PATTERN = re.compile(r'^[A-Za-z0-9_\-:.]{8,64}$')
TIER_LABELS = {'FULL': 'full refund', 'PARTIAL': 'partial refund', 'LATE': 'no refund', 'DOCTOR_CANCELLED': 'doctor cancelled'}
PHONE_PATTERN = re.compile(r'^\+?[0-9][0-9 \-]{5,22}$')
PATIENT_PROFILE_FORM = {          # form field -> API field
    'first_name': 'firstName', 'last_name': 'lastName', 'phone': 'phone', 'date_of_birth': 'dateOfBirth',
    'gender': 'gender', 'city': 'city', 'address': 'address', 'blood_group': 'bloodGroup',
    'emergency_contact_name': 'emergencyContactName', 'emergency_contact_phone': 'emergencyContactPhone',
}
SLOT_WINDOW_DAYS = 30
_filter_cache = {'at': 0.0, 'data': None}


def is_patient():
    return bool(session.get('token')) and session.get('role') == 'patient'


def public_get(path, params=None):
    """Unauthenticated GET for the public discovery endpoints."""
    return requests.get(API + path, params={k: v for k, v in (params or {}).items() if v not in (None, '')},
                        timeout=20)


def search_options():
    """Dropdown values for the search form (specialisations, cities, hospitals), cached for a minute."""
    if _filter_cache['data'] is not None and time.time() - _filter_cache['at'] < 60:
        return _filter_cache['data']
    empty = {'specializations': [], 'cities': [], 'hospitals': []}
    try:
        res = public_get('/doctors/filters')
        data = res.json() if res.ok else None
    except (requests.RequestException, ValueError):
        data = None
    if not isinstance(data, dict):
        return empty
    _filter_cache.update(at=time.time(), data=data)
    return data


def phone_ok(value):
    digits = re.sub(r'\D', '', value or '')
    return bool(PHONE_PATTERN.match((value or '').strip())) and 7 <= len(digits) <= 15


def dob_problem(value):
    try:
        born = date.fromisoformat(value)
    except (TypeError, ValueError):
        return 'Enter your date of birth.'
    if born > date.today():
        return 'Date of birth cannot be in the future.'
    if date.today().year - born.year > 120:
        return 'Check your date of birth.'
    return None


def end_temporary_login(data):
    """Revoke tokens obtained only for a background step (sign-up profile fields, wrong-portal sign-in)."""
    end_signup_login(data)


@app.route('/patient/register', methods=['GET', 'POST'])
@app.route('/patient/signup', methods=['GET', 'POST'])
def patient_register():
    if request.method == 'GET':
        return render_template('register.html', role='patient', values={}, genders=GENDER_OPTIONS,
                               blood_groups=BLOOD_GROUP_OPTIONS, cities=search_options().get('cities', []))
    form = {k: (v or '').strip() for k, v in request.form.items()}
    form['password'], form['confirm_password'] = request.form.get('password', ''), request.form.get('confirm_password', '')

    def again(message, status=400):
        flash(message, 'error')
        return render_template('register.html', role='patient', values=form, genders=GENDER_OPTIONS,
                               blood_groups=BLOOD_GROUP_OPTIONS, cities=search_options().get('cities', [])), status

    required = ['first_name', 'last_name', 'email', 'phone', 'password', 'confirm_password', 'date_of_birth',
                'gender', 'city']
    missing = [f.replace('_', ' ') for f in required if not form.get(f)]
    if missing:
        return again('Please fill in: ' + ', '.join(missing) + '.')
    if form['password'] != form['confirm_password']:
        return again('The passwords do not match.')
    if not phone_ok(form['phone']):
        return again('Enter a phone number with 7 to 15 digits.')
    if form.get('emergency_contact_phone') and not phone_ok(form['emergency_contact_phone']):
        return again('Enter an emergency contact phone number with 7 to 15 digits.')
    problem = dob_problem(form['date_of_birth'])
    if problem:
        return again(problem)
    if form['gender'] not in GENDER_OPTIONS:
        return again('Choose a gender option from the list.')
    if form.get('blood_group') and form['blood_group'] not in BLOOD_GROUP_OPTIONS:
        return again('Choose a blood group from the list.')

    email = form['email'].lower()
    try:
        created = requests.post(API + '/auth/register/patient', json={
            'email': email, 'password': form['password'], 'firstName': form['first_name'],
            'lastName': form['last_name'], 'phone': form['phone'], 'dateOfBirth': form['date_of_birth'],
            'gender': form['gender'], 'address': form.get('address', ''),
        }, timeout=20)
        if created.status_code == 409:
            return again('An account already exists for these details. Try signing in instead.', 409)
        if created.status_code != 201:
            return again(api_message(created, 'Registration failed. Please check your details.'), created.status_code)

        # Milestone 4 profile fields (city, blood group, emergency contact) are saved with PUT /patient/me,
        # using a short-lived sign-in that is revoked straight away. The patient then signs in themselves.
        extra = {api: form.get(field) or None for field, api in PATIENT_PROFILE_FORM.items()
                 if field in ('city', 'blood_group', 'emergency_contact_name', 'emergency_contact_phone')}
        logged_in = requests.post(API + '/auth/login', json={'email': email, 'password': form['password']}, timeout=20)
        if logged_in.ok and logged_in.json().get('role') == 'patient':
            data = logged_in.json()
            try:
                saved = api_request('PUT', '/patient/me', payload={k: v for k, v in extra.items() if v},
                                    token=data['accessToken'])
                if not saved.ok:
                    flash('Your account was created, but some profile details were not saved: '
                          + api_message(saved, 'please add them under My profile.'), 'warning')
            finally:
                end_temporary_login(data)
        else:
            flash('Your account was created. Add your city under My profile after signing in.', 'warning')
    except requests.RequestException:
        return again('MediSlot could not reach the API. Please try again in a moment.', 502)
    session.clear()
    flash('Your patient account is ready. Please sign in.', 'success')
    return redirect(url_for('patient_login', email=email))


@app.route('/patient/login', methods=['GET', 'POST'])
def patient_login():
    if request.method == 'GET' and is_patient():
        return redirect(url_for('patient_dashboard'))
    if request.method == 'POST':
        try:
            res = requests.post(API + '/auth/login', json={'email': request.form.get('email', '').strip(),
                                                           'password': request.form.get('password', '')}, timeout=20)
            data = res.json() if res.ok else {}
            if res.ok and data.get('role') == 'patient':
                session.clear()
                store_login(data, 'patient')
                return redirect(url_for('patient_dashboard'))
            if res.ok and data.get('role') == 'doctor':
                end_temporary_login(data)
                flash('That is a doctor account. Please use doctor sign in.', 'warning')
            else:
                flash('We could not sign you in. Check your email and password.', 'error')
        except (requests.RequestException, ValueError):
            flash('MediSlot could not reach the API. Please try again.', 'error')
    email = request.form.get('email') or request.args.get('email', '')
    return render_template('login.html', role='patient', email=email.strip()[:254])


@app.route('/patient/logout')
def patient_logout():
    if session.get('token'):
        try:
            requests.post(API + '/auth/logout', json={'refreshToken': session.get('refresh_token')},
                          headers={'Authorization': 'Bearer ' + session['token']}, timeout=10)
        except requests.RequestException:
            pass
    session.clear()
    flash('You have signed out.', 'success')
    return redirect(url_for('home'))


def search_state():
    """Read the search form from the query string. On the very first visit the patient's own city is used."""
    args = request.args
    first_visit = 'searched' not in args
    return {
        'name': args.get('name', '').strip()[:75],
        'specialization': args.get('specialization', '').strip()[:120],
        'hospital_id': args.get('hospital_id', '').strip() if args.get('hospital_id', '').isdigit() else '',
        'city': args.get('city', '').strip()[:100],
        'cursor': args.get('cursor', '').strip()[:600],
        'trail': [t for t in args.get('trail', '').split(',') if t][:50],
        'first_visit': first_visit,
    }


def page_links(state, next_cursor):
    base = {k: state[k] for k in ('name', 'specialization', 'hospital_id', 'city') if state[k]}
    base.update(tab='find', searched='1')
    nxt = prev = None
    if next_cursor:
        nxt = url_for('patient_dashboard', **base, cursor=next_cursor,
                      trail=','.join(state['trail'] + [state['cursor'] or '-']))
    if state['trail']:
        back = state['trail'][-1]
        prev = url_for('patient_dashboard', **base, **({'cursor': back} if back != '-' else {}),
                       trail=','.join(state['trail'][:-1]))
    return prev, nxt


@app.route('/patient/dashboard', methods=['GET', 'POST'])
def patient_dashboard():
    if not is_patient():
        return redirect(url_for('patient_login'))
    if request.method == 'POST':
        return handle_patient_post()
    tab = request.args.get('tab', 'find')
    if tab not in {k for k, _ in PATIENT_TABS}:
        tab = 'find'
    profile, results, search_error = {}, None, None
    booking = {'upcoming': [], 'history': [], 'refunds': None, 'error': None}
    state = search_state()
    try:
        me = api_request('GET', '/patient/me')
        if me.status_code in (401, 403):
            session.clear()
            flash('Your session has expired. Please sign in again.', 'warning')
            return redirect(url_for('patient_login'))
        if me.ok:
            profile = me.json()
        else:
            flash(api_message(me, 'Your profile could not be loaded.'), 'warning')
        if state['first_visit'] and profile.get('city'):
            state['city'] = profile['city']
        if tab == 'cancelled':              # top-bar "Cancelled": every cancelled appointment with its refund
            booking = load_booking_tab('appointments', 'cancelled')
            booking['refunded'] = money_total((a.get('refund') or {}).get('refundAmount') for a in booking['history'])
        elif tab in ('appointments', 'refunds'):
            history_filter = request.args.get('history', 'all')
            if history_filter not in {k for k, _ in HISTORY_FILTERS}:
                history_filter = 'all'
            booking = load_booking_tab(tab, history_filter)
        else:
            params = {'name': state['name'], 'specialization': state['specialization'],
                      'hospitalId': state['hospital_id'], 'city': state['city'], 'cursor': state['cursor'], 'limit': 8}
            res = public_get('/doctors', params)
            if res.ok:
                results = res.json()
            else:
                search_error = api_message(res, 'Search is unavailable right now.')
    except requests.RequestException:
        search_error = 'MediSlot could not reach the API. Please try again.'
    prev_link, next_link = page_links(state, (results or {}).get('page', {}).get('nextCursor'))
    return render_template('patient.html', view='find', tabs=PATIENT_TABS, active=tab, profile=profile, state=state,
                           results=results, search_error=search_error, options=search_options(),
                           prev_link=prev_link, next_link=next_link, page_no=len(state['trail']) + 1,
                           genders=GENDER_OPTIONS, blood_groups=BLOOD_GROUP_OPTIONS, booking=booking,
                           tier_labels=TIER_LABELS)


# ----------------------------------------------------------------------------- Milestone 5: booking pages

def local_label(iso_value, tz_name, fmt='%a %d %b, %I:%M %p'):
    """'2026-11-21T03:30:00Z' -> 'Sat 21 Nov, 9:00 AM' in the appointment's own timezone."""
    if not iso_value:
        return ''
    try:
        tz = ZoneInfo(tz_name or 'Asia/Kolkata')
    except Exception:
        tz = ZoneInfo('Asia/Kolkata')
    try:
        when = datetime.fromisoformat(str(iso_value).replace('Z', '+00:00')).astimezone(tz)
    except ValueError:
        return str(iso_value)
    return when.strftime(fmt).replace(' 0', ' ')


def decorate_appointment(a):
    tz = a.get('timezone')
    a['when'] = local_label(a.get('startsAt'), tz, '%a %d %b %Y, %I:%M %p')
    a['until'] = local_label(a.get('endsAt'), tz, '%I:%M %p')
    for step in (a.get('cancellation') or {}).get('schedule', []):
        step['by_label'] = local_label(step.get('cancelBy') or step.get('cancelBefore'), tz)
    return a


HISTORY_FILTERS = [('all', 'All history'), ('past', 'Past'), ('cancelled', 'Cancelled')]


def load_booking_tab(tab, history_filter='all'):
    """Data for the My appointments / Refunds tabs. Every read goes through API Gateway.

    history_filter: 'all' (past + cancelled), 'past' (seen / completed) or 'cancelled' (by the patient or doctor).
    """
    out = {'upcoming': [], 'history': [], 'refunds': None, 'error': None, 'filter': history_filter,
           'filters': HISTORY_FILTERS, 'counts': {}}
    if tab == 'appointments':
        views = {'upcoming': 'upcoming', 'all': 'all', 'past': 'past', 'cancelled': 'cancelled'}
        wanted = ['upcoming', 'past', 'cancelled']          # one call each, so every filter chip has its count
        with ThreadPoolExecutor(max_workers=len(wanted)) as pool:
            token = session.get('token')
            futures = {v: pool.submit(api_request, 'GET', '/patient/appointments',
                                      params={'view': views[v], 'limit': 50}, token=token) for v in wanted}
            got = {v: f.result() for v, f in futures.items()}
        upcoming = got['upcoming']
        if upcoming.ok:
            out['upcoming'] = [decorate_appointment(a) for a in upcoming.json().get('items', [])]
            live_ids = {a['appointmentId'] for a in out['upcoming']}
            lists = {v: [decorate_appointment(a) for a in api_items(got[v]) if a['appointmentId'] not in live_ids]
                     for v in ('past', 'cancelled')}
            for v in ('past', 'cancelled'):
                if not got[v].ok:
                    out['error'] = api_message(got[v], 'Part of your appointment history could not be loaded.')
            if history_filter == 'all':
                merged = lists['past'] + lists['cancelled']
                merged.sort(key=lambda a: str(a.get('startsAt') or ''), reverse=True)     # newest first
                out['history'] = merged
            else:
                out['history'] = lists[history_filter]
            out['counts'] = {'past': len(lists['past']), 'cancelled': len(lists['cancelled']),
                             'all': len(lists['past']) + len(lists['cancelled'])}
        else:
            out['error'] = api_message(upcoming, 'Your appointments could not be loaded.')
    else:
        res = api_request('GET', '/patient/refunds')
        if res.ok:
            out['refunds'] = res.json()
            for r in out['refunds'].get('items', []):
                r['when'] = local_label(r.get('appointmentStartsAt'), r.get('timezone'), '%d %b %Y, %I:%M %p')
                r['recorded'] = local_label(r.get('recordedAt'), r.get('timezone'), '%d %b %Y, %I:%M %p')
        else:
            out['error'] = api_message(res, 'Your refunds could not be loaded.')
    return out


def api_code(result):
    try:
        return result.json().get('code')
    except (ValueError, AttributeError):
        return None


def handle_booking_post(action):
    """book / cancel_appointment. Returns a redirect."""
    if action == 'book':
        slot_id, key = request.form.get('slot_id', ''), request.form.get('idem_key', '')
        doctor_id = request.form.get('doctor_id', '')
        back = url_for('patient_doctor', doctor_id=int(doctor_id)) if ID_PATTERN.match(doctor_id) else url_for('patient_dashboard')
        if not slot_id or len(slot_id) > 190 or '#' not in slot_id or not IDEM_PATTERN.match(key):
            flash('That booking form is no longer valid. Please choose the slot again.', 'error')
            return redirect(back)
        payload = {'slotId': slot_id}
        # The key was generated when the page was drawn: pressing Book twice (or Back + Book) sends the SAME key,
        # so the API returns the same appointment instead of booking a second seat.
        result = api_request('POST', '/patient/appointments', payload=payload, headers={'Idempotency-Key': key})
        if result.status_code == 201:
            data = result.json()
            message = (f"Booked with {data['doctor']['name']} on "
                       f"{local_label(data['startsAt'], data.get('timezone'), '%a %d %b, %I:%M %p')}. "
                       f"Reference {data['reference']}.")
            if data.get('replayed'):
                message += ' (That was the same booking sent twice - you have one appointment, not two.)'
            flash(message, 'success')
            return redirect(url_for('patient_dashboard', tab='appointments'))
        if result.status_code == 401:
            session.clear()
            flash('Your session has expired. Please sign in again.', 'warning')
            return redirect(url_for('patient_login'))
        if api_code(result) == 'SLOT_FULL':
            flash('Someone booked the last seat a moment before you. Please pick another slot.', 'warning')
        else:
            flash(api_message(result, 'The booking did not go through.'), 'error')
        return redirect(back)

    if action == 'cancel_appointment':
        appointment_id = request.form.get('appointment_id', '')
        if not ID_PATTERN.match(appointment_id):
            flash('Unknown appointment.', 'error')
        else:
            reason = re.sub(r'\s+', ' ', request.form.get('reason', '')).strip()[:300]
            result = api_request('POST', f'/patient/appointments/{appointment_id}/cancel',
                                 payload={'reason': reason} if reason else {})
            if result.ok:
                data = result.json()
                refund = data.get('refund') or {}
                if data.get('alreadyCancelled'):
                    flash(f"{data['reference']} was already cancelled.", 'warning')
                else:
                    flash(f"{data['reference']} cancelled. Refund: ₹{refund.get('refundAmount')} "
                          f"({refund.get('refundPercent')}% - {TIER_LABELS.get(refund.get('tier'), refund.get('tier'))}).",
                          'success')
            else:
                flash(api_message(result, 'Could not cancel the appointment.'), 'error')
        return redirect(url_for('patient_dashboard', tab='appointments'))

    return redirect(url_for('patient_dashboard'))


def handle_patient_post():
    action = request.form.get('action', '')
    try:
        if action in ('book', 'cancel_appointment'):
            return handle_booking_post(action)
        if action == 'profile':
            payload = {api: request.form.get(field, '').strip() for field, api in PATIENT_PROFILE_FORM.items()
                       if field in request.form}
            if request.form.get('version', '').isdigit():
                payload['version'] = int(request.form['version'])
            result = api_request('PUT', '/patient/me', payload=payload)
            if result.ok:
                flash('Profile saved.', 'success')
            elif result.status_code == 412:
                flash('Your profile was changed in another tab or device. The latest version is shown below; '
                      'make your change again and save.', 'warning')
            elif result.status_code == 401:
                session.clear()
                flash('Your session has expired. Please sign in again.', 'warning')
                return redirect(url_for('patient_login'))
            else:
                flash(api_message(result, 'Could not save your profile.'), 'error')
            return redirect(url_for('patient_dashboard', tab='profile'))
        if action == 'delete_account':
            if request.form.get('confirm', '').strip() != 'DELETE':
                flash('Type DELETE in capitals to confirm account deletion.', 'error')
                return redirect(url_for('patient_dashboard', tab='profile'))
            result = api_request('DELETE', '/patient/me',
                                 payload={'confirm': 'DELETE', 'refreshToken': session.get('refresh_token')})
            if result.ok:
                session.clear()
                flash('Your account has been deleted. We are sorry to see you go.', 'success')
                return redirect(url_for('home'))
            flash(api_message(result, 'Could not delete your account.'), 'error')
            return redirect(url_for('patient_dashboard', tab='profile'))
    except requests.RequestException:
        flash('MediSlot could not reach the API. Please try again.', 'error')
    return redirect(url_for('patient_dashboard'))


def twelve_hour(hhmm_value):
    try:
        return datetime.strptime(hhmm_value, '%H:%M').strftime('%I:%M %p').lstrip('0')
    except (TypeError, ValueError):
        return hhmm_value


@app.get('/patient/doctors/<int:doctor_id>')
def patient_doctor(doctor_id):
    if not is_patient():
        return redirect(url_for('patient_login'))
    try:
        res = public_get(f'/doctors/{doctor_id}')
        if res.status_code == 404:
            flash('That doctor is no longer available.', 'warning')
            return redirect(url_for('patient_dashboard'))
        if not res.ok:
            flash(api_message(res, 'Could not load the doctor profile.'), 'error')
            return redirect(url_for('patient_dashboard'))
        doctor = res.json()
        try:
            tz = ZoneInfo(doctor.get('timezone') or 'Asia/Kolkata')
        except Exception:
            tz = ZoneInfo('Asia/Kolkata')
        today = datetime.now(tz).date()
        try:
            start = date.fromisoformat(request.args.get('from', ''))
        except ValueError:
            start = today
        start = min(max(start, today), today + timedelta(days=SLOT_WINDOW_DAYS - 1))
        end = min(start + timedelta(days=6), today + timedelta(days=SLOT_WINDOW_DAYS - 1))
        type_id = request.args.get('type', '')
        if type_id and type_id not in {str(t['consultationTypeId']) for t in doctor.get('consultationTypes', [])}:
            type_id = ''
        only_free = request.args.get('free') == '1'
        slots_res = public_get(f'/doctors/{doctor_id}/slots', {
            'from': start.isoformat(), 'to': end.isoformat(), 'consultationTypeId': type_id,
            'includeFull': 'false' if only_free else 'true'})
        slots = slots_res.json() if slots_res.ok else None
        slot_error = None if slots_res.ok else api_message(slots_res, 'Slots could not be loaded.')
    except requests.RequestException:
        flash('MediSlot could not reach the API. Please try again.', 'error')
        return redirect(url_for('patient_dashboard'))
    for day in (slots or {}).get('days', []):
        day['label'] = date.fromisoformat(day['date']).strftime('%a %d %b')
        for slot in day['slots']:
            slot['start12'], slot['end12'] = twelve_hour(slot['localStart']), twelve_hour(slot['localEnd'])
            slot['idem_key'] = 'web-' + uuid.uuid4().hex     # one key per Book button (Milestone 5 idempotency)
    nav = {'type': type_id or None, 'free': '1' if only_free else None}
    prev_week = (url_for('patient_doctor', doctor_id=doctor_id, **{k: v for k, v in nav.items() if v},
                         **{'from': max(today, start - timedelta(days=7)).isoformat()}) if start > today else None)
    last_day = today + timedelta(days=SLOT_WINDOW_DAYS - 1)
    next_week = (url_for('patient_doctor', doctor_id=doctor_id, **{k: v for k, v in nav.items() if v},
                         **{'from': (start + timedelta(days=7)).isoformat()}) if end < last_day else None)
    return render_template('patient.html', view='doctor', doctor=doctor, slots=slots, slot_error=slot_error,
                           start=start, end=end, type_id=type_id, only_free=only_free,
                           prev_week=prev_week, next_week=next_week,
                           next_open_time=twelve_hour((doctor.get('nextOpenSlot') or {}).get('localStart')))


@app.errorhandler(413)
def file_too_large(_error):
    return render_signup({}, 413)
    #