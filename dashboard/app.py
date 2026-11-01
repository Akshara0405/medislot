import os
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
        return render_template('signup.html', values={})

    photo = request.files.get('photo')
    required = ['first_name', 'last_name', 'email', 'phone', 'password', 'confirm_password',
                'specialization', 'license_number', 'hospital_id', 'qualification', 'experience_years']
    missing = [field for field in required if not request.form.get(field, '').strip()]
    if missing or not photo or not photo.filename:
        flash('Complete every required field and choose a profile photo.', 'error')
        return render_template('signup.html', values=request.form), 400
    if request.form['password'] != request.form['confirm_password']:
        flash('The passwords do not match.', 'error')
        return render_template('signup.html', values=request.form), 400
    if photo.mimetype not in ALLOWED_PHOTO_TYPES:
        flash('Upload a JPG, PNG or WEBP profile photo.', 'error')
        return render_template('signup.html', values=request.form), 400
    try:
        experience = int(request.form['experience_years'])
        hospital_id = int(request.form['hospital_id'])
        if not 0 <= experience <= 80 or hospital_id <= 0:
            raise ValueError()
    except ValueError:
        flash('Enter a valid hospital ID and experience between 0 and 80 years.', 'error')
        return render_template('signup.html', values=request.form), 400

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
            return render_template('signup.html', values=request.form), registered.status_code

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
        return render_template('signup.html', values=request.form), 502


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


@app.route('/doctor/dashboard', methods=['GET', 'POST'])
def dashboard():
    if 'token' not in session or session.get('role') != 'doctor':
        return redirect(url_for('login'))
    if request.method == 'POST':
        if request.form.get('action') == 'profile':
            payload = {k: request.form[k] for k in (
                'first_name', 'last_name', 'phone', 'specialization', 'qualification', 'timezone', 'languages'
            ) if k in request.form}
            result = api_request('PUT', '/doctor/me', payload=payload)
        else:
            try:
                patterns = __import__('json').loads(request.form['patterns'])
                result = api_request('PUT', '/doctor/me/availability', payload={'patterns': patterns})
            except (ValueError, KeyError):
                result = None
        flash('Changes saved.' if result is not None and result.ok else
              ('Availability JSON is invalid.' if result is None else api_message(result, 'Could not save changes.')),
              'success' if result is not None and result.ok else 'error')
        return redirect(url_for('dashboard'))

    try:
        profile = api_request('GET', '/doctor/me')
        availability = api_request('GET', '/doctor/me/availability')
        day = request.args.get('date') or __import__('datetime').date.today().isoformat()
        schedule = api_request('GET', '/doctor/me/schedule', params={'date': day})
        if profile.status_code == 401:
            session.clear()
            return redirect(url_for('login'))
        return render_template(
            'dashboard.html',
            profile=profile.json() if profile.ok else {},
            patterns=availability.json().get('items', []) if availability.ok else [],
            schedule=schedule.json().get('slots', []) if schedule.ok else [],
            day=day,
        )
    except requests.RequestException:
        flash('Unable to load your dashboard from the API.', 'error')
        return render_template('dashboard.html', profile={}, patterns=[], schedule=[], day='')


@app.errorhandler(413)
def file_too_large(_error):
    return render_template('signup.html', values={}), 413