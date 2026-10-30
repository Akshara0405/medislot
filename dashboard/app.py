import os,requests
from flask import Flask,render_template,request,redirect,session,url_for,flash
app=Flask(__name__);app.secret_key=os.environ['FLASK_SECRET_KEY'];app.config.update(SESSION_COOKIE_HTTPONLY=True,SESSION_COOKIE_SECURE=os.environ.get("COOKIE_SECURE", "false").lower() == "true",SESSION_COOKIE_SAMESITE="Lax")
API=os.environ['API_BASE_URL'].rstrip('/')
def call(method,path,payload=None,params=None):
    return requests.request(method,API+path,headers={'Authorization':'Bearer '+session['token']},json=payload,params=params,timeout=15)
@app.route('/doctor/login',methods=['GET','POST'])
def login():
    if request.method=='POST':
        r=requests.post(API+'/auth/login',json={'email':request.form['email'],'password':request.form['password']},timeout=15)
        if r.ok and r.json().get('role')=='doctor':session['token']=r.json()['accessToken'];return redirect('/doctor/dashboard')
        flash('Doctor login failed')
    return render_template('login.html')
@app.route('/doctor/logout')
def logout():session.clear();return redirect('/doctor/login')
@app.route('/doctor/dashboard',methods=['GET','POST'])
def dashboard():
    if 'token' not in session:return redirect('/doctor/login')
    if request.method=='POST':
        if request.form.get('action')=='profile':
            payload={k:request.form[k] for k in ('first_name','last_name','phone','specialization','qualification','timezone','languages') if k in request.form}
            r=call('PUT','/doctor/me',payload)
        else:
            try:patterns=__import__('json').loads(request.form['patterns'])
            except ValueError:patterns=None
            r=call('PUT','/doctor/me/availability',{'patterns':patterns}) if patterns is not None else None
        flash('Saved' if r is not None and r.ok else ('Invalid JSON' if r is None else r.text[:200]))
        return redirect('/doctor/dashboard')
    profile=call('GET','/doctor/me');availability=call('GET','/doctor/me/availability')
    day=request.args.get('date') or __import__('datetime').date.today().isoformat()
    schedule=call('GET','/doctor/me/schedule',params={'date':day})
    if profile.status_code==401:session.clear();return redirect('/doctor/login')
    return render_template('dashboard.html',profile=profile.json() if profile.ok else {},patterns=availability.json().get('items',[]) if availability.ok else [],schedule=schedule.json().get('slots',[]) if schedule.ok else [],day=day)
