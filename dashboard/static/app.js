/* MediSlot dashboard scripts (served as /static/app.js). Each part only runs on the page that has its elements. */
(function () {
  'use strict';
  var $ = function (id) { return document.getElementById(id); };
  var DAYS = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'];

  /* ------------------------------------------------ registration forms (doctor + patient) */
  var form = $('register-form');
  if (form) {
    var pw = $('password'), pw2 = $('confirm_password'), btn = $('submit-btn'), dob = $('date_of_birth');
    var photo = $('photo'), preview = $('photo-preview');
    if (dob) dob.max = new Date().toISOString().slice(0, 10);
    var check = function () {
      pw2.setCustomValidity(pw2.value && pw.value !== pw2.value ? 'Passwords do not match.' : '');
      pw.setCustomValidity(pw.value && !(/[A-Za-z]/.test(pw.value) && /\d/.test(pw.value)) ? 'Include at least one letter and one number.' : '');
    };
    pw.addEventListener('input', check);
    pw2.addEventListener('input', check);
    if (photo) {
      photo.addEventListener('change', function () {
        var file = photo.files[0];
        preview.textContent = '＋';
        if (!file) return;
        if (file.size > 5 * 1024 * 1024) { photo.value = ''; alert('Please choose an image smaller than 5 MB.'); return; }
        var img = document.createElement('img');
        img.src = URL.createObjectURL(file);
        preview.textContent = '';
        preview.appendChild(img);
      });
    }
    form.addEventListener('submit', function (e) {
      check();
      if (!form.checkValidity()) { e.preventDefault(); form.reportValidity(); return; }
      btn.textContent = 'Creating your account…';
      btn.disabled = true;
    });
  }

  /* ------------------------------------------------ doctor workspace: tabs */
  var tabs = document.querySelectorAll('.tab[data-tab]'), panels = document.querySelectorAll('.panel');
  tabs.forEach(function (t) {
    t.addEventListener('click', function (e) {
      e.preventDefault();
      tabs.forEach(function (x) { var on = x === t; x.classList.toggle('active', on); x.setAttribute('aria-selected', on); });
      panels.forEach(function (p) { p.classList.toggle('active', p.id === 'panel-' + t.dataset.tab); });
      var q = new URLSearchParams(location.search); q.set('tab', t.dataset.tab);
      history.replaceState(null, '', '?' + q.toString());
    });
  });

  /* ------------------------------------------------ doctor workspace: "Add a consultation type" capacity box */
  var tName = $('t-name'), tCap = $('t-cap');
  if (tName && tCap) {
    var syncCap = function () {
      var opt = tName.options[tName.selectedIndex], min = +opt.dataset.min, max = +opt.dataset.max;
      tCap.min = min; tCap.max = max;
      tCap.disabled = min === max;    // normal consultations: always 1 seat (a disabled box is not sent; the API uses 1)
      if (+tCap.value < min || +tCap.value > max) tCap.value = min;
    };
    tName.addEventListener('change', syncCap);
    syncCap();
  }

  /* ------------------------------------------------ doctor workspace: weekly availability editor */
  var rowsEl = $('avail-rows');
  if (!rowsEl) return;
  var data = JSON.parse($('dash-data').textContent);        // {patterns: [...], types: [{id, label, duration}]}
  var typeById = {};
  data.types.forEach(function (t) { typeById[t.id] = t; });

  function option(value, text, selected) {
    var o = document.createElement('option'); o.value = value; o.textContent = text; o.selected = !!selected; return o;
  }
  function toMin(t) { var a = t.split(':'); return +a[0] * 60 + +a[1]; }
  function hhmm(m) { return ('0' + Math.floor(m / 60)).slice(-2) + ':' + ('0' + (m % 60)).slice(-2); }

  function addRow(p) {
    p = p || {};
    var row = document.createElement('div'); row.className = 'slot-row inline';
    var day = document.createElement('select'); day.className = 'f-day';
    DAYS.forEach(function (d, i) { day.appendChild(option(i, d, i === p.day_of_week)); });
    var type = document.createElement('select'); type.className = 'f-type';
    data.types.forEach(function (t) { type.appendChild(option(t.id, t.label, t.id === p.consultation_type_id)); });
    var start = document.createElement('input'); start.type = 'time'; start.className = 'f-start'; start.value = p.start_time || '09:00'; start.required = true;
    var end = document.createElement('input'); end.type = 'time'; end.className = 'f-end'; end.value = p.end_time || '13:00'; end.required = true;
    var preview = document.createElement('span'); preview.className = 'slot-preview';
    var remove = document.createElement('button'); remove.type = 'button'; remove.className = 'danger'; remove.textContent = 'Remove';
    remove.addEventListener('click', function () { row.remove(); });
    [day, type, start, end, preview, remove].forEach(function (el) { row.appendChild(el); });

    function refresh() {        // "8 slots × 30 min" next to the row
      var t = typeById[type.value];
      if (!t || !start.value || !end.value) { preview.textContent = ''; return; }
      var n = Math.floor((toMin(end.value) - toMin(start.value)) / t.duration);
      preview.className = 'slot-preview' + (n < 1 ? ' bad' : '');
      preview.textContent = n < 1 ? 'Too short for ' + t.duration + ' min' : n + ' slot' + (n === 1 ? '' : 's') + ' × ' + t.duration + ' min';
    }
    [type, start, end].forEach(function (el) { el.addEventListener('change', refresh); el.addEventListener('input', refresh); });
    rowsEl.appendChild(row);
    refresh();
  }

  function lastSlotEnd(p) {   // 09:00-09:34 with a 15-minute type really ends at 09:30
    var t = typeById[p.consultation_type_id], s = toMin(p.start_time), e = toMin(p.end_time);
    return t ? s + Math.floor((e - s) / t.duration) * t.duration : e;
  }

  function findProblem(list) {  // same rules as the API: no duplicate rows, no overlapping rows
    var name = function (id) { return (typeById[id] || {}).label.split(' · ')[0]; };
    var seen = {};
    for (var k = 0; k < list.length; k++) {
      var q = list[k], key = [q.day_of_week, q.consultation_type_id, q.start_time, q.end_time].join('|');
      if (seen[key]) return 'Pattern already exists: ' + name(q.consultation_type_id) + ' ' + q.start_time + '-' + q.end_time + ' on ' + DAYS[q.day_of_week] + '. Remove the duplicate row; nothing was saved.';
      seen[key] = true;
    }
    var byDay = {};
    list.forEach(function (p) { (byDay[p.day_of_week] = byDay[p.day_of_week] || []).push(p); });
    for (var d in byDay) {
      var r = byDay[d].slice().sort(function (a, b) { return toMin(a.start_time) - toMin(b.start_time); });
      var prev = r[0], prevEnd = lastSlotEnd(prev);
      for (var i = 1; i < r.length; i++) {
        if (toMin(r[i].start_time) < prevEnd) {
          return 'Overlapping availability on ' + DAYS[d] + ': ' + name(prev.consultation_type_id) + ' ' + prev.start_time + '-' + hhmm(prevEnd) +
                 ' clashes with ' + name(r[i].consultation_type_id) + ' ' + r[i].start_time + '-' + hhmm(lastSlotEnd(r[i])) + '. Change one of the times; nothing was saved.';
        }
        if (lastSlotEnd(r[i]) > prevEnd) { prev = r[i]; prevEnd = lastSlotEnd(r[i]); }
      }
    }
    return '';
  }

  data.patterns.forEach(addRow);
  $('avail-add').addEventListener('click', function () { addRow(); });
  $('avail-form').addEventListener('submit', function (e) {
    var out = [];
    rowsEl.querySelectorAll('.slot-row').forEach(function (r) {
      out.push({ day_of_week: +r.querySelector('.f-day').value, consultation_type_id: +r.querySelector('.f-type').value,
                 start_time: r.querySelector('.f-start').value, end_time: r.querySelector('.f-end').value });
    });
    var err = $('avail-error'), problem = findProblem(out);
    err.hidden = !problem;
    err.textContent = problem;
    if (problem) { e.preventDefault(); return; }
    $('avail-json').value = JSON.stringify(out);
  });
})();