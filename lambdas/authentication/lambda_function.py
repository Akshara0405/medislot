import json
import os
import re
import secrets
import time
import uuid

import bcrypt
import jwt
from botocore.exceptions import ClientError

from common import auth_table, body, config, correlation, dbconn, emit, get_param, response, token_hash


def handler(event, context):
    event = event or {}
    # A direct invocation used by the deployment pipeline to verify private RDS connectivity.
    if event.get("action") == "health":
        conn = None
        try:
            c = config()
            conn = dbconn(c)
            with conn.cursor() as cur:
                cur.execute("SELECT 1 AS connected")
                row = cur.fetchone()
            return {"statusCode": 200, "body": {"connected": bool(row and row["connected"] == 1)}}
        finally:
            if conn:
                conn.close()

    cid = correlation(event)
    path = str(event.get("path", "")).rstrip("/").lower()
    method = str(event.get("httpMethod", "")).upper()
    data = body(event)
    if not isinstance(data, dict):
        data = {}

    try:
        if method == "POST" and path.endswith("/auth/register/patient"):
            return register(data, "patient", cid)
        if method == "POST" and path.endswith("/auth/register/doctor"):
            return register(data, "doctor", cid)
        if method == "POST" and path.endswith("/auth/login"):
            return login(data, cid)
        if method == "POST" and path.endswith("/auth/refresh"):
            return refresh(data, cid)
        if method == "POST" and path.endswith("/auth/logout"):
            return logout(event, data, cid)
        if method == "GET" and (path.endswith("/patient/me") or path.endswith("/doctor/me")):
            ctx = ((event.get("requestContext") or {}).get("authorizer") or {})
            return response(200, {"userId": ctx.get("userId"), "role": ctx.get("role"), "message": "Authorization successful"})
        return response(404, {"message": "Route not found", "correlationId": cid})
    except Exception:
        emit("error", "authentication_request_failed", cid, route=path)
        return response(500, {"message": "Internal server error", "correlationId": cid})


def register(data, role, cid):
    email = str(data.get("email", "")).strip().lower()
    password = data.get("password", "")
    first_name = str(data.get("firstName", "")).strip()
    last_name = str(data.get("lastName", "")).strip()
    phone = str(data.get("phone", "")).strip()
    c = config()
    minimum = int(get_param(f"{c['prefix']}/auth/password-min-length"))
    if (not re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", email) or len(email) > 254
            or not first_name or len(first_name) > 75 or not last_name or len(last_name) > 75
            or not phone or len(phone) > 25):
        return response(400, {"message": "Valid email, firstName, lastName, and phone are required", "correlationId": cid})
    if (not isinstance(password, str) or len(password) < minimum or len(password.encode()) > 72
            or not re.search(r"[A-Za-z]", password) or not re.search(r"\d", password)):
        return response(400, {"message": f"Password must be at least {minimum} characters and include a letter and a digit", "correlationId": cid})

    if role == "patient":
        date_of_birth = data.get("dateOfBirth") or None
        gender = str(data.get("gender", "")).strip() or None
        address = str(data.get("address", "")).strip() or None
        if gender and len(gender) > 30:
            return response(400, {"message": "gender must be at most 30 characters", "correlationId": cid})
        if address and len(address) > 500:
            return response(400, {"message": "address must be at most 500 characters", "correlationId": cid})
    else:
        specialization = str(data.get("specialization", "")).strip()
        license_number = str(data.get("licenseNumber", "")).strip().upper()
        hospital_id = data.get("hospitalId")
        qualification = str(data.get("qualification", "")).strip()
        experience = data.get("experienceYears", 0)
        if not re.fullmatch(r"[A-Z]{2}-\d{6}", license_number):
            return response(400, {"message": "licenseNumber must match the format ST-123456 (two-letter state code, hyphen, six digits)", "correlationId": cid})
        if not specialization or len(specialization) > 120 or not qualification or len(qualification) > 150:
            return response(400, {"message": "specialization and qualification are required", "correlationId": cid})
        try:
            hospital_id = int(hospital_id)
            experience = int(experience)
            if hospital_id <= 0 or experience < 0 or experience > 65535:
                raise ValueError()
        except (TypeError, ValueError):
            return response(400, {"message": "A valid existing hospitalId and non-negative experienceYears are required", "correlationId": cid})

    hashed = bcrypt.hashpw(password.encode(), bcrypt.gensalt(rounds=12)).decode()
    conn = dbconn(c)
    try:
        with conn.cursor() as cur:
            if role == "doctor":
                cur.execute("SELECT hospital_id FROM hospitals WHERE hospital_id=%s AND is_active=TRUE", (hospital_id,))
                if not cur.fetchone():
                    conn.rollback()
                    return response(400, {"message": "hospitalId must reference an existing active hospital", "correlationId": cid})
            cur.execute("INSERT INTO users(email,password_hash,role,full_name) VALUES(%s,%s,%s,NULL)", (email, hashed, role))
            user_id = int(cur.lastrowid)
            if role == "patient":
                cur.execute("""INSERT INTO patients(user_id,first_name,last_name,phone,date_of_birth,gender,address)
                               VALUES(%s,%s,%s,%s,%s,%s,%s)""",
                            (user_id, first_name, last_name, phone, date_of_birth, gender, address))
            else:
                cur.execute("""INSERT INTO doctors(user_id,first_name,last_name,phone,specialization,license_number,
                               hospital_id,qualification,experience_years) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                            (user_id, first_name, last_name, phone, specialization, license_number,
                             hospital_id, qualification, experience))
        conn.commit()
    except Exception as exc:
        conn.rollback()
        if "Duplicate entry" in str(exc):
            return response(409, {"message": "Unable to register with those details", "correlationId": cid})
        raise
    finally:
        conn.close()
    emit("info", "registration_succeeded", cid, role=role, userId=str(user_id))
    return response(201, {"userId": str(user_id), "role": role, "message": "Registration successful"})


def issue_access_token(user_id, role, secret, minutes):
    now = int(time.time())
    claims = {"sub": str(user_id), "role": role, "iat": now, "exp": now + minutes * 60, "jti": str(uuid.uuid4())}
    return jwt.encode(claims, secret, algorithm="HS256"), now, claims


def login(data, cid):
    email = str(data.get("email", "")).strip().lower()
    password = data.get("password", "")
    if not email or not isinstance(password, str):
        return response(400, {"message": "Email and password are required", "correlationId": cid})
    c = config()
    conn = dbconn(c)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT user_id,password_hash,role,is_active FROM users WHERE email=%s", (email,))
            user = cur.fetchone()
    finally:
        conn.close()
    if not user or not user["is_active"] or not bcrypt.checkpw(password.encode(), user["password_hash"].encode()):
        emit("warning", "login_rejected", cid)
        return response(401, {"message": "Invalid email or password", "correlationId": cid})
    minutes = int(get_param(f"{c['prefix']}/auth/access-token-minutes"))
    days = int(get_param(f"{c['prefix']}/auth/refresh-token-days"))
    access, now, claims = issue_access_token(user["user_id"], user["role"], c["jwt"], minutes)
    raw = secrets.token_urlsafe(48)
    auth_table().put_item(Item={"token_key": "REFRESH#" + token_hash(raw), "token_type": "REFRESH", "user_id": str(user["user_id"]), "role": user["role"], "created_at": now, "expires_at": now + days * 86400, "status": "ACTIVE"})
    emit("info", "login_succeeded", cid, userId=str(user["user_id"]), role=user["role"])
    return response(200, {"accessToken": access, "tokenType": "Bearer", "expiresIn": minutes * 60, "refreshToken": raw, "userId": str(user["user_id"]), "role": user["role"]})


def refresh(data, cid):
    raw = data.get("refreshToken")
    if not isinstance(raw, str) or not raw:
        return response(400, {"message": "refreshToken is required", "correlationId": cid})
    c = config()
    table = auth_table()
    key = "REFRESH#" + token_hash(raw)
    item = table.get_item(Key={"token_key": key, "token_type": "REFRESH"}).get("Item")
    now = int(time.time())
    if not item or item.get("status") != "ACTIVE" or int(item.get("expires_at", 0)) <= now:
        return response(401, {"message": "Invalid or revoked refresh token", "correlationId": cid})
    minutes = int(get_param(f"{c['prefix']}/auth/access-token-minutes"))
    access, _, _ = issue_access_token(item["user_id"], item["role"], c["jwt"], minutes)
    # Keep the original refresh token and expiry; refreshing only creates a new access token.
    return response(200, {"accessToken": access, "tokenType": "Bearer", "expiresIn": minutes * 60, "refreshToken": raw})


def logout(event, data, cid):
    headers = event.get("headers") or {}
    authorization = headers.get("authorization") or headers.get("Authorization", "")
    if not authorization.startswith("Bearer "):
        return response(401, {"message": "Bearer access token required", "correlationId": cid})
    c = config()
    claims = jwt.decode(authorization[7:], c["jwt"], algorithms=["HS256"], options={"require": ["sub", "role", "iat", "exp", "jti"]})
    table = auth_table()
    raw = data.get("refreshToken")
    if isinstance(raw, str) and raw:
        key = "REFRESH#" + token_hash(raw)
        item = table.get_item(Key={"token_key": key, "token_type": "REFRESH"}).get("Item")
        if item and item.get("user_id") == claims["sub"]:
            table.delete_item(Key={"token_key": key, "token_type": "REFRESH"})
    table.put_item(Item={"token_key": "JTI#" + claims["jti"], "token_type": "DENYLIST", "user_id": claims["sub"], "created_at": int(time.time()), "expires_at": int(claims["exp"]), "status": "REVOKED"})
    return response(200, {"message": "Logged out"})
    