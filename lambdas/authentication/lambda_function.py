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
    full_name = str(data.get("fullName", "")).strip()
    c = config()
    minimum = int(get_param(f"{c['prefix']}/auth/password-min-length"))
    if not re.fullmatch(r"[^@\s]+@[^@\s]+\.[^@\s]+", email) or len(email) > 254 or not full_name or len(full_name) > 150:
        return response(400, {"message": "Valid email and fullName are required", "correlationId": cid})
    if (not isinstance(password, str) or len(password) < minimum or len(password.encode()) > 72
            or not re.search(r"[A-Za-z]", password) or not re.search(r"\d", password)):
        return response(400, {"message": f"Password must be at least {minimum} characters and include a letter and a digit", "correlationId": cid})
    hashed = bcrypt.hashpw(password.encode(), bcrypt.gensalt(rounds=12)).decode()
    conn = dbconn(c)
    try:
        with conn.cursor() as cur:
            cur.execute("INSERT INTO users(email,password_hash,role,full_name) VALUES(%s,%s,%s,%s)", (email, hashed, role, full_name))
            user_id = str(cur.lastrowid)
        conn.commit()
    except Exception as exc:
        conn.rollback()
        if "Duplicate entry" in str(exc):
            return response(409, {"message": "Unable to register with those details", "correlationId": cid})
        raise
    finally:
        conn.close()
    emit("info", "registration_succeeded", cid, role=role, userId=user_id)
    return response(201, {"userId": user_id, "role": role, "message": "Registration successful"})


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
    try:
        table.update_item(Key={"token_key": key, "token_type": "REFRESH"}, UpdateExpression="SET #s=:revoked", ConditionExpression="#s=:active", ExpressionAttributeNames={"#s": "status"}, ExpressionAttributeValues={":revoked": "REVOKED", ":active": "ACTIVE"})
    except ClientError:
        return response(401, {"message": "Invalid or already used refresh token", "correlationId": cid})
    minutes = int(get_param(f"{c['prefix']}/auth/access-token-minutes"))
    days = int(get_param(f"{c['prefix']}/auth/refresh-token-days"))
    access, now, _ = issue_access_token(item["user_id"], item["role"], c["jwt"], minutes)
    new_raw = secrets.token_urlsafe(48)
    table.put_item(Item={"token_key": "REFRESH#" + token_hash(new_raw), "token_type": "REFRESH", "user_id": item["user_id"], "role": item["role"], "created_at": now, "expires_at": now + days * 86400, "status": "ACTIVE"})
    return response(200, {"accessToken": access, "tokenType": "Bearer", "expiresIn": minutes * 60, "refreshToken": new_raw})


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
            table.update_item(Key={"token_key": key, "token_type": "REFRESH"}, UpdateExpression="SET #s=:revoked", ExpressionAttributeNames={"#s": "status"}, ExpressionAttributeValues={":revoked": "REVOKED"})
    table.put_item(Item={"token_key": "JTI#" + claims["jti"], "token_type": "DENYLIST", "user_id": claims["sub"], "created_at": int(time.time()), "expires_at": int(claims["exp"]), "status": "REVOKED"})
    return response(200, {"message": "Logged out"})
