import os, time, jwt
from common import get_param, auth_table, emit
def handler(event, context):
    method_arn=event.get("methodArn","*")
    token=(event.get("authorizationToken") or "").strip()
    cid=(event.get("headers") or {}).get("x-correlation-id")
    if token.lower().startswith("bearer "): token=token[7:].strip()
    try:
        if not token: raise ValueError("missing")
        env=os.environ.get("ENVIRONMENT","dev")
        signing_secret=get_param(f"/medislot/{env}/auth/jwt-secret",True)
        claims=jwt.decode(token,signing_secret,algorithms=["HS256"],options={"require":["exp","iat","sub","jti","role"]})
        if claims["role"] not in ("patient","doctor"): raise ValueError("role")
        # Enforce route role from the invoked resource path, not from client input.
        arn_parts=method_arn.split("/")
        route="/".join(arn_parts[2:]) if len(arn_parts)>2 else ""
        expected="doctor" if "/doctor/" in ("/"+route+"/") else ("patient" if "/patient/" in ("/"+route+"/") else None)
        if expected and claims["role"] != expected: raise PermissionError("wrong_role")
        now=int(time.time())
        if int(claims["exp"])<=now: raise ValueError("expired")
        key="JTI#"+str(claims["jti"])
        item=auth_table().get_item(Key={"token_key":key,"token_type":"DENYLIST"}).get("Item")
        if item and int(item.get("expires_at",0))>now: raise ValueError("revoked")
        return {"principalId":str(claims["sub"]),"policyDocument":{"Version":"2012-10-17","Statement":[{"Action":"execute-api:Invoke","Effect":"Allow","Resource":method_arn}]},"context":{"userId":str(claims["sub"]),"role":claims["role"],"jti":str(claims["jti"])}}
    except PermissionError:
        emit("warning","authorization_wrong_role",cid)
        return {"principalId":"forbidden","policyDocument":{"Version":"2012-10-17","Statement":[{"Action":"execute-api:Invoke","Effect":"Deny","Resource":method_arn}]}}
    except Exception:
        emit("warning","authorization_rejected",cid)
        raise Exception("Unauthorized")
