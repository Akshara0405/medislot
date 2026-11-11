"""API Gateway TOKEN authorizer: checks the JWT, the role for the route, and the logout denylist."""
import os
import time

import jwt

from common import auth_table, emit, get_param


def policy(principal, effect, method_arn, context=None):
    out = {'principalId': principal,
           'policyDocument': {'Version': '2012-10-17',
                              'Statement': [{'Action': 'execute-api:Invoke', 'Effect': effect, 'Resource': method_arn}]}}
    if context:
        out['context'] = context
    return out


def handler(event, context):
    started = time.time()
    method_arn = event.get('methodArn', '*')
    token = (event.get('authorizationToken') or '').strip()
    if token.lower().startswith('bearer '):
        token = token[7:].strip()
    # methodArn = arn:aws:execute-api:region:account:apiId/stage/METHOD/resource/path
    arn_parts = method_arn.split('/')
    route = '/'.join(arn_parts[2:]) if len(arn_parts) > 2 else ''
    log = {'function': 'authorization', 'route': f"{arn_parts[2] if len(arn_parts) > 2 else ''} /{'/'.join(arn_parts[3:])}",
           'requestId': getattr(context, 'aws_request_id', None)}
    claims = {}
    try:
        if not token:
            raise ValueError('missing token')
        env = os.environ.get('ENVIRONMENT', 'dev')
        secret = get_param(f'/medislot/{env}/auth/jwt-secret', True)
        claims = jwt.decode(token, secret, algorithms=['HS256'], options={'require': ['exp', 'iat', 'sub', 'jti', 'role']})
        if claims['role'] not in ('patient', 'doctor'):
            raise ValueError('unknown role')
        # The role a route needs comes from the route itself, never from client input.
        expected = 'doctor' if '/doctor/' in f'/{route}/' else ('patient' if '/patient/' in f'/{route}/' else None)
        if expected and claims['role'] != expected:
            raise PermissionError(f'{claims["role"]} token used on a {expected} route')
        now = int(time.time())
        if int(claims['exp']) <= now:
            raise ValueError('expired')
        item = auth_table().get_item(Key={'token_key': 'JTI#' + str(claims['jti']), 'token_type': 'DENYLIST'}).get('Item')
        if item and int(item.get('expires_at', 0)) > now:
            raise ValueError('token revoked (logged out)')
    except PermissionError as exc:
        emit('warning', 'authorization_denied', None, **log, status=403, reason=str(exc), userId=claims.get('sub'),
             durationMs=int((time.time() - started) * 1000))
        return policy('forbidden', 'Deny', method_arn)
    except Exception as exc:
        emit('warning', 'authorization_rejected', None, **log, status=401, reason=f'{type(exc).__name__}: {exc}'[:300],
             durationMs=int((time.time() - started) * 1000))
        raise Exception('Unauthorized')         # API Gateway turns exactly this message into a 401
    emit('info', 'authorization_allowed', None, **log, status=200, userId=str(claims['sub']), role=claims['role'],
         durationMs=int((time.time() - started) * 1000))
    return policy(str(claims['sub']), 'Allow', method_arn,
                  {'userId': str(claims['sub']), 'role': claims['role'], 'jti': str(claims['jti'])})
