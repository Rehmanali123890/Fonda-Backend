from functools import wraps
from flask import Response, request, g, jsonify
from utilities.helpers import *


def validate_token_middleware(func):
    @wraps(func)
    def decorated_function(*args, **kwargs):
        if "HTTP_AUTHORIZATION" in request.environ:
            auth_token = request.environ['HTTP_AUTHORIZATION']
            auth_token = auth_token.replace('Bearer ', '')

            userId = validateLoginToken(auth_token)
            if userId:
                g.token = auth_token
                g.userId = userId

                return func(*args, **kwargs)

        headers = {
            "Content-Type": "application/json",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Credentials": "true"
        }
        return Response("Token Authorization Failed.",
            mimetype='text/plain', 
            status=401, 
            headers=headers
        )

    return decorated_function