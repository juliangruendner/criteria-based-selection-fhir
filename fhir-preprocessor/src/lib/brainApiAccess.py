from flask import g
from flask_restful import abort
import functools
from flask import request
import socket


class BrainApiAccess(object):
    def __init__(self):
        return

    def __call__(self, fn):
        @functools.wraps(fn)
        def decorated(*args, **kwargs):
            if not is_brain_api():
                abort(403, message="only brain api may use this function")
            return fn(*args, **kwargs)
        return decorated


def is_brain_api():
    return True
    ip_list = []
    ais = socket.getaddrinfo("ketos_brain", 0, 0, 0, 0)
    for result in ais:
        ip_list.append(result[-1][0])
    ip_list = list(set(ip_list))

    if not (request.environ['REMOTE_ADDR'] in ip_list):
            return False
    
    return True
