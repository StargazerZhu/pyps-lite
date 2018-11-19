from contextlib import closing
import socket
import os


def get_available_port():
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
        sock.bind(('localhost', 0))
        _, port = sock.getsockname()
    return port


def get_env(env_key, default_val):
    if env_key in os.environ:
        ret = os.environ[env_key]
        if isinstance(default_val, int):
            ret = int(default_val)
        elif isinstance(default_val, float):
            ret = float(default_val)
        elif isinstance(default_val, bool):
            ret = bool(default_val)
        return ret
    else:
        return default_val

