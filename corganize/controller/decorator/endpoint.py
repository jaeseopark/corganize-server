_ENDPOINTS = dict()


def _normalize(path: str, httpmethod: str):
    return path.upper().strip("/") + httpmethod.upper()


def endpoint(path: str, httpmethod: str):
    def wrapper(func):
        _ENDPOINTS[_normalize(path, httpmethod)] = func
        return func

    return wrapper


def lookup(path: str, httpmethod: str):
    key = _normalize(path, httpmethod)
    return _ENDPOINTS.get(key)
