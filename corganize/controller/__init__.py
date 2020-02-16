from importlib import import_module as _im
from pathlib import Path as _p

from corganize.controller.decorator.endpoint import lookup
from corganize.error import ResourceNotFoundError as _rnfe

__all__ = [
    _im(f".{f.stem}", __package__)
    for f in _p(__file__).parent.glob("*.py")
    if "__" not in f.stem
]


def get_handler(path: str, httpmethod: str):
    handler = lookup(path, httpmethod)
    if not handler:
        raise _rnfe
    return handler
