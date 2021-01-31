import json
from corganize.controller.decorator.endpoint import endpoint
from corganize.core.files import get_files, get_incomplete_files, update_file, get_active_files, create_file, \
    delete_file
from corganize.error import (BadRequestError)

_FILE_ALLOWED_FIELDS = [
    "fileid",
    "filename",
    "size",
    "storageservice",
    "locationref",
    "tags",
    "sourceurl",
    "isactive",
    "ispublic",
    "mimetype"
]

_REDACTED_FIELDS = [
    "userid",
    "userfileid",
    "isactive"
]


def _redact_item(item: dict, redacted_fields: list = None):
    all_redacted_fields = _REDACTED_FIELDS + (redacted_fields or list())
    for key in [key for key in item.keys() if key in all_redacted_fields]:
        item.pop(key)
    return item


def _to_int(value: str):
    if value is not None:
        try:
            return int(value)
        except ValueError:
            raise BadRequestError(f"'{value}' cannot be converted to an integer")
    return None


def _create_or_update_file(core_func, userid: str, body: dict, *args, **kwargs):
    if not isinstance(body, dict):
        raise BadRequestError("The request body must be a dictionary")

    if "fileid" not in body:
        raise BadRequestError("'fileid' is missing")

    unrecognized_fields = [k for k in body.keys() if k not in _FILE_ALLOWED_FIELDS]
    if unrecognized_fields:
        raise BadRequestError(f"Unrecognized fields: {json.dumps(unrecognized_fields)}")

    try:
        return {
            "body": _redact_item(core_func(userid, body))
        }
    except FileExistsError as e:
        raise BadRequestError(str(e))


@endpoint(path="/files", httpmethod="GET")
def files_get(userid: str, nexttoken: str = None, *args, **kwargs):
    ddb_response = get_files(userid, next_token=nexttoken)
    files = [_redact_item(file) for file in ddb_response.items]
    return {
        "body": {
            "metadata": ddb_response.metadata,
            "files": files
        }
    }


@endpoint(path="/files/incomplete", httpmethod="GET")
def files_get_incomplete(userid: str, nexttoken: str = None, *args, **kwargs):
    ddb_response = get_incomplete_files(userid, next_token=nexttoken)
    files = [_redact_item(file, ["storageservice"]) for file in ddb_response.items]
    return {
        "body": {
            "metadata": ddb_response.metadata,
            "files": files
        }
    }


@endpoint(path="/files/active", httpmethod="GET")
def files_get_active(userid: str, nexttoken: str = None, *args, **kwargs):
    ddb_response = get_active_files(userid, next_token=nexttoken)
    files = [_redact_item(file) for file in ddb_response.items]
    return {
        "body": {
            "metadata": ddb_response.metadata,
            "files": files
        }
    }


@endpoint(path="/files/upsert", httpmethod="POST")  # TODO: delete this when the PATCH endpoint becomes live.
def upsert(userid, body, *args, **kwargs):
    return {
        "body": [update_file(userid, file) for file in body.get("files")]
    }


@endpoint("/files", httpmethod="PATCH")
def update(*args, **kwargs):
    return _create_or_update_file(update_file, *args, **kwargs)


@endpoint("/files", httpmethod="POST")
def create(*args, **kwargs):
    return _create_or_update_file(create_file, *args, **kwargs)


@endpoint("/files", httpmethod="DELETE")
def delete(userid, body, *args, **kwargs):
    status = delete_file(userid, body)
    return {
        "statusCode": status,
        "body": {
            "message": "success"
        } if status == 200 else {}
    }
