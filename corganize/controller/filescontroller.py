from corganize.const import (PATH_FILES_INCOMPLETE, RESPONSE_BODY)
from corganize.controller.decorator.endpoint import endpoint
from corganize.core.files import get_files, get_incomplete_files, update_file, get_active_files, create_file
from corganize.error import (BadRequestError, InvalidArgumentError,
                             MissingFieldError, UnrecognizedFieldError)


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

    try:
        return {
            RESPONSE_BODY: core_func(userid, body)
        }
    except (MissingFieldError, UnrecognizedFieldError, FileExistsError) as e:
        raise BadRequestError(str(e))


@endpoint(path="/files", httpmethod="GET")
def files_get(userid: str, nexttoken: str = None, *args, **kwargs):
    try:
        return {RESPONSE_BODY: get_files(userid, next_token=nexttoken)}
    except InvalidArgumentError as e:
        raise BadRequestError(str(e))


@endpoint(path=PATH_FILES_INCOMPLETE, httpmethod="GET")
def files_get_incomplete(userid: str, nexttoken: str = None, *args, **kwargs):
    try:
        incompete_files = get_incomplete_files(userid, next_token=nexttoken)
        return {RESPONSE_BODY: incompete_files}
    except InvalidArgumentError as e:
        raise BadRequestError(str(e))


@endpoint(path="/files/active", httpmethod="GET")
def files_get_active(userid: str, nexttoken: str = None, *args, **kwargs):
    try:
        active_files = get_active_files(userid, next_token=nexttoken)
        return {RESPONSE_BODY: active_files}
    except InvalidArgumentError as e:
        raise BadRequestError(str(e))


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
