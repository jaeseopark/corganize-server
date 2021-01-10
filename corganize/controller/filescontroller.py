from corganize.const import (GET, PATH_FILES, PATH_FILES_INCOMPLETE,
                             PATH_FILES_UPSERT, POST,
                             REQUEST_BODY_FILES, RESPONSE_BODY)
from corganize.controller.decorator.endpoint import endpoint
from corganize.core.files import get_files, get_incomplete_files, upsert_file, get_active_files
from corganize.error import (BadRequestError, InvalidArgumentError,
                             MissingFieldError, UnrecognizedFieldError)


def _to_int(value: str):
    if value is not None:
        try:
            return int(value)
        except ValueError:
            raise BadRequestError(f"'{value}' cannot be converted to an integer")
    return None


@endpoint(path=PATH_FILES, httpmethod=GET)
def files_get(userid: str, nexttoken: str = None, *args, **kwargs):
    try:
        return {RESPONSE_BODY: get_files(userid, next_token=nexttoken)}
    except InvalidArgumentError as e:
        raise BadRequestError(str(e))


@endpoint(path=PATH_FILES_INCOMPLETE, httpmethod=GET)
def files_get_incomplete(userid: str, nexttoken: str = None, *args, **kwargs):
    try:
        incompete_files = get_incomplete_files(userid, next_token=nexttoken)
        return {RESPONSE_BODY: incompete_files}
    except InvalidArgumentError as e:
        raise BadRequestError(str(e))


@endpoint(path="/files/active", httpmethod=GET)
def files_get_active(userid: str, nexttoken: str = None, *args, **kwargs):
    try:
        active_files = get_active_files(userid, next_token=nexttoken)
        return {RESPONSE_BODY: active_files}
    except InvalidArgumentError as e:
        raise BadRequestError(str(e))


@endpoint(path=PATH_FILES_UPSERT, httpmethod=POST)
def upsert(userid: str, body: dict, *args, **kwargs):
    files = body.get(REQUEST_BODY_FILES)

    if not files:
        raise BadRequestError(f"'{REQUEST_BODY_FILES}' missing or empty")

    if not isinstance(files, list):
        raise BadRequestError(f"'{REQUEST_BODY_FILES}' must be a list")

    for element in files:
        if not isinstance(element, dict):
            raise BadRequestError(f"All elements of '{REQUEST_BODY_FILES}' must be dictionaries")

    try:
        upserted_files = [upsert_file(userid, file) for file in files]

        return {
            RESPONSE_BODY: upserted_files
        }
    except (MissingFieldError, UnrecognizedFieldError) as e:
        raise BadRequestError(str(e))
