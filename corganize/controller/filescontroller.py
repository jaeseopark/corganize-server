from corganize.const import PATH_FILES, RESPONSE_BODY, GET, PATH_FILES_UPSERT, POST, REQUEST_BODY_FILE, \
    PATH_FILES_INCOMPLETE
from corganize.controller.decorator.endpoint import endpoint
from corganize.core.files import get_files, upsert_file, FileRetrievalFilter
from corganize.error import UnrecognizedFieldError, MissingFieldError, BadRequestError, InvalidArgumentError


def _to_int(value: str):
    if value is not None:
        try:
            return int(value)
        except ValueError:
            raise BadRequestError(f"'{value}' cannot be converted to an integer")
    return None


@endpoint(path=PATH_FILES, httpmethod=GET)
def files_get(userid: str, limit: str = None, *args, **kwargs):
    print(limit)
    try:
        return {RESPONSE_BODY: get_files(userid, _to_int(limit))}
    except InvalidArgumentError as e:
        raise BadRequestError(str(e))


@endpoint(path=PATH_FILES_INCOMPLETE, httpmethod=GET)
def files_get_incomplete(userid: str, limit=None, *args, **kwargs):
    try:
        return {RESPONSE_BODY: get_files(userid, _to_int(limit), filters=[FileRetrievalFilter.INCOMPLETE])}
    except InvalidArgumentError as e:
        raise BadRequestError(str(e))


@endpoint(path=PATH_FILES_UPSERT, httpmethod=POST)
def single_file_upsert(userid: str, body: dict, *args, **kwargs):
    file = body.get(REQUEST_BODY_FILE)
    if not file:
        raise BadRequestError(f"'{REQUEST_BODY_FILE}' is missing")
    try:
        upserted_file = upsert_file(userid, file)
        return {
            RESPONSE_BODY: upserted_file
        }
    except (MissingFieldError, UnrecognizedFieldError) as e:
        raise BadRequestError(str(e))
