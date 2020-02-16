from corganize.const.http import PATH_FILES, RESPONSE_BODY, GET, PATH_FILES_UPSERT, POST, REQUEST_BODY_FILE
from corganize.core.files import get_files, upsert_file
from corganize.decorator.endpoint import endpoint
from corganize.error import UnrecognizedFieldError, MissingFieldError, BadRequestError


@endpoint(path=PATH_FILES, httpmethod=GET)
def files_get(userid: str, headers: dict, body: dict):
    files = get_files(userid)
    return {
        RESPONSE_BODY: files
    }


@endpoint(path=PATH_FILES_UPSERT, httpmethod=POST)
def single_file_upsert(userid: str, headers: dict, body: dict):
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
