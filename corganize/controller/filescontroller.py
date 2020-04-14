from corganize.const import (GET, PATH_FILES, PATH_FILES_INCOMPLETE,
                             PATH_FILES_UPSERT, POST, REQUEST_BODY_FILE,
                             REQUEST_BODY_FILES, RESPONSE_BODY, RESPONSE_FILES)
from corganize.controller.decorator.endpoint import endpoint
from corganize.core.enum.fileretrievalfilter import FileRetrievalFilter
from corganize.core.files import get_files, upsert_file
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
        incompete_files = get_files(userid, next_token=nexttoken, filters=[FileRetrievalFilter.INCOMPLETE])

        # ----start of temporary fix----
        # FileRetrievalFilter.INCOMPLETE isn't working.. so here is the temporary fix
        # need to look into DDB table for the permanent fix
        incompete_files[RESPONSE_FILES] = [f for f in incompete_files[RESPONSE_FILES] if not f.get("locationref")]
        # ----end of temporary fix----

        return {RESPONSE_BODY: incompete_files}
    except InvalidArgumentError as e:
        raise BadRequestError(str(e))


@endpoint(path=PATH_FILES_UPSERT, httpmethod=POST)
def upsert(userid: str, body: dict, *args, **kwargs):
    files = body.get(REQUEST_BODY_FILES)

    if not files:
        file = body.get(REQUEST_BODY_FILE)
        if file:
            if not isinstance(file, dict):
                raise BadRequestError(f"'{REQUEST_BODY_FILE}' must be a dictionary")
            files = [file]
        else:
            raise BadRequestError(f"'{REQUEST_BODY_FILES}' missing")

    if not isinstance(files, list):
        raise BadRequestError(f"'{REQUEST_BODY_FILES}' must be a list")

    try:
        upserted_files = [upsert_file(userid, file) for file in files]

        return {
            RESPONSE_BODY: upserted_files
        }
    except (MissingFieldError, UnrecognizedFieldError) as e:
        raise BadRequestError(str(e))
