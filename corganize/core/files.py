import json
from enum import Enum

from corganize.const import FILES, FILES_INDEX_USERID, FILES_FIELD_USERID, FILES_FIELD_USERFILEID, \
    FILES_FIELD_FILEID, FILES_FIELD_LAST_UPDATED, FILES_FIELD_LOCATION, FILES_FIELD_STORAGESERVICE, \
    FILES_FIELD_FILENAME, FILES_FIELD_SIZE, FILES_FIELD_TAGS, FILES_FIELD_USERSTORAGELOCATION, \
    DDB_REQUEST_FILTER_EXPRESSION, EXPRESSION_ATTRIBUTE_VALUES
from corganize.core.util.datetimeutil import get_posix_now
from corganize.error import MissingFieldError, UnrecognizedFieldError
from corganize.externalclient import ddb

_FILE_ALLOWED_FIELDS = [
    FILES_FIELD_FILEID,
    FILES_FIELD_FILENAME,
    FILES_FIELD_SIZE,
    FILES_FIELD_STORAGESERVICE,
    FILES_FIELD_LOCATION,
    FILES_FIELD_TAGS
]

_REDACTED_FIELDS = [
    FILES_FIELD_USERID,
    FILES_FIELD_USERFILEID,
    FILES_FIELD_USERSTORAGELOCATION
]

_DDB_CLIENT = ddb.DDB(FILES, FILES_FIELD_USERID, FILES_INDEX_USERID)


class FileRetrievalFilter(Enum):
    INCOMPLETE = 1


def _redact_item(item: dict):
    for key in [key for key in item.keys() if key in _REDACTED_FIELDS]:
        item.pop(key)
    return item


def get_files(userid: str, limit: int = None, filters: list = None):
    kwargs = dict()
    if filters:
        converted_filters = list()
        exp_attr_values = dict()
        for i in range(len(filters)):
            filter = filters[i]
            var = f":var{i}"

            if not isinstance(filter, FileRetrievalFilter):
                raise TypeError(f"Invalid filter: {filter}")
            if filter == FileRetrievalFilter.INCOMPLETE:
                exp_attr_values[var] = userid
                converted_filters.append(f"{FILES_FIELD_USERSTORAGELOCATION} = {var}")
        kwargs[DDB_REQUEST_FILTER_EXPRESSION] = " and ".join(converted_filters)
        kwargs[EXPRESSION_ATTRIBUTE_VALUES] = exp_attr_values

    items = _DDB_CLIENT.query(userid, limit=limit, **kwargs)
    return [_redact_item(item) for item in items]


def upsert_file(userid, file: dict):
    fileid = file.get(FILES_FIELD_FILEID)
    if not fileid:
        raise MissingFieldError(f"'{FILES_FIELD_FILEID}' is missing")
    storageservice = file.get(FILES_FIELD_STORAGESERVICE, '')
    location = file.get(FILES_FIELD_LOCATION, '')

    unrecognized_fields = [k for k in file.keys() if k not in _FILE_ALLOWED_FIELDS]
    if unrecognized_fields:
        raise UnrecognizedFieldError(f"Unrecognized fields: {json.dumps(unrecognized_fields)}")

    metadata = {
        FILES_FIELD_USERID: userid,
        FILES_FIELD_USERFILEID: userid + fileid,
        FILES_FIELD_USERSTORAGELOCATION: userid + storageservice + location,
        FILES_FIELD_LAST_UPDATED: get_posix_now()
    }

    item = _DDB_CLIENT.upsert({**file, **metadata}, key_field_override=FILES_FIELD_USERFILEID)
    return _redact_item(item)
