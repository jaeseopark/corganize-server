import json

from corganize.const.ddb import FILES, FILES_INDEX_USERID, FILES_FIELD_USERID, FILES_FIELD_USERFILEID, \
    FILES_FIELD_FILEID, FILES_FIELD_LAST_UPDATED, FILES_FIELD_LOCATION, FILES_FIELD_STORAGESERVICE, \
    FILES_FIELD_FILENAME, FILES_FIELD_SIZE, FILES_FIELD_TAGS
from corganize.core.client import ddb
from corganize.error import MissingFieldError, UnrecognizedFieldError
from corganize.util.datetimeutil import get_posix_now

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
    FILES_FIELD_USERFILEID
]

_DDB_CLIENT = ddb.DDB(FILES, FILES_FIELD_USERID, FILES_INDEX_USERID)


def _redact_item(item: dict):
    for key in [key for key in item.keys() if key in _REDACTED_FIELDS]:
        item.pop(key)
    return item


def get_files(userid: str):
    items = _DDB_CLIENT.query(userid)
    # TODO: [nice-to-have] add computed fields
    return [_redact_item(item) for item in items]


def upsert_file(userid, file: dict):
    fileid = file.get(FILES_FIELD_FILEID)
    if not fileid:
        raise MissingFieldError(f"'{FILES_FIELD_FILEID}' is missing")

    unrecognized_fields = [k for k in file.keys() if k not in _FILE_ALLOWED_FIELDS]
    if unrecognized_fields:
        raise UnrecognizedFieldError(f"Unrecognized fields: {json.dumps(unrecognized_fields)}")

    item = {
        **file,
        FILES_FIELD_USERID: userid,
        FILES_FIELD_USERFILEID: userid + fileid,
        FILES_FIELD_LAST_UPDATED: get_posix_now()
    }
    item = _DDB_CLIENT.upsert(item, key_field_override=FILES_FIELD_USERFILEID)
    return _redact_item(item)
