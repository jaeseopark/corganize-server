import json

from corganize.const.ddb import FILES, FILES_INDEX_USERID, FILES_FIELD_USERID, FILES_FIELD_USERFILEID
from corganize.core.client import ddb
from corganize.error import MissingFieldError, UnrecognizedFieldError
from corganize.util.datetimeutil import get_posix_now

FILE_ALLOWED_FIELDS = [
    "fileid",
    "filename",
    "size",
    "strageservice",
    "location",
    "tags"
]

DDB_CLIENT = ddb.DDB(FILES, FILES_FIELD_USERID, FILES_INDEX_USERID)


def get_files(userid: str):
    items = DDB_CLIENT.query(userid)
    # TODO: [nice-to-have] add computed fields
    return items


def upsert_file(userid, file: dict):
    fileid = file.get("fileid")
    if not fileid:
        raise MissingFieldError(f"'fileid' is missing")

    unrecognized_fields = [k for k in file.keys() if k not in FILE_ALLOWED_FIELDS]
    if unrecognized_fields:
        raise UnrecognizedFieldError(f"Unrecognized fields: {json.dumps(unrecognized_fields)}")

    item = {
        **file,
        "userid": userid,
        "userfileid": userid + fileid,
        "lastupdated": get_posix_now()
    }
    return DDB_CLIENT.upsert(item, key_field_override=FILES_FIELD_USERFILEID)
