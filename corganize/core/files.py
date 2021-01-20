import json

from boto3.dynamodb.conditions import Key

from corganize.const import (FILES_FIELD_FILEID, FILES_FIELD_FILENAME,
                             FILES_FIELD_LOCATION,
                             FILES_FIELD_SIZE, FILES_FIELD_SOURCEURL,
                             FILES_FIELD_STORAGESERVICE, FILES_FIELD_TAGS,
                             FILES_FIELD_USERFILEID, FILES_FIELD_USERID,
                             FILES_FIELD_USERSTORAGELOCATION)
from corganize.core.util.datetimeutil import get_posix_now
from corganize.error import MissingFieldError, UnrecognizedFieldError
from corganize.externalclient import ddb

_FILE_ALLOWED_FIELDS = [
    FILES_FIELD_FILEID,
    FILES_FIELD_FILENAME,
    FILES_FIELD_SIZE,
    FILES_FIELD_STORAGESERVICE,
    FILES_FIELD_LOCATION,
    FILES_FIELD_TAGS,
    FILES_FIELD_SOURCEURL,
    "isactive",
    "ispublic"
]

_REDACTED_FIELDS = [
    FILES_FIELD_USERID,
    FILES_FIELD_USERFILEID,
    FILES_FIELD_USERSTORAGELOCATION,
    "isactive"
]

_DDB_CLIENT = ddb.DDB(table="corganize-files", key_field="userid", index="userid-index")


def _redact_item(item: dict):
    for key in [key for key in item.keys() if key in _REDACTED_FIELDS]:
        item.pop(key)
    return item


def get_active_files(userid: str, next_token: str = None):
    params = {
        "IndexName": "userid-dateactivated-index",
        "KeyConditionExpression": Key("userid").eq(userid),
        "ScanIndexForward": False  # this is equivalent to: ORDER BY DESC
    }
    query_response = _DDB_CLIENT.query(key="dummykey-refactorme", next_token=next_token, **params)
    return {
        "metadata": query_response.metadata,
        "files": [_redact_item(item) for item in query_response.items]
    }


def get_incomplete_files(userid: str, next_token: str = None, filters: list = None):
    # This function will have its own index later.. for now use get_files with local filtering.
    #
    # params = {
    #     "IndexName": "userid-locationref-index",
    #     "KeyConditionExpression": Key("userid").eq(userid) & Key("locationref").eq(None)
    # }
    # query_response = _DDB_CLIENT.query("dummykey-refactorme", next_token=next_token, params=params)
    # return {
    #     "metadata": query_response.metadata,
    #     "files": [_redact_item(item) for item in query_response.items]
    # }
    response = get_files(userid, next_token)
    return {
        "metadata": response["metadata"],
        "files": [f for f in response["files"] if not f.get("locationref") and f.get("ispublic", True)]
    }


def get_files(userid: str, next_token: str = None):
    params = {
        "IndexName": "userid-lastupdated-index",
        "KeyConditionExpression": Key("userid").eq(userid),
        "ScanIndexForward": False  # this is equivalent to: ORDER BY DESC
    }
    query_response = _DDB_CLIENT.query(key="dummykey-refactorme", next_token=next_token, **params)
    return {
        "metadata": query_response.metadata,
        "files": [_redact_item(item) for item in query_response.items]
    }


def upsert_file(userid, file: dict):
    fileid = file.get(FILES_FIELD_FILEID)
    if not fileid:
        raise MissingFieldError(f"'{FILES_FIELD_FILEID}' is missing")

    unrecognized_fields = [k for k in file.keys() if k not in _FILE_ALLOWED_FIELDS]
    if unrecognized_fields:
        raise UnrecognizedFieldError(f"Unrecognized fields: {json.dumps(unrecognized_fields)}")

    metadata = {
        "userid": userid,
        "userfileid": userid + fileid,
        "lastupdated": get_posix_now()
    }

    if "isactive" in file:
        isactive = file.pop("isactive")
        if isactive:
            metadata["dateactivated"] = file.get("dateactivated", metadata["lastupdated"])
        else:
            _DDB_CLIENT.remove_attrs(metadata, "userfileid", ["dateactivated"])

    item = _DDB_CLIENT.upsert({**file, **metadata}, key_field=FILES_FIELD_USERFILEID)
    return _redact_item(item)
