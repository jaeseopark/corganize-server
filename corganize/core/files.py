from boto3.dynamodb.conditions import Key

from corganize.const import (FILES_FIELD_USERFILEID)
from corganize.core.util.datetimeutil import get_posix_now
from corganize.externalclient import ddb

_DDB_CLIENT = ddb.DDB(table="corganize-files", key_field="userid", index="userid-index")


def _create_metadata(userid: str, file: dict):
    metadata = {
        "userid": userid,
        "userfileid": userid + file.get("fileid"),
        "lastupdated": get_posix_now()
    }
    return metadata


def get_active_files(userid: str, next_token: str = None):
    params = {
        "IndexName": "userid-dateactivated-index",
        "KeyConditionExpression": Key("userid").eq(userid),
        "ScanIndexForward": False  # this is equivalent to: ORDER BY DESC
    }
    return _DDB_CLIENT.query(**params, next_token=next_token)


def get_incomplete_files(userid: str, next_token: str = None):
    params = {
        "IndexName": "userid-storageservice-index-optimized",
        "KeyConditionExpression": Key("userid").eq(userid) & Key("storageservice").eq("None")
    }
    r = _DDB_CLIENT.query(**params, next_token=next_token)
    for file in r.items:
        file["fileid"] = file["userfileid"][len(userid):]
    return r


def get_files(userid: str, next_token: str = None):
    params = {
        "IndexName": "userid-lastupdated-index",
        "KeyConditionExpression": Key("userid").eq(userid),
        "ScanIndexForward": False  # this is equivalent to: ORDER BY DESC
    }
    return _DDB_CLIENT.query(**params, next_token=next_token)


def create_file(userid, file: dict):
    metadata = _create_metadata(userid, file)
    if "isactive" in file:
        isactive = file.pop("isactive")
        if isactive:
            metadata["dateactivated"] = metadata["lastupdated"]

    return _DDB_CLIENT.put({**file, **metadata})


def update_file(userid, file: dict):
    metadata = _create_metadata(userid, file)
    if "isactive" in file:
        isactive = file.pop("isactive")
        if isactive:
            metadata["dateactivated"] = metadata["lastupdated"]
        else:
            _DDB_CLIENT.remove_attrs(metadata, "userfileid", ["dateactivated"])

    return _DDB_CLIENT.upsert({**file, **metadata}, key_field=FILES_FIELD_USERFILEID)
