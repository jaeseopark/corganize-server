from corganize.const.ddb import USERS_FIELD_USERID, USERS, USERS_INDEX_APIKEY, USERS_FIELD_APIKEY
from corganize.core.client import ddb
from corganize.error import InvalidApiKeyError


def get_userid(apikey):
    if apikey:
        items = ddb.DDB(USERS, USERS_FIELD_APIKEY, USERS_INDEX_APIKEY).query(apikey)
        if len(items) == 1:
            return items[0][USERS_FIELD_USERID]
    return None


def validate_and_get_userid(apikey):
    userid = get_userid(apikey)
    if not userid:
        raise InvalidApiKeyError
    return userid
