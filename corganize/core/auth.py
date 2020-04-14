import logging

from corganize.const import (USERS, USERS_FIELD_APIKEY, USERS_FIELD_USERID,
                             USERS_INDEX_APIKEY)
from corganize.error import InvalidApiKeyError
from corganize.externalclient import ddb

LOGGER = logging.getLogger(__name__)


def get_userid(apikey: str):
    if apikey:
        items = ddb.DDB(USERS, USERS_FIELD_APIKEY, USERS_INDEX_APIKEY).query(apikey)
        if len(items) == 1:
            return items[0][USERS_FIELD_USERID]
    return None


def validate_and_get_userid(apikey: str):
    userid = get_userid(apikey)
    if not userid:
        apikey_redacted = apikey[:4] + "*" * (len(apikey) - 4)
        LOGGER.info(f"Invalid apikey='{apikey_redacted}'")
        raise InvalidApiKeyError
    return userid
