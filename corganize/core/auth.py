import logging

from corganize.const import (USERS, USERS_FIELD_APIKEY, USERS_FIELD_USERID,
                             USERS_INDEX_APIKEY)
from corganize.error import InvalidApiKeyError
from corganize.externalclient import ddb

LOGGER = logging.getLogger(__name__)


def _redact_apikey(apikey: str):
    if not apikey:
        return ""

    return apikey[:4] + "*" * (len(apikey) - 4)


def get_userid(apikey: str):
    if apikey:
        redacted_apikey = _redact_apikey(apikey)
        query_response = ddb.DDB(USERS, USERS_FIELD_APIKEY, USERS_INDEX_APIKEY).query(apikey)
        if len(query_response.items) >= 1:
            LOGGER.error(f"Too many users found for apikey={redacted_apikey}")
        elif len(query_response.items) == 1:
            LOGGER.debug(f"userid found for apikey={redacted_apikey}")
            return query_response.items[0][USERS_FIELD_USERID]
        else:
            LOGGER.debug(f"No userid found for apikey='{redacted_apikey}'")
    else:
        LOGGER.debug(f"apikey='{apikey}'")

    return None


def validate_and_get_userid(apikey: str):
    userid = get_userid(apikey)
    if not userid:
        LOGGER.info(f"Invalid apikey='{_redact_apikey(apikey)}'")
        raise InvalidApiKeyError
    return userid
