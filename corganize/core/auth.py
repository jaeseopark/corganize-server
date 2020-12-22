import logging

from corganize.const import (USERS_FIELD_USERID)
from corganize.error import CorganizeError, InvalidApiKeyError
from corganize.externalclient import ddb

LOGGER = logging.getLogger(__name__)

_REDACT_CLEARTEXT_LEN = 3


def _redact_apikey(apikey: str):
    if not apikey:
        return ""

    return apikey[:_REDACT_CLEARTEXT_LEN] + "*" * (len(apikey) - _REDACT_CLEARTEXT_LEN)


def get_userid(apikey: str):
    if apikey:
        redacted_apikey = _redact_apikey(apikey)
        query_response = ddb.DDB(table="corganize-users", key_field="apikey", index="apikey-index").query(apikey)
        if len(query_response.items) > 1:
            raise CorganizeError(f"Too many users found for apikey={redacted_apikey} len(items)={len(query_response.items)}")
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
