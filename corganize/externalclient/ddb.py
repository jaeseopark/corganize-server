import base64
import decimal
import json
import logging

import boto3
from boto3.dynamodb.conditions import Key

from corganize.const import (DDB_REQUEST_INDEX_NAME,
                             DDB_REQUEST_KEY_CONDITION_EXPRESSION,
                             DDB_RESOURCE_NAME,
                             DDB_RESPONSE_ATTRIBUTES, DDB_RESPONSE_ITEMS,
                             RETURN_VALUES_UPDATED_NEW)

_dynamodb = boto3.resource(DDB_RESOURCE_NAME)

LOGGER = logging.getLogger(__name__)


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        return super(DecimalEncoder, self).default(o)


def _remove_decimals(obj: dict):
    return json.loads(json.dumps(obj, cls=DecimalEncoder))


# def _to_next_token(ddb_base_params: dict, last_evaluated_key: dict, encoding: str = 'utf-8') -> str:
#     ddb_new_params = {
#         **ddb_base_params,
#         "LastEvaluatedKey": last_evaluated_key
#     }
#     ddb_new_params_str = json.dumps(ddb_new_params)
#     return base64.b64encode(ddb_new_params_str.encode(encoding)).decode(encoding)


def _to_query_params(next_token: str, encoding: str) -> str:
    ddb_params_str = base64.b64decode(next_token.encode(encoding)).decode(encoding)
    return json.loads(ddb_params_str)


class DDBQueryResponse:
    def __init__(self, items: list, metadata: dict):
        self.items = items
        self.metadata = metadata


class DDB:
    def __init__(self, table: str, key_field: str = None, index: str = None):
        self.table = _dynamodb.Table(table)
        self.key_field = key_field
        self.index = index

    def query(self, key, next_token: str = None, filters: list = None, params: dict = None) -> DDBQueryResponse:
        items = list()

        if next_token:
            LOGGER.info("Next token found in the request. converting it to params...")
            params = _to_query_params(next_token)
        elif not params:
            params = {
                DDB_REQUEST_INDEX_NAME: self.index,
                DDB_REQUEST_KEY_CONDITION_EXPRESSION: Key(self.key_field).eq(key)
            }

        response = self.table.query(**params)

        items = response.get(DDB_RESPONSE_ITEMS, list())

        metadata = dict()
        last_evaluated_key = response.get("LastEvaluatedKey")
        if last_evaluated_key:
            LOGGER.info("The DDB response has the next token")
            # metadata.update({
            #     "LastEvaluatedKey": _to_next_token(params, last_evaluated_key=last_evaluated_key)
            # })

        return DDBQueryResponse(_remove_decimals(items), metadata)

    def upsert(self, item, key_field, **kwargs) -> dict:
        item_keys = [k for k in item.keys() if k != key_field]
        key_to_var = {item_keys[i]: f":var{i}" for i in range(len(item_keys))}
        response = self.table.update_item(
            UpdateExpression="set " + ", ".join([f"{k}={v}" for k, v in key_to_var.items()]),
            ExpressionAttributeValues={f"{v}": item[k] for k, v in key_to_var.items()},
            Key={key_field: item[key_field]},
            ReturnValues=RETURN_VALUES_UPDATED_NEW,
            **kwargs
        )
        return _remove_decimals(response[DDB_RESPONSE_ATTRIBUTES])
