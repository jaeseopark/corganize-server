import decimal
import json

import boto3
from boto3.dynamodb.conditions import Key

from corganize.const import NEXT_TOKEN, DDB_RESOURCE_NAME, DDB_RESPONSE_ITEMS, \
    DDB_REQUEST_KEY_CONDITION_EXPRESSION, DDB_REQUEST_INDEX_NAME, RETURN_VALUES_UPDATED_NEW, DDB_RESPONSE_ATTRIBUTES, \
    DDB_REQUEST_LIMIT, REQUEST_HEADER_LIMIT
from corganize.error import InvalidArgumentError

_dynamodb = boto3.resource(DDB_RESOURCE_NAME)


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


class DDB:
    def __init__(self, table: str, key_field: str, index: str = None):
        self.table = _dynamodb.Table(table)
        self.key_field = key_field
        self.index = index

    def query(self, key, key_field_override=None, index_override=None, limit=None, next_token=None, **kwargs):
        items = list()

        while True:
            params = {
                DDB_REQUEST_INDEX_NAME: index_override or self.index,
                DDB_REQUEST_KEY_CONDITION_EXPRESSION: Key(key_field_override or self.key_field).eq(key)
            }

            if next_token:
                params[NEXT_TOKEN] = next_token

            if limit is not None:
                if limit < 1 or not isinstance(limit, int):
                    raise InvalidArgumentError(f"'{REQUEST_HEADER_LIMIT}' must be a positive integer")
                params[DDB_REQUEST_LIMIT] = limit

            response = self.table.query(**params, **kwargs)

            if DDB_RESPONSE_ITEMS in response:
                items += response[DDB_RESPONSE_ITEMS]

            next_token = response.get(NEXT_TOKEN)

            if not next_token or (limit is not None and len(response) > limit):
                break

        if limit:
            items = items[:limit]

        return _remove_decimals(items)

    def upsert(self, item, key_field_override=None, **kwargs):
        key_field = key_field_override or self.key_field
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
