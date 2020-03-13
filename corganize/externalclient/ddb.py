import decimal
import json

import boto3
from boto3.dynamodb.conditions import Key

from corganize.const import (DDB_REQUEST_INDEX_NAME,
                             DDB_REQUEST_KEY_CONDITION_EXPRESSION,
                             DDB_REQUEST_LIMIT, DDB_RESOURCE_NAME,
                             DDB_RESPONSE_ATTRIBUTES, DDB_RESPONSE_ITEMS,
                             FILES_FIELD_USERSTORAGELOCATION,
                             FILES_INDEX_USERSTORAGELOCATION, NEXT_TOKEN,
                             REQUEST_HEADER_LIMIT, RETURN_VALUES_UPDATED_NEW)
from corganize.core.enum.fileretrievalfilter import FileRetrievalFilter
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

    def query(self, key, key_field_override=None, index_override=None, limit=None, next_token=None, filters: list = None, **kwargs):
        items = list()

        while True:
            params = {
                DDB_REQUEST_INDEX_NAME: index_override or self.index,
                DDB_REQUEST_KEY_CONDITION_EXPRESSION: Key(key_field_override or self.key_field).eq(key)
            }

            if filters:
                converted_filters = list()
                exp_attr_values = dict()
                for i in range(len(filters)):
                    filter_i = filters[i]
                    var = f":var{i}"
                    if not isinstance(filter_i, FileRetrievalFilter):
                        raise TypeError(f"Invalid filter: {filter_i}")
                    if filter_i == FileRetrievalFilter.INCOMPLETE:
                        params[DDB_REQUEST_INDEX_NAME] = FILES_INDEX_USERSTORAGELOCATION
                        params[DDB_REQUEST_KEY_CONDITION_EXPRESSION] = Key(FILES_FIELD_USERSTORAGELOCATION).eq(key)

                if converted_filters:
                    # This block of code isn't used today
                    params[DDB_REQUEST_FILTER_EXPRESSION] = " and ".join(converted_filters)
                    params[EXPRESSION_ATTRIBUTE_VALUES] = exp_attr_values

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
