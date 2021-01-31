import base64
import decimal
import json
import logging

import botocore
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


class DDBQueryResponse:
    def __init__(self, items: list, metadata: dict):
        self.items = items
        self.metadata = metadata


class DDB:
    def __init__(self, table: str, key_field: str = None, index: str = None):
        self.table = _dynamodb.Table(table)
        self.key_field = key_field
        self.index = index

    def query(self, key=None, next_token: str = None, **kwargs) -> DDBQueryResponse:
        params = {
            DDB_REQUEST_INDEX_NAME: self.index,
            DDB_REQUEST_KEY_CONDITION_EXPRESSION: Key(self.key_field).eq(key),
            "Limit": 500,
            **kwargs
        }

        if next_token:
            LOGGER.info("'nexttoken' found in the request. Adding it to the DDB query params...")
            params["ExclusiveStartKey"] = json.loads(base64.b64decode(next_token.encode("utf-8")).decode("utf-8"))

        response = self.table.query(**params)

        items = _remove_decimals(response.get(DDB_RESPONSE_ITEMS, list()))

        metadata = dict()
        last_evaluated_key = response.get("LastEvaluatedKey")
        if last_evaluated_key and len(items) > 0:
            LOGGER.info("The DDB response has the next token")
            metadata.update({
                "nexttoken": base64.b64encode(json.dumps(_remove_decimals(last_evaluated_key)).encode("utf-8")).decode("utf-8")
            })

        return DDBQueryResponse(items, metadata)

    def put(self, item) -> dict:
        try:
            self.table.put_item(
                Item=item,
                ReturnValues="ALL_OLD",
                ConditionExpression="attribute_not_exists(userfileid)"
            )
        except botocore.exceptions.ClientError as e:
            if e.response['Error']['Code'] == 'ConditionalCheckFailedException':
                raise FileExistsError("Primary Key already exists")
            raise
        return _remove_decimals(item)

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

    def remove_attrs(self, item, key_field, attrs_to_remove) -> dict:
        attrs = ", ".join(attrs_to_remove)
        self.table.update_item(
            UpdateExpression=f"remove {attrs}",
            Key={key_field: item[key_field]},
            ReturnValues="NONE"
        )

    def delete(self, item):
        r = self.table.delete_item(Key=item)
        return r["ResponseMetadata"]["HTTPStatusCode"]
