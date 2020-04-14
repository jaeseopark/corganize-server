import json
import logging
from json import JSONDecodeError

from corganize.core import auth
from corganize.const import REQUEST_PATH, REQUEST_API_KEY, REQUEST_HEADERS, \
    RESPONSE_BODY, RESPONSE_STATUS, REQUEST_BODY, REQUEST_HTTP_METHOD
from corganize.controller import get_handler
from corganize.error import InvalidApiKeyError, ResourceNotFoundError, BadRequestError

LOGGER = logging.getLogger(__name__)


def make_case_insensitive(headers: dict):
    headers_lowercase = {k.lower(): v for k, v in headers.items()}
    headers_uppercase = {k.upper(): v for k, v in headers.items()}
    return {**headers, **headers_lowercase, **headers_uppercase}


def result_to_response(result: dict):
    status = result.get(RESPONSE_STATUS, 200)
    if status < 200 or status > 299:
        msg = f"the status for a successful response should be between 200 and 299. status={status}"
        LOGGER.warning(msg)
    response_body = result.get(RESPONSE_BODY)
    if not isinstance(response_body, str):
        # TODO [nice-to-have] implement a custom JSONEncoder for more advanced stuff
        response_body = json.dumps(response_body)
    return {
        RESPONSE_STATUS: status,
        RESPONSE_BODY: response_body
    }


def lambda_handler(event, context):
    response_body = None

    try:
        # Parse event
        headers = make_case_insensitive(event.get(REQUEST_HEADERS) or dict())
        request_body = event.get(REQUEST_BODY) or dict()
        path = event.get(REQUEST_PATH)
        http_method = event.get(REQUEST_HTTP_METHOD).upper()

        # Authenticate
        apikey = headers.get(REQUEST_API_KEY)
        userid = auth.validate_and_get_userid(apikey)

        # Get request handler
        handler = get_handler(path, http_method)

        # Process/handle request
        try:
            if request_body and not isinstance(request_body, dict):
                request_body = json.loads(request_body)
        except (TypeError, JSONDecodeError):
            raise BadRequestError("request body cannot be parsed")
        result = handler(userid, **headers, body=request_body)

        # Return 200-level response
        return result_to_response(result)
    except BadRequestError as e:
        status = 400
        response_body = str(e)
    except InvalidApiKeyError:
        status = 401
    except ResourceNotFoundError:
        status = 404
    except:
        LOGGER.exception("")
        status = 500

    # Return non-200 level response
    return {
        RESPONSE_STATUS: status,
        RESPONSE_BODY: response_body
    }
