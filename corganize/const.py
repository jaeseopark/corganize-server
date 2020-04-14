# HTTP METHODS
GET = "GET"
POST = "POST"

# HTTP PATHS
PATH_FILES_UPSERT = "/files/upsert"
PATH_FILES = "/files"
PATH_FILES_INCOMPLETE = "/files/incomplete"

# REQUEST
REQUEST_HTTP_METHOD = "httpMethod"
REQUEST_HEADERS = "headers"
REQUEST_API_KEY = "apikey"
REQUEST_PATH = "path"
REQUEST_BODY = "body"
REQUEST_BODY_FILTER = "filter"
REQUEST_BODY_FILES = "files"

# RESPONSE
RESPONSE_BODY = "body"
RESPONSE_STATUS = "statusCode"
RESPONSE_FILES = "files"
RESPONSE_METADATA = "metadata"

# REQUEST/RESPONSE COMMON HEADERS
NEXT_TOKEN = "nexttoken"

# DDB SDK
DDB_REQUEST_INDEX_NAME = "IndexName"
DDB_REQUEST_KEY_CONDITION_EXPRESSION = "KeyConditionExpression"
DDB_REQUEST_FILTER_EXPRESSION = "FilterExpression"
DDB_REQUEST_ITEM = "Item"
DDB_RESOURCE_NAME = "dynamodb"
DDB_RESPONSE_ITEM = "Item"
DDB_RESPONSE_ITEMS = "Items"
DDB_RESPONSE_ATTRIBUTES = "Attributes"
DDB_NEXT_TOKEN = "NextToken"
RETURN_VALUES_ALL_OLD = "ALL_OLD"
RETURN_VALUES_UPDATED_NEW = "UPDATED_NEW"
UPDATE_EXPRESSION = "UpdateExpression"
EXPRESSION_ATTRIBUTE_VALUES = "ExpressionAttributeValues"

# DDB FILES TABLE
FILES = "corganize-files"
FILES_INDEX_USERID = "userid-index"
FILES_INDEX_USERSTORAGELOCATION = "userstoragelocation-index"
FILES_FIELD_USERFILEID = "userfileid"
FILES_FIELD_USERSTORAGELOCATION = "userstoragelocation"
FILES_FIELD_USERID = "userid"
FILES_FIELD_FILEID = "fileid"
FILES_FIELD_FILENAME = "filename"
FILES_FIELD_SIZE = "size"
FILES_FIELD_TAGS = "tags"
FILES_FIELD_STORAGESERVICE = "storageservice"
FILES_FIELD_LOCATION = "locationref"
FILES_FIELD_LAST_UPDATED = "lastupdated"
FILES_FIELD_SOURCEURL = "sourceurl"
FILES_FIELD_ISACTIVE = "isactive"
FILES_FIELD_ISPUBLIC = "ispublic"

# DDB USERS TABLE
USERS = "corganize-users"
USERS_INDEX_APIKEY = "apikey-index"
USERS_FIELD_USERID = "userid"
USERS_FIELD_APIKEY = "apikey"
