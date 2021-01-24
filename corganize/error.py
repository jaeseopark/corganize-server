class CorganizeError(Exception):
    pass


# API Errors

class InvalidApiKeyError(CorganizeError):
    pass


class ResourceNotFoundError(CorganizeError):
    pass


class BadRequestError(CorganizeError):
    pass


class MissingFieldError(CorganizeError):
    pass


# Core Application Errors

class UnrecognizedFieldError(CorganizeError):
    pass


class InvalidArgumentError(CorganizeError):
    pass
