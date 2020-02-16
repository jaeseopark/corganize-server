class CorganizeError(Exception):
    pass


# API Errors

class InvalidApiKeyError(CorganizeError):
    pass


class ResourceNotFoundError(CorganizeError):
    pass


class BadRequestError(CorganizeError):
    pass


# Core Application Errors

class MissingFieldError(CorganizeError):
    pass


class UnrecognizedFieldError(CorganizeError):
    pass
