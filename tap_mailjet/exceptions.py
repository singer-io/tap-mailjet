class mailjetError(Exception):
    """class representing Generic Http error."""

    def __init__(self, message=None, response=None):
        super().__init__(message)
        self.message = message
        self.response = response


class mailjetBackoffError(mailjetError):
    """class representing backoff error handling."""
    pass

class mailjetBadRequestError(mailjetError):
    """class representing 400 status code."""
    pass

class mailjetUnauthorizedError(mailjetError):
    """class representing 401 status code."""
    pass


class mailjetForbiddenError(mailjetError):
    """class representing 403 status code."""
    pass

class mailjetNotFoundError(mailjetError):
    """class representing 404 status code."""
    pass

class mailjetConflictError(mailjetError):
    """class representing 409 status code."""
    pass

class mailjetUnprocessableEntityError(mailjetBackoffError):
    """class representing 422 status code."""
    pass

class mailjetRateLimitError(mailjetBackoffError):
    """class representing 429 status code."""
    pass

class mailjetInternalServerError(mailjetBackoffError):
    """class representing 500 status code."""
    pass

class mailjetNotImplementedError(mailjetBackoffError):
    """class representing 501 status code."""
    pass

class mailjetBadGatewayError(mailjetBackoffError):
    """class representing 502 status code."""
    pass

class mailjetServiceUnavailableError(mailjetBackoffError):
    """class representing 503 status code."""
    pass

ERROR_CODE_EXCEPTION_MAPPING = {
    400: {
        "raise_exception": mailjetBadRequestError,
        "message": "A validation exception has occurred."
    },
    401: {
        "raise_exception": mailjetUnauthorizedError,
        "message": "The access token provided is expired, revoked, malformed or invalid for other reasons."
    },
    403: {
        "raise_exception": mailjetForbiddenError,
        "message": "You are missing the following required scopes: read"
    },
    404: {
        "raise_exception": mailjetNotFoundError,
        "message": "The resource you have specified cannot be found."
    },
    409: {
        "raise_exception": mailjetConflictError,
        "message": "The API request cannot be completed because the requested operation would conflict with an existing item."
    },
    422: {
        "raise_exception": mailjetUnprocessableEntityError,
        "message": "The request content itself is not processable by the server."
    },
    429: {
        "raise_exception": mailjetRateLimitError,
        "message": "The API rate limit for your organisation/application pairing has been exceeded."
    },
    500: {
        "raise_exception": mailjetInternalServerError,
        "message": "The server encountered an unexpected condition which prevented" \
            " it from fulfilling the request."
    },
    501: {
        "raise_exception": mailjetNotImplementedError,
        "message": "The server does not support the functionality required to fulfill the request."
    },
    502: {
        "raise_exception": mailjetBadGatewayError,
        "message": "Server received an invalid response."
    },
    503: {
        "raise_exception": mailjetServiceUnavailableError,
        "message": "API service is currently unavailable."
    }
}

