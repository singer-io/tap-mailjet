class MailjetError(Exception):
    """class representing Generic Http error."""

    def __init__(self, message=None, response=None):
        super().__init__(message)
        self.message = message
        self.response = response


class MailjetBackoffError(MailjetError):
    """class representing backoff error handling."""
    pass

class MailjetBadRequestError(MailjetError):
    """class representing 400 status code."""
    pass

class MailjetUnauthorizedError(MailjetError):
    """class representing 401 status code."""
    pass


class MailjetForbiddenError(MailjetError):
    """class representing 403 status code."""
    pass

class MailjetNotFoundError(MailjetError):
    """class representing 404 status code."""
    pass

class MailjetConflictError(MailjetError):
    """class representing 409 status code."""
    pass

class MailjetUnprocessableEntityError(MailjetBackoffError):
    """class representing 422 status code."""
    pass

class MailjetRateLimitError(MailjetBackoffError):
    """class representing 429 status code."""
    pass

class MailjetInternalServerError(MailjetBackoffError):
    """class representing 500 status code."""
    pass

ERROR_CODE_EXCEPTION_MAPPING = {
    400: {
        "raise_exception": MailjetBadRequestError,
        "message": "A validation exception has occurred."
    },
    401: {
        "raise_exception": MailjetUnauthorizedError,
        "message": "The access token provided is expired, revoked, malformed or invalid for other reasons."
    },
    403: {
        "raise_exception": MailjetForbiddenError,
        "message": "You are missing the following required scopes: read"
    },
    404: {
        "raise_exception": MailjetNotFoundError,
        "message": "The resource you have specified cannot be found."
    },
    409: {
        "raise_exception": MailjetConflictError,
        "message": "The API request cannot be completed because the requested operation would conflict with an existing item."
    },
    422: {
        "raise_exception": MailjetUnprocessableEntityError,
        "message": "The request content itself is not processable by the server."
    },
    429: {
        "raise_exception": MailjetRateLimitError,
        "message": "The API rate limit for your organisation/application pairing has been exceeded."
    },
    500: {
        "raise_exception": MailjetInternalServerError,
        "message": "The server encountered an unexpected condition which prevented" \
            " it from fulfilling the request."
    }
}

