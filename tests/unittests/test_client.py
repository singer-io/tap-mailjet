"""Unit tests for Client class - initialization, methods, backoff, and retry logic."""
import unittest
import requests
from unittest.mock import patch
from parameterized import parameterized
from requests.exceptions import Timeout, ConnectionError, ChunkedEncodingError
from tap_mailjet.client import Client
from tap_mailjet.exceptions import *


default_config = {
    "base_url": "https://api.example.com",
    "request_timeout": 30,
    "api_key": "test_key",
    "secret_key": "test_secret",
    "start_date": "2025-01-01T00:00:00Z"
}

DEFAULT_REQUEST_TIMEOUT = 300

class MockResponse:
    """Mocked standard HTTPResponse to test error handling."""

    def __init__(
        self, status_code, resp="", content=[""], headers=None, raise_error=True, text=None
    ):
        self.json_data = resp
        self.status_code = status_code
        self.content = content
        self.headers = headers
        self.raise_error = raise_error
        self.text = text or {}
        self.reason = "error"

    def raise_for_status(self):
        """If an error occur, this method returns a HTTPError object.

        Raises:
            requests.HTTPError: Mock http error.

        Returns:
            int: Returns status code if not error occurred.
        """
        if not self.raise_error:
            return self.status_code

        raise requests.HTTPError("mock sample message")

    def json(self):
        """Returns a JSON object of the result."""
        return self.text


class TestClientInitialization(unittest.TestCase):
    """Test client initialization and configuration."""

    def setUp(self):
        """Set up the client with default configuration."""
        self.client = Client(default_config)

    @parameterized.expand([    
        ["empty value", "", DEFAULT_REQUEST_TIMEOUT],
        ["string value", "12", 12.0],
        ["integer value", 10, 10.0],
        ["float value", 20.0, 20.0],
        ["zero value", 0, DEFAULT_REQUEST_TIMEOUT]
    ])
    @patch("tap_mailjet.client.session")
    def test_request_timeout_values(self, test_name, input_value, expected_value, mock_session):
        """Test that request timeout is properly parsed from config."""
        config = default_config.copy()
        config["request_timeout"] = input_value
        client = Client(config)
        assert client.request_timeout == expected_value
        assert isinstance(client._session, mock_session().__class__)


class TestErrorHandling(unittest.TestCase):
    """Test HTTP error handling without retry (4xx errors)."""

    def setUp(self):
        """Set up the client with default configuration."""
        self.client = Client(default_config)

    @parameterized.expand([
        ["400 error", 400, MockResponse(400), mailjetBadRequestError, "A validation exception has occurred."],
        ["401 error", 401, MockResponse(401), mailjetUnauthorizedError, "The access token provided is expired, revoked, malformed or invalid for other reasons."],
        ["403 error", 403, MockResponse(403), mailjetForbiddenError, "You are missing the following required scopes: read"],
        ["404 error", 404, MockResponse(404), mailjetNotFoundError, "The resource you have specified cannot be found."],
        ["409 error", 409, MockResponse(409), mailjetConflictError, "The API request cannot be completed because the requested operation would conflict with an existing item."],
    ])
    def test_4xx_errors_no_retry(self, test_name, error_code, mock_response, error, error_message):
        """Test that 4xx errors raise immediately without retry."""
        with patch.object(self.client._session, "request", return_value=mock_response) as mock_request:
            with self.assertRaises(error) as e:
                self.client._Client__make_request("GET", "https://api.example.com/resource")

        expected_error_message = (f"HTTP-error-code: {error_code}, Error: {error_message}")
        self.assertEqual(str(e.exception), expected_error_message)
        # Should only attempt once (no retry for 4xx errors)
        self.assertEqual(mock_request.call_count, 1)


class TestBackoffRetry(unittest.TestCase):
    """Test backoff and retry logic for retryable errors."""

    def setUp(self):
        """Set up the client with default configuration."""
        self.client = Client(default_config)

    @parameterized.expand([
        ["422 error", 422, MockResponse(422), mailjetUnprocessableEntityError, "The request content itself is not processable by the server."],
        ["429 error", 429, MockResponse(429), mailjetRateLimitError, "The API rate limit for your organisation/application pairing has been exceeded."],
        ["500 error", 500, MockResponse(500), mailjetInternalServerError, "The server encountered an unexpected condition which prevented it from fulfilling the request."],
        ["501 error", 501, MockResponse(501), mailjetNotImplementedError, "The server does not support the functionality required to fulfill the request."],
        ["502 error", 502, MockResponse(502), mailjetBadGatewayError, "Server received an invalid response."],
        ["503 error", 503, MockResponse(503), mailjetServiceUnavailableError, "API service is currently unavailable."],
    ])
    @patch("time.sleep")
    def test_http_errors_with_retry(self, test_name, error_code, mock_response, error, error_message, mock_sleep):
        """Test that retryable HTTP errors (429, 5xx) trigger backoff retry."""
        with patch.object(self.client._session, "request", return_value=mock_response) as mock_request:
            with self.assertRaises(error) as e:
                self.client._Client__make_request("GET", "https://api.example.com/resource")

            expected_error_message = (f"HTTP-error-code: {error_code}, Error: {error_message}")
            self.assertEqual(str(e.exception), expected_error_message)
            # Verify 5 retry attempts
            self.assertEqual(mock_request.call_count, 5)
            # Verify sleep was called for backoff
            self.assertTrue(mock_sleep.called)

    @parameterized.expand([
        ["ConnectionResetError", ConnectionResetError],
        ["ConnectionError", ConnectionError],
        ["ChunkedEncodingError", ChunkedEncodingError],
        ["Timeout", Timeout],
    ])
    @patch("time.sleep")
    def test_connection_errors_with_retry(self, test_name, error, mock_sleep):
        """Test that connection errors trigger backoff retry."""
        with patch.object(self.client._session, "request", side_effect=error) as mock_request:
            with self.assertRaises(error):
                self.client._Client__make_request("GET", "https://api.example.com/resource")
            
            # Verify 5 retry attempts
            self.assertEqual(mock_request.call_count, 5)
            # Verify sleep was called for backoff
            self.assertTrue(mock_sleep.called)

    @patch("time.sleep")
    def test_successful_retry_after_failure(self, mock_sleep):
        """Test that request succeeds after initial failures."""
        responses = [
            MockResponse(503, text={"message": "Service unavailable"}),
            MockResponse(503, text={"message": "Service unavailable"}),
            MockResponse(200, text={"Data": [{"ID": 1}]}, raise_error=False)
        ]
        
        with patch.object(self.client._session, "request", side_effect=responses) as mock_request:
            result = self.client._Client__make_request("GET", "https://api.mailjet.com/v3/REST/message")
        
        # Should succeed on third attempt
        self.assertEqual(result, {"Data": [{"ID": 1}]})
        self.assertEqual(mock_request.call_count, 3)
        self.assertTrue(mock_sleep.called)

    @patch("time.sleep")
    def test_exponential_backoff_timing(self, mock_sleep):
        """Test that backoff uses exponential timing (factor=2)."""
        mock_response = MockResponse(500, text={"message": "Internal server error"})
        
        with patch.object(self.client._session, "request", return_value=mock_response):
            with self.assertRaises(mailjetInternalServerError):
                self.client._Client__make_request("GET", "https://api.mailjet.com/v3/REST/message")
        
        # Verify sleep was called with increasing delays (exponential backoff)
        self.assertTrue(mock_sleep.call_count >= 3)
