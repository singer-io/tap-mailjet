
  # Changelog

## [0.0.2]
- Add 5xx error handling with exponential backoff retry
- Add `MailjetInternalServerError` exception for HTTP 500
- Unmapped 5xx errors automatically fall back to `MailjetBackoffError` and trigger retry
- Retry logic: 5 max tries with exponential backoff (factor=2) for 422, 429, 5xx, and connection errors
- Add unit tests for error handling, backoff retry, and connection error retry

## [0.0.1]
- Initial release
