
  # Changelog

## [0.0.2]
- Add 5xx error handling with exponential backoff retry (max 5 tries, factor=2) [#8](https://github.com/singer-io/tap-mailjet/pull/8)
- Unmapped 5xx errors fall back to `MailjetBackoffError` for automatic retry
- Add unit tests for error handling, backoff retry, and boundary value validation

## [0.0.1]
- Initial release
