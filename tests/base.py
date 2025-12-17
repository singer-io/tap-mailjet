import copy
import os
import unittest
from datetime import datetime as dt
from datetime import timedelta

import dateutil.parser
import pytz
from tap_tester import connections, menagerie, runner
from tap_tester.logger import LOGGER
from tap_tester.base_suite_tests.base_case import BaseCase


class mailjetBaseTest(BaseCase):
    """Setup expectations for test sub classes.

    Metadata describing streams. A bunch of shared methods that are used
    in tap-tester tests. Shared tap-specific methods (as needed).
    """
    start_date = "2019-01-01T00:00:00Z"

    @staticmethod
    def tap_name():
        """The name of the tap."""
        return "tap-mailjet"

    @staticmethod
    def get_type():
        """The name of the tap."""
        return "platform.mailjet"

    @classmethod
    def expected_metadata(cls):
        """The expected streams and metadata about the streams."""
        return {
            "messages": {
                cls.PRIMARY_KEYS: { "ID" },
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: { "ArrivedAt" },
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "contacts": {
                cls.PRIMARY_KEYS: { "ID" },
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "contacts_list": {
                cls.PRIMARY_KEYS: { "ID" },
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "list_recipient": {
                cls.PRIMARY_KEYS: { "ID" },
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "campaigns": {
                cls.PRIMARY_KEYS: { "ID" },
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: { "CreatedAt" },
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "template": {
                cls.PRIMARY_KEYS: { "ID" },
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "message_information": {
            cls.PRIMARY_KEYS: { "ID" },
            cls.REPLICATION_METHOD: cls.INCREMENTAL,
            cls.REPLICATION_KEYS: { "CreatedAt" },
            cls.OBEYS_START_DATE: False,
            cls.API_LIMIT: 100
            },
            "geo_statistics": {
                cls.PRIMARY_KEYS: set(),
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "click_statistics": {
                cls.PRIMARY_KEYS: { "ID" },
                cls.REPLICATION_METHOD: cls.INCREMENTAL,
                cls.REPLICATION_KEYS: { "ClickedAt" },
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "top_link_clicked": {
                cls.PRIMARY_KEYS: { "LinkId" },
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            },
            "campaign_overview": {
                cls.PRIMARY_KEYS: { "ID" },
                cls.REPLICATION_METHOD: cls.FULL_TABLE,
                cls.REPLICATION_KEYS: set(),
                cls.OBEYS_START_DATE: False,
                cls.API_LIMIT: 100
            }
        }

    @staticmethod
    def get_credentials():
        """Authentication information for the test account."""
        credentials_dict = {}
        creds = {'api_key': 'TAP_MAILJET_API_KEY', 'secret_key': 'TAP_MAILJET_SECRET_KEY'}

        for cred in creds:
            credentials_dict[cred] = os.getenv(creds[cred])

        return credentials_dict

    def get_properties(self, original: bool = True):
        """Configuration of properties required for the tap."""
        return_value = {
            "start_date": "2025-01-01T00:00:00Z"
        }
        if original:
            return return_value

        return_value["start_date"] = self.start_date
        return return_value

