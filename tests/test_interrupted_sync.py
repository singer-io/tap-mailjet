
from base import mailjetBaseTest
from tap_tester.base_suite_tests.interrupted_sync_test import InterruptedSyncTest


class mailjetInterruptedSyncTest(InterruptedSyncTest, mailjetBaseTest):
    """Test tap sets a bookmark and respects it for the next sync of a stream.
    
    Note: test_interrupted_sync_stream_order is skipped because with only one
    incremental stream (messages) available for testing, the stream order validation
    logic cannot be properly tested. The test requires multiple streams to verify
    that already-synced streams are not re-synced during recovery.
    """

    @staticmethod
    def name():
        return "tap_tester_mailjet_interrupted_sync_test"

    def streams_to_test(self):
        streams_to_exclude = {
            # Unsupported Full-Table Streams
            'contacts',
            'contacts_list',
            'list_recipient',
            'template',
            'geo_statistics',
            'top_link_clicked',
            'campaign_overview',
            # Incremental streams with insufficient test data
            'campaigns',
            'message_information',
            'click_statistics'
        }
        return self.expected_stream_names().difference(streams_to_exclude)


    def manipulate_state(self):
        """Set up interrupted state.
        
        Since only 'messages' stream has test data, we simulate an interrupted sync
        where messages stream was being synced when the tap was interrupted.
        The test will verify that sync resumes from messages and continues correctly.
        """
        return {
            "currently_syncing": "messages",
            "bookmarks": {
                "messages": { "ArrivedAt" : "2025-11-15T00:00:00Z"},
            }
        }
    
    def test_interrupted_sync_stream_order(self):
        """Skip stream order test when only one stream is available.
        
        This test validates that already-synced streams are not re-synced during
        recovery. Since we only have one incremental stream (messages) with test data,
        there are no "already-synced" streams to validate against, making this test
        inapplicable for the current test environment.
        """
        self.skipTest(
            "Skipping stream order validation; only 'messages' stream has test data. "
            "This test requires multiple incremental streams to validate sync order logic."
        )

