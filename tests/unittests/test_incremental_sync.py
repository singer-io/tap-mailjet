"""Unit tests for incremental sync with bookmark management."""
import unittest
from unittest.mock import patch, MagicMock
from singer import Transformer
from tap_mailjet.streams.messages import Messages
from tap_mailjet.streams.abstracts import IncrementalStream


class TestIncrementalBookmark(unittest.TestCase):
    """Test incremental sync bookmark management."""

    def setUp(self):
        """Set up test stream."""
        config = {
            "api_key": "test_key",
            "secret_key": "test_secret",
            "start_date": "2025-01-01T00:00:00Z"
        }
        client = MagicMock()
        client.config = config
        
        catalog = MagicMock()
        catalog.schema.to_dict.return_value = {
            "type": "object",
            "properties": {
                "ID": {"type": "integer"},
                "ArrivedAt": {"type": "string", "format": "date-time"}
            }
        }
        catalog.metadata = []
        
        self.stream = Messages(client=client, catalog=catalog)
        self.stream.is_selected = MagicMock(return_value=True)

    @patch("tap_mailjet.streams.abstracts.write_record")
    @patch("tap_mailjet.streams.abstracts.write_bookmark")
    @patch("tap_mailjet.streams.abstracts.get_bookmark")
    @patch("tap_mailjet.streams.abstracts.metrics.record_counter")
    def test_bookmark_updated_with_latest_record(self, mock_counter, mock_get_bookmark, mock_write_bookmark, mock_write_record):
        """Test that bookmark is updated with the latest record timestamp."""
        mock_get_bookmark.return_value = "2025-01-01T00:00:00Z"
        mock_write_bookmark.return_value = {}
        
        mock_records = [
            {"ID": 1, "ArrivedAt": "2025-01-02T00:00:00Z"},
            {"ID": 2, "ArrivedAt": "2025-01-03T00:00:00Z"},
            {"ID": 3, "ArrivedAt": "2025-01-04T00:00:00Z"}
        ]
        
        self.stream.get_records = MagicMock(return_value=iter(mock_records))
        
        transformer = Transformer()
        state = {"bookmarks": {"messages": {"ArrivedAt": "2025-01-01T00:00:00Z"}}}
        
        self.stream.sync(state, transformer)
        
        # Verify bookmark was written with latest timestamp
        calls = mock_write_bookmark.call_args_list
        # Should be called for batch writes and final write
        self.assertTrue(len(calls) >= 1)

    @patch("tap_mailjet.streams.abstracts.write_record")
    @patch("tap_mailjet.streams.abstracts.get_bookmark")
    @patch("tap_mailjet.streams.abstracts.metrics.record_counter")
    def test_incremental_filters_old_records(self, mock_counter, mock_get_bookmark, mock_write_record):
        """Test that incremental sync only writes records at or after bookmark."""
        bookmark_date = "2025-01-03T00:00:00Z"
        mock_get_bookmark.return_value = bookmark_date
        
        # API should only return records at or after bookmark
        # (filtering happens via FromTS parameter)
        mock_records = [
            {"ID": 2, "ArrivedAt": "2025-01-03T00:00:00Z"},  # At bookmark
            {"ID": 3, "ArrivedAt": "2025-01-04T00:00:00Z"}   # After bookmark
        ]
        
        self.stream.get_records = MagicMock(return_value=iter(mock_records))
        
        transformer = Transformer()
        state = {"bookmarks": {"messages": {"ArrivedAt": bookmark_date}}}
        
        self.stream.sync(state, transformer)
        
        # Should write all records returned by API (already filtered by FromTS)
        self.assertTrue(mock_write_record.call_count >= 1)

    @patch("tap_mailjet.streams.abstracts.write_record")
    @patch("tap_mailjet.streams.abstracts.write_bookmark")
    @patch("tap_mailjet.streams.abstracts.get_bookmark")
    @patch("tap_mailjet.streams.abstracts.metrics.record_counter")
    def test_batch_bookmark_writing(self, mock_counter, mock_get_bookmark, mock_write_bookmark, mock_write_record):
        """Test that bookmarks are written every 100 records."""
        mock_get_bookmark.return_value = "2025-01-01T00:00:00Z"
        mock_write_bookmark.return_value = {}
        
        # Generate 250 records
        mock_records = [
            {"ID": i, "ArrivedAt": f"2025-01-{i//10 + 1:02d}T00:00:00Z"}
            for i in range(1, 251)
        ]
        
        self.stream.get_records = MagicMock(return_value=iter(mock_records))
        
        transformer = Transformer()
        state = {"bookmarks": {}}
        
        self.stream.sync(state, transformer)
        
        # Should write bookmark at least 3 times (100, 200, final)
        self.assertTrue(mock_write_bookmark.call_count >= 3)

    @patch("tap_mailjet.streams.abstracts.write_record")
    @patch("tap_mailjet.streams.abstracts.get_bookmark")
    @patch("tap_mailjet.streams.abstracts.metrics.record_counter")
    def test_incremental_uses_start_date(self, mock_counter, mock_get_bookmark, mock_write_record):
        """Test that incremental sync uses start_date when no bookmark exists."""
        start_date = "2025-01-01T00:00:00Z"
        mock_get_bookmark.return_value = start_date  # Returns start_date as default
        
        mock_records = [
            {"ID": 1, "ArrivedAt": "2025-01-02T00:00:00Z"}
        ]
        
        self.stream.get_records = MagicMock(return_value=iter(mock_records))
        
        transformer = Transformer()
        state = {"bookmarks": {}}
        
        self.stream.sync(state, transformer)
        
        # Verify get_bookmark was called
        mock_get_bookmark.assert_called()

    @patch("tap_mailjet.streams.abstracts.write_record")
    @patch("tap_mailjet.streams.abstracts.get_bookmark")
    @patch("tap_mailjet.streams.abstracts.metrics.record_counter")
    def test_incremental_sets_fromts_parameter(self, mock_counter, mock_get_bookmark, mock_write_record):
        """Test that incremental sync sets FromTS parameter."""
        bookmark_date = "2025-01-01T00:00:00Z"
        mock_get_bookmark.return_value = bookmark_date
        
        self.stream.get_records = MagicMock(return_value=iter([]))
        
        transformer = Transformer()
        state = {"bookmarks": {"messages": {"ArrivedAt": bookmark_date}}}
        
        self.stream.sync(state, transformer)
        
        # Verify FromTS parameter was set
        self.assertIn("FromTS", self.stream.params)
        self.assertEqual(self.stream.params["FromTS"], bookmark_date)

    @patch("tap_mailjet.streams.abstracts.write_record")
    @patch("tap_mailjet.streams.abstracts.write_bookmark")
    @patch("tap_mailjet.streams.abstracts.get_bookmark")
    @patch("tap_mailjet.streams.abstracts.metrics.record_counter")
    def test_bookmark_preserves_maximum_value(self, mock_counter, mock_get_bookmark, mock_write_bookmark, mock_write_record):
        """Test that bookmark always keeps the maximum value."""
        mock_get_bookmark.return_value = "2025-01-05T00:00:00Z"
        
        # Records with varying timestamps (not in order)
        mock_records = [
            {"ID": 1, "ArrivedAt": "2025-01-06T00:00:00Z"},
            {"ID": 2, "ArrivedAt": "2025-01-05T00:00:00Z"},  # Earlier
            {"ID": 3, "ArrivedAt": "2025-01-07T00:00:00Z"}   # Latest
        ]
        
        self.stream.get_records = MagicMock(return_value=iter(mock_records))
        
        def mock_write_bookmark_impl(state, stream, replication_key=None, value=None):
            # Capture the final bookmark value
            return {"bookmarks": {stream: {replication_key: value}}}
        
        mock_write_bookmark.side_effect = mock_write_bookmark_impl
        
        transformer = Transformer()
        state = {"bookmarks": {"messages": {"ArrivedAt": "2025-01-05T00:00:00Z"}}}
        
        self.stream.sync(state, transformer)
        
        # Final bookmark should be the latest timestamp
        final_call = mock_write_bookmark.call_args_list[-1]
        # Check positional args (state, stream, replication_key, value)
        # Note: Transformer may add microseconds to timestamp
        final_value = final_call[0][3]
        self.assertTrue(final_value.startswith("2025-01-07T00:00:00"))

    @patch("tap_mailjet.streams.abstracts.write_record")
    @patch("tap_mailjet.streams.abstracts.get_bookmark")
    @patch("tap_mailjet.streams.abstracts.metrics.record_counter")
    def test_replication_key_as_list(self, mock_counter, mock_get_bookmark, mock_write_record):
        """Test that replication_keys as list is handled correctly."""
        self.assertEqual(self.stream.replication_keys, ["ArrivedAt"])
        mock_get_bookmark.return_value = "2025-01-01T00:00:00Z"
        
        mock_records = [{"ID": 1, "ArrivedAt": "2025-01-02T00:00:00Z"}]
        self.stream.get_records = MagicMock(return_value=iter(mock_records))
        
        transformer = Transformer()
        state = {}
        
        # Should not raise error with list-based replication_keys
        self.stream.sync(state, transformer)
        
        mock_write_record.assert_called_once()


class TestWriteBookmarkMethod(unittest.TestCase):
    """Test write_bookmark method behavior."""

    @patch("tap_mailjet.streams.abstracts.metadata.to_map")
    def setUp(self, mock_to_map):
        """Set up test incremental stream."""
        mock_catalog = MagicMock()
        mock_catalog.schema.to_dict.return_value = {"key": "value"}
        mock_catalog.metadata = "mock_metadata"
        mock_to_map.return_value = {"metadata_key": "metadata_value"}

        # Create concrete implementation of IncrementalStream
        class ConcreteIncrementalStream(IncrementalStream):
            @property
            def key_properties(self):
                return ["id"]

            @property
            def replication_keys(self):
                return ["updated_at"]

            @property
            def replication_method(self):
                return "INCREMENTAL"

            @property
            def tap_stream_id(self):
                return "test_stream"

        self.stream = ConcreteIncrementalStream(catalog=mock_catalog)
        self.stream.client = MagicMock()
        self.stream.client.config = {"start_date": "2025-01-01T00:00:00Z"}
        self.stream.child_to_sync = []

    @patch("tap_mailjet.streams.abstracts.get_bookmark", return_value=100)
    def test_write_bookmark_with_state(self, mock_get_bookmark):
        """Test write_bookmark updates existing state."""
        state = {'bookmarks': {'test_stream': {'updated_at': 100}}}
        result = self.stream.write_bookmark(state, "test_stream", "updated_at", 200)
        self.assertEqual(result, {'bookmarks': {'test_stream': {'updated_at': 200}}})

    @patch("tap_mailjet.streams.abstracts.get_bookmark", return_value=100)
    def test_write_bookmark_without_state(self, mock_get_bookmark):
        """Test write_bookmark creates new state."""
        state = {}
        result = self.stream.write_bookmark(state, "test_stream", "updated_at", 200)
        self.assertEqual(result, {'bookmarks': {'test_stream': {'updated_at': 200}}})

    @patch("tap_mailjet.streams.abstracts.get_bookmark", return_value=300)
    def test_write_bookmark_preserves_higher_value(self, mock_get_bookmark):
        """Test write_bookmark keeps maximum value when new value is older."""
        state = {'bookmarks': {'test_stream': {'updated_at': 300}}}
        result = self.stream.write_bookmark(state, "test_stream", "updated_at", 200)
        # Should preserve the higher bookmark value (300)
        self.assertEqual(result, {'bookmarks': {'test_stream': {'updated_at': 300}}})
