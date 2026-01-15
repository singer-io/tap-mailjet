"""Unit tests for pagination logic."""
import unittest
from unittest.mock import patch, MagicMock
from tap_mailjet.streams.messages import Messages


class TestPaginationFlow(unittest.TestCase):
    """Test pagination logic in get_records method."""

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
        catalog.schema.to_dict.return_value = {"type": "object", "properties": {}}
        catalog.metadata = []
        
        self.stream = Messages(client=client, catalog=catalog)

    def test_pagination_single_page(self):
        """Test pagination with single page of results."""
        mock_response = {
            "Data": [
                {"ID": 1, "ArrivedAt": "2025-01-01T00:00:00Z"},
                {"ID": 2, "ArrivedAt": "2025-01-02T00:00:00Z"}
            ]
        }
        
        self.stream.client.make_request = MagicMock(return_value=mock_response)
        
        records = list(self.stream.get_records())
        
        self.assertEqual(len(records), 2)
        self.assertEqual(records[0]["ID"], 1)
        self.stream.client.make_request.assert_called_once()

    def test_pagination_multiple_pages(self):
        """Test pagination with multiple pages."""
        # First page: 100 records (full page)
        page1 = {"Data": [{"ID": i, "ArrivedAt": f"2025-01-01T00:00:0{i}Z"} for i in range(100)]}
        # Second page: 50 records (partial page)
        page2 = {"Data": [{"ID": i, "ArrivedAt": f"2025-01-02T00:00:0{i}Z"} for i in range(100, 150)]}
        
        self.stream.client.make_request = MagicMock(side_effect=[page1, page2])
        self.stream.page_size = 100
        
        records = list(self.stream.get_records())
        
        self.assertEqual(len(records), 150)
        self.assertEqual(self.stream.client.make_request.call_count, 2)

    def test_pagination_empty_results(self):
        """Test pagination with empty results."""
        mock_response = {"Data": []}
        
        self.stream.client.make_request = MagicMock(return_value=mock_response)
        
        records = list(self.stream.get_records())
        
        self.assertEqual(len(records), 0)
        self.stream.client.make_request.assert_called_once()

    def test_pagination_offset_calculation(self):
        """Test that offset is correctly calculated."""
        page1 = {"Data": [{"ID": i} for i in range(10)]}
        page2 = {"Data": [{"ID": i} for i in range(10, 15)]}  # Partial page
        
        self.stream.client.make_request = MagicMock(side_effect=[page1, page2])
        self.stream.page_size = 10
        
        list(self.stream.get_records())
        
        # Check that Offset was updated correctly
        calls = self.stream.client.make_request.call_args_list
        # First call should have Offset=0
        self.assertEqual(self.stream.params.get("Offset"), 10)  # Updated after first page

    def test_pagination_limit_parameter(self):
        """Test that Limit parameter is set correctly."""
        mock_response = {"Data": [{"ID": 1}]}
        
        self.stream.client.make_request = MagicMock(return_value=mock_response)
        self.stream.page_size = 50
        
        list(self.stream.get_records())
        
        self.assertEqual(self.stream.params.get("Limit"), 50)

    def test_pagination_partial_last_page(self):
        """Test that pagination stops when partial page received."""
        page1 = {"Data": [{"ID": i} for i in range(100)]}  # Full page
        page2 = {"Data": [{"ID": i} for i in range(100, 150)]}  # Partial page (50 records)
        
        self.stream.client.make_request = MagicMock(side_effect=[page1, page2])
        self.stream.page_size = 100
        
        records = list(self.stream.get_records())
        
        # Should stop after page2 (partial page)
        self.assertEqual(len(records), 150)
        self.assertEqual(self.stream.client.make_request.call_count, 2)

    def test_pagination_data_key_extraction(self):
        """Test that data is extracted using correct data_key."""
        mock_response = {
            "Data": [
                {"ID": 1, "Status": "sent"},
                {"ID": 2, "Status": "pending"}
            ],
            "Total": 2
        }
        
        self.stream.client.make_request = MagicMock(return_value=mock_response)
        self.stream.data_key = "Data"
        
        records = list(self.stream.get_records())
        
        self.assertEqual(len(records), 2)
        self.assertEqual(records[0]["Status"], "sent")

    def test_pagination_preserves_params(self):
        """Test that pagination preserves existing params."""
        self.stream.params = {"FromTS": "2025-01-01T00:00:00Z"}
        mock_response = {"Data": [{"ID": 1}]}
        
        self.stream.client.make_request = MagicMock(return_value=mock_response)
        
        list(self.stream.get_records())
        
        # Original params should be preserved
        self.assertIn("FromTS", self.stream.params)
        self.assertEqual(self.stream.params["FromTS"], "2025-01-01T00:00:00Z")
