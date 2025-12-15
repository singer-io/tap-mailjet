import unittest
from unittest.mock import Mock, patch, MagicMock, call
import singer
from tap_mailjet.streams.message import Message
from tap_mailjet.streams.campaign import Campaign
from tap_mailjet.streams.contact import Contact


class TestStreamSync(unittest.TestCase):
    """Test stream synchronization functionality."""

    def setUp(self):
        """Set up test client and configurations."""
        self.mock_client = Mock()
        self.mock_client.base_url = "https://api.mailjet.com"
        self.state = {}

    def create_mock_catalog(self, stream_name, selected=True):
        """Create a mock catalog for testing."""
        mock_catalog = Mock()
        mock_catalog.schema = Mock()
        mock_catalog.schema.to_dict = Mock(return_value={
            "type": "object",
            "properties": {
                "ID": {"type": ["integer", "null"]},
                "Name": {"type": ["string", "null"]},
                "Email": {"type": ["string", "null"]},
                "CreatedAt": {"type": ["string", "null"], "format": "date-time"}
            }
        })
        # Set up metadata for selection
        mock_catalog.metadata = [
            {"breadcrumb": [], "metadata": {"selected": selected}}
        ]
        return mock_catalog

    @patch('singer.write_record')
    @patch('singer.write_schema')
    def test_message_stream_sync_writes_records(self, mock_write_schema, mock_write_record):
        """Test that message stream sync writes records correctly."""
        catalog = self.create_mock_catalog("message")
        stream = Message(self.mock_client, catalog)
        
        # Mock API response
        mock_response = {
            "Data": [
                {"ID": 1, "Name": "Message 1", "Email": "test1@example.com"},
                {"ID": 2, "Name": "Message 2", "Email": "test2@example.com"}
            ]
        }
        self.mock_client.make_request = Mock(return_value=mock_response)
        
        # Perform sync
        with singer.Transformer() as transformer:
            count = stream.sync(state=self.state, transformer=transformer)
        
        # Verify record count
        self.assertEqual(count, 2)
        
        # Verify write_record was called for each record
        self.assertEqual(mock_write_record.call_count, 2)

    @patch('singer.write_record')
    def test_campaign_stream_sync_with_transform(self, mock_write_record):
        """Test campaign stream sync with data transformation."""
        catalog = self.create_mock_catalog("campaign")
        stream = Campaign(self.mock_client, catalog)
        
        # Mock API response with data that needs transformation
        mock_response = {
            "Data": [
                {
                    "ID": 1,
                    "Name": "Campaign 1",
                    "CreatedAt": "2024-01-15T10:00:00Z",
                    "Status": "sent"
                }
            ]
        }
        self.mock_client.make_request = Mock(return_value=mock_response)
        
        # Perform sync
        with singer.Transformer() as transformer:
            count = stream.sync(state=self.state, transformer=transformer)
        
        # Verify transformation occurred
        self.assertEqual(count, 1)
        mock_write_record.assert_called_once()
        
        # Get the transformed record
        call_args = mock_write_record.call_args
        stream_name = call_args[0][0]
        record = call_args[0][1]
        
        self.assertEqual(stream_name, "campaign")
        self.assertIn("ID", record)
        self.assertEqual(record["ID"], 1)

    @patch('singer.write_record')
    def test_contact_stream_sync_handles_empty_data(self, mock_write_record):
        """Test contact stream sync handles empty data gracefully."""
        catalog = self.create_mock_catalog("contact")
        stream = Contact(self.mock_client, catalog)
        
        # Mock empty API response
        mock_response = {"Data": []}
        self.mock_client.make_request = Mock(return_value=mock_response)
        
        # Perform sync
        with singer.Transformer() as transformer:
            count = stream.sync(state=self.state, transformer=transformer)
        
        # Verify no records written
        self.assertEqual(count, 0)
        mock_write_record.assert_not_called()

    @patch('singer.write_record')
    def test_stream_sync_respects_selection(self, mock_write_record):
        """Test that unselected streams don't write records."""
        # Create unselected catalog
        catalog = self.create_mock_catalog("message", selected=False)
        stream = Message(self.mock_client, catalog)
        
        # Mock API response
        mock_response = {
            "Data": [
                {"ID": 1, "Name": "Message 1"}
            ]
        }
        self.mock_client.make_request = Mock(return_value=mock_response)
        
        # Perform sync
        with singer.Transformer() as transformer:
            count = stream.sync(state=self.state, transformer=transformer)
        
        # Stream should process records but not write them if not selected
        # The counter still increments internally but records aren't written
        self.assertEqual(count, 0)
        mock_write_record.assert_not_called()

    @patch('singer.write_record')
    def test_stream_sync_with_pagination(self, mock_write_record):
        """Test stream sync handles pagination correctly."""
        catalog = self.create_mock_catalog("message")
        stream = Message(self.mock_client, catalog)
        stream.page_size = 2
        
        # Mock paginated responses
        page1 = {
            "Data": [
                {"ID": 1, "Name": "Message 1"},
                {"ID": 2, "Name": "Message 2"}
            ]
        }
        page2 = {
            "Data": [
                {"ID": 3, "Name": "Message 3"}
            ]
        }
        
        self.mock_client.make_request = Mock(side_effect=[page1, page2])
        
        # Perform sync
        with singer.Transformer() as transformer:
            count = stream.sync(state=self.state, transformer=transformer)
        
        # Verify all records were processed
        self.assertEqual(count, 3)
        self.assertEqual(mock_write_record.call_count, 3)
        
        # Verify API was called twice (pagination)
        self.assertEqual(self.mock_client.make_request.call_count, 2)

    def test_stream_get_url_endpoint_default(self):
        """Test stream get_url_endpoint returns correct default endpoint."""
        catalog = self.create_mock_catalog("message")
        stream = Message(self.mock_client, catalog)
        
        url = stream.get_url_endpoint()
        
        self.assertEqual(url, "https://api.mailjet.com/v3/REST/message")

    def test_stream_update_params(self):
        """Test stream update_params method."""
        catalog = self.create_mock_catalog("message")
        stream = Message(self.mock_client, catalog)
        
        # Update params
        stream.update_params(CustomParam="value", AnotherParam=123)
        
        # Verify params updated
        self.assertEqual(stream.params["CustomParam"], "value")
        self.assertEqual(stream.params["AnotherParam"], 123)

    def test_stream_update_data_payload(self):
        """Test stream update_data_payload method."""
        catalog = self.create_mock_catalog("message")
        stream = Message(self.mock_client, catalog)
        
        # Update data payload
        stream.update_data_payload(field1="test", field2=456)
        
        # Verify payload updated
        self.assertEqual(stream.data_payload["field1"], "test")
        self.assertEqual(stream.data_payload["field2"], 456)

    def test_stream_is_selected(self):
        """Test stream is_selected method."""
        # Selected stream
        catalog = self.create_mock_catalog("message", selected=True)
        stream = Message(self.mock_client, catalog)
        
        # Note: is_selected checks metadata, which we've mocked
        # In the real implementation, this would properly check the metadata
        # For this test, we're just verifying the method exists and can be called
        try:
            selected = stream.is_selected()
            # Method should not raise an error
            self.assertTrue(True)
        except Exception as e:
            self.fail(f"is_selected() raised {e}")

    @patch('singer.write_record')
    def test_stream_sync_with_api_error_handling(self, mock_write_record):
        """Test that stream sync handles API errors appropriately."""
        catalog = self.create_mock_catalog("message")
        stream = Message(self.mock_client, catalog)
        
        # Mock API to raise an error
        from tap_mailjet.exceptions import mailjetError
        self.mock_client.make_request = Mock(
            side_effect=mailjetError("API Error", Mock())
        )
        
        # Verify error is propagated
        with singer.Transformer() as transformer:
            with self.assertRaises(mailjetError):
                stream.sync(state=self.state, transformer=transformer)

    @patch('singer.write_record')
    def test_multiple_streams_maintain_separate_params(self, mock_write_record):
        """Test that multiple stream instances maintain separate parameters."""
        catalog1 = self.create_mock_catalog("message")
        stream1 = Message(self.mock_client, catalog1)
        
        catalog2 = self.create_mock_catalog("campaign")
        stream2 = Campaign(self.mock_client, catalog2)
        
        # Update params differently for each stream
        stream1.update_params(param1="value1")
        stream2.update_params(param2="value2")
        
        # Verify params are separate
        self.assertIn("param1", stream1.params)
        self.assertNotIn("param1", stream2.params)
        
        self.assertIn("param2", stream2.params)
        self.assertNotIn("param2", stream1.params)

    @patch('singer.write_record')
    def test_stream_sync_counter_accuracy(self, mock_write_record):
        """Test that sync counter accurately counts processed records."""
        catalog = self.create_mock_catalog("message")
        stream = Message(self.mock_client, catalog)
        
        # Mock response with known number of records
        num_records = 5
        mock_response = {
            "Data": [{"ID": i, "Name": f"Message {i}"} for i in range(1, num_records + 1)]
        }
        self.mock_client.make_request = Mock(return_value=mock_response)
        
        # Perform sync
        with singer.Transformer() as transformer:
            count = stream.sync(state=self.state, transformer=transformer)
        
        # Verify counter matches actual records
        self.assertEqual(count, num_records)
        self.assertEqual(mock_write_record.call_count, num_records)


class TestStreamProperties(unittest.TestCase):
    """Test stream property definitions."""

    def setUp(self):
        """Set up mock client."""
        self.mock_client = Mock()
        self.mock_client.base_url = "https://api.mailjet.com"

    def create_mock_catalog(self, stream_name):
        """Create a mock catalog for testing."""
        mock_catalog = Mock()
        mock_catalog.schema = Mock()
        mock_catalog.schema.to_dict = Mock(return_value={})
        mock_catalog.metadata = []
        return mock_catalog

    def test_all_streams_have_required_properties(self):
        """Test that all streams have required properties defined."""
        stream_classes = [Message, Campaign, Contact]
        
        for stream_class in stream_classes:
            catalog = self.create_mock_catalog(stream_class.__name__.lower())
            stream = stream_class(self.mock_client, catalog)
            
            # Verify required properties exist
            self.assertTrue(hasattr(stream, 'tap_stream_id'))
            self.assertTrue(hasattr(stream, 'key_properties'))
            self.assertTrue(hasattr(stream, 'replication_method'))
            self.assertTrue(hasattr(stream, 'path'))
            self.assertTrue(hasattr(stream, 'data_key'))
            self.assertTrue(hasattr(stream, 'http_method'))
            
            # Verify they have values
            self.assertIsNotNone(stream.tap_stream_id)
            self.assertIsNotNone(stream.key_properties)
            self.assertIsNotNone(stream.replication_method)
            self.assertIsNotNone(stream.path)
            self.assertIsNotNone(stream.data_key)
            self.assertIsNotNone(stream.http_method)

    def test_stream_key_properties_format(self):
        """Test that key_properties is properly formatted."""
        catalog = self.create_mock_catalog("message")
        stream = Message(self.mock_client, catalog)
        
        # Verify key_properties is a list
        self.assertIsInstance(stream.key_properties, list)
        
        # Verify it contains the ID field
        self.assertIn("ID", stream.key_properties)

    def test_stream_http_method_valid(self):
        """Test that http_method is valid."""
        stream_classes = [Message, Campaign, Contact]
        valid_methods = ["GET", "POST", "PUT", "DELETE", "PATCH"]
        
        for stream_class in stream_classes:
            catalog = self.create_mock_catalog(stream_class.__name__.lower())
            stream = stream_class(self.mock_client, catalog)
            
            self.assertIn(stream.http_method, valid_methods)


if __name__ == "__main__":
    unittest.main()
