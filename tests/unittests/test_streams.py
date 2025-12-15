import unittest
from unittest.mock import Mock, MagicMock, patch
from tap_mailjet.streams.message import Message
from tap_mailjet.streams.campaign import Campaign
from tap_mailjet.streams.contact import Contact
from tap_mailjet.streams.contactslist import Contactslist
from tap_mailjet.streams.listrecipient import Listrecipient
from singer import metadata


class TestStreamConfigurations(unittest.TestCase):
    """Test stream configuration and properties."""

    def setUp(self):
        """Set up mock client and catalog for testing."""
        self.mock_client = Mock()
        self.mock_client.base_url = "https://api.mailjet.com"
        
    def create_mock_catalog(self, stream_name):
        """Create a mock catalog for testing."""
        mock_catalog = Mock()
        mock_catalog.schema = Mock()
        mock_catalog.schema.to_dict = Mock(return_value={
            "type": "object",
            "properties": {
                "ID": {"type": ["integer", "null"]},
                "Name": {"type": ["string", "null"]}
            }
        })
        mock_catalog.metadata = []
        return mock_catalog

    def test_message_stream_configuration(self):
        """Test Message stream has correct configuration."""
        catalog = self.create_mock_catalog("message")
        stream = Message(self.mock_client, catalog)
        
        self.assertEqual(stream.tap_stream_id, "message")
        self.assertEqual(stream.key_properties, ["ID"])
        self.assertEqual(stream.replication_method, "FULL_TABLE")
        self.assertEqual(stream.path, "/v3/REST/message")
        self.assertEqual(stream.data_key, "Data")
        self.assertEqual(stream.http_method, "GET")

    def test_campaign_stream_configuration(self):
        """Test Campaign stream has correct configuration."""
        catalog = self.create_mock_catalog("campaign")
        stream = Campaign(self.mock_client, catalog)
        
        self.assertEqual(stream.tap_stream_id, "campaign")
        self.assertEqual(stream.key_properties, ["ID"])
        self.assertEqual(stream.replication_method, "FULL_TABLE")
        self.assertEqual(stream.path, "/v3/REST/campaign")
        self.assertEqual(stream.data_key, "Data")
        self.assertEqual(stream.http_method, "GET")

    def test_contact_stream_configuration(self):
        """Test Contact stream has correct configuration."""
        catalog = self.create_mock_catalog("contact")
        stream = Contact(self.mock_client, catalog)
        
        self.assertEqual(stream.tap_stream_id, "contact")
        self.assertEqual(stream.key_properties, ["ID"])
        self.assertEqual(stream.replication_method, "FULL_TABLE")
        self.assertEqual(stream.path, "/v3/REST/contact")
        self.assertEqual(stream.data_key, "Data")
        self.assertEqual(stream.http_method, "GET")

    def test_contactslist_stream_configuration(self):
        """Test Contactslist stream has correct configuration."""
        catalog = self.create_mock_catalog("contactslist")
        stream = Contactslist(self.mock_client, catalog)
        
        self.assertEqual(stream.tap_stream_id, "contactslist")
        self.assertEqual(stream.key_properties, ["ID"])
        self.assertEqual(stream.replication_method, "FULL_TABLE")
        self.assertEqual(stream.path, "/v3/REST/contactslist")
        self.assertEqual(stream.data_key, "Data")
        self.assertEqual(stream.http_method, "GET")

    def test_listrecipient_stream_configuration(self):
        """Test Listrecipient stream has correct configuration."""
        catalog = self.create_mock_catalog("listrecipient")
        stream = Listrecipient(self.mock_client, catalog)
        
        self.assertEqual(stream.tap_stream_id, "listrecipient")
        self.assertEqual(stream.key_properties, ["ID"])
        self.assertEqual(stream.replication_method, "FULL_TABLE")
        self.assertEqual(stream.path, "/v3/REST/listrecipient")
        self.assertEqual(stream.data_key, "Data")
        self.assertEqual(stream.http_method, "GET")


class TestStreamPagination(unittest.TestCase):
    """Test stream pagination logic."""

    def setUp(self):
        """Set up mock client and catalog for testing."""
        self.mock_client = Mock()
        self.mock_client.base_url = "https://api.mailjet.com"

    def create_mock_catalog(self, stream_name):
        """Create a mock catalog for testing."""
        mock_catalog = Mock()
        mock_catalog.schema = Mock()
        mock_catalog.schema.to_dict = Mock(return_value={
            "type": "object",
            "properties": {
                "ID": {"type": ["integer", "null"]},
                "Name": {"type": ["string", "null"]}
            }
        })
        mock_catalog.metadata = []
        return mock_catalog

    def test_message_stream_pagination_single_page(self):
        """Test Message stream handles single page of results."""
        catalog = self.create_mock_catalog("message")
        stream = Message(self.mock_client, catalog)
        
        # Mock API response with less than page_size records
        mock_response = {
            "Data": [
                {"ID": 1, "Name": "Message 1"},
                {"ID": 2, "Name": "Message 2"}
            ]
        }
        self.mock_client.make_request = Mock(return_value=mock_response)
        
        # Get records
        records = list(stream.get_records())
        
        # Verify
        self.assertEqual(len(records), 2)
        self.assertEqual(records[0]["ID"], 1)
        self.assertEqual(records[1]["ID"], 2)
        self.mock_client.make_request.assert_called_once()

    def test_message_stream_pagination_multiple_pages(self):
        """Test Message stream handles multiple pages of results."""
        catalog = self.create_mock_catalog("message")
        stream = Message(self.mock_client, catalog)
        stream.page_size = 2  # Set small page size for testing
        
        # Mock API responses for multiple pages
        page1_response = {
            "Data": [
                {"ID": 1, "Name": "Message 1"},
                {"ID": 2, "Name": "Message 2"}
            ]
        }
        page2_response = {
            "Data": [
                {"ID": 3, "Name": "Message 3"}
            ]
        }
        self.mock_client.make_request = Mock(side_effect=[page1_response, page2_response])
        
        # Get records
        records = list(stream.get_records())
        
        # Verify
        self.assertEqual(len(records), 3)
        self.assertEqual(records[0]["ID"], 1)
        self.assertEqual(records[2]["ID"], 3)
        self.assertEqual(self.mock_client.make_request.call_count, 2)

    def test_message_stream_pagination_empty_response(self):
        """Test Message stream handles empty response."""
        catalog = self.create_mock_catalog("message")
        stream = Message(self.mock_client, catalog)
        
        # Mock empty API response
        mock_response = {"Data": []}
        self.mock_client.make_request = Mock(return_value=mock_response)
        
        # Get records
        records = list(stream.get_records())
        
        # Verify
        self.assertEqual(len(records), 0)
        self.mock_client.make_request.assert_called_once()

    def test_campaign_stream_api_call_parameters(self):
        """Test Campaign stream calls API with correct parameters."""
        catalog = self.create_mock_catalog("campaign")
        stream = Campaign(self.mock_client, catalog)
        
        # Mock API response
        mock_response = {
            "Data": [{"ID": 1, "Name": "Campaign 1"}]
        }
        self.mock_client.make_request = Mock(return_value=mock_response)
        
        # Get records
        list(stream.get_records())
        
        # Verify API call parameters
        call_args = self.mock_client.make_request.call_args
        self.assertEqual(call_args[0][0], "GET")  # HTTP method
        self.assertIn("Limit", call_args[0][2])  # params should have Limit
        self.assertIn("Offset", call_args[0][2])  # params should have Offset
        self.assertEqual(call_args[0][2]["Limit"], stream.page_size)
        self.assertEqual(call_args[0][2]["Offset"], 0)


class TestStreamRecordRetrieval(unittest.TestCase):
    """Test stream record retrieval and transformation."""

    def setUp(self):
        """Set up mock client and catalog for testing."""
        self.mock_client = Mock()
        self.mock_client.base_url = "https://api.mailjet.com"

    def create_mock_catalog(self, stream_name):
        """Create a mock catalog for testing."""
        mock_catalog = Mock()
        mock_catalog.schema = Mock()
        mock_catalog.schema.to_dict = Mock(return_value={
            "type": "object",
            "properties": {
                "ID": {"type": ["integer", "null"]},
                "Name": {"type": ["string", "null"]},
                "Email": {"type": ["string", "null"]}
            }
        })
        mock_catalog.metadata = []
        return mock_catalog

    def test_contact_stream_record_retrieval(self):
        """Test Contact stream retrieves and yields records correctly."""
        catalog = self.create_mock_catalog("contact")
        stream = Contact(self.mock_client, catalog)
        
        # Mock API response
        mock_response = {
            "Data": [
                {"ID": 1, "Name": "John Doe", "Email": "john@example.com"},
                {"ID": 2, "Name": "Jane Smith", "Email": "jane@example.com"}
            ]
        }
        self.mock_client.make_request = Mock(return_value=mock_response)
        
        # Get records
        records = list(stream.get_records())
        
        # Verify records
        self.assertEqual(len(records), 2)
        self.assertEqual(records[0]["Name"], "John Doe")
        self.assertEqual(records[1]["Email"], "jane@example.com")

    def test_contactslist_stream_uses_correct_endpoint(self):
        """Test Contactslist stream uses the correct API endpoint."""
        catalog = self.create_mock_catalog("contactslist")
        stream = Contactslist(self.mock_client, catalog)
        
        # Mock API response
        mock_response = {"Data": []}
        self.mock_client.make_request = Mock(return_value=mock_response)
        
        # Get records
        list(stream.get_records())
        
        # Verify endpoint
        call_args = self.mock_client.make_request.call_args
        # The path keyword argument should be set correctly
        self.assertEqual(call_args[1]["path"], "/v3/REST/contactslist")

    def test_listrecipient_stream_handles_nested_data(self):
        """Test Listrecipient stream handles nested data structures."""
        catalog = self.create_mock_catalog("listrecipient")
        stream = Listrecipient(self.mock_client, catalog)
        
        # Mock API response with nested data
        mock_response = {
            "Data": [
                {
                    "ID": 1,
                    "ListID": 100,
                    "ContactID": 200,
                    "IsUnsubscribed": False
                }
            ]
        }
        self.mock_client.make_request = Mock(return_value=mock_response)
        
        # Get records
        records = list(stream.get_records())
        
        # Verify
        self.assertEqual(len(records), 1)
        self.assertEqual(records[0]["ID"], 1)
        self.assertEqual(records[0]["ListID"], 100)


class TestStreamURLEndpoint(unittest.TestCase):
    """Test stream URL endpoint generation."""

    def setUp(self):
        """Set up mock client and catalog for testing."""
        self.mock_client = Mock()
        self.mock_client.base_url = "https://api.mailjet.com"

    def create_mock_catalog(self, stream_name):
        """Create a mock catalog for testing."""
        mock_catalog = Mock()
        mock_catalog.schema = Mock()
        mock_catalog.schema.to_dict = Mock(return_value={
            "type": "object",
            "properties": {"ID": {"type": ["integer", "null"]}}
        })
        mock_catalog.metadata = []
        return mock_catalog

    def test_message_stream_url_endpoint(self):
        """Test Message stream generates correct URL endpoint."""
        catalog = self.create_mock_catalog("message")
        stream = Message(self.mock_client, catalog)
        
        url_endpoint = stream.get_url_endpoint()
        
        self.assertEqual(url_endpoint, "https://api.mailjet.com/v3/REST/message")

    def test_campaign_stream_url_endpoint(self):
        """Test Campaign stream generates correct URL endpoint."""
        catalog = self.create_mock_catalog("campaign")
        stream = Campaign(self.mock_client, catalog)
        
        url_endpoint = stream.get_url_endpoint()
        
        self.assertEqual(url_endpoint, "https://api.mailjet.com/v3/REST/campaign")

    def test_contact_stream_url_endpoint(self):
        """Test Contact stream generates correct URL endpoint."""
        catalog = self.create_mock_catalog("contact")
        stream = Contact(self.mock_client, catalog)
        
        url_endpoint = stream.get_url_endpoint()
        
        self.assertEqual(url_endpoint, "https://api.mailjet.com/v3/REST/contact")


if __name__ == "__main__":
    unittest.main()
