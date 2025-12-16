"""Unit tests for discovery flow."""
import unittest
from unittest.mock import patch, MagicMock
from singer.catalog import Catalog, CatalogEntry, Schema
from tap_mailjet.discover import discover


class TestDiscoveryFlow(unittest.TestCase):
    """Test discovery mode functionality."""

    @patch("tap_mailjet.discover.get_schemas")
    def test_discover_returns_catalog(self, mock_get_schemas):
        """Test that discover returns a valid Catalog object."""
        mock_get_schemas.return_value = (
            {
                "messages": {
                    "type": "object",
                    "properties": {
                        "ID": {"type": ["null", "integer"]},
                        "ArrivedAt": {"type": ["null", "string"], "format": "date-time"}
                    }
                }
            },
            {
                "messages": [
                    {"metadata": {"table-key-properties": ["ID"]}, "breadcrumb": []}
                ]
            }
        )
        
        catalog = discover()
        
        self.assertIsInstance(catalog, Catalog)
        self.assertEqual(len(catalog.streams), 1)
        self.assertEqual(catalog.streams[0].stream, "messages")
        self.assertEqual(catalog.streams[0].tap_stream_id, "messages")

    @patch("tap_mailjet.discover.get_schemas")
    def test_discover_all_streams(self, mock_get_schemas):
        """Test that discover returns all available streams."""
        mock_schemas = {
            "messages": {"type": "object", "properties": {"ID": {"type": "integer"}}},
            "campaigns": {"type": "object", "properties": {"ID": {"type": "integer"}}},
            "contacts": {"type": "object", "properties": {"ID": {"type": "integer"}}},
            "contacts_list": {"type": "object", "properties": {"ID": {"type": "integer"}}},
        }
        mock_metadata = {
            stream: [{"metadata": {"table-key-properties": ["ID"]}, "breadcrumb": []}]
            for stream in mock_schemas.keys()
        }
        mock_get_schemas.return_value = (mock_schemas, mock_metadata)
        
        catalog = discover()
        
        self.assertEqual(len(catalog.streams), 4)
        stream_names = {stream.stream for stream in catalog.streams}
        self.assertEqual(stream_names, {"messages", "campaigns", "contacts", "contacts_list"})

    @patch("tap_mailjet.discover.get_schemas")
    def test_discover_schema_validation(self, mock_get_schemas):
        """Test that schemas are properly validated during discovery."""
        mock_get_schemas.return_value = (
            {
                "messages": {
                    "type": "object",
                    "properties": {
                        "ID": {"type": ["null", "integer"]},
                        "ArrivedAt": {"type": ["null", "string"], "format": "date-time"},
                        "Status": {"type": ["null", "string"]}
                    }
                }
            },
            {
                "messages": [
                    {"metadata": {"table-key-properties": ["ID"]}, "breadcrumb": []}
                ]
            }
        )
        
        catalog = discover()
        
        # Verify schema structure
        stream = catalog.streams[0]
        self.assertIsInstance(stream.schema, Schema)
        schema_dict = stream.schema.to_dict()
        self.assertEqual(schema_dict["type"], "object")
        self.assertIn("ID", schema_dict["properties"])
        self.assertIn("ArrivedAt", schema_dict["properties"])

    @patch("tap_mailjet.discover.get_schemas")
    def test_discover_key_properties(self, mock_get_schemas):
        """Test that key properties are correctly set."""
        mock_get_schemas.return_value = (
            {
                "messages": {"type": "object", "properties": {"ID": {"type": "integer"}}}
            },
            {
                "messages": [
                    {"metadata": {"table-key-properties": ["ID"]}, "breadcrumb": []}
                ]
            }
        )
        
        catalog = discover()
        
        stream = catalog.streams[0]
        self.assertEqual(stream.key_properties, ["ID"])

    @patch("tap_mailjet.discover.get_schemas")
    def test_discover_metadata_structure(self, mock_get_schemas):
        """Test that metadata is properly structured."""
        mock_metadata = {
            "messages": [
                {
                    "metadata": {
                        "table-key-properties": ["ID"],
                        "forced-replication-method": "INCREMENTAL",
                        "valid-replication-keys": ["ArrivedAt"]
                    },
                    "breadcrumb": []
                }
            ]
        }
        mock_get_schemas.return_value = (
            {"messages": {"type": "object", "properties": {"ID": {"type": "integer"}}}},
            mock_metadata
        )
        
        catalog = discover()
        
        stream = catalog.streams[0]
        self.assertIsNotNone(stream.metadata)
        self.assertEqual(len(stream.metadata), 1)

    @patch("tap_mailjet.discover.get_schemas")
    @patch("tap_mailjet.discover.LOGGER")
    def test_discover_handles_schema_error(self, mock_logger, mock_get_schemas):
        """Test that discovery handles schema errors properly."""
        mock_get_schemas.return_value = (
            {
                "invalid_stream": None  # Invalid schema
            },
            {
                "invalid_stream": []
            }
        )
        
        with self.assertRaises(Exception):
            discover()
        
        # Verify error was logged
        self.assertTrue(mock_logger.error.called)

    @patch("tap_mailjet.discover.get_schemas")
    def test_discover_empty_schemas(self, mock_get_schemas):
        """Test discovery with no schemas."""
        mock_get_schemas.return_value = ({}, {})
        
        catalog = discover()
        
        self.assertIsInstance(catalog, Catalog)
        self.assertEqual(len(catalog.streams), 0)

    @patch("tap_mailjet.discover.get_schemas")
    def test_discover_catalog_entry_attributes(self, mock_get_schemas):
        """Test that CatalogEntry has all required attributes."""
        mock_get_schemas.return_value = (
            {
                "messages": {
                    "type": "object",
                    "properties": {"ID": {"type": "integer"}}
                }
            },
            {
                "messages": [
                    {"metadata": {"table-key-properties": ["ID"]}, "breadcrumb": []}
                ]
            }
        )
        
        catalog = discover()
        
        entry = catalog.streams[0]
        self.assertTrue(hasattr(entry, "stream"))
        self.assertTrue(hasattr(entry, "tap_stream_id"))
        self.assertTrue(hasattr(entry, "key_properties"))
        self.assertTrue(hasattr(entry, "schema"))
        self.assertTrue(hasattr(entry, "metadata"))
