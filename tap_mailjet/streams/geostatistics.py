from tap_mailjet.streams.abstracts import FullTableStream

class Geostatistics(FullTableStream):
    tap_stream_id = "geostatistics"
    key_properties = ["Country"]
    replication_method = "FULL_TABLE"
    path = "/v3/REST/geostatistics"
    data_key = "Data"
    http_method = "GET"
