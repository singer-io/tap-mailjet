from tap_mailjet.streams.abstracts import FullTableStream

class Clickstatistics(FullTableStream):
    tap_stream_id = "clickstatistics"
    key_properties = ["ID"]
    replication_method = "FULL_TABLE"
    path = "/v3/REST/clickstatistics"
    data_key = "Data"
    http_method = "GET"
