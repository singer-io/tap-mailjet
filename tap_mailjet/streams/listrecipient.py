from tap_mailjet.streams.abstracts import FullTableStream

class Listrecipient(FullTableStream):
    tap_stream_id = "listrecipient"
    key_properties = ["ID"]
    replication_method = "FULL_TABLE"
    path = "/v3/REST/listrecipient"
    data_key = "Data"
    http_method = "GET"

