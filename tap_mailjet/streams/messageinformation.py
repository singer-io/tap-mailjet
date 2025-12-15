from tap_mailjet.streams.abstracts import FullTableStream

class Messageinformation(FullTableStream):
    tap_stream_id = "messageinformation"
    key_properties = ["ID"]
    replication_method = "FULL_TABLE"
    path = "/v3/REST/messageinformation"
    data_key = "Data"
    http_method = "GET"
