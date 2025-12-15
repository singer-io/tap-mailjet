from tap_mailjet.streams.abstracts import FullTableStream

class Message(FullTableStream):
    tap_stream_id = "message"
    key_properties = ["ID"]
    replication_method = "FULL_TABLE"
    path = "/v3/REST/message"
    data_key = "Data"
    http_method = "GET"

