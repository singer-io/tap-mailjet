from tap_mailjet.streams.abstracts import FullTableStream

class Message(FullTableStream):
    tap_stream_id = "message"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    path = "/v3/REST/message"

