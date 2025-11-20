from tap_mailjet.streams.abstracts import FullTableStream

class Listrecipient(FullTableStream):
    tap_stream_id = "listrecipient"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    path = "/v3/REST/listrecipient"

