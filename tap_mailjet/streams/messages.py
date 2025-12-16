from tap_mailjet.streams.abstracts import IncrementalStream

class Messages(IncrementalStream):
    tap_stream_id = "messages"
    key_properties = ["ID"]
    replication_method = "INCREMENTAL"
    replication_keys = "ArrivedAt"
    data_key = "Data"
    path = "message"

