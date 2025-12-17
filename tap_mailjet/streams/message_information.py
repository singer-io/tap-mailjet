from tap_mailjet.streams.abstracts import IncrementalStream

class MessageInformation(IncrementalStream):
    tap_stream_id = "message_information"
    key_properties = ["ID"]
    replication_method = "INCREMENTAL"
    replication_keys = ["CreatedAt"]
    data_key = "Data"
    path = "messageinformation"

