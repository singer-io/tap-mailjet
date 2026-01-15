from tap_mailjet.streams.abstracts import FullTableStream

class ListRecipient(FullTableStream):
    tap_stream_id = "list_recipient"
    key_properties = ["ID"]
    replication_method = "FULL_TABLE"
    data_key = "Data"
    path = "listrecipient"

