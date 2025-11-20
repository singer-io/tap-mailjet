from tap_mailjet.streams.abstracts import FullTableStream

class Contact(FullTableStream):
    tap_stream_id = "contact"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    path = "/v3/REST/contact"

