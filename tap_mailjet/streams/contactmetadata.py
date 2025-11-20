from tap_mailjet.streams.abstracts import FullTableStream

class Contactmetadata(FullTableStream):
    tap_stream_id = "contactmetadata"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    path = "/v3/REST/contactmetadata"

