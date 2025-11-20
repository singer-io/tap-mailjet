from tap_mailjet.streams.abstracts import FullTableStream

class Contactfilter(FullTableStream):
    tap_stream_id = "contactfilter"
    key_properties = ["id"]
    replication_method = "FULL_TABLE"
    path = "/v3/REST/contactfilter"

